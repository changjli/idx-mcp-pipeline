package usecase

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// The Measurement Harness's fixed numbers. Every one is a discipline parameter
// from the spec, not a tunable: changing a horizon or the small-n floor changes
// what "a passing grade" means, so it is a deliberate edit, never a call option.
var measurementHorizons = []int{5, 10, 20}

const (
	// maxMeasurementHorizon is the longest Forward Return horizon: a Replay
	// anchor needs at least this many post-anchor Trading Days to be gradable.
	maxMeasurementHorizon = 20
	// measurementLookaheadDays is how far past the anchor the market-wide
	// trading-day axis is read when resolving the forward targets. 20 trading
	// days is ~4 weeks; 60 calendar days covers it with room for IDX holidays.
	measurementLookaheadDays = 60
	// smallNSurvivors is the small-n floor: a comparison resting on fewer
	// survivors is flagged, never blocked (spec: "discipline is encouraged by
	// output, not enforced by refusal").
	smallNSurvivors = 30
	// smallNReplayDates is the Replay-date floor. A single-anchor report is
	// always flagged, because one date is one regime.
	smallNReplayDates = 3
	// screenGradeKind discriminates a screening grade report from the other
	// graders' reports (Anomaly, flow, Disclosure, journal) a reader may meet.
	screenGradeKind = "screen_grade"
)

// BigCapBasket is the fixed 8-ticker IHSG proxy the report carries as a
// secondary reference line. It is never the baseline — the Liquidity Baseline
// is — because a basket of the same eight names is not like-for-like with a
// screener's survivors.
var BigCapBasket = []string{"BBCA", "BBRI", "BMRI", "BBNI", "TLKM", "ASII", "ADRO", "ANTM"}

// ErrAnchorUnreplayable marks an anchor that cannot produce Forward Returns
// because it has too few post-anchor Trading Days. A Grade skips and records
// such an anchor; a single-anchor Measure surfaces it as an invalid argument.
var ErrAnchorUnreplayable = errors.New("replay anchor is not gradable")

// UnreplayableAnchorError is the structured reason an anchor has too little
// post-anchor history. It satisfies both ErrAnchorUnreplayable (so a Grade can
// skip it) and ErrInvalidArgument (so a single-anchor caller sees the house
// invalid-argument sentinel).
type UnreplayableAnchorError struct {
	Anchor   string
	PostDays int
	Need     int
}

func (e *UnreplayableAnchorError) Error() string {
	return fmt.Sprintf("anchor %s has %d post-anchor trading days; Forward Returns need %d",
		e.Anchor, e.PostDays, e.Need)
}

func (e *UnreplayableAnchorError) Is(target error) bool {
	return target == ErrAnchorUnreplayable || target == ErrInvalidArgument
}

// MeasurementScreener is the Replay seam: the screener usecase's uncapped run.
// The concrete *ScreenStocksUseCase satisfies it; tests wire a fake.
type MeasurementScreener interface {
	ScreenAllSurvivors(ctx context.Context, req ScreenStocksRequest) (*ScreenStocksResponse, error)
}

// MeasurementPriceSource is the read surface the engine needs beyond the
// Screen: the hard-filter candidates (the Liquidity Baseline), the market-wide
// Trading Day calendar, and one batched forward window per ticker. All three are
// existing *repository.DailyPriceRepository readers — the harness adds no new
// price query and no new table.
type MeasurementPriceSource interface {
	ScreenCandidates(db *sqlx.DB, anchor time.Time, minValue int64, suspensionDays int) ([]repository.ScreenCandidate, error)
	TradingDaysInRangeAll(db *sqlx.DB, from, to time.Time) ([]time.Time, error)
	FindByTickersUpTo(db *sqlx.DB, tickers []string, to time.Time, limit int) ([]entity.DailyPrice, error)
}

// ReturnStats is one population's Forward Return summary at one horizon. Median
// and HitRate are nil when no Forward Return was resolvable, so "no data" never
// renders as a zero return or a zero hit rate.
type ReturnStats struct {
	N          int      `json:"n"`
	Unresolved int      `json:"unresolved"`
	Median     *float64 `json:"median"`
	HitRate    *float64 `json:"hit_rate"`
}

// HorizonStats is one horizon's survivor-vs-baseline comparison. Excess is the
// survivor median minus the Liquidity Baseline median; it is nil until both
// medians exist — the only sense in which a filter set "works" is a positive
// Excess (CONTEXT: Excess).
type HorizonStats struct {
	Horizon  int         `json:"horizon_days"`
	Survivor ReturnStats `json:"survivor"`
	Baseline ReturnStats `json:"liquidity_baseline"`
	BigCap   ReturnStats `json:"big_cap_basket"`
	Excess   *float64    `json:"excess"`
	SmallN   bool        `json:"small_n"`
}

// TickerForwardReturns is one ticker observation's close-to-close Forward
// Returns at every horizon, keyed by horizon in trading days. A nil entry is an
// unresolved horizon (no stored close on the target day), counted in
// ReturnStats.Unresolved and excluded from every median — never zero-imputed.
//
// AsOf is the Replay anchor: a pooled Grade concatenates many anchors' rows, so
// a row is only attributable by its date. The coverage fields are carried off
// the Screen row that selected the ticker (CONTEXT: Column Coverage, Warm-up
// Bar), so a drill-down can see why a column is null without re-running the
// screen.
type TickerForwardReturns struct {
	Ticker       string                          `json:"ticker"`
	AsOf         string                          `json:"as_of,omitempty"`
	AnchorClose  *float64                        `json:"anchor_close"`
	Returns      map[int]*float64                `json:"forward_returns"`
	Insufficient []string                        `json:"insufficient,omitempty"`
	Coverage     map[string]ScreenColumnCoverage `json:"coverage,omitempty"`
	ForeignNet   *int64                          `json:"foreign_net,omitempty"`

	// Anomaly Grade drill-down, empty on a screen row: the triggering anomaly
	// types, the resolved Direction Bucket, the ticker-day's total net
	// accumulation over the Flow Lead-in Window (nil when unobserved), its
	// Broker Confirmation state, and whether a passed-filter Disclosure landed
	// in the gate window (CONTEXT: Direction Bucket, Broker Confirmation).
	AnomalyTypes    []string `json:"anomaly_types,omitempty"`
	Direction       string   `json:"direction,omitempty"`
	BrokerNet       *int64   `json:"broker_net,omitempty"`
	BrokerState     string   `json:"broker_state,omitempty"`
	DisclosureState string   `json:"disclosure_state,omitempty"`
}

// MeasurementCounts is the funnel in flat counts: the universe the Screen saw,
// the Liquidity Baseline (hard-filter survivors), the graded survivors, and the
// big-cap basket size. Across a pooled Grade every count is a sum over Replay
// Dates — an observation count.
type MeasurementCounts struct {
	Universe  int `json:"universe"`
	Baseline  int `json:"liquidity_baseline"`
	Survivors int `json:"survivors"`
	BigCap    int `json:"big_cap_basket"`
}

// ReplayParams echoes the effective Screen parameters the report was computed
// against, so any past number is reproducible from its own report.
type ReplayParams struct {
	Window               int                  `json:"window"`
	MinValue             int64                `json:"min_value"`
	SuspensionWindowDays int                  `json:"suspension_window_days"`
	FlowWindowDays       int                  `json:"flow_window_days"`
	Indicators           []string             `json:"indicators"`
	Filters              []ScreenStocksFilter `json:"filters"`
	Sort                 string               `json:"sort"`
	Order                string               `json:"order"`
}

// SuccessCriterion is one of the spec's three standing questions plus this
// report's answer, so every report restates why the loop exists.
type SuccessCriterion struct {
	Question string `json:"question"`
	Answer   string `json:"answer"`
}

// DateGrade is one Replay Date's slice of a pooled Grade: its own funnel and
// per-horizon stats, retained so the pooled headline can be read against the
// per-date dispersion it rests on.
type DateGrade struct {
	AsOf     string             `json:"as_of"`
	Funnel   ScreenStocksFunnel `json:"funnel"`
	Counts   MeasurementCounts  `json:"counts"`
	Horizons []HorizonStats     `json:"horizons"`
	Flags    []string           `json:"flags"`
}

// SkippedAnchor is an anchor the Grade could not replay (too little post-anchor
// history), recorded so a shortened Replay-date set never reads as the set the
// caller asked for.
type SkippedAnchor struct {
	AsOf   string `json:"as_of"`
	Reason string `json:"reason"`
}

// MeasurementReport is one filter set's pooled result within a Grade. For a
// single-anchor Grade it is the whole result; for a multi-anchor one it is the
// pooled population, with PerDate retaining each Replay Date beneath. AsOf holds
// the label (single date, or `first_to_last`); AsOfStart/AsOfEnd are the
// structured range.
type MeasurementReport struct {
	AsOf                 string                 `json:"as_of"`
	AsOfStart            string                 `json:"as_of_start,omitempty"`
	AsOfEnd              string                 `json:"as_of_end,omitempty"`
	FilterSet            string                 `json:"filter_set"`
	FilterSetDescription string                 `json:"filter_set_description,omitempty"`
	HoldoutStart         string                 `json:"holdout_start,omitempty"`
	SealedWindow         bool                   `json:"sealed_window"`
	ReplayDates          []string               `json:"replay_dates"`
	GitRevision          string                 `json:"git_revision"`
	GeneratedAt          string                 `json:"generated_at"`
	DistinctTickers      int                    `json:"distinct_tickers"`
	Params               ReplayParams           `json:"params"`
	Funnel               ScreenStocksFunnel     `json:"funnel"`
	Counts               MeasurementCounts      `json:"counts"`
	Horizons             []HorizonStats         `json:"horizons"`
	Survivors            []TickerForwardReturns `json:"survivors"`
	BigCapBasket         []TickerForwardReturns `json:"big_cap_basket"`
	PerDate              []DateGrade            `json:"per_date,omitempty"`
	Flags                []string               `json:"flags"`
	SuccessCriteria      []SuccessCriterion     `json:"success_criteria"`
	Notes                []string               `json:"notes"`
}

// HorizonDelta is one horizon's two-set comparison. ExcessA and ExcessB are each
// set's Excess against its own Liquidity Baseline; ExcessDelta is their
// difference (CONTEXT: Excess Delta).
type HorizonDelta struct {
	Horizon     int      `json:"horizon_days"`
	ExcessA     *float64 `json:"excess_a"`
	ExcessB     *float64 `json:"excess_b"`
	ExcessDelta *float64 `json:"excess_delta"`
	SmallN      bool     `json:"small_n"`
}

// ScreenComparison is the two-filter-set comparison block. LikeForLike is false
// when the sets' hard-filter parameters differ, because their Excess figures
// then rest on different Liquidity Baselines.
type ScreenComparison struct {
	A           string         `json:"a"`
	B           string         `json:"b"`
	LikeForLike bool           `json:"like_for_like"`
	Notes       []string       `json:"notes,omitempty"`
	Horizons    []HorizonDelta `json:"horizons"`
}

// ScreenGradeReport is the artifact one `grade screen` run writes: the pooled
// result for each filter set, the comparison when two were asked for, and the
// run-level bookkeeping (dates, revision, skipped anchors).
type ScreenGradeReport struct {
	Kind           string              `json:"kind"`
	AsOfStart      string              `json:"as_of_start"`
	AsOfEnd        string              `json:"as_of_end"`
	ReplayDates    []string            `json:"replay_dates"`
	GitRevision    string              `json:"git_revision"`
	GeneratedAt    string              `json:"generated_at"`
	HoldoutStart   string              `json:"holdout_start,omitempty"`
	SealedWindow   bool                `json:"sealed_window"`
	SkippedAnchors []SkippedAnchor     `json:"skipped_anchors"`
	FilterSets     []MeasurementReport `json:"filter_sets"`
	Comparison     *ScreenComparison   `json:"comparison,omitempty"`
	Flags          []string            `json:"flags"`
	Notes          []string            `json:"notes"`
}

// MeasurementEngine runs Replays and scores them. It is read-only with respect
// to app behavior: it never writes to the database, and the MCP server never
// depends on it.
type MeasurementEngine struct {
	DB     *sqlx.DB
	Log    *logrus.Logger
	Screen MeasurementScreener
	Prices MeasurementPriceSource
	// Filters resolves the named filter set to grade. A nil registry falls
	// back to the built-in default only, so an engine constructed without the
	// tuning files still works (tests, and a checkout with no filtersets dir).
	Filters *MeasurementFilterRegistry
	// Revision is the git revision stamped into every report. The CLI supplies
	// it so the engine stays pure and testable.
	Revision string
	// Anomalies, Disclosures and BrokerNet are the Anomaly Grade's reads. The
	// screen grader never touches them, so the engine's original construction
	// is unchanged; GradeAnomalies requires all three to be set.
	Anomalies   MeasurementAnomalySource
	Disclosures MeasurementDisclosureSource
	BrokerNet   MeasurementBrokerNetSource
	// ForeignFlow is the Flow Grade's read of the Flow Column (Σ f_nval). The
	// other graders never touch it; GradeFlow requires it to be set.
	ForeignFlow MeasurementForeignFlowSource
	// DisclosureFilings is the Disclosure Grade's read of a filing day's passed
	// and rejected disclosures. The other graders never touch it;
	// GradeDisclosures requires it to be set.
	DisclosureFilings MeasurementDisclosureFilingSource
	// Journal and JournalPrices are the Journal Grade's reads. The screen and
	// anomaly graders never touch them; GradeJournal requires both to be set.
	Journal       MeasurementJournalSource
	JournalPrices JournalPriceSource
}

func NewMeasurementEngine(db *sqlx.DB, log *logrus.Logger, screen MeasurementScreener, prices MeasurementPriceSource, filters *MeasurementFilterRegistry, revision string) *MeasurementEngine {
	return &MeasurementEngine{DB: db, Log: log, Screen: screen, Prices: prices, Filters: filters, Revision: revision}
}

// MeasurementOptions is a parsed single-anchor CLI request.
type MeasurementOptions struct {
	// AsOf is the Replay anchor Trading Day.
	AsOf time.Time
	// FilterSet names the registry entry to grade; empty uses the default set.
	FilterSet string
	// HoldoutStart, when set, seals the window: anchors before it are rejected,
	// so a validation run can only read data the tuning run never saw.
	HoldoutStart *time.Time
}

// ScreenGradeOptions is a parsed `grade screen` request: one or more Replay
// Dates, one filter set, and optionally a second to compare against.
type ScreenGradeOptions struct {
	// Anchors are the Replay Dates; they are deduped and sorted before use.
	Anchors []time.Time
	// FilterSet names the primary set; empty uses the default set.
	FilterSet string
	// Compare, when set, names the second set to compare side by side.
	Compare string
	// HoldoutStart seals the window for every anchor.
	HoldoutStart *time.Time
}

// replayResult is one anchor's replay before it is collapsed into a report: the
// single-anchor report plus the raw populations a pooled Grade concatenates.
// The Liquidity Baseline rows are not serialized on their own, so they are kept
// here rather than read back off the report.
type replayResult struct {
	anchor    time.Time
	report    *MeasurementReport
	survivors []TickerForwardReturns
	baseline  []TickerForwardReturns
	bigCap    []TickerForwardReturns
}

// Measure validates the options, replays the named filter set for one anchor,
// and scores it against the Liquidity Baseline and the Big-Cap Basket.
func (e *MeasurementEngine) Measure(ctx context.Context, opts MeasurementOptions) (*MeasurementReport, error) {
	registry := e.Filters
	if registry == nil {
		registry = newBuiltinMeasurementFilterRegistry()
	}
	set, err := registry.Resolve(opts.FilterSet)
	if err != nil {
		return nil, err
	}
	return e.replay(ctx, set, opts)
}

// replay is Measure with the set already resolved, so tests can inject a set
// without mutating the registry.
func (e *MeasurementEngine) replay(ctx context.Context, set MeasurementFilterSet, opts MeasurementOptions) (*MeasurementReport, error) {
	res, err := e.replayOnce(ctx, set, opts)
	if err != nil {
		return nil, err
	}
	return res.report, nil
}

// replayOnce performs the per-(set, anchor) work shared by the single-anchor
// Measure and the pooled GradeScreen: resolve the forward targets, run the
// Screen, read the Liquidity Baseline, and build the forward rows. Splitting it
// out is what keeps one implementation of the read path across both callers.
func (e *MeasurementEngine) replayOnce(ctx context.Context, set MeasurementFilterSet, opts MeasurementOptions) (*replayResult, error) {
	anchor := opts.AsOf
	if opts.HoldoutStart != nil && anchor.Before(*opts.HoldoutStart) {
		return nil, fmt.Errorf("%w: as_of %s is before the holdout window (starts %s); a holdout run only reads anchors on or after the seal",
			ErrInvalidArgument, anchor.Format("2006-01-02"), opts.HoldoutStart.Format("2006-01-02"))
	}

	// Resolve the forward targets first: an anchor without a full 20 post-anchor
	// Trading Days cannot be graded, and finding that out before the screen read
	// keeps a too-recent anchor cheap and unambiguous.
	targets, readLimit, err := e.resolveForwardTargets(anchor)
	if err != nil {
		return nil, err
	}

	req := set.Params
	req.AsOf = &anchor
	req.Limit = maxScreenerLimit
	replay, err := e.Screen.ScreenAllSurvivors(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("replay filter set %q as of %s: %w", set.Name, anchor.Format("2006-01-02"), err)
	}

	// The Liquidity Baseline is the hard-filter stage alone, with the same
	// floor and suspension window the Screen resolved — like-for-like, before
	// structural filters and ranking.
	baseline, err := e.Prices.ScreenCandidates(e.DB, anchor, replay.MinValue, replay.SuspensionWindowDays)
	if err != nil {
		return nil, fmt.Errorf("read liquidity baseline as of %s: %w", anchor.Format("2006-01-02"), err)
	}

	survivorTickers := make([]string, 0, len(replay.Rows))
	for _, row := range replay.Rows {
		survivorTickers = append(survivorTickers, row.Ticker)
	}
	baselineTickers := make([]string, 0, len(baseline))
	baselineCloses := make(map[string]*float64, len(baseline))
	for _, candidate := range baseline {
		baselineTickers = append(baselineTickers, candidate.Ticker)
		baselineCloses[candidate.Ticker] = candidate.Close
	}

	// One batched read covers every population's forward window (survivors are a
	// subset of the baseline, and the big caps are eight names).
	allTickers := unionTickers(baselineTickers, survivorTickers, BigCapBasket)
	var windows map[string][]entity.DailyPrice
	if len(allTickers) > 0 {
		prices, err := e.Prices.FindByTickersUpTo(e.DB, allTickers, targets[maxMeasurementHorizon], readLimit)
		if err != nil {
			return nil, fmt.Errorf("read forward prices as of %s: %w", anchor.Format("2006-01-02"), err)
		}
		windows = groupPricesByTicker(prices)
	}

	survivors := survivorForwardRows(replay.Rows, anchor, targets, windows)
	baselineRows := forwardRows(baselineTickers, baselineCloses, anchor, targets, windows)
	bigCapRows := forwardRows(BigCapBasket, nil, anchor, targets, windows)

	horizons := comparisonByHorizon(survivors, baselineRows, bigCapRows)
	replayDates := []string{anchor.Format("2006-01-02")}

	report := &MeasurementReport{
		AsOf:                 anchor.Format("2006-01-02"),
		AsOfStart:            anchor.Format("2006-01-02"),
		AsOfEnd:              anchor.Format("2006-01-02"),
		FilterSet:            set.Name,
		FilterSetDescription: set.Description,
		HoldoutStart:         formatOptionalDay(opts.HoldoutStart),
		SealedWindow:         opts.HoldoutStart != nil,
		ReplayDates:          replayDates,
		GitRevision:          e.Revision,
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
		DistinctTickers:      distinctTickers(survivors),
		Params:               replayParams(replay),
		Funnel:               replay.Funnel,
		Counts: MeasurementCounts{
			Universe:  replay.Funnel.Universe,
			Baseline:  len(baselineTickers),
			Survivors: len(survivorTickers),
			BigCap:    len(BigCapBasket),
		},
		Horizons:        horizons,
		Survivors:       survivors,
		BigCapBasket:    bigCapRows,
		Flags:           measurementFlags(horizons, len(replayDates)),
		SuccessCriteria: measurementSuccessCriteria(horizons),
		Notes:           measurementNotes(),
	}
	return &replayResult{anchor: anchor, report: report, survivors: survivors, baseline: baselineRows, bigCap: bigCapRows}, nil
}

// GradeScreen is the screen grader: it replays a named filter set (or two,
// compared) across one or more Replay Dates, pools each set's observations into
// one population, and writes one report. Data-gap anchors are skipped and
// recorded; a holdout violation is a hard error, because crossing the seal is a
// discipline breach rather than a data gap.
func (e *MeasurementEngine) GradeScreen(ctx context.Context, opts ScreenGradeOptions) (*ScreenGradeReport, error) {
	registry := e.Filters
	if registry == nil {
		registry = newBuiltinMeasurementFilterRegistry()
	}

	anchors := dedupeSortedDays(opts.Anchors)
	if len(anchors) == 0 {
		return nil, fmt.Errorf("%w: at least one --as-of anchor is required", ErrInvalidArgument)
	}
	if opts.HoldoutStart != nil {
		for _, anchor := range anchors {
			if anchor.Before(*opts.HoldoutStart) {
				return nil, fmt.Errorf("%w: as_of %s is before the holdout window (starts %s); a holdout run only reads anchors on or after the seal",
					ErrInvalidArgument, anchor.Format("2006-01-02"), opts.HoldoutStart.Format("2006-01-02"))
			}
		}
	}

	primary, err := registry.Resolve(opts.FilterSet)
	if err != nil {
		return nil, err
	}
	sets := []MeasurementFilterSet{primary}
	if strings.TrimSpace(opts.Compare) != "" {
		second, err := registry.Resolve(opts.Compare)
		if err != nil {
			return nil, err
		}
		if second.Name == primary.Name {
			return nil, fmt.Errorf("%w: --compare %q names the same filter set as --filters", ErrInvalidArgument, opts.Compare)
		}
		sets = append(sets, second)
	}

	gradable, skipped, err := e.gradableAnchors(anchors)
	if err != nil {
		return nil, err
	}
	if len(gradable) == 0 {
		return nil, fmt.Errorf("%w: none of the %d requested anchor(s) can be graded (all lack %d post-anchor Trading Days)",
			ErrInvalidArgument, len(anchors), maxMeasurementHorizon)
	}

	overlap, err := e.anchorsOverlap(gradable)
	if err != nil {
		return nil, err
	}

	reports := make([]MeasurementReport, 0, len(sets))
	for _, set := range sets {
		pooled, err := e.poolSet(ctx, set, gradable, opts.HoldoutStart)
		if err != nil {
			return nil, err
		}
		if overlap {
			pooled.Flags = append(pooled.Flags, overlapFlag())
		}
		reports = append(reports, pooled)
	}

	grade := &ScreenGradeReport{
		Kind:           screenGradeKind,
		AsOfStart:      gradable[0].Format("2006-01-02"),
		AsOfEnd:        gradable[len(gradable)-1].Format("2006-01-02"),
		ReplayDates:    replayDateStrings(gradable),
		GitRevision:    e.Revision,
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
		HoldoutStart:   formatOptionalDay(opts.HoldoutStart),
		SealedWindow:   opts.HoldoutStart != nil,
		SkippedAnchors: skipped,
		FilterSets:     reports,
		Flags:          []string{},
		Notes: []string{
			"Pooled across Replay Dates: each horizon's median, hit rate, and n are observation counts, not independent-sample counts.",
			"distinct_tickers beside each set's n is the number of distinct names the observations rest on.",
		},
	}
	if len(reports) == 2 {
		grade.Comparison = compareScreenSets(reports[0], reports[1])
	}
	return grade, nil
}

// poolSet replays one filter set across every gradable anchor and pools the
// populations into a single MeasurementReport. Per-date blocks are retained for
// dispersion; every run-level figure (funnel, counts, per-horizon stats) is a
// sum or a union over the Replay Dates.
func (e *MeasurementEngine) poolSet(ctx context.Context, set MeasurementFilterSet, anchors []time.Time, holdoutStart *time.Time) (MeasurementReport, error) {
	var (
		survivors []TickerForwardReturns
		baseline  []TickerForwardReturns
		bigCap    []TickerForwardReturns
		perDate   []DateGrade
		funnel    ScreenStocksFunnel
		counts    MeasurementCounts
		params    ReplayParams
	)

	for _, anchor := range anchors {
		res, err := e.replayOnce(ctx, set, MeasurementOptions{AsOf: anchor, HoldoutStart: holdoutStart})
		if err != nil {
			return MeasurementReport{}, err
		}
		r := res.report
		survivors = append(survivors, res.survivors...)
		baseline = append(baseline, res.baseline...)
		bigCap = append(bigCap, res.bigCap...)
		perDate = append(perDate, DateGrade{
			AsOf:     r.AsOf,
			Funnel:   r.Funnel,
			Counts:   r.Counts,
			Horizons: r.Horizons,
			Flags:    r.Flags,
		})
		funnel.Universe += r.Funnel.Universe
		funnel.AfterValue += r.Funnel.AfterValue
		funnel.AfterStructure += r.Funnel.AfterStructure
		counts.Universe += r.Counts.Universe
		counts.Baseline += r.Counts.Baseline
		counts.Survivors += r.Counts.Survivors
		counts.BigCap += r.Counts.BigCap
		params = r.Params
	}

	horizons := comparisonByHorizon(survivors, baseline, bigCap)
	report := MeasurementReport{
		AsOf:                 labelRange(anchors),
		AsOfStart:            anchors[0].Format("2006-01-02"),
		AsOfEnd:              anchors[len(anchors)-1].Format("2006-01-02"),
		FilterSet:            set.Name,
		FilterSetDescription: set.Description,
		HoldoutStart:         formatOptionalDay(holdoutStart),
		SealedWindow:         holdoutStart != nil,
		ReplayDates:          replayDateStrings(anchors),
		GitRevision:          e.Revision,
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
		DistinctTickers:      distinctTickers(survivors),
		Params:               params,
		Funnel:               funnel,
		Counts:               counts,
		Horizons:             horizons,
		Survivors:            survivors,
		BigCapBasket:         bigCap,
		PerDate:              perDate,
		Flags:                measurementFlags(horizons, len(anchors)),
		SuccessCriteria:      measurementSuccessCriteria(horizons),
		Notes:                append(measurementNotes(), poolingNote(len(anchors), distinctTickers(survivors))),
	}
	return report, nil
}

// gradableAnchors partitions the requested anchors into the ones that can be
// replayed and the ones skipped for too little post-anchor history. A read
// failure is neither: it propagates, because a database error must never look
// like a data gap.
func (e *MeasurementEngine) gradableAnchors(anchors []time.Time) ([]time.Time, []SkippedAnchor, error) {
	gradable := make([]time.Time, 0, len(anchors))
	skipped := make([]SkippedAnchor, 0)
	for _, anchor := range anchors {
		if _, _, err := e.resolveForwardTargets(anchor); err != nil {
			if errors.Is(err, ErrAnchorUnreplayable) {
				skipped = append(skipped, SkippedAnchor{AsOf: anchor.Format("2006-01-02"), Reason: err.Error()})
				continue
			}
			return nil, nil, err
		}
		gradable = append(gradable, anchor)
	}
	return gradable, skipped, nil
}

// anchorsOverlap reports whether any two consecutive anchors are closer than
// the longest horizon in Trading Days. Past that distance no two forward windows
// can share a price path, so the flag is exactly the point where pooled n starts
// double-counting a move.
func (e *MeasurementEngine) anchorsOverlap(anchors []time.Time) (bool, error) {
	if len(anchors) < 2 {
		return false, nil
	}
	days, err := e.Prices.TradingDaysInRangeAll(e.DB, anchors[0], anchors[len(anchors)-1])
	if err != nil {
		return false, fmt.Errorf("read market trading days for overlap check: %w", err)
	}
	index := make(map[string]int, len(days))
	for i, day := range days {
		index[day.Format("2006-01-02")] = i
	}
	for i := 1; i < len(anchors); i++ {
		prev, okPrev := index[anchors[i-1].Format("2006-01-02")]
		curr, okCurr := index[anchors[i].Format("2006-01-02")]
		if okPrev && okCurr && curr-prev < maxMeasurementHorizon {
			return true, nil
		}
	}
	return false, nil
}

// compareScreenSets builds the two-set comparison: each horizon's Excess Delta,
// and a not-like-for-like note when the sets' hard-filter parameters differ, so
// the delta is never read as resting on one shared baseline when it does not.
func compareScreenSets(a, b MeasurementReport) *ScreenComparison {
	like := a.Params.MinValue == b.Params.MinValue && a.Params.SuspensionWindowDays == b.Params.SuspensionWindowDays
	comparison := &ScreenComparison{A: a.FilterSet, B: b.FilterSet, LikeForLike: like}
	if !like {
		comparison.Notes = append(comparison.Notes,
			"hard-filter parameters differ (min_value or suspension window); the two Excess figures rest on different Liquidity Baselines")
	}
	for _, ha := range a.Horizons {
		var hb *HorizonStats
		for i := range b.Horizons {
			if b.Horizons[i].Horizon == ha.Horizon {
				hb = &b.Horizons[i]
				break
			}
		}
		if hb == nil {
			continue
		}
		delta := HorizonDelta{Horizon: ha.Horizon, ExcessA: ha.Excess, ExcessB: hb.Excess, SmallN: ha.SmallN || hb.SmallN}
		if ha.Excess != nil && hb.Excess != nil {
			value := *ha.Excess - *hb.Excess
			delta.ExcessDelta = &value
		}
		comparison.Horizons = append(comparison.Horizons, delta)
	}
	return comparison
}

// resolveForwardTargets resolves the h-th post-anchor Trading Day for each
// horizon and the batched-read limit that provably covers [anchor, target20].
// An anchor with fewer than 20 post-anchor days is an UnreplayableAnchorError:
// no Forward Return could be computed, so grading it would be a false negative.
func (e *MeasurementEngine) resolveForwardTargets(anchor time.Time) (map[int]time.Time, int, error) {
	days, err := e.Prices.TradingDaysInRangeAll(e.DB, anchor, anchor.AddDate(0, 0, measurementLookaheadDays))
	if err != nil {
		return nil, 0, fmt.Errorf("read market trading days from %s: %w", anchor.Format("2006-01-02"), err)
	}

	post := make([]time.Time, 0, len(days))
	for _, day := range days {
		if day.After(anchor) {
			post = append(post, day)
		}
	}
	if len(post) < maxMeasurementHorizon {
		return nil, 0, &UnreplayableAnchorError{
			Anchor:   anchor.Format("2006-01-02"),
			PostDays: len(post),
			Need:     maxMeasurementHorizon,
		}
	}

	targets := make(map[int]time.Time, len(measurementHorizons))
	for _, horizon := range measurementHorizons {
		targets[horizon] = post[horizon-1]
	}

	// The read limit is the number of market trading days at or before the last
	// target. A ticker cannot have more rows than that inside the window, so the
	// most-recent-N read returns every in-window row for every ticker.
	last := targets[maxMeasurementHorizon]
	limit := 0
	for _, day := range days {
		if !day.After(last) {
			limit++
		}
	}
	if limit < 1 {
		limit = 1
	}
	return targets, limit, nil
}

// resolvePartialForwardTargets resolves whichever Forward Return horizons have a
// post-anchor Trading Day for anchor, leaving the rest absent. Unlike
// resolveForwardTargets it never rejects an anchor for too little history: the
// Disclosure Grade counts an unresolvable horizon as unresolved rather than
// skipping the filing (ticket 06). The read limit provably covers
// [anchor, last resolvable target].
func (e *MeasurementEngine) resolvePartialForwardTargets(anchor time.Time) (map[int]time.Time, int, error) {
	days, err := e.Prices.TradingDaysInRangeAll(e.DB, anchor, anchor.AddDate(0, 0, measurementLookaheadDays))
	if err != nil {
		return nil, 0, fmt.Errorf("read market trading days from %s: %w", anchor.Format("2006-01-02"), err)
	}

	post := make([]time.Time, 0, len(days))
	for _, day := range days {
		if day.After(anchor) {
			post = append(post, day)
		}
	}
	targets := make(map[int]time.Time, len(measurementHorizons))
	last := anchor
	for _, horizon := range measurementHorizons {
		if len(post) >= horizon {
			targets[horizon] = post[horizon-1]
			last = post[horizon-1]
		}
	}

	limit := 0
	for _, day := range days {
		if !day.Before(anchor) && !day.After(last) {
			limit++
		}
	}
	if limit < 1 {
		limit = 1
	}
	return targets, limit, nil
}

// survivorForwardRows builds the survivor drill-down rows, carrying the Screen
// row's coverage flags so a null column is attributable without a re-run.
func survivorForwardRows(rows []ScreenStocksRow, anchor time.Time, targets map[int]time.Time, windows map[string][]entity.DailyPrice) []TickerForwardReturns {
	out := make([]TickerForwardReturns, 0, len(rows))
	for _, row := range rows {
		forward := forwardReturns(row.Ticker, anchor, row.Close, targets, windows[row.Ticker])
		forward.Insufficient = row.Insufficient
		forward.Coverage = row.Coverage
		forward.ForeignNet = row.ForeignNet
		out = append(out, forward)
	}
	return out
}

// forwardRows computes Forward Returns for one population, using each row's
// stored anchor close as a fallback for the anchor row (a candidate carries it
// even if the batched read missed it).
func forwardRows(tickers []string, anchorCloses map[string]*float64, anchor time.Time, targets map[int]time.Time, windows map[string][]entity.DailyPrice) []TickerForwardReturns {
	rows := make([]TickerForwardReturns, 0, len(tickers))
	for _, ticker := range tickers {
		rows = append(rows, forwardReturns(ticker, anchor, anchorCloses[ticker], targets, windows[ticker]))
	}
	return rows
}

// forwardReturns is one ticker's close-to-close Forward Returns. The anchor
// close is the denominator; a horizon whose target day carries no stored close
// is nil (unresolved), never a zero — a halted ticker has no return, not a flat
// one.
func forwardReturns(ticker string, anchor time.Time, anchorClose *float64, targets map[int]time.Time, series []entity.DailyPrice) TickerForwardReturns {
	anchorKey := anchor.Format("2006-01-02")
	closes := make(map[string]*float64, len(series))
	var anchorFromSeries *float64
	for _, price := range series {
		key := price.TradingDay.Format("2006-01-02")
		closes[key] = price.Close
		if key == anchorKey {
			anchorFromSeries = price.Close
		}
	}

	base := anchorClose
	if base == nil {
		base = anchorFromSeries
	}

	out := TickerForwardReturns{
		Ticker:      ticker,
		AsOf:        anchorKey,
		AnchorClose: base,
		Returns:     make(map[int]*float64, len(targets)),
	}
	for _, horizon := range measurementHorizons {
		close := closes[targets[horizon].Format("2006-01-02")]
		if base == nil || *base <= 0 || close == nil {
			out.Returns[horizon] = nil
			continue
		}
		value := *close / *base - 1
		out.Returns[horizon] = &value
	}
	return out
}

// comparisonByHorizon summarizes each population per horizon and computes the
// Excess (survivor median − baseline median) and the small-n flag.
func comparisonByHorizon(survivors, baseline, bigCap []TickerForwardReturns) []HorizonStats {
	horizons := make([]HorizonStats, 0, len(measurementHorizons))
	for _, horizon := range measurementHorizons {
		survivor := summarizeReturns(survivors, horizon)
		base := summarizeReturns(baseline, horizon)
		stats := HorizonStats{
			Horizon:  horizon,
			Survivor: survivor,
			Baseline: base,
			BigCap:   summarizeReturns(bigCap, horizon),
		}
		if survivor.Median != nil && base.Median != nil {
			excess := *survivor.Median - *base.Median
			stats.Excess = &excess
		}
		stats.SmallN = survivor.N < smallNSurvivors || base.N < smallNSurvivors
		horizons = append(horizons, stats)
	}
	return horizons
}

// summarizeReturns computes one population's stats at one horizon. A nil
// Forward Return is unresolved and excluded; the median and hit rate are nil
// (not zero) when nothing was resolvable.
func summarizeReturns(rows []TickerForwardReturns, horizon int) ReturnStats {
	values := make([]float64, 0, len(rows))
	unresolved := 0
	for _, row := range rows {
		value, ok := row.Returns[horizon]
		if !ok || value == nil {
			unresolved++
			continue
		}
		values = append(values, *value)
	}

	stats := ReturnStats{N: len(values), Unresolved: unresolved}
	if len(values) == 0 {
		return stats
	}

	median := median(values)
	stats.Median = &median

	hits := 0
	for _, value := range values {
		if value > 0 {
			hits++
		}
	}
	hitRate := float64(hits) / float64(len(values))
	stats.HitRate = &hitRate
	return stats
}

// median returns the middle value, averaging the two middle values for an even
// count. The input is not mutated.
func median(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

// measurementFlags renders the discipline warnings the report must carry: each
// small-n comparison, and the replay-date floor. They are flags, not errors —
// the tool's job is to make a weak comparison visible, never to refuse it.
func measurementFlags(horizons []HorizonStats, replayDates int) []string {
	flags := []string{}
	if replayDates < smallNReplayDates {
		flags = append(flags, fmt.Sprintf(
			"small-n: %d replay date(s) < %d; aggregate more anchors before adopting a filter change",
			replayDates, smallNReplayDates))
	}
	for _, stats := range horizons {
		if stats.Survivor.N == 0 {
			flags = append(flags, fmt.Sprintf("no data: no survivor Forward Return resolvable at %dd", stats.Horizon))
		} else if stats.Survivor.N < smallNSurvivors {
			flags = append(flags, fmt.Sprintf("small-n: survivor n=%d < %d at %dd", stats.Survivor.N, smallNSurvivors, stats.Horizon))
		}
		if stats.Baseline.N == 0 {
			flags = append(flags, fmt.Sprintf("no data: no liquidity-baseline Forward Return resolvable at %dd", stats.Horizon))
		} else if stats.Baseline.N < smallNSurvivors {
			flags = append(flags, fmt.Sprintf("small-n: liquidity baseline n=%d < %d at %dd", stats.Baseline.N, smallNSurvivors, stats.Horizon))
		}
		if stats.Survivor.Unresolved > 0 {
			flags = append(flags, fmt.Sprintf(
				"excluded: %d survivor(s) with no stored close on the %dd target", stats.Survivor.Unresolved, stats.Horizon))
		}
	}
	return flags
}

// overlapFlag warns that the pooled population rests on overlapping forward
// windows, so its n overstates the independent evidence.
func overlapFlag() string {
	return fmt.Sprintf(
		"overlap: consecutive anchors are closer than %d trading days; their Forward Returns share price paths, so pooled n is an observation count, not an independent-sample count",
		maxMeasurementHorizon)
}

// measurementSuccessCriteria answers the spec's three standing questions with
// this report's numbers. "No edge anywhere" is a passing grade, so a negative
// answer is stated plainly.
func measurementSuccessCriteria(horizons []HorizonStats) []SuccessCriterion {
	return []SuccessCriterion{
		{Question: "Do screener survivors beat the Liquidity Baseline at any horizon?", Answer: excessAnswer(horizons)},
		{Question: "Is edge front-loaded (<=2 days) or spread over 10-20d?", Answer: frontLoadAnswer(horizons)},
		{Question: "Does broker/disclosure confirmation split Anomalies into different Forward Return populations?", Answer: "Not measured by this report: Anomaly populations are graded separately (anomaly grading)."},
	}
}

func excessAnswer(horizons []HorizonStats) string {
	positives := make([]string, 0, len(horizons))
	measured := false
	for _, stats := range horizons {
		if stats.Excess == nil {
			continue
		}
		measured = true
		if *stats.Excess > 0 {
			positives = append(positives, fmt.Sprintf("%dd %+.2f%%", stats.Horizon, *stats.Excess*100))
		}
	}
	switch {
	case len(positives) > 0:
		return "Yes — positive Excess at " + strings.Join(positives, ", ") + "."
	case measured:
		return "No — Excess is non-positive at every measured horizon."
	default:
		return "Unresolved — no horizon had both survivor and baseline Forward Returns."
	}
}

func frontLoadAnswer(horizons []HorizonStats) string {
	var short, long *float64
	for _, stats := range horizons {
		switch stats.Horizon {
		case 5:
			short = stats.Excess
		case 20:
			long = stats.Excess
		}
	}
	if short == nil || long == nil {
		return "Unresolved — need both the 5d and 20d Excess."
	}
	if *short >= *long {
		return fmt.Sprintf("Front-loaded: the 5d Excess (%+.2f%%) is at least the 20d Excess (%+.2f%%); the <=2-day horizon is not directly measured.", *short*100, *long*100)
	}
	return fmt.Sprintf("Spread: the 20d Excess (%+.2f%%) exceeds the 5d Excess (%+.2f%%).", *long*100, *short*100)
}

// replayParams copies the effective resolved parameters off the Screen response.
func replayParams(res *ScreenStocksResponse) ReplayParams {
	return ReplayParams{
		Window:               res.Window,
		MinValue:             res.MinValue,
		SuspensionWindowDays: res.SuspensionWindowDays,
		FlowWindowDays:       res.FlowWindowDays,
		Indicators:           res.Indicators,
		Filters:              res.Filters,
		Sort:                 res.Sort,
		Order:                res.Order,
	}
}

// measurementNotes are the standing caveats every screen report carries.
func measurementNotes() []string {
	return []string{
		"Forward Returns are close-to-close over market-wide trading days; execution realism (fills, slippage, fees) is not modeled.",
		"The Liquidity Baseline is the hard-filter stage alone (traded on the anchor day, value floor, suspension window) — before structural filters and ranking.",
		"The Big-Cap Basket is a secondary reference line, not the baseline.",
	}
}

// poolingNote records the pooled-count caveat for a multi-anchor report.
func poolingNote(replayDates, distinct int) string {
	return fmt.Sprintf(
		"Pooled across %d Replay Dates: n counts observations, and they rest on %d distinct ticker(s); repeated names and overlapping windows keep n from being an independent-sample count.",
		replayDates, distinct)
}

// unionTickers deduplicates the populations' tickers, preserving first-seen
// order so the batched read is deterministic.
func unionTickers(groups ...[]string) []string {
	seen := make(map[string]bool)
	union := make([]string, 0)
	for _, group := range groups {
		for _, ticker := range group {
			if seen[ticker] {
				continue
			}
			seen[ticker] = true
			union = append(union, ticker)
		}
	}
	return union
}

// distinctTickers counts the distinct tickers among forward rows, the number a
// pooled n rests on.
func distinctTickers(rows []TickerForwardReturns) int {
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		seen[row.Ticker] = true
	}
	return len(seen)
}

// dedupeSortedDays drops repeated anchors and orders them ascending, so a Grade
// pools each date once regardless of flag order.
func dedupeSortedDays(days []time.Time) []time.Time {
	seen := make(map[string]bool, len(days))
	out := make([]time.Time, 0, len(days))
	for _, day := range days {
		key := day.Format("2006-01-02")
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// replayDateStrings renders anchors as YYYY-MM-DD in order.
func replayDateStrings(days []time.Time) []string {
	out := make([]string, 0, len(days))
	for _, day := range days {
		out = append(out, day.Format("2006-01-02"))
	}
	return out
}

// labelRange is the pooled report's AsOf: the single date, or `first_to_last`.
func labelRange(days []time.Time) string {
	if len(days) == 0 {
		return ""
	}
	if len(days) == 1 {
		return days[0].Format("2006-01-02")
	}
	return days[0].Format("2006-01-02") + "_to_" + days[len(days)-1].Format("2006-01-02")
}

// formatOptionalDay renders a day as YYYY-MM-DD, or "" for nil.
func formatOptionalDay(day *time.Time) string {
	if day == nil {
		return ""
	}
	return day.Format("2006-01-02")
}

// compile-time checks: the concrete repositories and screen usecase satisfy the
// engine's seams.
var (
	_ MeasurementScreener    = (*ScreenStocksUseCase)(nil)
	_ MeasurementPriceSource = (*repository.DailyPriceRepository)(nil)
)
