package usecase

import (
	"context"
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
)

// BigCapBasket is the fixed 8-ticker IHSG proxy the report carries as a
// secondary reference line. It is never the baseline — the Liquidity Baseline
// is — because a basket of the same eight names is not like-for-like with a
// screener's survivors.
var BigCapBasket = []string{"BBCA", "BBRI", "BMRI", "BBNI", "TLKM", "ASII", "ADRO", "ANTM"}

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

// TickerForwardReturns is one ticker's close-to-close Forward Returns at every
// horizon, keyed by horizon in trading days. A nil entry is an unresolved
// horizon (no stored close on the target day), counted in ReturnStats.Unresolved
// and excluded from every median — never zero-imputed.
type TickerForwardReturns struct {
	Ticker      string           `json:"ticker"`
	AnchorClose *float64         `json:"anchor_close"`
	Returns     map[int]*float64 `json:"forward_returns"`
}

// MeasurementCounts is the funnel in flat counts: the universe the Screen saw,
// the Liquidity Baseline (hard-filter survivors), the graded survivors, and the
// big-cap basket size.
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

// MeasurementReport is the JSON artifact under measurement/reports/. It carries
// everything needed to read the number without re-running anything: the anchor,
// the named filter set, the resolved params, the git revision, the funnel, and
// the per-horizon stats and drill-down rows.
type MeasurementReport struct {
	AsOf                 string                 `json:"as_of"`
	FilterSet            string                 `json:"filter_set"`
	FilterSetDescription string                 `json:"filter_set_description,omitempty"`
	HoldoutStart         string                 `json:"holdout_start,omitempty"`
	SealedWindow         bool                   `json:"sealed_window"`
	ReplayDates          []string               `json:"replay_dates"`
	GitRevision          string                 `json:"git_revision"`
	GeneratedAt          string                 `json:"generated_at"`
	Params               ReplayParams           `json:"params"`
	Funnel               ScreenStocksFunnel     `json:"funnel"`
	Counts               MeasurementCounts      `json:"counts"`
	Horizons             []HorizonStats         `json:"horizons"`
	Survivors            []TickerForwardReturns `json:"survivors"`
	BigCapBasket         []TickerForwardReturns `json:"big_cap_basket"`
	Flags                []string               `json:"flags"`
	SuccessCriteria      []SuccessCriterion     `json:"success_criteria"`
	Notes                []string               `json:"notes"`
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
}

func NewMeasurementEngine(db *sqlx.DB, log *logrus.Logger, screen MeasurementScreener, prices MeasurementPriceSource, filters *MeasurementFilterRegistry, revision string) *MeasurementEngine {
	return &MeasurementEngine{DB: db, Log: log, Screen: screen, Prices: prices, Filters: filters, Revision: revision}
}

// MeasurementOptions is a parsed CLI request.
type MeasurementOptions struct {
	// AsOf is the Replay anchor Trading Day.
	AsOf time.Time
	// FilterSet names the registry entry to grade; empty uses the default set.
	FilterSet string
	// HoldoutStart, when set, seals the window: anchors before it are rejected,
	// so a validation run can only read data the tuning run never saw.
	HoldoutStart *time.Time
}

// Measure validates the options, replays the named filter set, and scores it
// against the Liquidity Baseline and the Big-Cap Basket.
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
	survivorCloses := make(map[string]*float64, len(replay.Rows))
	for _, row := range replay.Rows {
		survivorTickers = append(survivorTickers, row.Ticker)
		survivorCloses[row.Ticker] = row.Close
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

	survivors := forwardRows(survivorTickers, survivorCloses, anchor, targets, windows)
	baselineRows := forwardRows(baselineTickers, baselineCloses, anchor, targets, windows)
	bigCapRows := forwardRows(BigCapBasket, nil, anchor, targets, windows)

	horizons := comparisonByHorizon(survivors, baselineRows, bigCapRows)
	replayDates := []string{anchor.Format("2006-01-02")}

	return &MeasurementReport{
		AsOf:                 anchor.Format("2006-01-02"),
		FilterSet:            set.Name,
		FilterSetDescription: set.Description,
		HoldoutStart:         formatOptionalDay(opts.HoldoutStart),
		SealedWindow:         opts.HoldoutStart != nil,
		ReplayDates:          replayDates,
		GitRevision:          e.Revision,
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
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
		Notes: []string{
			"Forward Returns are close-to-close over market-wide trading days; execution realism (fills, slippage, fees) is not modeled.",
			"The Liquidity Baseline is the hard-filter stage alone (traded on the anchor day, value floor, suspension window) — before structural filters and ranking.",
			"The Big-Cap Basket is a secondary reference line, not the baseline.",
		},
	}, nil
}

// resolveForwardTargets resolves the h-th post-anchor Trading Day for each
// horizon and the batched-read limit that provably covers [anchor, target20].
// An anchor with fewer than 20 post-anchor days is a structured error: no
// Forward Return could be computed, so grading it would be a false negative.
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
		return nil, 0, fmt.Errorf("%w: anchor %s has %d post-anchor trading days; Forward Returns need %d",
			ErrInvalidArgument, anchor.Format("2006-01-02"), len(post), maxMeasurementHorizon)
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
