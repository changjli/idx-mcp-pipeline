package usecase

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// The Disclosure Grade's fixed vocabulary. Every string here is a report key, so
// changing one changes what a reader parses; they are listed in CONTEXT.md
// (Disclosure Grade, Filing Day, Reaction Anchor, Reject Sample).
const (
	disclosureGradeKind = "disclosure_grade"

	// defaultRejectSampleN is the fixed monthly Reject Sample size. The reject
	// review skims a handful of filings by hand; 20 is small enough to do
	// monthly and large enough that one material miss is visible.
	defaultRejectSampleN = 20
	// maxRejectSampleN is the ceiling, far above any month's reject volume.
	maxRejectSampleN = 500
)

// MeasurementDisclosureFilingSource is the Disclosure Grade's read: a filing
// day's disclosures by filter verdict. The concrete
// *repository.DisclosureRepository satisfies it.
type MeasurementDisclosureFilingSource interface {
	FindByAnnouncementDate(db *sqlx.DB, day time.Time, passed bool) ([]entity.Disclosure, error)
}

// DisclosureGradeOptions is a parsed `grade disclosures` request: the Filing
// Days to grade, an optional holdout seal, and the Reject Sample size.
type DisclosureGradeOptions struct {
	// Anchors are the Filing Days (announcement dates) to grade; deduped and
	// sorted before use.
	Anchors []time.Time
	// HoldoutStart, when set, seals the window for every filing day.
	HoldoutStart *time.Time
	// RejectSampleN is the manual Reject Sample size; 0 uses
	// defaultRejectSampleN.
	RejectSampleN int
}

// DisclosureGradeRow is one passed-filter disclosure's drill-down: the filing
// metadata a reader follows into the PDF, and its ticker's close-to-close
// Forward Returns from the Reaction Anchor. Multiple rows can share a
// (ticker, Filing Day) — the per-horizon statistics dedupe to ticker-days, the
// drill-down keeps every filing.
type DisclosureGradeRow struct {
	DisclosureID   int64            `json:"disclosure_id"`
	Ticker         string           `json:"ticker"`
	FilingDate     string           `json:"filing_date"`
	ReactionAnchor string           `json:"reaction_anchor"`
	Title          string           `json:"title"`
	PDFURL         string           `json:"pdf_url"`
	Categories     []string         `json:"categories"`
	AnchorClose    *float64         `json:"anchor_close"`
	Returns        map[int]*float64 `json:"forward_returns"`
	Unresolved     []int            `json:"unresolved_horizons,omitempty"`
}

// RejectSampleRow is one manually-reviewed rejected disclosure: the metadata a
// reader skims, its ticker's Forward Returns for context, and the Material
// verdict the agent fills in by hand (nil until then). The tool never judges
// materiality; it only picks the deterministic sample.
type RejectSampleRow struct {
	DisclosureID   int64            `json:"disclosure_id"`
	Ticker         string           `json:"ticker"`
	FilingDate     string           `json:"filing_date"`
	ReactionAnchor string           `json:"reaction_anchor"`
	Title          string           `json:"title"`
	PDFURL         string           `json:"pdf_url"`
	AnchorClose    *float64         `json:"anchor_close"`
	Returns        map[int]*float64 `json:"forward_returns"`
	Unresolved     []int            `json:"unresolved_horizons,omitempty"`
	Material       *bool            `json:"material"`
	Note           string           `json:"note,omitempty"`
}

// RejectSampling is the manual monthly review block: the fixed sample size, the
// seed pool it was drawn from, the deterministic sample, and the procedure. It
// is the false-negative counterweight the keyword whitelist never sees.
type RejectSampling struct {
	N         int               `json:"n"`
	Pool      int               `json:"rejected_pool"`
	Sampled   int               `json:"sampled"`
	Procedure string            `json:"procedure"`
	Rows      []RejectSampleRow `json:"rows"`
}

// DisclosureCounts is the run-level population block: the Liquidity Baseline
// pooled across Filing Days, the passed ticker-days that carry the statistics,
// the raw passed disclosure count, the distinct names, the fully-unresolved
// observations, and the rejected seed pool.
type DisclosureCounts struct {
	Baseline        int `json:"liquidity_baseline"`
	Observations    int `json:"passed_ticker_days"`
	Disclosures     int `json:"passed_disclosures"`
	DistinctTickers int `json:"distinct_tickers"`
	Unresolved      int `json:"unresolved_observations"`
	RejectedPool    int `json:"rejected_pool"`
}

// DisclosureDateGrade is one Filing Day's slice of a pooled Disclosure Grade,
// retained so the pooled headline can be read against its per-date dispersion.
type DisclosureDateGrade struct {
	AsOf     string         `json:"as_of"`
	Baseline int            `json:"liquidity_baseline"`
	Passed   int            `json:"passed_disclosures"`
	Rejected int            `json:"rejected_disclosures"`
	Horizons []HorizonStats `json:"horizons"`
}

// DisclosureCategoryBucket is one keyword-whitelist category's population: the
// passed ticker-days carrying that category, scored against the pool's
// Liquidity Baseline. A multi-category filing appears in each of its category
// buckets, so the buckets are not disjoint.
type DisclosureCategoryBucket struct {
	Category string         `json:"category"`
	N        int            `json:"n"`
	Horizons []HorizonStats `json:"horizons"`
}

// DisclosureGradeReport is the artifact one `grade disclosures` run writes.
type DisclosureGradeReport struct {
	Kind                   string                     `json:"kind"`
	AsOfStart              string                     `json:"as_of_start"`
	AsOfEnd                string                     `json:"as_of_end"`
	ReplayDates            []string                   `json:"replay_dates"`
	GitRevision            string                     `json:"git_revision"`
	GeneratedAt            string                     `json:"generated_at"`
	HoldoutStart           string                     `json:"holdout_start,omitempty"`
	SealedWindow           bool                       `json:"sealed_window"`
	DisclosureLookbackDays int                        `json:"disclosure_lookback_days"`
	SkippedAnchors         []SkippedAnchor            `json:"skipped_anchors"`
	Counts                 DisclosureCounts           `json:"counts"`
	Horizons               []HorizonStats             `json:"horizons"`
	Baseline               []HorizonStats             `json:"liquidity_baseline"`
	Categories             []DisclosureCategoryBucket `json:"categories"`
	Rows                   []DisclosureGradeRow       `json:"rows"`
	PerDate                []DisclosureDateGrade      `json:"per_date"`
	RejectSampling         RejectSampling             `json:"reject_sampling"`
	Flags                  []string                   `json:"flags"`
	SuccessCriteria        []SuccessCriterion         `json:"success_criteria"`
	Notes                  []string                   `json:"notes"`
}

// disclosureObservation is one deduped (ticker, Filing Day) before it is
// summarized: the union of the day's filing categories and the ticker-day's
// Forward Returns.
type disclosureObservation struct {
	ticker     string
	categories []string
	forward    TickerForwardReturns
}

// disclosureReplayResult is one Filing Day's Disclosure Grade inputs before
// pooling.
type disclosureReplayResult struct {
	observations   []disclosureObservation
	baseline       []TickerForwardReturns
	bigCap         []TickerForwardReturns
	rows           []DisclosureGradeRow
	rejected       []RejectSampleRow
	reactionAnchor string
	passedCount    int
	rejectedCount  int
}

// GradeDisclosures is the Disclosure Grader: it grades each requested Filing
// Day's passed-filter disclosures by their ticker's Forward Returns from the
// Reaction Anchor against the Liquidity Baseline, splits by keyword category,
// and draws the fixed monthly Reject Sample for manual review (ADR-0015).
func (e *MeasurementEngine) GradeDisclosures(_ context.Context, opts DisclosureGradeOptions) (*DisclosureGradeReport, error) {
	if e.DisclosureFilings == nil {
		return nil, fmt.Errorf("%w: disclosure grade requires a disclosure-filing source", ErrInvalidArgument)
	}
	rejectN, err := resolveRejectSampleN(opts.RejectSampleN)
	if err != nil {
		return nil, err
	}

	anchors := dedupeSortedDays(opts.Anchors)
	if len(anchors) == 0 {
		return nil, fmt.Errorf("%w: at least one --as-of filing day is required", ErrInvalidArgument)
	}
	if opts.HoldoutStart != nil {
		for _, anchor := range anchors {
			if anchor.Before(*opts.HoldoutStart) {
				return nil, fmt.Errorf("%w: as_of %s is before the holdout window (starts %s); a holdout run only reads filing days on or after the seal",
					ErrInvalidArgument, anchor.Format("2006-01-02"), opts.HoldoutStart.Format("2006-01-02"))
			}
		}
	}

	var (
		observations []disclosureObservation
		baselineRows []TickerForwardReturns
		bigCapRows   []TickerForwardReturns
		rows         []DisclosureGradeRow
		rejectedPool []RejectSampleRow
		perDate      []DisclosureDateGrade
		counts       DisclosureCounts
		reactionSeen = map[string]bool{}
		reactionSet  []time.Time
	)

	for _, day := range anchors {
		passed, err := e.DisclosureFilings.FindByAnnouncementDate(e.DB, day, true)
		if err != nil {
			return nil, fmt.Errorf("read passed disclosures announced on %s: %w", day.Format("2006-01-02"), err)
		}
		rejected, err := e.DisclosureFilings.FindByAnnouncementDate(e.DB, day, false)
		if err != nil {
			return nil, fmt.Errorf("read rejected disclosures announced on %s: %w", day.Format("2006-01-02"), err)
		}

		res, err := e.replayDisclosuresAt(day, passed, rejected)
		if err != nil {
			return nil, err
		}

		observations = append(observations, res.observations...)
		baselineRows = append(baselineRows, res.baseline...)
		bigCapRows = append(bigCapRows, res.bigCap...)
		rows = append(rows, res.rows...)
		rejectedPool = append(rejectedPool, res.rejected...)
		counts.Baseline += len(res.baseline)
		counts.Observations += len(res.observations)
		counts.Disclosures += res.passedCount
		counts.RejectedPool += res.rejectedCount
		counts.Unresolved += fullyUnresolved(res.observations)

		perDate = append(perDate, DisclosureDateGrade{
			AsOf:     day.Format("2006-01-02"),
			Baseline: len(res.baseline),
			Passed:   res.passedCount,
			Rejected: res.rejectedCount,
			Horizons: comparisonByHorizon(observationRows(res.observations), res.baseline, res.bigCap),
		})

		if res.reactionAnchor != "" && !reactionSeen[res.reactionAnchor] {
			reactionSeen[res.reactionAnchor] = true
			if parsed, err := time.Parse("2006-01-02", res.reactionAnchor); err == nil {
				reactionSet = append(reactionSet, parsed)
			}
		}
	}
	counts.DistinctTickers = distinctObservationTickers(observations)

	horizons := comparisonByHorizon(observationRows(observations), baselineRows, bigCapRows)
	categories := categoryBuckets(observations, baselineRows, bigCapRows)

	flags := disclosureGradeFlags(len(anchors), horizons, counts)
	if overlap, err := e.anchorsOverlap(reactionSet); err != nil {
		return nil, err
	} else if overlap {
		flags = append(flags, overlapFlag())
	}
	if counts.RejectedPool > rejectN {
		flags = append(flags, fmt.Sprintf(
			"reject-sample: %d rejected filing(s) pooled; the fixed sample is %d, drawn evenly across the pool",
			counts.RejectedPool, rejectN))
	}

	return &DisclosureGradeReport{
		Kind:                   disclosureGradeKind,
		AsOfStart:              anchors[0].Format("2006-01-02"),
		AsOfEnd:                anchors[len(anchors)-1].Format("2006-01-02"),
		ReplayDates:            replayDateStrings(anchors),
		GitRevision:            e.Revision,
		GeneratedAt:            time.Now().UTC().Format(time.RFC3339),
		HoldoutStart:           formatOptionalDay(opts.HoldoutStart),
		SealedWindow:           opts.HoldoutStart != nil,
		DisclosureLookbackDays: pipeline.DisclosureFilterLookbackDays,
		SkippedAnchors:         []SkippedAnchor{},
		Counts:                 counts,
		Horizons:               horizons,
		Baseline:               comparisonByHorizon(baselineRows, baselineRows, bigCapRows),
		Categories:             categories,
		Rows:                   rows,
		PerDate:                perDate,
		RejectSampling: RejectSampling{
			N:         rejectN,
			Pool:      counts.RejectedPool,
			Sampled:   min(len(rejectedPool), rejectN),
			Procedure: rejectSamplingProcedure,
			Rows:      sampleRejects(rejectedPool, rejectN),
		},
		Flags:           flags,
		SuccessCriteria: disclosureSuccessCriteria(horizons, categories),
		Notes:           disclosureGradeNotes(),
	}, nil
}

// replayDisclosuresAt performs one Filing Day's Disclosure Grade reads: the
// Reaction Anchor, the day's passed ticker-days (deduped) and their Forward
// Returns, the Liquidity Baseline on the same anchor, and the rejected
// disclosures' Forward Returns for the Reject Sample.
func (e *MeasurementEngine) replayDisclosuresAt(filingDay time.Time, passed, rejected []entity.Disclosure) (*disclosureReplayResult, error) {
	anchor, err := e.disclosureReactionAnchor(filingDay)
	if err != nil {
		return nil, err
	}

	passedTickers := disclosureTickers(passed)
	rejectedRows := rejectRowsFrom(rejected) // ticker-less rejects dropped: they have no price series

	// No stored trading day follows the filing: every horizon is unresolved, so
	// the filings are reported with nil returns rather than dropped.
	if anchor == nil {
		observations := observationsFrom(filingDay, passed, nil)
		return &disclosureReplayResult{
			observations:  observations,
			rows:          disclosureRowsFrom(filingDay, "", passed, nil),
			rejected:      rejectedRows,
			passedCount:   len(passed),
			rejectedCount: len(rejectedRows),
		}, nil
	}

	targets, readLimit, err := e.resolvePartialForwardTargets(*anchor)
	if err != nil {
		return nil, err
	}
	last := lastTarget(*anchor, targets)

	baseline, err := e.Prices.ScreenCandidates(e.DB, *anchor, defaultScreenerMinValue, defaultSuspensionWindowDays)
	if err != nil {
		return nil, fmt.Errorf("read liquidity baseline as of %s: %w", anchor.Format("2006-01-02"), err)
	}
	baselineTickers := make([]string, 0, len(baseline))
	baselineCloses := make(map[string]*float64, len(baseline))
	for _, candidate := range baseline {
		baselineTickers = append(baselineTickers, candidate.Ticker)
		baselineCloses[candidate.Ticker] = candidate.Close
	}

	rejectedTickers := make([]string, 0, len(rejectedRows))
	for _, row := range rejectedRows {
		rejectedTickers = append(rejectedTickers, row.Ticker)
	}

	allTickers := unionTickers(baselineTickers, passedTickers, rejectedTickers, BigCapBasket)
	var windows map[string][]entity.DailyPrice
	if len(allTickers) > 0 {
		prices, err := e.Prices.FindByTickersUpTo(e.DB, allTickers, last, readLimit)
		if err != nil {
			return nil, fmt.Errorf("read forward prices as of %s: %w", anchor.Format("2006-01-02"), err)
		}
		windows = groupPricesByTicker(prices)
	}

	observations := observationsFrom(filingDay, passed, func(ticker string) TickerForwardReturns {
		return forwardReturns(ticker, *anchor, baselineCloses[ticker], targets, windows[ticker])
	})

	// The Reject Sample rows carry the same Forward Returns the statistics use,
	// so a manual reviewer sees the reaction beside the filing.
	for i := range rejectedRows {
		forward := forwardReturns(rejectedRows[i].Ticker, *anchor, nil, targets, windows[rejectedRows[i].Ticker])
		rejectedRows[i].ReactionAnchor = anchor.Format("2006-01-02")
		rejectedRows[i].AnchorClose = forward.AnchorClose
		rejectedRows[i].Returns = forward.Returns
		rejectedRows[i].Unresolved = unresolvedHorizons(forward.Returns)
	}

	return &disclosureReplayResult{
		observations: observations,
		baseline:     forwardRows(baselineTickers, baselineCloses, *anchor, targets, windows),
		bigCap:       forwardRows(BigCapBasket, nil, *anchor, targets, windows),
		rows: disclosureRowsFrom(filingDay, anchor.Format("2006-01-02"), passed, func(ticker string) TickerForwardReturns {
			return forwardReturns(ticker, *anchor, baselineCloses[ticker], targets, windows[ticker])
		}),
		rejected:       rejectedRows,
		reactionAnchor: anchor.Format("2006-01-02"),
		passedCount:    len(passed),
		rejectedCount:  len(rejectedRows),
	}, nil
}

// disclosureReactionAnchor resolves a Filing Day's Reaction Anchor: the first
// stored market Trading Day on or after the announcement date. A nil return
// means no stored trading day follows the filing (a future or out-of-range
// filing); every horizon is then unresolved rather than the filing being
// dropped.
func (e *MeasurementEngine) disclosureReactionAnchor(filingDay time.Time) (*time.Time, error) {
	days, err := e.Prices.TradingDaysInRangeAll(e.DB, filingDay, filingDay.AddDate(0, 0, measurementLookaheadDays))
	if err != nil {
		return nil, fmt.Errorf("read market trading days from %s: %w", filingDay.Format("2006-01-02"), err)
	}
	for _, day := range days {
		if !day.Before(filingDay) {
			d := day
			return &d, nil
		}
	}
	return nil, nil
}

// observationsFrom dedupes a Filing Day's passed disclosures to one
// observation per ticker, unioning the day's categories. forward computes a
// ticker's Forward Returns; a nil forward (no reaction anchor) leaves them all
// unresolved.
func observationsFrom(filingDay time.Time, passed []entity.Disclosure, forward func(ticker string) TickerForwardReturns) []disclosureObservation {
	byTicker := make(map[string]*disclosureObservation, len(passed))
	order := make([]string, 0, len(passed))
	for _, d := range passed {
		ticker := disclosureTicker(d)
		if ticker == "" {
			continue
		}
		obs, ok := byTicker[ticker]
		if !ok {
			row := TickerForwardReturns{Ticker: ticker, AsOf: filingDay.Format("2006-01-02")}
			if forward != nil {
				row = forward(ticker)
			}
			obs = &disclosureObservation{ticker: ticker, forward: row}
			byTicker[ticker] = obs
			order = append(order, ticker)
		}
		for _, category := range []string(d.Categories) {
			if !stringIn(obs.categories, category) {
				obs.categories = append(obs.categories, category)
			}
		}
	}
	out := make([]disclosureObservation, 0, len(order))
	for _, ticker := range order {
		out = append(out, *byTicker[ticker])
	}
	return out
}

// disclosureRowsFrom builds the per-disclosure drill-down rows. It is not
// deduped: every passed filing keeps its own pdf_url and categories.
func disclosureRowsFrom(filingDay time.Time, anchorLabel string, passed []entity.Disclosure, forward func(ticker string) TickerForwardReturns) []DisclosureGradeRow {
	rows := make([]DisclosureGradeRow, 0, len(passed))
	for _, d := range passed {
		ticker := disclosureTicker(d)
		if ticker == "" {
			continue
		}
		row := TickerForwardReturns{Ticker: ticker, AsOf: filingDay.Format("2006-01-02")}
		if forward != nil {
			row = forward(ticker)
		}
		rows = append(rows, DisclosureGradeRow{
			DisclosureID:   d.ID,
			Ticker:         ticker,
			FilingDate:     d.AnnouncementDate.Format("2006-01-02"),
			ReactionAnchor: anchorLabel,
			Title:          d.Title,
			PDFURL:         d.PdfURL,
			Categories:     categoriesOf(d),
			AnchorClose:    row.AnchorClose,
			Returns:        row.Returns,
			Unresolved:     unresolvedHorizons(row.Returns),
		})
	}
	return rows
}

// rejectRowsFrom turns a Filing Day's rejected disclosures into Reject Sample
// rows. A ticker-less reject has no price series and is dropped here; the pool
// count still reflects only the rejects a reviewer could skim.
func rejectRowsFrom(rejected []entity.Disclosure) []RejectSampleRow {
	rows := make([]RejectSampleRow, 0, len(rejected))
	for _, d := range rejected {
		ticker := disclosureTicker(d)
		if ticker == "" {
			continue
		}
		rows = append(rows, RejectSampleRow{
			DisclosureID: d.ID,
			Ticker:       ticker,
			FilingDate:   d.AnnouncementDate.Format("2006-01-02"),
			Title:        d.Title,
			PDFURL:       d.PdfURL,
		})
	}
	return rows
}

// observationRows projects observations to the forward-row shape the shared
// comparison functions summarize.
func observationRows(observations []disclosureObservation) []TickerForwardReturns {
	rows := make([]TickerForwardReturns, 0, len(observations))
	for _, obs := range observations {
		rows = append(rows, obs.forward)
	}
	return rows
}

// categoryBuckets scores each keyword-whitelist category's passed ticker-days
// against the pool's Liquidity Baseline. Buckets are not disjoint: a filing
// carrying two whitelist categories is counted in both, so the split answers
// "which keyword earns its place", not "how the ticker-days partition".
func categoryBuckets(observations []disclosureObservation, baseline, bigCap []TickerForwardReturns) []DisclosureCategoryBucket {
	seen := map[string]bool{}
	categories := make([]string, 0)
	for _, obs := range observations {
		for _, category := range obs.categories {
			if !seen[category] {
				seen[category] = true
				categories = append(categories, category)
			}
		}
	}
	sort.Strings(categories)

	buckets := make([]DisclosureCategoryBucket, 0, len(categories))
	for _, category := range categories {
		group := make([]TickerForwardReturns, 0)
		for _, obs := range observations {
			if stringIn(obs.categories, category) {
				group = append(group, obs.forward)
			}
		}
		buckets = append(buckets, DisclosureCategoryBucket{
			Category: category,
			N:        len(group),
			Horizons: comparisonByHorizon(group, baseline, bigCap),
		})
	}
	return buckets
}

// fullyUnresolved counts observations with no resolvable horizon at all — a
// filing whose ticker has no anchor close or no post-anchor prices. Partial
// unresolved horizons are counted per horizon in ReturnStats.Unresolved.
func fullyUnresolved(observations []disclosureObservation) int {
	count := 0
	for _, obs := range observations {
		if len(unresolvedHorizons(obs.forward.Returns)) == len(measurementHorizons) {
			count++
		}
	}
	return count
}

// unresolvedHorizons lists the horizons a row could not resolve, in horizon
// order. A horizon is unresolved when the ticker has no stored close on its
// target day.
func unresolvedHorizons(returns map[int]*float64) []int {
	out := make([]int, 0, len(measurementHorizons))
	for _, horizon := range measurementHorizons {
		if value, ok := returns[horizon]; !ok || value == nil {
			out = append(out, horizon)
		}
	}
	return out
}

// sampleRejects draws the fixed-size Reject Sample evenly across the pooled
// rejected filings, so a month's sample is not biased to one day or sector. The
// pool is already ordered by (Filing Day, id), so the sample is deterministic.
func sampleRejects(pool []RejectSampleRow, n int) []RejectSampleRow {
	if n <= 0 || len(pool) <= n {
		out := make([]RejectSampleRow, len(pool))
		copy(out, pool)
		return out
	}
	out := make([]RejectSampleRow, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, pool[i*len(pool)/n])
	}
	return out
}

// resolveRejectSampleN resolves the Reject Sample size, rejecting an
// out-of-range ask rather than silently truncating it.
func resolveRejectSampleN(n int) (int, error) {
	if n == 0 {
		return defaultRejectSampleN, nil
	}
	if n < 1 || n > maxRejectSampleN {
		return 0, fmt.Errorf("%w: reject_sample must be 1..%d, got %d", ErrInvalidArgument, maxRejectSampleN, n)
	}
	return n, nil
}

// lastTarget returns the furthest resolvable horizon's target, or the anchor
// itself when none resolve.
func lastTarget(anchor time.Time, targets map[int]time.Time) time.Time {
	last := anchor
	for _, horizon := range measurementHorizons {
		if target, ok := targets[horizon]; ok && target.After(last) {
			last = target
		}
	}
	return last
}

// disclosureTicker reads a disclosure's ticker, or "" when it has none.
func disclosureTicker(d entity.Disclosure) string {
	if d.Ticker == nil {
		return ""
	}
	return *d.Ticker
}

// disclosureTickers lists the deduped tickers a Filing Day's disclosures carry.
func disclosureTickers(rows []entity.Disclosure) []string {
	seen := map[string]bool{}
	tickers := make([]string, 0, len(rows))
	for _, d := range rows {
		ticker := disclosureTicker(d)
		if ticker == "" || seen[ticker] {
			continue
		}
		seen[ticker] = true
		tickers = append(tickers, ticker)
	}
	return tickers
}

// categoriesOf renders a disclosure's categories, never nil, so a rejected
// row's empty categories serialize as [] rather than null.
func categoriesOf(d entity.Disclosure) []string {
	if len(d.Categories) == 0 {
		return []string{}
	}
	return append([]string(nil), d.Categories...)
}

// distinctObservationTickers counts the distinct names the passed population
// rests on.
func distinctObservationTickers(observations []disclosureObservation) int {
	seen := map[string]bool{}
	for _, obs := range observations {
		seen[obs.ticker] = true
	}
	return len(seen)
}

// disclosureGradeFlags renders the discipline warnings the report must carry:
// the replay-date floor, the passed population's small-n and unresolved
// horizons, an empty run, and the low-precision whitelist reminder. Flags, never
// refusals.
func disclosureGradeFlags(replayDates int, horizons []HorizonStats, counts DisclosureCounts) []string {
	flags := []string{}
	if replayDates < smallNReplayDates {
		flags = append(flags, fmt.Sprintf(
			"small-n: %d filing date(s) < %d; aggregate more Filing Days before adopting a change",
			replayDates, smallNReplayDates))
	}
	for _, stats := range horizons {
		if stats.Survivor.N == 0 {
			flags = append(flags, fmt.Sprintf("no data: no passed-disclosure Forward Return resolvable at %dd", stats.Horizon))
		} else if stats.Survivor.N < smallNSurvivors {
			flags = append(flags, fmt.Sprintf("small-n: passed disclosure n=%d < %d at %dd", stats.Survivor.N, smallNSurvivors, stats.Horizon))
		}
		if stats.Baseline.N == 0 {
			flags = append(flags, fmt.Sprintf("no data: no liquidity-baseline Forward Return resolvable at %dd", stats.Horizon))
		}
		if stats.Survivor.Unresolved > 0 {
			flags = append(flags, fmt.Sprintf(
				"unresolved: %d passed ticker-day(s) with no stored close on the %dd target (counted, not dropped)",
				stats.Survivor.Unresolved, stats.Horizon))
		}
	}
	if counts.Observations == 0 {
		flags = append(flags, "no data: no passed-filter disclosures announced on the requested filing day(s)")
	}
	if counts.Observations > 0 && counts.Observations < smallNSurvivors {
		flags = append(flags,
			"precision: the passed population is small; the whitelist's false-negative rate is the Reject Sample's subject, recorded by hand")
	}
	return flags
}

// disclosureSuccessCriteria answers the ticket's standing questions: does a
// passed disclosure beat the Baseline, is the reaction front-loaded, and does
// the keyword category separate the reaction.
func disclosureSuccessCriteria(horizons []HorizonStats, categories []DisclosureCategoryBucket) []SuccessCriterion {
	return []SuccessCriterion{
		{Question: "Do passed-filter disclosures beat the Liquidity Baseline at any horizon?", Answer: excessAnswer(horizons)},
		{Question: "Is the disclosure reaction front-loaded (<=2 days) or spread over 10-20d?", Answer: frontLoadAnswer(horizons)},
		{Question: "Does the keyword-whitelist category separate the reaction?", Answer: categoryAnswer(categories)},
	}
}

// categoryAnswer reads the best- and worst-performing whitelist categories at
// 20d by Excess, so the reader sees which keyword earns its place.
func categoryAnswer(categories []DisclosureCategoryBucket) string {
	type stat struct {
		name   string
		excess float64
		n      int
	}
	stats := make([]stat, 0, len(categories))
	for _, bucket := range categories {
		if bucket.N == 0 {
			continue
		}
		excess := bucketExcess20(bucket.Horizons)
		if excess == nil {
			continue
		}
		stats = append(stats, stat{name: bucket.Category, excess: *excess, n: bucket.N})
	}
	if len(stats) == 0 {
		return "Unresolved — no category bucket had both a passed and baseline median at 20d."
	}
	sort.SliceStable(stats, func(i, j int) bool { return stats[i].excess > stats[j].excess })
	best := stats[0]
	worst := stats[len(stats)-1]
	if best.name == worst.name {
		return fmt.Sprintf("%s alone carries the population: 20d Excess %+.2f%% (n=%d).", best.name, best.excess*100, best.n)
	}
	return fmt.Sprintf("%s leads at 20d Excess %+.2f%% (n=%d); %s trails at %+.2f%% (n=%d).",
		best.name, best.excess*100, best.n, worst.name, worst.excess*100, worst.n)
}

// bucketExcess20 finds a category bucket's 20d Excess, or nil when unavailable.
func bucketExcess20(horizons []HorizonStats) *float64 {
	for _, stats := range horizons {
		if stats.Horizon == 20 {
			return stats.Excess
		}
	}
	return nil
}

// rejectSamplingProcedure is the documented monthly manual procedure. It lives
// in the report so the loop cannot forget it and so the sample's verdicts land
// with the evidence that prompted them.
const rejectSamplingProcedure = "Monthly, fixed N. Sample the rejected filings below, open each PDF (read_idx_disclosure / fetch_disclosure_pdf), and record a Material verdict by hand. A reject is material when the filing names a corporate action or event a swing trader would act on — dividend, stock split, rights issue, warrant, buyback, material transaction, change of control, suspension. False-negative rate = material / N. The tool never judges materiality; it only draws the deterministic sample."

// disclosureGradeNotes are the standing caveats every Disclosure Grade carries.
func disclosureGradeNotes() []string {
	return []string{
		"Forward Returns are close-to-close over market-wide trading days; execution realism (fills, slippage, fees) is not modeled.",
		"A Filing Day is the disclosure's announcement date; the Reaction Anchor is the first stored market Trading Day on or after it, and Forward Returns run from that anchor's close. A filing with no stored post-anchor prices is counted as unresolved, never dropped.",
		fmt.Sprintf("The passed population is the 3-layer filter's output: a ticker with an anomaly within %d days after the filing (anomaly gate) and a whitelist keyword in the title (exclusion override). Pass status is sticky and only known once the anomaly window has elapsed, so this grades the disclosure's reaction, not a tradable signal at the filing instant.", pipeline.DisclosureFilterLookbackDays),
		"The per-horizon statistics dedupe passed filings to (ticker, Filing Day) ticker-days; the rows retain every filing for PDF drill-down.",
		"The Reject Sample is a fixed per-run sample of rejected filings, listed with the procedure in reject_sampling; category buckets are not disjoint (a multi-category filing is counted in each).",
		"The Liquidity Baseline is the hard-filter stage on shipped screener defaults (traded on the Reaction Anchor, value floor, suspension window), before structural filters and ranking.",
	}
}

// compile-time check: the concrete repository satisfies the Disclosure Grade seam.
var _ MeasurementDisclosureFilingSource = (*repository.DisclosureRepository)(nil)
