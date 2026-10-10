package usecase

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// The Flow Grade's fixed vocabulary. Every string here is a report key, so
// changing one changes what a reader parses; they are listed in CONTEXT.md
// (Flow Grade, Flow Bucket).
const (
	flowGradeKind = "flow_grade"

	// Sign bucket states (CONTEXT: Flow Bucket).
	flowSignPositive   = "positive"
	flowSignNegative   = "negative"
	flowSignFlat       = "flat"
	flowSignUnobserved = "unobserved"

	// Signed magnitude terciles over the observed rows, ascending by net.
	flowTercileLow  = "low"
	flowTercileMid  = "mid"
	flowTercileHigh = "high"
)

// MeasurementForeignFlowSource is the Flow Grade's read: per-ticker foreign net
// (Σ f_nval) over the Flow Column's window. The concrete
// *repository.BrokerStockSummaryRepository satisfies it, and it is the same read
// the screener's Flow Column makes — one definition, so the Grader scores
// exactly the column the screener exposes.
type MeasurementForeignFlowSource interface {
	SumForeignNetByTickers(db *sqlx.DB, tickers []string, to time.Time, tradingDays int) (*repository.ForeignNetWindow, error)
}

// FlowGradeOptions is a parsed `grade flow` request: one or more Replay Dates,
// an optional holdout seal, and the Flow Column window length.
type FlowGradeOptions struct {
	// Anchors are the Replay Dates; deduped and sorted before use.
	Anchors []time.Time
	// HoldoutStart, when set, seals the window for every anchor.
	HoldoutStart *time.Time
	// FlowWindowDays is the Flow Column window in stored trading days, ending on
	// the anchor; 0 uses defaultFlowWindowDays.
	FlowWindowDays int
}

// FlowCoverageSummary describes how wide a bucket's observed days spread: the
// Flow Column is a raw Σ over however many of the window's days carried a
// stored footer, so a bucket whose median covered-days is low rests on thinner
// evidence than its n suggests. DaysMedian is nil when the bucket has no
// observed row.
type FlowCoverageSummary struct {
	DaysMin    int      `json:"days_min"`
	DaysMedian *float64 `json:"days_median"`
	DaysMax    int      `json:"days_max"`
}

// FlowBucket is one Flow Bucket: a sign population and the same
// survivor-vs-baseline per-horizon stats every Grader reports (CONTEXT: Flow
// Bucket).
type FlowBucket struct {
	State    string              `json:"state"`
	N        int                 `json:"n"`
	Coverage FlowCoverageSummary `json:"coverage"`
	Horizons []HorizonStats      `json:"horizons"`
}

// FlowTercile is one signed magnitude tercile over the observed rows: its net
// range (the boundary a prospective filter would cut on) plus the same stats.
// MinNet/MaxNet are nil for an empty tercile.
type FlowTercile struct {
	Tercile  string              `json:"tercile"`
	N        int                 `json:"n"`
	MinNet   *int64              `json:"min_net"`
	MaxNet   *int64              `json:"max_net"`
	Coverage FlowCoverageSummary `json:"coverage"`
	Horizons []HorizonStats      `json:"horizons"`
}

// FlowCounts is the run-level population block: the Liquidity Baseline, how much
// of it had an observed foreign net, the unobserved remainder, and the distinct
// observed names the buckets rest on.
type FlowCounts struct {
	Baseline         int `json:"liquidity_baseline"`
	Observed         int `json:"observed"`
	Unobserved       int `json:"unobserved"`
	DistinctObserved int `json:"distinct_observed_tickers"`
}

// FlowDateGrade is one Replay Date's slice of a pooled Flow Grade, so a pooled
// headline can be read against its per-date dispersion.
type FlowDateGrade struct {
	AsOf             string       `json:"as_of"`
	Baseline         int          `json:"liquidity_baseline"`
	Observed         int          `json:"observed"`
	FlowDaysInWindow int          `json:"flow_days_in_window"`
	Buckets          []FlowBucket `json:"sign_buckets"`
}

// FlowGradeReport is the artifact one `grade flow` run writes.
type FlowGradeReport struct {
	Kind                 string                 `json:"kind"`
	AsOfStart            string                 `json:"as_of_start"`
	AsOfEnd              string                 `json:"as_of_end"`
	ReplayDates          []string               `json:"replay_dates"`
	GitRevision          string                 `json:"git_revision"`
	GeneratedAt          string                 `json:"generated_at"`
	HoldoutStart         string                 `json:"holdout_start,omitempty"`
	SealedWindow         bool                   `json:"sealed_window"`
	FlowWindowDays       int                    `json:"flow_window_days"`
	FlowDaysInWindow     int                    `json:"flow_days_in_window"`
	MinValue             int64                  `json:"min_value"`
	SuspensionWindowDays int                    `json:"suspension_window_days"`
	SkippedAnchors       []SkippedAnchor        `json:"skipped_anchors"`
	Counts               FlowCounts             `json:"counts"`
	LiquidityBaseline    []HorizonStats         `json:"liquidity_baseline"`
	SignBuckets          []FlowBucket           `json:"sign_buckets"`
	Magnitude            []FlowTercile          `json:"magnitude_terciles"`
	Rows                 []TickerForwardReturns `json:"rows"`
	PerDate              []FlowDateGrade        `json:"per_date"`
	Flags                []string               `json:"flags"`
	SuccessCriteria      []SuccessCriterion     `json:"success_criteria"`
	Notes                []string               `json:"notes"`
}

// flowReplayResult is one anchor's Flow Grade inputs before pooling.
type flowReplayResult struct {
	rows       []TickerForwardReturns
	bigCapRows []TickerForwardReturns
	tradeDays  int
}

// GradeFlow is the Flow Grader: it replays one or more Replay Dates, reads the
// Flow Column for the hard-filter-stage universe, buckets it by foreign-net sign
// and signed magnitude, and scores each bucket's Forward Returns against the
// Liquidity Baseline (CONTEXT: Flow Grade; ADR-0015).
func (e *MeasurementEngine) GradeFlow(_ context.Context, opts FlowGradeOptions) (*FlowGradeReport, error) {
	if e.ForeignFlow == nil {
		return nil, fmt.Errorf("%w: flow grade requires a foreign-flow source", ErrInvalidArgument)
	}
	flowWindow, err := resolveFlowWindow(opts.FlowWindowDays)
	if err != nil {
		return nil, err
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

	gradable, skipped, err := e.gradableAnchors(anchors)
	if err != nil {
		return nil, err
	}
	if len(gradable) == 0 {
		return nil, fmt.Errorf("%w: none of the %d requested anchor(s) can be graded (all lack %d post-anchor Trading Days)",
			ErrInvalidArgument, len(anchors), maxMeasurementHorizon)
	}

	var (
		rows             []TickerForwardReturns
		bigCapRows       []TickerForwardReturns
		perDate          []FlowDateGrade
		counts           FlowCounts
		flowDaysInWindow int
	)
	for i, anchor := range gradable {
		res, err := e.replayFlowAt(anchor, flowWindow)
		if err != nil {
			return nil, err
		}
		rows = append(rows, res.rows...)
		bigCapRows = append(bigCapRows, res.bigCapRows...)
		if i == 0 || res.tradeDays < flowDaysInWindow {
			flowDaysInWindow = res.tradeDays
		}
		observed := observedCount(res.rows)
		counts.Baseline += len(res.rows)
		counts.Observed += observed
		counts.Unobserved += len(res.rows) - observed
		perDate = append(perDate, FlowDateGrade{
			AsOf:             anchor.Format("2006-01-02"),
			Baseline:         len(res.rows),
			Observed:         observed,
			FlowDaysInWindow: res.tradeDays,
			Buckets:          signBuckets(res.rows, res.rows, res.bigCapRows),
		})
	}
	counts.DistinctObserved = distinctTickers(filterBy(rows, isObservedFlow))

	sign := signBuckets(rows, rows, bigCapRows)
	magnitude := magnitudeTerciles(rows, rows, bigCapRows)

	flags := flowGradeFlags(len(gradable), sign, magnitude, counts)
	if overlap, err := e.anchorsOverlap(gradable); err != nil {
		return nil, err
	} else if overlap {
		flags = append(flags, overlapFlag())
	}

	return &FlowGradeReport{
		Kind:                 flowGradeKind,
		AsOfStart:            gradable[0].Format("2006-01-02"),
		AsOfEnd:              gradable[len(gradable)-1].Format("2006-01-02"),
		ReplayDates:          replayDateStrings(gradable),
		GitRevision:          e.Revision,
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
		HoldoutStart:         formatOptionalDay(opts.HoldoutStart),
		SealedWindow:         opts.HoldoutStart != nil,
		FlowWindowDays:       flowWindow,
		FlowDaysInWindow:     flowDaysInWindow,
		MinValue:             defaultScreenerMinValue,
		SuspensionWindowDays: defaultSuspensionWindowDays,
		SkippedAnchors:       skipped,
		Counts:               counts,
		LiquidityBaseline:    comparisonByHorizon(rows, rows, bigCapRows),
		SignBuckets:          sign,
		Magnitude:            magnitude,
		Rows:                 rows,
		PerDate:              perDate,
		Flags:                flags,
		SuccessCriteria:      flowSuccessCriteria(sign, magnitude),
		Notes:                flowGradeNotes(flowWindow),
	}, nil
}

// replayFlowAt performs one anchor's Flow Grade reads: the Liquidity Baseline
// (the hard-filter stage), the Flow Column over that population ending ON the
// anchor, and the batched forward window. It attaches each row's foreign net and
// coverage so the observation carries its own denominator.
func (e *MeasurementEngine) replayFlowAt(anchor time.Time, flowWindow int) (*flowReplayResult, error) {
	targets, readLimit, err := e.resolveForwardTargets(anchor)
	if err != nil {
		return nil, err
	}

	baseline, err := e.Prices.ScreenCandidates(e.DB, anchor, defaultScreenerMinValue, defaultSuspensionWindowDays)
	if err != nil {
		return nil, fmt.Errorf("read liquidity baseline as of %s: %w", anchor.Format("2006-01-02"), err)
	}
	baselineTickers := make([]string, 0, len(baseline))
	baselineCloses := make(map[string]*float64, len(baseline))
	for _, candidate := range baseline {
		baselineTickers = append(baselineTickers, candidate.Ticker)
		baselineCloses[candidate.Ticker] = candidate.Close
	}

	// The Flow Column read: Σ f_nval over `flowWindow` stored trading days
	// ending ON the anchor — deliberately not the Anomaly Grade's Flow Lead-in
	// Window, which ends the day before.
	flow, err := e.ForeignFlow.SumForeignNetByTickers(e.DB, baselineTickers, anchor, flowWindow)
	if err != nil {
		return nil, fmt.Errorf("read foreign net as of %s: %w", anchor.Format("2006-01-02"), err)
	}
	observed := make(map[string]repository.TickerForeignNet, len(flow.Rows))
	for _, row := range flow.Rows {
		observed[row.Ticker] = row
	}

	allTickers := unionTickers(baselineTickers, BigCapBasket)
	var windows map[string][]entity.DailyPrice
	if len(allTickers) > 0 {
		prices, err := e.Prices.FindByTickersUpTo(e.DB, allTickers, targets[maxMeasurementHorizon], readLimit)
		if err != nil {
			return nil, fmt.Errorf("read forward prices as of %s: %w", anchor.Format("2006-01-02"), err)
		}
		windows = groupPricesByTicker(prices)
	}

	rows := make([]TickerForwardReturns, 0, len(baselineTickers))
	for _, ticker := range baselineTickers {
		row := forwardReturns(ticker, anchor, baselineCloses[ticker], targets, windows[ticker])
		if net, ok := observed[ticker]; ok {
			netValue := net.ForeignNet
			row.ForeignNet = &netValue
			row.Coverage = map[string]ScreenColumnCoverage{coverageForeignNet: {Observed: true, Days: net.Days}}
		} else {
			row.Coverage = map[string]ScreenColumnCoverage{coverageForeignNet: {Observed: false}}
		}
		rows = append(rows, row)
	}

	return &flowReplayResult{
		rows:       rows,
		bigCapRows: forwardRows(BigCapBasket, nil, anchor, targets, windows),
		tradeDays:  flow.TradeDays,
	}, nil
}

// signBuckets partitions rows into the four Flow Buckets and scores each against
// the full Liquidity Baseline. A bucket is never nil, so the report shape is
// stable across runs (an empty flat bucket is normal).
func signBuckets(rows, baseline, bigCap []TickerForwardReturns) []FlowBucket {
	buckets := make([]FlowBucket, 0, 4)
	for _, state := range []string{flowSignPositive, flowSignNegative, flowSignFlat, flowSignUnobserved} {
		group := filterBy(rows, func(row TickerForwardReturns) bool { return flowSignState(row.ForeignNet) == state })
		buckets = append(buckets, FlowBucket{
			State:    state,
			N:        len(group),
			Coverage: coverageSummary(group),
			Horizons: comparisonByHorizon(group, baseline, bigCap),
		})
	}
	return buckets
}

// magnitudeTerciles cuts the observed rows into signed thirds ascending by net:
// low (most negative) → high (most positive). The split mirrors the sort key and
// a prospective one-sided `foreign_net >= X` filter, so the high tercile is the
// tail a threshold would try to capture. Cut points are pooled across Replay
// Dates, matching the shipped column's absolute (unranked) value.
func magnitudeTerciles(rows, baseline, bigCap []TickerForwardReturns) []FlowTercile {
	sorted := filterBy(rows, isObservedFlow)
	sort.SliceStable(sorted, func(i, j int) bool {
		ni, nj := *sorted[i].ForeignNet, *sorted[j].ForeignNet
		if ni != nj {
			return ni < nj
		}
		if sorted[i].Ticker != sorted[j].Ticker {
			return sorted[i].Ticker < sorted[j].Ticker
		}
		return sorted[i].AsOf < sorted[j].AsOf
	})

	n := len(sorted)
	first, second := n/3, (2*n)/3
	groups := []struct {
		state string
		rows  []TickerForwardReturns
	}{
		{flowTercileLow, sorted[:first]},
		{flowTercileMid, sorted[first:second]},
		{flowTercileHigh, sorted[second:]},
	}
	out := make([]FlowTercile, 0, len(groups))
	for _, group := range groups {
		out = append(out, FlowTercile{
			Tercile:  group.state,
			N:        len(group.rows),
			MinNet:   minForeignNet(group.rows),
			MaxNet:   maxForeignNet(group.rows),
			Coverage: coverageSummary(group.rows),
			Horizons: comparisonByHorizon(group.rows, baseline, bigCap),
		})
	}
	return out
}

// flowSignState maps an observation's foreign net to its Flow Bucket. A nil net
// is unobserved; an observed zero is its own flat bucket, never folded into a
// sign (CONTEXT: Flow Bucket).
func flowSignState(net *int64) string {
	if net == nil {
		return flowSignUnobserved
	}
	switch {
	case *net > 0:
		return flowSignPositive
	case *net < 0:
		return flowSignNegative
	default:
		return flowSignFlat
	}
}

// isObservedFlow reports whether a row carries an observed foreign net.
func isObservedFlow(row TickerForwardReturns) bool { return row.ForeignNet != nil }

// observedCount counts a replay's rows with an observed foreign net.
func observedCount(rows []TickerForwardReturns) int {
	count := 0
	for _, row := range rows {
		if isObservedFlow(row) {
			count++
		}
	}
	return count
}

// coverageSummary summarizes the covered-day spread of a bucket's observed rows.
func coverageSummary(rows []TickerForwardReturns) FlowCoverageSummary {
	days := make([]float64, 0, len(rows))
	for _, row := range rows {
		if !isObservedFlow(row) {
			continue
		}
		days = append(days, float64(row.Coverage[coverageForeignNet].Days))
	}
	if len(days) == 0 {
		return FlowCoverageSummary{}
	}
	sorted := append([]float64(nil), days...)
	sort.Float64s(sorted)
	median := median(sorted)
	return FlowCoverageSummary{
		DaysMin:    int(sorted[0]),
		DaysMedian: &median,
		DaysMax:    int(sorted[len(sorted)-1]),
	}
}

// minForeignNet returns the smallest observed net in a bucket, or nil when
// empty.
func minForeignNet(rows []TickerForwardReturns) *int64 {
	var out *int64
	for _, row := range rows {
		if row.ForeignNet == nil {
			continue
		}
		if out == nil || *row.ForeignNet < *out {
			value := *row.ForeignNet
			out = &value
		}
	}
	return out
}

// maxForeignNet returns the largest observed net in a bucket, or nil when empty.
func maxForeignNet(rows []TickerForwardReturns) *int64 {
	var out *int64
	for _, row := range rows {
		if row.ForeignNet == nil {
			continue
		}
		if out == nil || *row.ForeignNet > *out {
			value := *row.ForeignNet
			out = &value
		}
	}
	return out
}

// flowGradeFlags renders the discipline warnings the report must carry: the
// replay-date floor, each bucket below the small-n floor, the unobserved
// circularity, and the sparse-coverage caveat. Flags, never refusals.
func flowGradeFlags(replayDates int, sign []FlowBucket, magnitude []FlowTercile, counts FlowCounts) []string {
	flags := []string{}
	if replayDates < smallNReplayDates {
		flags = append(flags, fmt.Sprintf(
			"small-n: %d replay date(s) < %d; aggregate more anchors before adopting a change",
			replayDates, smallNReplayDates))
	}
	for _, bucket := range sign {
		if bucket.N > 0 && bucket.N < smallNSurvivors {
			flags = append(flags, fmt.Sprintf("small-n: %s n=%d < %d", bucket.State, bucket.N, smallNSurvivors))
		}
	}
	for _, tercile := range magnitude {
		if tercile.N > 0 && tercile.N < smallNSurvivors {
			flags = append(flags, fmt.Sprintf("small-n: magnitude/%s n=%d < %d", tercile.Tercile, tercile.N, smallNSurvivors))
		}
	}
	if counts.Unobserved > 0 {
		flags = append(flags, fmt.Sprintf(
			"unobserved: %d of %d liquidity-baseline row(s) had no stored foreign-net footer in the flow window; excluded from the signed buckets and reported as their own bucket",
			counts.Unobserved, counts.Baseline))
		flags = append(flags,
			"circular-baseline: the unobserved bucket dominates the Liquidity Baseline, so its Excess is near-zero by construction — read its absolute median and hit rate, never its Excess")
	}
	if counts.Observed > 0 && counts.Unobserved > counts.Observed {
		flags = append(flags, fmt.Sprintf(
			"sparse-coverage: observed foreign net on %d of %d pooled row(s) (%.0f%%); bucket medians rest on the sparse anomaly-gated broker population",
			counts.Observed, counts.Baseline, 100*float64(counts.Observed)/float64(counts.Baseline)))
	}
	return flags
}

// flowSuccessCriteria answers the ticket's standing questions: does accumulation
// beat the baseline, at what horizon, and does magnitude add beyond sign.
func flowSuccessCriteria(sign []FlowBucket, magnitude []FlowTercile) []SuccessCriterion {
	return []SuccessCriterion{
		{Question: "Does foreign accumulation beat the Liquidity Baseline at any horizon?", Answer: flowExcessAnswer(sign)},
		{Question: "Is the accumulation edge front-loaded or spread over 10-20d?", Answer: flowFrontLoadAnswer(sign)},
		{Question: "Does signed magnitude separate Forward Returns, or does sign alone exhaust the signal?", Answer: flowMagnitudeAnswer(sign, magnitude)},
	}
}

func flowExcessAnswer(sign []FlowBucket) string {
	positive := findFlowBucket(sign, flowSignPositive)
	if positive == nil || positive.N == 0 {
		return "Unresolved — no observed positive-flow rows."
	}
	positives := make([]string, 0)
	measured := false
	for _, stats := range positive.Horizons {
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
		return "No — the positive bucket's Excess is non-positive at every measured horizon."
	default:
		return "Unresolved — no positive-flow Forward Return and baseline median to compare."
	}
}

func flowFrontLoadAnswer(sign []FlowBucket) string {
	positive := findFlowBucket(sign, flowSignPositive)
	if positive == nil || positive.N == 0 {
		return "Unresolved — no observed positive-flow rows."
	}
	var short, long *float64
	for _, stats := range positive.Horizons {
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
		return fmt.Sprintf("Front-loaded: the positive bucket's 5d Excess (%+.2f%%) is at least its 20d Excess (%+.2f%%).", *short*100, *long*100)
	}
	return fmt.Sprintf("Spread: the positive bucket's 20d Excess (%+.2f%%) exceeds its 5d Excess (%+.2f%%).", *long*100, *short*100)
}

func flowMagnitudeAnswer(sign []FlowBucket, magnitude []FlowTercile) string {
	high := findFlowTercile(magnitude, flowTercileHigh)
	low := findFlowTercile(magnitude, flowTercileLow)
	if high == nil || low == nil || high.N == 0 {
		return "Unresolved — no observed-flow rows to tercile."
	}
	highMedian, highN := flowTercileMedian(high, 20)
	lowMedian, lowN := flowTercileMedian(low, 20)
	positive := findFlowBucket(sign, flowSignPositive)
	posMedian, posN := flowBucketMedian(positive, 20)
	return fmt.Sprintf(
		"20d median — high tercile %s (n=%d) vs low tercile %s (n=%d); sign alone (positive bucket) %s (n=%d). A high-tercile median above the positive bucket's would mark a threshold beyond sign; otherwise sign exhausts the signal.",
		formatPercent(highMedian), highN, formatPercent(lowMedian), lowN, formatPercent(posMedian), posN)
}

// findFlowBucket locates a sign bucket by state.
func findFlowBucket(buckets []FlowBucket, state string) *FlowBucket {
	for i := range buckets {
		if buckets[i].State == state {
			return &buckets[i]
		}
	}
	return nil
}

// findFlowTercile locates a magnitude tercile by label.
func findFlowTercile(terciles []FlowTercile, state string) *FlowTercile {
	for i := range terciles {
		if terciles[i].Tercile == state {
			return &terciles[i]
		}
	}
	return nil
}

// flowBucketMedian finds a sign bucket's survivor median and n at one horizon.
func flowBucketMedian(bucket *FlowBucket, horizon int) (*float64, int) {
	if bucket == nil {
		return nil, 0
	}
	for _, stats := range bucket.Horizons {
		if stats.Horizon == horizon {
			return stats.Survivor.Median, stats.Survivor.N
		}
	}
	return nil, 0
}

// flowTercileMedian finds a tercile's survivor median and n at one horizon.
func flowTercileMedian(tercile *FlowTercile, horizon int) (*float64, int) {
	if tercile == nil {
		return nil, 0
	}
	for _, stats := range tercile.Horizons {
		if stats.Horizon == horizon {
			return stats.Survivor.Median, stats.Survivor.N
		}
	}
	return nil, 0
}

// flowGradeNotes are the standing caveats every Flow Grade carries.
func flowGradeNotes(flowWindow int) []string {
	return []string{
		"Forward Returns are close-to-close over market-wide trading days; execution realism (fills, slippage, fees) is not modeled.",
		fmt.Sprintf("The Flow Column is Σ f_nval over the %d stored trading days ending ON the anchor — the screener's foreign-net window, deliberately distinct from the Anomaly Grade's Flow Lead-in Window ending the day before.", flowWindow),
		"foreign_net is a raw Σ over however many of the window's days carried a stored footer; each bucket declares its covered-day spread, and coverage is anomaly-gated and sparse by design, so a coverage-driven result is visible rather than hidden.",
		"The Liquidity Baseline is the hard-filter stage on shipped screener defaults (traded on the anchor day, value floor, suspension window), before structural filters and ranking; the liquidity_baseline block repeats it in both the survivor and baseline slots (Excess 0) so its medians are readable in one place.",
		"Unobserved rows are excluded from the signed buckets and reported as their own bucket, never zero-imputed.",
	}
}

// resolveFlowWindow resolves the Flow Column window length, rejecting an
// over-long ask rather than silently truncating it.
func resolveFlowWindow(days int) (int, error) {
	if days == 0 {
		return defaultFlowWindowDays, nil
	}
	if days < 1 || days > maxFlowWindowDays {
		return 0, fmt.Errorf("%w: flow_window_days must be 1..%d, got %d", ErrInvalidArgument, maxFlowWindowDays, days)
	}
	return days, nil
}

// compile-time check: the concrete repository satisfies the Flow Grade seam.
var _ MeasurementForeignFlowSource = (*repository.BrokerStockSummaryRepository)(nil)
