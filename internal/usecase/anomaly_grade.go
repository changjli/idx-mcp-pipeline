package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// The Anomaly Grade's fixed vocabulary. Every string here is a report key, so
// changing one changes what a reader parses; they are listed in CONTEXT.md
// (Direction Bucket, Broker Confirmation, Anomaly Confluence).
const (
	anomalyGradeKind = "anomaly_grade"

	// Direction buckets (CONTEXT: Direction Bucket).
	directionUp          = "up"
	directionDown        = "down"
	directionVolumeSpike = "volume_spike"

	// Anomaly types as the detector stores them.
	anomalyTypePrice  = "price"
	anomalyTypeVolume = "volume"

	// Broker Confirmation states. confirmed / not_confirmed apply to the
	// directional buckets; accumulation / distribution / net_zero to the
	// directionless volume_spike; unobserved to any.
	brokerConfirmed    = "confirmed"
	brokerNotConfirmed = "not_confirmed"
	brokerAccumulation = "accumulation"
	brokerDistribution = "distribution"
	brokerNetZero      = "net_zero"
	brokerUnobserved   = "unobserved"

	// Disclosure presence states.
	disclosurePresent = "present"
	disclosureAbsent  = "absent"

	// brokerWindowEndLabel names the Flow Lead-in Window's alignment in the
	// report, so it is never mistaken for the Flow Column's.
	brokerWindowEndLabel = "anchor_minus_1"
)

// MeasurementAnomalySource is the Anomaly Grade's stored-anomaly read. The
// concrete *repository.AnomalyRepository satisfies it.
type MeasurementAnomalySource interface {
	FindByDate(db *sqlx.DB, tradingDay string) ([]entity.Anomaly, error)
}

// MeasurementDisclosureSource is the Anomaly Grade's disclosure-presence read:
// the tickers with a passed-filter Disclosure in the gate window. The concrete
// *repository.DisclosureRepository satisfies it.
type MeasurementDisclosureSource interface {
	PassedTickersInWindow(db *sqlx.DB, tickers []string, from, to time.Time) ([]string, error)
}

// MeasurementBrokerNetSource is the Anomaly Grade's broker read: per-ticker
// total net accumulation over the Flow Lead-in Window. The concrete
// *repository.BrokerStockSummaryRepository satisfies it.
type MeasurementBrokerNetSource interface {
	SumNetByTickers(db *sqlx.DB, tickers []string, to time.Time, tradingDays int) ([]repository.TickerNet, error)
}

// AnomalyGradeOptions is a parsed `grade anomalies` request: one or more Replay
// Dates, an optional holdout seal, and the Flow Lead-in Window length.
type AnomalyGradeOptions struct {
	// Anchors are the Replay Dates; deduped and sorted before use.
	Anchors []time.Time
	// HoldoutStart, when set, seals the window for every anchor.
	HoldoutStart *time.Time
	// FlowWindowDays is the Flow Lead-in Window length in stored trading days;
	// 0 uses defaultFlowWindowDays.
	FlowWindowDays int
}

// AnomalyBucket is one confluence population within a Direction Bucket: its
// state label and the same survivor-vs-baseline per-horizon stats every Grader
// reports (CONTEXT: Anomaly Confluence).
type AnomalyBucket struct {
	State    string         `json:"state"`
	N        int            `json:"n"`
	Horizons []HorizonStats `json:"horizons"`
}

// AnomalyDirectionGrade is one Direction Bucket's block: the population overall,
// then nested by broker confirmation and disclosure presence, then their cross
// over observed-flow rows only. Rows is the per-observation drill-down.
type AnomalyDirectionGrade struct {
	Direction  string                 `json:"direction"`
	N          int                    `json:"n"`
	Overall    []HorizonStats         `json:"overall"`
	Broker     []AnomalyBucket        `json:"broker"`
	Disclosure []AnomalyBucket        `json:"disclosure"`
	Cross      []AnomalyBucket        `json:"cross"`
	Rows       []TickerForwardReturns `json:"rows"`
}

// AnomalyCounts is the run-level population block: the Liquidity Baseline and
// the anomaly observations pooled across Replay Dates, the distinct names the
// observations rest on, and how many were broker-unobserved.
type AnomalyCounts struct {
	Baseline        int `json:"liquidity_baseline"`
	Observations    int `json:"anomaly_observations"`
	DistinctTickers int `json:"distinct_tickers"`
	Unobserved      int `json:"broker_unobserved"`
}

// DirectionExcess is one Replay Date's single-direction block, retained so a
// pooled headline can be read against its per-date dispersion.
type DirectionExcess struct {
	Direction string         `json:"direction"`
	N         int            `json:"n"`
	Horizons  []HorizonStats `json:"horizons"`
}

// AnomalyDateGrade is one Replay Date's slice of a pooled Anomaly Grade.
type AnomalyDateGrade struct {
	AsOf       string            `json:"as_of"`
	Baseline   int               `json:"liquidity_baseline"`
	Anomalies  int               `json:"anomalies"`
	Directions []DirectionExcess `json:"directions"`
}

// AnomalyGradeReport is the artifact one `grade anomalies` run writes.
type AnomalyGradeReport struct {
	Kind                   string                  `json:"kind"`
	AsOfStart              string                  `json:"as_of_start"`
	AsOfEnd                string                  `json:"as_of_end"`
	ReplayDates            []string                `json:"replay_dates"`
	GitRevision            string                  `json:"git_revision"`
	GeneratedAt            string                  `json:"generated_at"`
	HoldoutStart           string                  `json:"holdout_start,omitempty"`
	SealedWindow           bool                    `json:"sealed_window"`
	FlowWindowDays         int                     `json:"flow_window_days"`
	BrokerWindowEnd        string                  `json:"broker_window_end"`
	DisclosureLookbackDays int                     `json:"disclosure_lookback_days"`
	SkippedAnchors         []SkippedAnchor         `json:"skipped_anchors"`
	Counts                 AnomalyCounts           `json:"counts"`
	Directions             []AnomalyDirectionGrade `json:"directions"`
	PerDate                []AnomalyDateGrade      `json:"per_date"`
	Flags                  []string                `json:"flags"`
	SuccessCriteria        []SuccessCriterion      `json:"success_criteria"`
	Notes                  []string                `json:"notes"`
}

// anomalyReplayResult is one anchor's Anomaly Grade inputs before pooling.
type anomalyReplayResult struct {
	anomalyRows  []TickerForwardReturns
	baselineRows []TickerForwardReturns
	bigCapRows   []TickerForwardReturns
}

// anomalyObservation is one deduped anomaly ticker-day: the triggering types and
// the resolved Direction Bucket.
type anomalyObservation struct {
	Ticker    string
	Types     []string
	Direction string
}

// GradeAnomalies is the Anomaly Grader: it replays one or more Replay Dates,
// dedupes each anchor's stored Anomalies to one observation per ticker-day,
// scores them against the Liquidity Baseline, and splits them by Direction
// Bucket and by Anomaly Confluence (CONTEXT; ADR-0013).
func (e *MeasurementEngine) GradeAnomalies(ctx context.Context, opts AnomalyGradeOptions) (*AnomalyGradeReport, error) {
	if e.Anomalies == nil || e.Disclosures == nil || e.BrokerNet == nil {
		return nil, fmt.Errorf("%w: anomaly grade requires anomaly, disclosure and broker-net sources", ErrInvalidArgument)
	}
	flowWindow, err := resolveAnomalyFlowWindow(opts.FlowWindowDays)
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
		anomalyRows  []TickerForwardReturns
		baselineRows []TickerForwardReturns
		bigCapRows   []TickerForwardReturns
		counts       AnomalyCounts
		perDate      []AnomalyDateGrade
	)
	for _, anchor := range gradable {
		res, err := e.replayAnomaliesAt(ctx, anchor, flowWindow)
		if err != nil {
			return nil, err
		}
		anomalyRows = append(anomalyRows, res.anomalyRows...)
		baselineRows = append(baselineRows, res.baselineRows...)
		bigCapRows = append(bigCapRows, res.bigCapRows...)
		counts.Baseline += len(res.baselineRows)
		counts.Observations += len(res.anomalyRows)
		perDate = append(perDate, AnomalyDateGrade{
			AsOf:       anchor.Format("2006-01-02"),
			Baseline:   len(res.baselineRows),
			Anomalies:  len(res.anomalyRows),
			Directions: directionExcesses(res.anomalyRows, res.baselineRows, res.bigCapRows),
		})
	}
	counts.DistinctTickers = distinctTickers(anomalyRows)
	for _, row := range anomalyRows {
		if row.BrokerState == brokerUnobserved {
			counts.Unobserved++
		}
	}

	directions := directionGrades(anomalyRows, baselineRows, bigCapRows)
	flags := anomalyGradeFlags(len(gradable), directions, counts)
	if overlap, err := e.anchorsOverlap(gradable); err != nil {
		return nil, err
	} else if overlap {
		flags = append(flags, overlapFlag())
	}

	return &AnomalyGradeReport{
		Kind:                   anomalyGradeKind,
		AsOfStart:              gradable[0].Format("2006-01-02"),
		AsOfEnd:                gradable[len(gradable)-1].Format("2006-01-02"),
		ReplayDates:            replayDateStrings(gradable),
		GitRevision:            e.Revision,
		GeneratedAt:            time.Now().UTC().Format(time.RFC3339),
		HoldoutStart:           formatOptionalDay(opts.HoldoutStart),
		SealedWindow:           opts.HoldoutStart != nil,
		FlowWindowDays:         flowWindow,
		BrokerWindowEnd:        brokerWindowEndLabel,
		DisclosureLookbackDays: pipeline.DisclosureFilterLookbackDays,
		SkippedAnchors:         skipped,
		Counts:                 counts,
		Directions:             directions,
		PerDate:                perDate,
		Flags:                  flags,
		SuccessCriteria:        anomalySuccessCriteria(directions),
		Notes:                  anomalyGradeNotes(flowWindow),
	}, nil
}

// replayAnomaliesAt performs one anchor's Anomaly Grade reads: the stored
// anomalies (deduped to ticker-days), the Liquidity Baseline, the broker net
// over the Flow Lead-in Window, disclosure presence over the gate window, and
// the batched forward window.
func (e *MeasurementEngine) replayAnomaliesAt(ctx context.Context, anchor time.Time, flowWindow int) (*anomalyReplayResult, error) {
	targets, readLimit, err := e.resolveForwardTargets(anchor)
	if err != nil {
		return nil, err
	}

	anomalies, err := e.Anomalies.FindByDate(e.DB, anchor.Format("2006-01-02"))
	if err != nil {
		return nil, fmt.Errorf("read anomalies for %s: %w", anchor.Format("2006-01-02"), err)
	}
	observations := anomalyTickerDays(anomalies)
	tickers := observationTickers(observations)

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

	// Broker Confirmation: total net accumulation over the Flow Lead-in Window,
	// the `flowWindow` stored trading days ending the day BEFORE the anchor.
	netByTicker := make(map[string]repository.TickerNet, len(tickers))
	if len(tickers) > 0 {
		nets, err := e.BrokerNet.SumNetByTickers(e.DB, tickers, anchor.AddDate(0, 0, -1), flowWindow)
		if err != nil {
			return nil, fmt.Errorf("read broker net as of %s: %w", anchor.Format("2006-01-02"), err)
		}
		for _, net := range nets {
			netByTicker[net.Ticker] = net
		}
	}

	// Disclosure presence: a passed-filter Disclosure for the ticker announced
	// within the anomaly gate's lookback, matching get_market_anomalies.
	passedSet := make(map[string]bool, len(tickers))
	if len(tickers) > 0 {
		from := anchor.AddDate(0, 0, -pipeline.DisclosureFilterLookbackDays)
		passed, err := e.Disclosures.PassedTickersInWindow(e.DB, tickers, from, anchor)
		if err != nil {
			return nil, fmt.Errorf("read passed disclosures as of %s: %w", anchor.Format("2006-01-02"), err)
		}
		for _, ticker := range passed {
			passedSet[ticker] = true
		}
	}

	allTickers := unionTickers(baselineTickers, tickers, BigCapBasket)
	var windows map[string][]entity.DailyPrice
	if len(allTickers) > 0 {
		prices, err := e.Prices.FindByTickersUpTo(e.DB, allTickers, targets[maxMeasurementHorizon], readLimit)
		if err != nil {
			return nil, fmt.Errorf("read forward prices as of %s: %w", anchor.Format("2006-01-02"), err)
		}
		windows = groupPricesByTicker(prices)
	}

	anomalyRows := make([]TickerForwardReturns, 0, len(observations))
	for _, obs := range observations {
		row := forwardReturns(obs.Ticker, anchor, baselineCloses[obs.Ticker], targets, windows[obs.Ticker])
		row.AnomalyTypes = obs.Types
		row.Direction = obs.Direction
		if passedSet[obs.Ticker] {
			row.DisclosureState = disclosurePresent
		} else {
			row.DisclosureState = disclosureAbsent
		}
		if net, ok := netByTicker[obs.Ticker]; ok {
			netValue := net.Net
			row.BrokerNet = &netValue
			row.BrokerState = brokerState(obs.Direction, net.Net)
		} else {
			row.BrokerState = brokerUnobserved
		}
		anomalyRows = append(anomalyRows, row)
	}

	return &anomalyReplayResult{
		anomalyRows:  anomalyRows,
		baselineRows: forwardRows(baselineTickers, baselineCloses, anchor, targets, windows),
		bigCapRows:   forwardRows(BigCapBasket, nil, anchor, targets, windows),
	}, nil
}

// anomalyTickerDays dedupes the anchor's anomaly rows to one observation per
// ticker. A price row wins the Direction Bucket over a volume row on the same
// ticker-day; a ticker with only volume rows is a directionless volume_spike.
// Types are retained in first-seen order for drill-down.
func anomalyTickerDays(rows []entity.Anomaly) []anomalyObservation {
	byTicker := make(map[string]*anomalyObservation, len(rows))
	order := make([]string, 0, len(rows))
	for _, row := range rows {
		obs, ok := byTicker[row.Ticker]
		if !ok {
			obs = &anomalyObservation{Ticker: row.Ticker}
			byTicker[row.Ticker] = obs
			order = append(order, row.Ticker)
		}
		if !stringIn(obs.Types, row.Type) {
			obs.Types = append(obs.Types, row.Type)
		}
		if row.Type == anomalyTypePrice {
			obs.Direction = row.Direction
		}
	}
	out := make([]anomalyObservation, 0, len(order))
	for _, ticker := range order {
		obs := byTicker[ticker]
		if obs.Direction == "" {
			obs.Direction = directionVolumeSpike
		}
		out = append(out, *obs)
	}
	return out
}

// brokerState maps a ticker-day's Direction Bucket and net accumulation to its
// Broker Confirmation state. up/down are directionally aligned (positive net
// confirms up, negative confirms down; a flat net is observed-not-confirmed);
// volume_spike, having no direction, is split by net sign (CONTEXT: Broker
// Confirmation).
func brokerState(direction string, net int64) string {
	switch direction {
	case directionUp:
		if net > 0 {
			return brokerConfirmed
		}
		return brokerNotConfirmed
	case directionDown:
		if net < 0 {
			return brokerConfirmed
		}
		return brokerNotConfirmed
	default:
		switch {
		case net > 0:
			return brokerAccumulation
		case net < 0:
			return brokerDistribution
		default:
			return brokerNetZero
		}
	}
}

// brokerStateList is the full state set reported for a direction, including
// empty buckets so a report's shape is stable across runs.
func brokerStateList(direction string) []string {
	if direction == directionVolumeSpike {
		return []string{brokerAccumulation, brokerDistribution, brokerNetZero, brokerUnobserved}
	}
	return []string{brokerConfirmed, brokerNotConfirmed, brokerUnobserved}
}

// directionGrades builds the three Direction Buckets and nests the confluence
// splits inside each.
func directionGrades(rows, baseline, bigCap []TickerForwardReturns) []AnomalyDirectionGrade {
	grades := make([]AnomalyDirectionGrade, 0, 3)
	for _, direction := range []string{directionUp, directionDown, directionVolumeSpike} {
		group := filterDirection(rows, direction)
		grade := AnomalyDirectionGrade{
			Direction: direction,
			N:         len(group),
			Overall:   comparisonByHorizon(group, baseline, bigCap),
			Rows:      group,
		}
		for _, state := range brokerStateList(direction) {
			grade.Broker = append(grade.Broker, bucketGrade(group, baseline, bigCap, state,
				func(row TickerForwardReturns) string { return row.BrokerState }))
		}
		for _, state := range []string{disclosurePresent, disclosureAbsent} {
			grade.Disclosure = append(grade.Disclosure, bucketGrade(group, baseline, bigCap, state,
				func(row TickerForwardReturns) string { return row.DisclosureState }))
		}
		// The cross is restricted to observed-flow rows: an unobserved row has no
		// broker state to cross with, and is already reported in the broker split.
		for _, state := range brokerStateList(direction) {
			if state == brokerUnobserved {
				continue
			}
			for _, disclosure := range []string{disclosurePresent, disclosureAbsent} {
				crossRows := filterBy(group, func(row TickerForwardReturns) bool {
					return row.BrokerState == state && row.DisclosureState == disclosure
				})
				grade.Cross = append(grade.Cross, AnomalyBucket{
					State:    state + "+" + disclosure,
					N:        len(crossRows),
					Horizons: comparisonByHorizon(crossRows, baseline, bigCap),
				})
			}
		}
		grades = append(grades, grade)
	}
	return grades
}

// directionExcesses is one anchor's per-direction overall stats, for the
// per-date breakdown.
func directionExcesses(rows, baseline, bigCap []TickerForwardReturns) []DirectionExcess {
	out := make([]DirectionExcess, 0, 3)
	for _, direction := range []string{directionUp, directionDown, directionVolumeSpike} {
		group := filterDirection(rows, direction)
		out = append(out, DirectionExcess{
			Direction: direction,
			N:         len(group),
			Horizons:  comparisonByHorizon(group, baseline, bigCap),
		})
	}
	return out
}

// bucketGrade summarizes one filtered bucket against the pool's baseline.
func bucketGrade(group, baseline, bigCap []TickerForwardReturns, state string, key func(TickerForwardReturns) string) AnomalyBucket {
	bucket := filterBy(group, func(row TickerForwardReturns) bool { return key(row) == state })
	return AnomalyBucket{
		State:    state,
		N:        len(bucket),
		Horizons: comparisonByHorizon(bucket, baseline, bigCap),
	}
}

// filterDirection returns the rows in one Direction Bucket.
func filterDirection(rows []TickerForwardReturns, direction string) []TickerForwardReturns {
	return filterBy(rows, func(row TickerForwardReturns) bool { return row.Direction == direction })
}

// filterBy returns the rows matching a predicate, never nil.
func filterBy(rows []TickerForwardReturns, keep func(TickerForwardReturns) bool) []TickerForwardReturns {
	out := make([]TickerForwardReturns, 0, len(rows))
	for _, row := range rows {
		if keep(row) {
			out = append(out, row)
		}
	}
	return out
}

// observationTickers lists the deduped observation tickers in order.
func observationTickers(observations []anomalyObservation) []string {
	out := make([]string, 0, len(observations))
	for _, obs := range observations {
		out = append(out, obs.Ticker)
	}
	return out
}

// stringIn reports whether a string slice contains a value.
func stringIn(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// anomalyGradeFlags renders the discipline warnings the report must carry:
// the replay-date floor, each bucket below the small-n floor, and the
// unobserved count. Flags, never refusals.
func anomalyGradeFlags(replayDates int, directions []AnomalyDirectionGrade, counts AnomalyCounts) []string {
	flags := []string{}
	if replayDates < smallNReplayDates {
		flags = append(flags, fmt.Sprintf(
			"small-n: %d replay date(s) < %d; aggregate more anchors before adopting a change",
			replayDates, smallNReplayDates))
	}
	for _, direction := range directions {
		if direction.N > 0 && direction.N < smallNSurvivors {
			flags = append(flags, fmt.Sprintf("small-n: %s n=%d < %d", direction.Direction, direction.N, smallNSurvivors))
		}
		for _, bucket := range direction.Broker {
			if bucket.N > 0 && bucket.N < smallNSurvivors {
				flags = append(flags, fmt.Sprintf("small-n: %s/%s n=%d < %d", direction.Direction, bucket.State, bucket.N, smallNSurvivors))
			}
		}
		for _, bucket := range direction.Disclosure {
			if bucket.N > 0 && bucket.N < smallNSurvivors {
				flags = append(flags, fmt.Sprintf("small-n: %s/disclosure:%s n=%d < %d", direction.Direction, bucket.State, bucket.N, smallNSurvivors))
			}
		}
	}
	if counts.Unobserved > 0 {
		flags = append(flags, fmt.Sprintf(
			"unobserved: %d anomaly ticker-day(s) had no stored broker rows in the flow lead-in window; excluded from confirmed/not and reported as their own bucket",
			counts.Unobserved))
	}
	return flags
}

// anomalySuccessCriteria answers the spec's three standing questions with this
// report's anomaly numbers. The third — the confluence question the screen
// report stubs out — is the one this Grader exists to answer.
func anomalySuccessCriteria(directions []AnomalyDirectionGrade) []SuccessCriterion {
	return []SuccessCriterion{
		{Question: "Do anomaly ticker-days beat the Liquidity Baseline at any horizon?", Answer: anomalyExcessAnswer(directions)},
		{Question: "Is anomaly follow-through front-loaded or spread over 10-20d?", Answer: anomalyFrontLoadAnswer(directions)},
		{Question: "Does broker/disclosure confirmation split Anomalies into different Forward Return populations?", Answer: anomalyConfluenceAnswer(directions)},
	}
}

func anomalyExcessAnswer(directions []AnomalyDirectionGrade) string {
	positives := make([]string, 0)
	measured := false
	for _, direction := range directions {
		for _, stats := range direction.Overall {
			if stats.Excess == nil {
				continue
			}
			measured = true
			if *stats.Excess > 0 {
				positives = append(positives, fmt.Sprintf("%s %dd %+.2f%%", direction.Direction, stats.Horizon, *stats.Excess*100))
			}
		}
	}
	switch {
	case len(positives) > 0:
		return "Yes — positive Excess at " + strings.Join(positives, ", ") + "."
	case measured:
		return "No — Excess is non-positive at every measured direction/horizon."
	default:
		return "Unresolved — no direction had both anomaly and baseline Forward Returns."
	}
}

func anomalyFrontLoadAnswer(directions []AnomalyDirectionGrade) string {
	target := focusDirection(directions)
	if target == nil {
		return "Unresolved — no anomaly observations."
	}
	var short, long *float64
	for _, stats := range target.Overall {
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
		return fmt.Sprintf("Front-loaded: %s 5d Excess %+.2f%% is at least the 20d Excess %+.2f%%.", target.Direction, *short*100, *long*100)
	}
	return fmt.Sprintf("Spread: %s 20d Excess %+.2f%% exceeds the 5d Excess %+.2f%%.", target.Direction, *long*100, *short*100)
}

func anomalyConfluenceAnswer(directions []AnomalyDirectionGrade) string {
	target := focusDirection(directions)
	if target == nil {
		return "Unresolved — no anomaly observations."
	}
	if target.Direction == directionVolumeSpike {
		accumulation, nAcc := bucketMedian(target.Broker, brokerAccumulation, 20)
		distribution, nDist := bucketMedian(target.Broker, brokerDistribution, 20)
		present, nPresent := bucketMedian(target.Disclosure, disclosurePresent, 20)
		absent, nAbsent := bucketMedian(target.Disclosure, disclosureAbsent, 20)
		return fmt.Sprintf("volume_spike has no direction to confirm: 20d net-accumulation median %s (n=%d) vs net-distribution %s (n=%d); disclosure present %s (n=%d) vs absent %s (n=%d).",
			formatPercent(accumulation), nAcc, formatPercent(distribution), nDist,
			formatPercent(present), nPresent, formatPercent(absent), nAbsent)
	}
	confirmed, nConfirmed := bucketMedian(target.Broker, brokerConfirmed, 20)
	notConfirmed, nNot := bucketMedian(target.Broker, brokerNotConfirmed, 20)
	present, nPresent := bucketMedian(target.Disclosure, disclosurePresent, 20)
	absent, nAbsent := bucketMedian(target.Disclosure, disclosureAbsent, 20)
	return fmt.Sprintf("%s 20d median — broker-confirmed %s (n=%d) vs not-confirmed %s (n=%d); disclosure-present %s (n=%d) vs absent %s (n=%d).",
		target.Direction, formatPercent(confirmed), nConfirmed, formatPercent(notConfirmed), nNot,
		formatPercent(present), nPresent, formatPercent(absent), nAbsent)
}

// focusDirection picks the direction the confluence answer reads first: `up`
// when it has observations (the actionable bucket), else the first non-empty.
func focusDirection(directions []AnomalyDirectionGrade) *AnomalyDirectionGrade {
	for i := range directions {
		if directions[i].Direction == directionUp && directions[i].N > 0 {
			return &directions[i]
		}
	}
	for i := range directions {
		if directions[i].N > 0 {
			return &directions[i]
		}
	}
	return nil
}

// bucketMedian finds a bucket's survivor median and n at one horizon.
func bucketMedian(buckets []AnomalyBucket, state string, horizon int) (*float64, int) {
	for _, bucket := range buckets {
		if bucket.State != state {
			continue
		}
		for _, stats := range bucket.Horizons {
			if stats.Horizon == horizon {
				return stats.Survivor.Median, stats.Survivor.N
			}
		}
	}
	return nil, 0
}

// anomalyGradeNotes are the standing caveats every Anomaly Grade carries.
func anomalyGradeNotes(flowWindow int) []string {
	return []string{
		"Forward Returns are close-to-close over market-wide trading days; execution realism (fills, slippage, fees) is not modeled.",
		fmt.Sprintf("Broker Confirmation reads total net accumulation (listed buy − sell plus the unlisted tail) over the %d stored trading days ending the day BEFORE the anchor — the Flow Lead-in Window, deliberately not the screener's foreign-net column, whose window ends on the anchor.", flowWindow),
		fmt.Sprintf("Disclosure presence is a passed_filter Disclosure for the ticker announced within %d days before the anchor, matching get_market_anomalies.", pipeline.DisclosureFilterLookbackDays),
		"Unobserved broker rows are excluded from the confirmed/not split and reported as their own bucket, never zero-imputed.",
		"Anomaly observations are bounded by the pipeline's 90-day anomaly retention, not by stored prices.",
		"The Liquidity Baseline is the hard-filter stage on shipped screener defaults (traded on the anchor day, value floor, suspension window), before structural filters and ranking.",
	}
}

// resolveAnomalyFlowWindow resolves the Flow Lead-in Window length; it shares
// one rule with the Flow Column's window so neither grader can drift.
func resolveAnomalyFlowWindow(days int) (int, error) {
	return resolveFlowWindow(days)
}

// compile-time checks: the concrete repositories satisfy the Anomaly Grade seams.
var (
	_ MeasurementAnomalySource    = (*repository.AnomalyRepository)(nil)
	_ MeasurementDisclosureSource = (*repository.DisclosureRepository)(nil)
	_ MeasurementBrokerNetSource  = (*repository.BrokerStockSummaryRepository)(nil)
)
