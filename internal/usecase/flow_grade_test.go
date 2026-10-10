package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// ─── Flow Grade fakes ────────────────────────────────────────────────────────

// fakeForeignFlowSource is the Flow Grade's Flow-Column seam: it returns a
// canned window filtered to the tickers the grader asked for, so a test can
// assert the read scope without a broker database.
type fakeForeignFlowSource struct {
	window *repository.ForeignNetWindow
	err    error

	gotTickers     []string
	gotTo          time.Time
	gotTradingDays int
}

func (f *fakeForeignFlowSource) SumForeignNetByTickers(_ *sqlx.DB, tickers []string, to time.Time, tradingDays int) (*repository.ForeignNetWindow, error) {
	f.gotTickers = tickers
	f.gotTo = to
	f.gotTradingDays = tradingDays
	if f.err != nil {
		return nil, f.err
	}
	out := &repository.ForeignNetWindow{Rows: []repository.TickerForeignNet{}}
	if f.window != nil {
		out.From = f.window.From
		out.TradeDays = f.window.TradeDays
		want := make(map[string]bool, len(tickers))
		for _, ticker := range tickers {
			want[ticker] = true
		}
		for _, row := range f.window.Rows {
			if want[row.Ticker] {
				out.Rows = append(out.Rows, row)
			}
		}
	}
	return out, nil
}

// ─── Fixture ─────────────────────────────────────────────────────────────────

// flowGradeScenario is a hand-computed one-anchor fixture: nine hard-filter
// survivors, seven with an observed foreign net spanning positive, negative and
// a flat zero, and two unobserved. Every ticker carries the same return at every
// horizon, so the baseline and bucket medians are readable by eye.
//
//	ticker  return  net (bn)  covered days   sign
//	POS1     +20%     +50        20          positive
//	POS2     +10%     +30        15          positive
//	POS3       0%     +10        10          positive
//	NEG1     -10%     -10         8          negative
//	NEG2     -20%     -30        12          negative
//	NEG3      -5%     -50        20          negative
//	FLAT      +5%       0         5          flat
//	UNOBS1   +20%     none        —          unobserved
//	UNOBS2   -20%     none        —          unobserved
//
// Baseline (all nine) median = 0%. Observed = seven rows; rank terciles of the
// seven sorted nets (-50,-30,-10,0,+10,+30,+50) split [2,2,3].
func flowGradeScenario() (time.Time, *fakeMeasurementPrices, *fakeForeignFlowSource) {
	anchor := measurementAnchor()
	days := measurementCalendar(anchor, 55)
	targetDays := []time.Time{anchor, days[5], days[10], days[20]}

	returns := map[string]float64{
		"POS1": 0.20, "POS2": 0.10, "POS3": 0.00,
		"NEG1": -0.10, "NEG2": -0.20, "NEG3": -0.05,
		"FLAT": 0.05, "UNOBS1": 0.20, "UNOBS2": -0.20,
	}
	candidates := make([]repository.ScreenCandidate, 0, len(returns))
	prices := make(map[string][]entity.DailyPrice, len(returns))
	for ticker, ret := range returns {
		candidates = append(candidates, repository.ScreenCandidate{
			Ticker: ticker, Value: i64p(100_000_000_000), Close: f64p(100),
		})
		prices[ticker] = measurementSeries(ticker, targetDays, 100, 100*(1+ret), 100*(1+ret), 100*(1+ret))
	}

	priceFake := &fakeMeasurementPrices{calendar: days, candidates: candidates, prices: prices}
	flowFake := &fakeForeignFlowSource{window: &repository.ForeignNetWindow{
		TradeDays: 20,
		Rows: []repository.TickerForeignNet{
			{Ticker: "POS1", ForeignNet: 50_000_000_000, Days: 20},
			{Ticker: "POS2", ForeignNet: 30_000_000_000, Days: 15},
			{Ticker: "POS3", ForeignNet: 10_000_000_000, Days: 10},
			{Ticker: "NEG1", ForeignNet: -10_000_000_000, Days: 8},
			{Ticker: "NEG2", ForeignNet: -30_000_000_000, Days: 12},
			{Ticker: "NEG3", ForeignNet: -50_000_000_000, Days: 20},
			{Ticker: "FLAT", ForeignNet: 0, Days: 5},
			// UNOBS1 and UNOBS2 are absent: unobserved, not zero.
		},
	}}
	return anchor, priceFake, flowFake
}

// newFlowGradeEngine wires an engine with a fake price source and the foreign
// flow seam set.
func newFlowGradeEngine(prices MeasurementPriceSource, flow MeasurementForeignFlowSource) *MeasurementEngine {
	engine := NewMeasurementEngine(nil, logrus.New(), nil, prices, nil, "testrev")
	engine.ForeignFlow = flow
	return engine
}

func findSignBucket(t *testing.T, grade *FlowGradeReport, state string) FlowBucket {
	t.Helper()
	bucket := findFlowBucket(grade.SignBuckets, state)
	if bucket == nil {
		t.Fatalf("sign bucket %q not in %+v", state, grade.SignBuckets)
	}
	return *bucket
}

func findMagnitudeTercile(t *testing.T, grade *FlowGradeReport, state string) FlowTercile {
	t.Helper()
	tercile := findFlowTercile(grade.Magnitude, state)
	if tercile == nil {
		t.Fatalf("tercile %q not in %+v", state, grade.Magnitude)
	}
	return *tercile
}

func bucketExcessAt(t *testing.T, bucket FlowBucket, horizon int) *float64 {
	t.Helper()
	for _, stats := range bucket.Horizons {
		if stats.Horizon == horizon {
			return stats.Excess
		}
	}
	t.Fatalf("horizon %d not in bucket %q: %+v", horizon, bucket.State, bucket.Horizons)
	return nil
}

func tercileExcessAt(t *testing.T, tercile FlowTercile, horizon int) *float64 {
	t.Helper()
	for _, stats := range tercile.Horizons {
		if stats.Horizon == horizon {
			return stats.Excess
		}
	}
	t.Fatalf("horizon %d not in tercile %q: %+v", horizon, tercile.Tercile, tercile.Horizons)
	return nil
}

// ─── Sign bucket tests ───────────────────────────────────────────────────────

// TestGradeFlow_SignBuckets — the four sign populations, the observed/zero/
// unobserved boundaries, and each bucket's Excess against the full baseline.
func TestGradeFlow_SignBuckets(t *testing.T) {
	anchor, prices, flow := flowGradeScenario()
	grade, err := newFlowGradeEngine(prices, flow).GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if grade.Kind != flowGradeKind || grade.GitRevision != "testrev" {
		t.Fatalf("kind/revision = %q/%q", grade.Kind, grade.GitRevision)
	}
	if grade.Counts != (FlowCounts{Baseline: 9, Observed: 7, Unobserved: 2, DistinctObserved: 7}) {
		t.Fatalf("counts = %+v, want 9/7/2/7", grade.Counts)
	}
	if grade.FlowWindowDays != defaultFlowWindowDays || grade.FlowDaysInWindow != 20 {
		t.Fatalf("flow window = %d/%d, want %d/20", grade.FlowWindowDays, grade.FlowDaysInWindow, defaultFlowWindowDays)
	}
	if flow.gotTradingDays != defaultFlowWindowDays {
		t.Fatalf("read trading days = %d, want %d", flow.gotTradingDays, defaultFlowWindowDays)
	}
	// The Flow Column window ends ON the anchor, not the day before.
	if !flow.gotTo.Equal(anchor) {
		t.Fatalf("read to = %s, want the anchor %s", flow.gotTo.Format("2006-01-02"), anchor.Format("2006-01-02"))
	}

	positive := findSignBucket(t, grade, flowSignPositive)
	negative := findSignBucket(t, grade, flowSignNegative)
	flat := findSignBucket(t, grade, flowSignFlat)
	unobserved := findSignBucket(t, grade, flowSignUnobserved)

	if positive.N != 3 || negative.N != 3 || flat.N != 1 || unobserved.N != 2 {
		t.Fatalf("bucket sizes = +%d/-%d/0:%d/unobs:%d, want 3/3/1/2",
			positive.N, negative.N, flat.N, unobserved.N)
	}

	// Baseline median is 0% at every horizon.
	measurementApprox(t, "positive 20d excess", *bucketExcessAt(t, positive, 20), 0.10)
	measurementApprox(t, "negative 20d excess", *bucketExcessAt(t, negative, 20), -0.10)
	measurementApprox(t, "flat 20d excess", *bucketExcessAt(t, flat, 20), 0.05)
	measurementApprox(t, "unobserved 20d excess", *bucketExcessAt(t, unobserved, 20), 0.00)

	// Coverage is declared: the positive bucket's three rows cover 20/15/10 days.
	if positive.Coverage.DaysMedian == nil || *positive.Coverage.DaysMedian != 15 ||
		positive.Coverage.DaysMin != 10 || positive.Coverage.DaysMax != 20 {
		t.Fatalf("positive coverage = %+v, want 15 (10–20)", positive.Coverage)
	}
	if unobserved.Coverage.DaysMedian != nil {
		t.Fatalf("unobserved coverage = %+v, want no observed days", unobserved.Coverage)
	}

	// Every drill-down row keeps its foreign net and coverage.
	if len(grade.Rows) != 9 {
		t.Fatalf("rows = %d, want 9", len(grade.Rows))
	}
	var pos1 *TickerForwardReturns
	for i := range grade.Rows {
		if grade.Rows[i].Ticker == "POS1" {
			pos1 = &grade.Rows[i]
		}
	}
	if pos1 == nil || pos1.ForeignNet == nil || *pos1.ForeignNet != 50_000_000_000 {
		t.Fatalf("POS1 row = %+v, want foreign_net +50bn", pos1)
	}
	if got := pos1.Coverage[coverageForeignNet]; !got.Observed || got.Days != 20 {
		t.Fatalf("POS1 coverage = %+v, want observed over 20 days", got)
	}
}

// TestGradeFlow_MagnitudeTerciles — the signed, pooled tercile cut points and
// the monotone Excess they expose.
func TestGradeFlow_MagnitudeTerciles(t *testing.T) {
	anchor, prices, flow := flowGradeScenario()
	grade, err := newFlowGradeEngine(prices, flow).GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(grade.Magnitude) != 3 {
		t.Fatalf("terciles = %d, want 3", len(grade.Magnitude))
	}

	low := findMagnitudeTercile(t, grade, flowTercileLow)
	mid := findMagnitudeTercile(t, grade, flowTercileMid)
	high := findMagnitudeTercile(t, grade, flowTercileHigh)

	// Rank terciles of seven sorted nets split [2,2,3].
	if low.N != 2 || mid.N != 2 || high.N != 3 {
		t.Fatalf("tercile sizes = %d/%d/%d, want 2/2/3", low.N, mid.N, high.N)
	}
	if low.MinNet == nil || *low.MinNet != -50_000_000_000 || low.MaxNet == nil || *low.MaxNet != -30_000_000_000 {
		t.Fatalf("low net range = %v..%v, want -50bn..-30bn", low.MinNet, low.MaxNet)
	}
	if high.MinNet == nil || *high.MinNet != 10_000_000_000 || high.MaxNet == nil || *high.MaxNet != 50_000_000_000 {
		t.Fatalf("high net range = %v..%v, want 10bn..50bn", high.MinNet, high.MaxNet)
	}

	// Returns: low {-5%,-20%} median -12.5%; mid {-10%,+5%} median -2.5%;
	// high {0%,+10%,+20%} median +10%; baseline 0%.
	measurementApprox(t, "low 20d excess", *tercileExcessAt(t, low, 20), -0.125)
	measurementApprox(t, "mid 20d excess", *tercileExcessAt(t, mid, 20), -0.025)
	measurementApprox(t, "high 20d excess", *tercileExcessAt(t, high, 20), 0.10)
}

// TestGradeFlow_FlagsAndCriteria — the unobserved circularity flag, the small-n
// flags, and the flow-specific success criteria are all present.
func TestGradeFlow_FlagsAndCriteria(t *testing.T) {
	anchor, prices, flow := flowGradeScenario()
	grade, err := newFlowGradeEngine(prices, flow).GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	flags := strings.Join(grade.Flags, "\n")
	if !strings.Contains(flags, "circular-baseline") {
		t.Fatalf("flags missing the unobserved circularity note:\n%s", flags)
	}
	if !strings.Contains(flags, "unobserved: 2 of 9") {
		t.Fatalf("flags missing the unobserved count:\n%s", flags)
	}
	if !strings.Contains(flags, "small-n: positive") {
		t.Fatalf("flags missing the positive small-n warning:\n%s", flags)
	}
	// The unobserved slice is a minority here, so no sparse-coverage flag.
	if strings.Contains(flags, "sparse-coverage") {
		t.Fatalf("unexpected sparse-coverage flag:\n%s", flags)
	}

	if len(grade.SuccessCriteria) != 3 {
		t.Fatalf("success criteria = %d, want 3", len(grade.SuccessCriteria))
	}
	if !strings.Contains(grade.SuccessCriteria[0].Answer, "Yes") {
		t.Fatalf("excess answer = %q, want a Yes", grade.SuccessCriteria[0].Answer)
	}
	if !strings.Contains(grade.SuccessCriteria[1].Answer, "Front-loaded") {
		t.Fatalf("front-load answer = %q, want Front-loaded (5d == 20d)", grade.SuccessCriteria[1].Answer)
	}
	if !strings.Contains(grade.SuccessCriteria[2].Answer, "high tercile") {
		t.Fatalf("magnitude answer = %q, want the tercile contrast", grade.SuccessCriteria[2].Answer)
	}
}

// TestGradeFlow_PoolsAndSkips — a data-gap anchor is skipped and recorded, and a
// holdout violation is a hard error, not a skip.
func TestGradeFlow_PoolsAndSkips(t *testing.T) {
	anchor, prices, flow := flowGradeScenario()
	engine := newFlowGradeEngine(prices, flow)

	// The fixture calendar is anchor + 55 days; a second gradable anchor is the
	// first date that still has 20 post-anchor days (day 30 of 55).
	second := prices.calendar[30]
	grade, err := engine.GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor, second}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(grade.ReplayDates) != 2 || grade.Counts.Baseline != 18 || grade.Counts.Observed != 14 {
		t.Fatalf("pooled counts = %+v over %v, want baseline 18 / observed 14 over two anchors", grade.Counts, grade.ReplayDates)
	}
	if len(grade.PerDate) != 2 {
		t.Fatalf("per-date blocks = %d, want 2", len(grade.PerDate))
	}

	seal := anchor.AddDate(0, 0, 1)
	if _, err := engine.GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor}, HoldoutStart: &seal}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("pre-seal anchor err = %v, want ErrInvalidArgument", err)
	}
}

// TestGradeFlow_Guards — the grader requires its source and validates the flow
// window rather than silently truncating it.
func TestGradeFlow_Guards(t *testing.T) {
	anchor, prices, _ := flowGradeScenario()

	if _, err := newFlowGradeEngine(prices, nil).GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing source err = %v, want ErrInvalidArgument", err)
	}
	if _, err := newFlowGradeEngine(prices, &fakeForeignFlowSource{}).GradeFlow(context.Background(), FlowGradeOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing anchor err = %v, want ErrInvalidArgument", err)
	}
	if _, err := newFlowGradeEngine(prices, &fakeForeignFlowSource{}).GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor}, FlowWindowDays: 61}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("over-long flow window err = %v, want ErrInvalidArgument", err)
	}
}
