package usecase

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// ─── Fakes at the engine's two seams ─────────────────────────────────────────

// fakeMeasurementScreener is the Replay seam: it returns a canned Screen
// response and records the request, so tests assert what the engine asked the
// funnel for without a database.
type fakeMeasurementScreener struct {
	response *ScreenStocksResponse
	err      error

	called bool
	gotReq ScreenStocksRequest
}

func (f *fakeMeasurementScreener) ScreenAllSurvivors(_ context.Context, req ScreenStocksRequest) (*ScreenStocksResponse, error) {
	f.called = true
	f.gotReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}

// fakeMeasurementPrices is the read seam: the hard-filter candidates (the
// Liquidity Baseline), the market calendar, and the batched forward window.
type fakeMeasurementPrices struct {
	calendar   []time.Time
	candidates []repository.ScreenCandidate
	prices     map[string][]entity.DailyPrice

	calendarErr   error
	candidatesErr error
	pricesErr     error

	gotMinValue       int64
	gotSuspensionDays int
	gotTickers        []string
	gotTo             time.Time
	gotLimit          int
}

func (f *fakeMeasurementPrices) ScreenCandidates(_ *sqlx.DB, _ time.Time, minValue int64, suspensionDays int) ([]repository.ScreenCandidate, error) {
	f.gotMinValue = minValue
	f.gotSuspensionDays = suspensionDays
	if f.candidatesErr != nil {
		return nil, f.candidatesErr
	}
	return f.candidates, nil
}

func (f *fakeMeasurementPrices) TradingDaysInRangeAll(_ *sqlx.DB, _, _ time.Time) ([]time.Time, error) {
	if f.calendarErr != nil {
		return nil, f.calendarErr
	}
	return f.calendar, nil
}

func (f *fakeMeasurementPrices) FindByTickersUpTo(_ *sqlx.DB, tickers []string, to time.Time, limit int) ([]entity.DailyPrice, error) {
	f.gotTickers = tickers
	f.gotTo = to
	f.gotLimit = limit
	if f.pricesErr != nil {
		return nil, f.pricesErr
	}
	rows := make([]entity.DailyPrice, 0, len(tickers)*limit)
	for _, ticker := range tickers {
		filtered := make([]entity.DailyPrice, 0, len(f.prices[ticker]))
		for _, price := range f.prices[ticker] {
			if !price.TradingDay.After(to) {
				filtered = append(filtered, price)
			}
		}
		if len(filtered) > limit {
			filtered = filtered[len(filtered)-limit:]
		}
		rows = append(rows, filtered...)
	}
	return rows, nil
}

// ─── Fixtures ────────────────────────────────────────────────────────────────

// measurementAnchor is the standard Replay anchor for the engine tests.
func measurementAnchor() time.Time {
	return time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
}

// measurementCalendar builds anchor + `post` consecutive stored trading days.
func measurementCalendar(anchor time.Time, post int) []time.Time {
	days := make([]time.Time, 0, post+1)
	days = append(days, anchor)
	for i := 1; i <= post; i++ {
		days = append(days, anchor.AddDate(0, 0, i))
	}
	return days
}

// measurementSeries builds a ticker's stored closes at the named days.
func measurementSeries(ticker string, days []time.Time, closes ...float64) []entity.DailyPrice {
	rows := make([]entity.DailyPrice, 0, len(days))
	for i, day := range days {
		if i >= len(closes) {
			break
		}
		rows = append(rows, entity.DailyPrice{Ticker: ticker, TradingDay: day, Close: f64p(closes[i])})
	}
	return rows
}

// measurementScenario is the hand-computed fixture every engine test starts
// from. Survivors AAA and BBB are a subset of the four hard-filter survivors;
// BBCA is the only big cap with a stored forward window.
//
//	AAA 1000 -> 1100 (+10%) -> 1050 (+5%)  ->  900 (-10%)
//	BBB 2000 -> 1900 (-5%)  -> 2200 (+10%) -> 2200 (+10%)
//	CCC  500 ->  500 (0%)   ->  550 (+10%) ->  475 (-5%)
//	DDD 1000 ->  900 (-10%) ->  900 (-10%) -> 1000 (0%)
//	BBCA1000 -> 1050 (+5%)  -> 1050 (+5%)  -> 1100 (+10%)
func measurementScenario() (time.Time, []time.Time, *fakeMeasurementScreener, *fakeMeasurementPrices) {
	anchor := measurementAnchor()
	days := measurementCalendar(anchor, 25)
	targetDays := []time.Time{anchor, days[5], days[10], days[20]}

	screener := &fakeMeasurementScreener{response: &ScreenStocksResponse{
		AsOf:                 anchor.Format("2006-01-02"),
		Window:               defaultIndicatorWindow,
		MinValue:             defaultScreenerMinValue,
		SuspensionWindowDays: defaultSuspensionWindowDays,
		FlowWindowDays:       defaultFlowWindowDays,
		FlowDaysInWindow:     20,
		Indicators:           ScreenerDefaultIndicators,
		Filters:              ScreenerDefaultFilters,
		Sort:                 screenerSortValue,
		Order:                sortOrderDesc,
		Limit:                maxScreenerLimit,
		Funnel:               ScreenStocksFunnel{Universe: 900, AfterValue: 4, AfterStructure: 2},
		TotalMatches:         2,
		Count:                2,
		Rows: []ScreenStocksRow{
			{Ticker: "AAA", Value: i64p(300_000_000_000), Close: f64p(1000)},
			{Ticker: "BBB", Value: i64p(200_000_000_000), Close: f64p(2000)},
		},
	}}

	prices := &fakeMeasurementPrices{
		calendar: days,
		candidates: []repository.ScreenCandidate{
			{Ticker: "AAA", Value: i64p(300_000_000_000), Close: f64p(1000)},
			{Ticker: "BBB", Value: i64p(200_000_000_000), Close: f64p(2000)},
			{Ticker: "CCC", Value: i64p(150_000_000_000), Close: f64p(500)},
			{Ticker: "DDD", Value: i64p(100_000_000_000), Close: f64p(1000)},
		},
		prices: map[string][]entity.DailyPrice{
			"AAA":  measurementSeries("AAA", targetDays, 1000, 1100, 1050, 900),
			"BBB":  measurementSeries("BBB", targetDays, 2000, 1900, 2200, 2200),
			"CCC":  measurementSeries("CCC", targetDays, 500, 500, 550, 475),
			"DDD":  measurementSeries("DDD", targetDays, 1000, 900, 900, 1000),
			"BBCA": measurementSeries("BBCA", targetDays, 1000, 1050, 1050, 1100),
		},
	}
	return anchor, days, screener, prices
}

// newMeasurementEngine wires the engine with the standard fake pair and the
// built-in filter-set registry (nil Filters).
func newMeasurementEngine(screener MeasurementScreener, prices MeasurementPriceSource) *MeasurementEngine {
	return NewMeasurementEngine(nil, logrus.New(), screener, prices, nil, "testrev")
}

// measurementApprox asserts two floats are equal within a small tolerance.
func measurementApprox(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

// measurementHorizon finds a horizon's stats.
func measurementHorizon(t *testing.T, report *MeasurementReport, horizon int) HorizonStats {
	t.Helper()
	for _, stats := range report.Horizons {
		if stats.Horizon == horizon {
			return stats
		}
	}
	t.Fatalf("horizon %d not in report: %+v", horizon, report.Horizons)
	return HorizonStats{}
}

// ─── Tests ───────────────────────────────────────────────────────────────────

// TestMeasurementEngine_HandComputedStats pins every number the engine reports:
// survivor/baseline medians, hit rates, n, and the Excess between them.
func TestMeasurementEngine_HandComputedStats(t *testing.T) {
	anchor, _, screener, prices := measurementScenario()
	engine := newMeasurementEngine(screener, prices)

	report, err := engine.Measure(context.Background(), MeasurementOptions{AsOf: anchor})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if report.AsOf != "2026-06-01" || report.FilterSet != "default" {
		t.Fatalf("as_of/filter = %s/%s, want 2026-06-01/default", report.AsOf, report.FilterSet)
	}
	if report.GitRevision != "testrev" {
		t.Fatalf("git revision = %s, want testrev", report.GitRevision)
	}
	if report.Counts != (MeasurementCounts{Universe: 900, Baseline: 4, Survivors: 2, BigCap: 8}) {
		t.Fatalf("counts = %+v, want 900/4/2/8", report.Counts)
	}

	// The screener is asked for the anchor with the full uncapped row budget.
	if !screener.called {
		t.Fatal("screener was not called")
	}
	if screener.gotReq.AsOf == nil || !screener.gotReq.AsOf.Equal(anchor) {
		t.Fatalf("replay anchor = %v, want %s", screener.gotReq.AsOf, anchor)
	}
	if screener.gotReq.Limit != maxScreenerLimit {
		t.Fatalf("replay limit = %d, want %d", screener.gotReq.Limit, maxScreenerLimit)
	}

	// The batched read covers every population at once, up to the 20d target.
	wantRead := strings.Join(append([]string{"AAA", "BBB", "CCC", "DDD"}, BigCapBasket...), ",")
	if strings.Join(prices.gotTickers, ",") != wantRead {
		t.Fatalf("forward read for %v, want baseline ∪ survivors ∪ big caps (%s)", prices.gotTickers, wantRead)
	}
	if want := anchor.AddDate(0, 0, 20); !prices.gotTo.Equal(want) {
		t.Fatalf("forward read to = %s, want the 20d target %s", prices.gotTo, want)
	}
	if prices.gotLimit != 21 {
		t.Fatalf("forward read limit = %d, want 21 (anchor..target20)", prices.gotLimit)
	}

	five := measurementHorizon(t, report, 5)
	measurementApprox(t, "5d survivor median", *five.Survivor.Median, 0.025) // (0.10 + -0.05)/2
	measurementApprox(t, "5d survivor hit rate", *five.Survivor.HitRate, 0.5)
	if five.Survivor.N != 2 || five.Survivor.Unresolved != 0 {
		t.Fatalf("5d survivor n/unresolved = %d/%d, want 2/0", five.Survivor.N, five.Survivor.Unresolved)
	}
	measurementApprox(t, "5d baseline median", *five.Baseline.Median, -0.025) // (-0.05 + 0)/2
	if five.Baseline.N != 4 {
		t.Fatalf("5d baseline n = %d, want 4", five.Baseline.N)
	}
	measurementApprox(t, "5d excess", *five.Excess, 0.05)

	ten := measurementHorizon(t, report, 10)
	measurementApprox(t, "10d survivor median", *ten.Survivor.Median, 0.075)
	measurementApprox(t, "10d baseline median", *ten.Baseline.Median, 0.075)
	measurementApprox(t, "10d excess", *ten.Excess, 0)

	twenty := measurementHorizon(t, report, 20)
	measurementApprox(t, "20d survivor median", *twenty.Survivor.Median, 0)
	measurementApprox(t, "20d baseline median", *twenty.Baseline.Median, -0.025)
	measurementApprox(t, "20d excess", *twenty.Excess, 0.025)

	// Big-Cap Basket: only BBCA has a stored window, so n=1 and the rest are
	// unresolved — a missing name is not a zero return.
	measurementApprox(t, "5d big cap median", *twenty.BigCap.Median, 0.10)
	if twenty.BigCap.N != 1 || twenty.BigCap.Unresolved != len(BigCapBasket)-1 {
		t.Fatalf("20d big cap n/unresolved = %d/%d, want 1/%d",
			twenty.BigCap.N, twenty.BigCap.Unresolved, len(BigCapBasket)-1)
	}

	// The single-anchor report is small-n by construction, at both the date and
	// the population level.
	if !strings.Contains(strings.Join(report.Flags, "\n"), "1 replay date(s) < 3") {
		t.Fatalf("flags missing the replay-date floor: %v", report.Flags)
	}
	if !strings.Contains(strings.Join(report.Flags, "\n"), "survivor n=2 < 30 at 5d") {
		t.Fatalf("flags missing the survivor small-n warning: %v", report.Flags)
	}
}

// TestMeasurementEngine_PassesFilterSetParams — a named set's parameters reach
// the Replay request unchanged (the engine only injects the anchor and the row
// budget), so a report graded a set is reproducible from the registry entry.
func TestMeasurementEngine_PassesFilterSetParams(t *testing.T) {
	anchor, _, screener, prices := measurementScenario()
	engine := newMeasurementEngine(screener, prices)

	empty := []ScreenStocksFilter{}
	set := MeasurementFilterSet{
		Name:        "wide",
		Description: "test set",
		Params: ScreenStocksRequest{
			Window:               120,
			MinValue:             i64p(7_000_000_000),
			SuspensionWindowDays: 3,
			FlowWindowDays:       10,
			Indicators:           []string{"rsi:14"},
			Filters:              &empty,
			Sort:                 "rsi:14",
			Order:                sortOrderAsc,
		},
	}

	report, err := engine.replay(context.Background(), set, MeasurementOptions{AsOf: anchor})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := screener.gotReq
	if got.Window != 120 || got.MinValue == nil || *got.MinValue != 7_000_000_000 ||
		got.SuspensionWindowDays != 3 || got.FlowWindowDays != 10 ||
		got.Sort != "rsi:14" || got.Order != sortOrderAsc {
		t.Fatalf("replay request = %+v, want the named set's params", got)
	}
	if got.Filters == nil || len(*got.Filters) != 0 {
		t.Fatalf("replay filters = %v, want the caller's explicit empty list", got.Filters)
	}
	if report.FilterSet != "wide" || report.FilterSetDescription != "test set" {
		t.Fatalf("report filter set = %q/%q, want wide/test set", report.FilterSet, report.FilterSetDescription)
	}
}

// TestMeasurementEngine_BaselineIsHardStageSurvivors — the Liquidity Baseline
// is the hard-filter stage (ScreenCandidates) with the Screen's own resolved
// floor and suspension window, not the structural survivors.
func TestMeasurementEngine_BaselineIsHardStageSurvivors(t *testing.T) {
	anchor, _, screener, prices := measurementScenario()
	customValue := int64(2_000_000_000)
	screener.response.MinValue = customValue
	screener.response.SuspensionWindowDays = 3
	engine := newMeasurementEngine(screener, prices)

	report, err := engine.Measure(context.Background(), MeasurementOptions{AsOf: anchor})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if prices.gotMinValue != customValue || prices.gotSuspensionDays != 3 {
		t.Fatalf("baseline read floor/window = %d/%d, want %d/3",
			prices.gotMinValue, prices.gotSuspensionDays, customValue)
	}
	if report.Counts.Baseline != 4 || report.Counts.Survivors != 2 {
		t.Fatalf("baseline/survivors = %d/%d, want 4/2 (baseline is the pre-filter stage)",
			report.Counts.Baseline, report.Counts.Survivors)
	}
	// CCC and DDD cleared the hard stage but not the structural filters, so they
	// carry baseline returns and no survivor returns.
	if report.Funnel.AfterValue != 4 || report.Funnel.AfterStructure != 2 {
		t.Fatalf("funnel = %+v, want 4 hard survivors / 2 structural survivors", report.Funnel)
	}
}

// TestMeasurementEngine_HoldoutBoundary — the seal is inclusive: an anchor equal
// to --holdout-start is inside the sealed window, and one day earlier is a
// structured error, not a silent tuning run.
func TestMeasurementEngine_HoldoutBoundary(t *testing.T) {
	anchor, _, screener, prices := measurementScenario()
	engine := newMeasurementEngine(screener, prices)

	seal := anchor
	report, err := engine.Measure(context.Background(), MeasurementOptions{AsOf: anchor, HoldoutStart: &seal})
	if err != nil {
		t.Fatalf("anchor on the seal must be allowed: %v", err)
	}
	if !report.SealedWindow || report.HoldoutStart != "2026-06-01" {
		t.Fatalf("sealed window = %t / %s, want true / 2026-06-01", report.SealedWindow, report.HoldoutStart)
	}

	before := anchor.AddDate(0, 0, 1)
	_, err = engine.Measure(context.Background(), MeasurementOptions{AsOf: anchor, HoldoutStart: &before})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("anchor before the seal: err = %v, want ErrInvalidArgument", err)
	}
}

// TestMeasurementEngine_AnchorTooRecent — an anchor without 20 post-anchor
// Trading Days is rejected before any screen read, because no Forward Return
// could be computed.
func TestMeasurementEngine_AnchorTooRecent(t *testing.T) {
	anchor, _, screener, prices := measurementScenario()
	prices.calendar = measurementCalendar(anchor, 10)
	engine := newMeasurementEngine(screener, prices)

	_, err := engine.Measure(context.Background(), MeasurementOptions{AsOf: anchor})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
	if screener.called {
		t.Fatal("screener was called for an anchor that cannot be graded")
	}
}

// TestMeasurementEngine_UnknownFilterSet — a typo is a structured error naming
// the known sets, never a silent fallback to the default.
func TestMeasurementEngine_UnknownFilterSet(t *testing.T) {
	_, _, screener, prices := measurementScenario()
	engine := newMeasurementEngine(screener, prices)

	_, err := engine.Measure(context.Background(), MeasurementOptions{AsOf: measurementAnchor(), FilterSet: "nope"})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
	if screener.called {
		t.Fatal("screener was called for an unknown filter set")
	}
}

// TestMeasurementEngine_UnresolvedExcluded — a ticker with no stored close on a
// target day is unresolved: excluded from the median and counted, never
// zero-imputed.
func TestMeasurementEngine_UnresolvedExcluded(t *testing.T) {
	anchor, days, screener, prices := measurementScenario()
	// BBB has no 10d close.
	prices.prices["BBB"] = measurementSeries("BBB", []time.Time{days[0], days[5], days[20]}, 2000, 1900, 2200)

	engine := newMeasurementEngine(screener, prices)
	report, err := engine.Measure(context.Background(), MeasurementOptions{AsOf: anchor})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ten := measurementHorizon(t, report, 10)
	if ten.Survivor.N != 1 || ten.Survivor.Unresolved != 1 {
		t.Fatalf("10d survivor n/unresolved = %d/%d, want 1/1", ten.Survivor.N, ten.Survivor.Unresolved)
	}
	measurementApprox(t, "10d survivor median", *ten.Survivor.Median, 0.05) // AAA only
	if !strings.Contains(strings.Join(report.Flags, "\n"), "excluded: 1 survivor(s) with no stored close on the 10d target") {
		t.Fatalf("flags missing the unresolved note: %v", report.Flags)
	}
}

// TestMeasurementEngine_ReadErrorsPropagate — a database failure is an error,
// never an empty report that would read as "no edge".
func TestMeasurementEngine_ReadErrorsPropagate(t *testing.T) {
	boom := errors.New("db exploded")
	cases := []struct {
		name   string
		mutate func(*fakeMeasurementPrices)
	}{
		{name: "calendar", mutate: func(p *fakeMeasurementPrices) { p.calendarErr = boom }},
		{name: "baseline", mutate: func(p *fakeMeasurementPrices) { p.candidatesErr = boom }},
		{name: "forward prices", mutate: func(p *fakeMeasurementPrices) { p.pricesErr = boom }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			anchor, _, screener, prices := measurementScenario()
			tc.mutate(prices)
			engine := newMeasurementEngine(screener, prices)
			_, err := engine.Measure(context.Background(), MeasurementOptions{AsOf: anchor})
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want the read failure", err)
			}
		})
	}
}

// TestMeasurementEngine_ScreenerErrorPropagates — a funnel failure is surfaced,
// not swallowed into an empty shortlist.
func TestMeasurementEngine_ScreenerErrorPropagates(t *testing.T) {
	anchor, _, screener, prices := measurementScenario()
	boom := errors.New("screen exploded")
	screener.err = boom
	engine := newMeasurementEngine(screener, prices)

	_, err := engine.Measure(context.Background(), MeasurementOptions{AsOf: anchor})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the screener failure", err)
	}
}

// TestMeasurementFlags_Boundaries — the small-n floor is flagged below the bar
// and silent at it, for both populations and the replay-date count.
func TestMeasurementFlags_Boundaries(t *testing.T) {
	atBar := []HorizonStats{{Horizon: 5, Survivor: ReturnStats{N: smallNSurvivors}, Baseline: ReturnStats{N: smallNSurvivors}}}
	if flags := measurementFlags(atBar, smallNReplayDates); len(flags) != 0 {
		t.Fatalf("flags at the bars = %v, want none", flags)
	}

	below := []HorizonStats{{Horizon: 5, Survivor: ReturnStats{N: smallNSurvivors - 1}, Baseline: ReturnStats{N: smallNSurvivors - 1}}}
	flags := strings.Join(measurementFlags(below, smallNReplayDates-1), "\n")
	if !strings.Contains(flags, "survivor n=29 < 30") || !strings.Contains(flags, "liquidity baseline n=29 < 30") {
		t.Fatalf("flags below the bars = %v, want both population warnings", flags)
	}
	if !strings.Contains(flags, "2 replay date(s) < 3") {
		t.Fatalf("flags = %v, want the replay-date warning", flags)
	}
}

// TestMeasurementSuccessCriteria — the three standing questions are answered
// from the report's own numbers, and "no edge" is stated plainly.
func TestMeasurementSuccessCriteria(t *testing.T) {
	posFive, posTwenty := 0.04, 0.01
	criteria := measurementSuccessCriteria([]HorizonStats{
		{Horizon: 5, Excess: &posFive},
		{Horizon: 10},
		{Horizon: 20, Excess: &posTwenty},
	})
	if len(criteria) != 3 {
		t.Fatalf("criteria = %d, want the spec's three questions", len(criteria))
	}
	if !strings.Contains(criteria[0].Answer, "positive Excess at 5d") {
		t.Fatalf("question 1 answer = %q, want the positive horizons", criteria[0].Answer)
	}
	if !strings.Contains(criteria[1].Answer, "Front-loaded") {
		t.Fatalf("question 2 answer = %q, want front-loaded for a larger 5d Excess", criteria[1].Answer)
	}
	if !strings.Contains(criteria[2].Answer, "Not measured") {
		t.Fatalf("question 3 answer = %q, want the out-of-scope note", criteria[2].Answer)
	}

	negative := -0.02
	noEdge := measurementSuccessCriteria([]HorizonStats{{Horizon: 5, Excess: &negative}})
	if !strings.Contains(noEdge[0].Answer, "No —") {
		t.Fatalf("no-edge answer = %q, want a plain negative", noEdge[0].Answer)
	}

	spreadShort, spreadLong := 0.01, 0.05
	spread := measurementSuccessCriteria([]HorizonStats{
		{Horizon: 5, Excess: &spreadShort},
		{Horizon: 20, Excess: &spreadLong},
	})
	if !strings.Contains(spread[1].Answer, "Spread") {
		t.Fatalf("spread answer = %q, want spread for a larger 20d Excess", spread[1].Answer)
	}
}

// TestScreenAllSurvivors_NoCap — the Replay path returns every survivor, while
// ScreenStocks still caps: the two share one funnel, so only the cap differs.
func TestScreenAllSurvivors_NoCap(t *testing.T) {
	candidates := make([]repository.ScreenCandidate, 0, 60)
	prices := make(map[string][]entity.DailyPrice, 60)
	for i := 0; i < 60; i++ {
		ticker := string(rune('A'+i/26)) + string(rune('A'+i%26))
		candidates = append(candidates, screenCandidate(ticker, int64(60-i)*1_000_000_000))
		prices[ticker] = screenSeries(ticker, 250)
	}
	source := &fakeScreenerSource{universe: 900, candidates: candidates, prices: prices}
	uc := newScreenTestUseCase(source)

	capped, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Filters: noFilters()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capped.Count != defaultScreenerLimit || capped.TotalMatches != 60 {
		t.Fatalf("capped run = count %d / total %d, want %d/60", capped.Count, capped.TotalMatches, defaultScreenerLimit)
	}

	all, err := uc.ScreenAllSurvivors(context.Background(), ScreenStocksRequest{Filters: noFilters()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if all.Count != 60 || all.TotalMatches != 60 {
		t.Fatalf("uncapped run = count %d / total %d, want 60/60", all.Count, all.TotalMatches)
	}
}
