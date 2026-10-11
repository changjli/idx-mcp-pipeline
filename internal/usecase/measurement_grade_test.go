package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// ─── Grade fixture ───────────────────────────────────────────────────────────

// measurementSeriesAt builds a ticker's stored closes at the named day offsets,
// in ascending offset order.
func measurementSeriesAt(ticker string, days []time.Time, closes map[int]float64) []entity.DailyPrice {
	offsets := make([]int, 0, len(closes))
	for offset := range closes {
		offsets = append(offsets, offset)
	}
	sort.Ints(offsets)
	rows := make([]entity.DailyPrice, 0, len(offsets))
	for _, offset := range offsets {
		rows = append(rows, entity.DailyPrice{Ticker: ticker, TradingDay: days[offset], Close: f64p(closes[offset])})
	}
	return rows
}

// measurementGradeScenario builds a two-anchor fixture with hand-picked closes.
// day0 and day30 are the anchors (30 trading days apart, so not overlapping);
// day5 exists only for the overlap test. Survivors are AAA+BBB at day0 and
// AAA+CCC at day30, so the pooled survivor set is four observations over three
// distinct names.
//
//	offset:  0     5     10    20    30    35     40    50
//	AAA     100   110   120   130   200   210    200   220
//	BBB     200   180   200   160   150   142.5  135   135
//	CCC      50    50    55    45    60    66     72    84
//	DDD     300   315   315   315   400   400    420   440
func measurementGradeScenario() ([]time.Time, *fakeMeasurementScreener, *fakeMeasurementPrices) {
	days := measurementCalendar(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), 55)
	day := func(offset int) string { return days[offset].Format("2006-01-02") }

	closes := map[string]map[int]float64{
		"AAA": {0: 100, 5: 110, 10: 120, 20: 130, 30: 200, 35: 210, 40: 200, 50: 220},
		"BBB": {0: 200, 5: 180, 10: 200, 20: 160, 30: 150, 35: 142.5, 40: 135, 50: 135},
		"CCC": {0: 50, 5: 50, 10: 55, 20: 45, 30: 60, 35: 66, 40: 72, 50: 84},
		"DDD": {0: 300, 5: 315, 10: 315, 20: 315, 30: 400, 35: 400, 40: 420, 50: 440},
	}
	prices := make(map[string][]entity.DailyPrice, len(closes))
	for ticker, byOffset := range closes {
		prices[ticker] = measurementSeriesAt(ticker, days, byOffset)
	}

	response := func(anchorOffset int, rows ...ScreenStocksRow) *ScreenStocksResponse {
		return &ScreenStocksResponse{
			AsOf:                 day(anchorOffset),
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
			Funnel:               ScreenStocksFunnel{Universe: 900, AfterValue: 4, AfterStructure: len(rows)},
			TotalMatches:         len(rows),
			Count:                len(rows),
			Rows:                 rows,
		}
	}

	// The day-0 AAA row carries coverage flags, so a test can prove they reach
	// the pooled drill-down.
	screener := &fakeMeasurementScreener{responsesByAnchor: map[string]*ScreenStocksResponse{
		day(0): response(0,
			ScreenStocksRow{
				Ticker:     "AAA",
				Close:      f64p(100),
				ForeignNet: i64p(5_000_000_000),
				Coverage:   map[string]ScreenColumnCoverage{"foreign_net": {Observed: true, Days: 20}},
			},
			ScreenStocksRow{Ticker: "BBB", Close: f64p(200)},
		),
		day(5):  response(5, ScreenStocksRow{Ticker: "AAA", Close: f64p(110)}, ScreenStocksRow{Ticker: "BBB", Close: f64p(180)}),
		day(30): response(30, ScreenStocksRow{Ticker: "AAA", Close: f64p(200)}, ScreenStocksRow{Ticker: "CCC", Close: f64p(60)}),
	}}

	candidate := func(ticker string, close float64) repository.ScreenCandidate {
		return repository.ScreenCandidate{Ticker: ticker, Value: i64p(1_000_000_000_000), Close: f64p(close)}
	}
	candidates := func(aaa, bbb, ccc, ddd float64) []repository.ScreenCandidate {
		return []repository.ScreenCandidate{candidate("AAA", aaa), candidate("BBB", bbb), candidate("CCC", ccc), candidate("DDD", ddd)}
	}
	priceFake := &fakeMeasurementPrices{
		calendar: days,
		candidatesByAnchor: map[string][]repository.ScreenCandidate{
			day(0):  candidates(100, 200, 50, 300),
			day(5):  candidates(110, 180, 50, 315),
			day(30): candidates(200, 150, 60, 400),
		},
		prices: prices,
	}
	return days, screener, priceFake
}

// newGradeEngine wires the engine with a fake pair and an optional registry.
func newGradeEngine(screener MeasurementScreener, prices MeasurementPriceSource, registry *MeasurementFilterRegistry) *MeasurementEngine {
	return NewMeasurementEngine(nil, logrus.New(), screener, prices, registry, "testrev")
}

// newTestMeasurementRegistry adds named sets to the built-in default.
func newTestMeasurementRegistry(sets ...MeasurementFilterSet) *MeasurementFilterRegistry {
	registry := newBuiltinMeasurementFilterRegistry()
	for _, set := range sets {
		registry.sets[strings.ToLower(set.Name)] = set
	}
	return registry
}

// measurementComparisonGrade builds a one-anchor, two-set grade: "momentum"
// selects AAA+BBB and "rsi" selects CCC+DDD, over the same hard-filter floor.
func measurementComparisonGrade(t *testing.T) *ScreenGradeReport {
	t.Helper()
	days, _, prices := measurementGradeScenario()

	base := ScreenStocksRequest{
		Window:               defaultIndicatorWindow,
		MinValue:             i64p(defaultScreenerMinValue),
		SuspensionWindowDays: defaultSuspensionWindowDays,
		FlowWindowDays:       defaultFlowWindowDays,
		Indicators:           ScreenerDefaultIndicators,
		Sort:                 screenerSortValue,
		Order:                sortOrderDesc,
	}
	momentum := MeasurementFilterSet{Name: "momentum", Params: base}
	rsiSet := MeasurementFilterSet{Name: "rsi", Params: base}
	rsiSet.Params.Sort = "rsi:14"
	registry := newTestMeasurementRegistry(momentum, rsiSet)

	screener := &fakeMeasurementScreener{handler: func(req ScreenStocksRequest) (*ScreenStocksResponse, error) {
		response := &ScreenStocksResponse{
			AsOf:                 req.AsOf.Format("2006-01-02"),
			Window:               defaultIndicatorWindow,
			MinValue:             defaultScreenerMinValue,
			SuspensionWindowDays: defaultSuspensionWindowDays,
			FlowWindowDays:       defaultFlowWindowDays,
			FlowDaysInWindow:     20,
			Indicators:           ScreenerDefaultIndicators,
			Sort:                 req.Sort,
			Order:                sortOrderDesc,
			Limit:                maxScreenerLimit,
			Funnel:               ScreenStocksFunnel{Universe: 900, AfterValue: 4, AfterStructure: 2},
			TotalMatches:         2,
			Count:                2,
		}
		if req.Sort == "rsi:14" {
			response.Rows = []ScreenStocksRow{{Ticker: "CCC", Close: f64p(50)}, {Ticker: "DDD", Close: f64p(300)}}
		} else {
			response.Rows = []ScreenStocksRow{{Ticker: "AAA", Close: f64p(100)}, {Ticker: "BBB", Close: f64p(200)}}
		}
		return response, nil
	}}

	grade, err := newGradeEngine(screener, prices, registry).GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:   []time.Time{days[0]},
		FilterSet: "momentum",
		Compare:   "rsi",
	})
	if err != nil {
		t.Fatalf("build comparison grade: %v", err)
	}
	return grade
}

// ─── Grade tests ─────────────────────────────────────────────────────────────

// TestGradeScreen_PoolsAnchors pins the pooled numbers across two anchors:
// pooled medians, hit rate, n, Excess, distinct tickers, summed counts, and the
// retained per-date blocks. Anchors are passed out of order and duplicated to
// prove dedupe-and-sort.
func TestGradeScreen_PoolsAnchors(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	engine := newGradeEngine(screener, prices, nil)

	grade, err := engine.GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:   []time.Time{days[30], days[0], days[30]},
		FilterSet: "default",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if grade.Kind != screenGradeKind {
		t.Fatalf("kind = %q, want %q", grade.Kind, screenGradeKind)
	}
	if got := strings.Join(grade.ReplayDates, ","); got != days[0].Format("2006-01-02")+","+days[30].Format("2006-01-02") {
		t.Fatalf("replay dates = %q, want the two anchors deduped and sorted", got)
	}
	if grade.AsOfStart != days[0].Format("2006-01-02") || grade.AsOfEnd != days[30].Format("2006-01-02") {
		t.Fatalf("range = %s..%s, want day0..day30", grade.AsOfStart, grade.AsOfEnd)
	}
	if len(grade.SkippedAnchors) != 0 {
		t.Fatalf("skipped anchors = %+v, want none", grade.SkippedAnchors)
	}
	if grade.Comparison != nil {
		t.Fatalf("comparison = %+v, want nil without --compare", grade.Comparison)
	}
	if len(grade.FilterSets) != 1 {
		t.Fatalf("filter sets = %d, want 1", len(grade.FilterSets))
	}

	set := grade.FilterSets[0]
	if set.FilterSet != "default" || set.FilterSetDescription == "" {
		t.Fatalf("set identity = %q/%q, want the default set", set.FilterSet, set.FilterSetDescription)
	}
	if set.DistinctTickers != 3 {
		t.Fatalf("distinct tickers = %d, want 3 (AAA, BBB, CCC)", set.DistinctTickers)
	}
	if set.Counts != (MeasurementCounts{Universe: 1800, Baseline: 8, Survivors: 4, BigCap: 16}) {
		t.Fatalf("pooled counts = %+v, want 1800/8/4/16 (sums over both anchors)", set.Counts)
	}
	if set.Funnel != (ScreenStocksFunnel{Universe: 1800, AfterValue: 8, AfterStructure: 4}) {
		t.Fatalf("pooled funnel = %+v, want summed funnel counts", set.Funnel)
	}
	if len(set.PerDate) != 2 || set.PerDate[0].AsOf != days[0].Format("2006-01-02") {
		t.Fatalf("per-date blocks = %+v, want two in anchor order", set.PerDate)
	}
	if set.PerDate[0].Counts != (MeasurementCounts{Universe: 900, Baseline: 4, Survivors: 2, BigCap: 8}) {
		t.Fatalf("per-date counts = %+v, want the single-anchor funnel", set.PerDate[0].Counts)
	}

	// Every pooled survivor row carries its anchor date, and the coverage flags
	// survive the pool.
	if len(set.Survivors) != 4 {
		t.Fatalf("pooled survivors = %d, want 4", len(set.Survivors))
	}
	for _, row := range set.Survivors {
		if row.AsOf == "" {
			t.Fatalf("survivor row %s has no anchor date", row.Ticker)
		}
	}
	covered := survivorRow(set.Survivors, days[0].Format("2006-01-02"), "AAA")
	if covered == nil || covered.ForeignNet == nil || *covered.ForeignNet != 5_000_000_000 {
		t.Fatalf("day-0 AAA coverage = %+v, want foreign_net preserved", covered)
	}
	if flow, ok := covered.Coverage["foreign_net"]; !ok || !flow.Observed || flow.Days != 20 {
		t.Fatalf("day-0 AAA coverage map = %+v, want foreign_net observed over 20 days", covered.Coverage)
	}

	five := measurementHorizon(t, &set, 5)
	measurementApprox(t, "5d survivor median", *five.Survivor.Median, 0.075)
	measurementApprox(t, "5d survivor hit rate", *five.Survivor.HitRate, 0.75)
	if five.Survivor.N != 4 {
		t.Fatalf("5d survivor n = %d, want 4", five.Survivor.N)
	}
	measurementApprox(t, "5d baseline median", *five.Baseline.Median, 0.025)
	if five.Baseline.N != 8 {
		t.Fatalf("5d baseline n = %d, want 8", five.Baseline.N)
	}
	measurementApprox(t, "5d excess", *five.Excess, 0.05)

	ten := measurementHorizon(t, &set, 10)
	measurementApprox(t, "10d survivor median", *ten.Survivor.Median, 0.10)
	measurementApprox(t, "10d excess", *ten.Excess, 0.05)

	twenty := measurementHorizon(t, &set, 20)
	measurementApprox(t, "20d survivor median", *twenty.Survivor.Median, 0.20)
	measurementApprox(t, "20d excess", *twenty.Excess, 0.125)

	flags := strings.Join(set.Flags, "\n")
	if !strings.Contains(flags, "2 replay date(s) < 3") {
		t.Fatalf("flags missing the replay-date floor: %v", set.Flags)
	}
	if strings.Contains(flags, "overlap:") {
		t.Fatalf("anchors 30 trading days apart must not be flagged overlapping: %v", set.Flags)
	}
}

// TestGradeScreen_Comparison — two filter sets render side by side, and each
// horizon's Excess Delta is the difference of their Excess values.
func TestGradeScreen_Comparison(t *testing.T) {
	grade := measurementComparisonGrade(t)

	if grade.Comparison == nil {
		t.Fatal("comparison block missing")
	}
	if grade.Comparison.A != "momentum" || grade.Comparison.B != "rsi" {
		t.Fatalf("comparison = %s vs %s, want momentum vs rsi", grade.Comparison.A, grade.Comparison.B)
	}
	if !grade.Comparison.LikeForLike {
		t.Fatalf("like-for-like = false, want true (identical hard-filter params): %v", grade.Comparison.Notes)
	}
	if len(grade.FilterSets) != 2 {
		t.Fatalf("filter sets = %d, want both results in one report", len(grade.FilterSets))
	}

	// momentum Excess: 5d 0.000, 10d 0.025, 20d 0.075.
	// rsi      Excess: 5d 0.025, 10d 0.000, 20d 0.000.
	// Delta           5d -0.025, 10d 0.025, 20d 0.075.
	deltaAt := func(horizon int) *float64 {
		for _, delta := range grade.Comparison.Horizons {
			if delta.Horizon == horizon {
				return delta.ExcessDelta
			}
		}
		t.Fatalf("horizon %d missing from the comparison", horizon)
		return nil
	}
	measurementApprox(t, "5d excess delta", *deltaAt(5), -0.025)
	measurementApprox(t, "10d excess delta", *deltaAt(10), 0.025)
	measurementApprox(t, "20d excess delta", *deltaAt(20), 0.075)
}

// TestGradeScreen_NotLikeForLike — a differing hard-filter floor marks the
// comparison not-like-for-like rather than comparing across counterfactuals in
// silence.
func TestGradeScreen_NotLikeForLike(t *testing.T) {
	days, _, prices := measurementGradeScenario()

	base := ScreenStocksRequest{
		Window:               defaultIndicatorWindow,
		MinValue:             i64p(defaultScreenerMinValue),
		SuspensionWindowDays: defaultSuspensionWindowDays,
		FlowWindowDays:       defaultFlowWindowDays,
		Indicators:           ScreenerDefaultIndicators,
		Sort:                 screenerSortValue,
		Order:                sortOrderDesc,
	}
	a := MeasurementFilterSet{Name: "a", Params: base}
	b := MeasurementFilterSet{Name: "b", Params: base}
	b.Params = ScreenStocksRequest{
		Window:               defaultIndicatorWindow,
		MinValue:             i64p(50_000_000_000),
		SuspensionWindowDays: defaultSuspensionWindowDays,
		FlowWindowDays:       defaultFlowWindowDays,
		Indicators:           ScreenerDefaultIndicators,
		Sort:                 screenerSortValue,
		Order:                sortOrderDesc,
	}
	registry := newTestMeasurementRegistry(a, b)

	screener := &fakeMeasurementScreener{handler: func(req ScreenStocksRequest) (*ScreenStocksResponse, error) {
		minValue := defaultScreenerMinValue
		if req.MinValue != nil {
			minValue = *req.MinValue
		}
		return &ScreenStocksResponse{
			AsOf:                 req.AsOf.Format("2006-01-02"),
			Window:               defaultIndicatorWindow,
			MinValue:             minValue,
			SuspensionWindowDays: defaultSuspensionWindowDays,
			FlowWindowDays:       defaultFlowWindowDays,
			FlowDaysInWindow:     20,
			Indicators:           ScreenerDefaultIndicators,
			Sort:                 screenerSortValue,
			Order:                sortOrderDesc,
			Limit:                maxScreenerLimit,
			Funnel:               ScreenStocksFunnel{Universe: 900, AfterValue: 4, AfterStructure: 2},
			TotalMatches:         2,
			Count:                2,
			Rows:                 []ScreenStocksRow{{Ticker: "AAA", Close: f64p(100)}, {Ticker: "BBB", Close: f64p(200)}},
		}, nil
	}}

	grade, err := newGradeEngine(screener, prices, registry).GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors: []time.Time{days[0]}, FilterSet: "a", Compare: "b",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if grade.Comparison == nil {
		t.Fatal("comparison block missing")
	}
	if grade.Comparison.LikeForLike {
		t.Fatal("like-for-like = true, want false when min_value differs")
	}
	if len(grade.Comparison.Notes) == 0 {
		t.Fatal("not-like-for-like comparison carries no explanatory note")
	}
}

// TestGradeScreen_SkipsUnreplayableAnchors — an anchor with too little
// post-anchor history is skipped and recorded, never silently dropped, and the
// rest of the run proceeds.
func TestGradeScreen_SkipsUnreplayableAnchors(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	engine := newGradeEngine(screener, prices, nil)

	grade, err := engine.GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:   []time.Time{days[0], days[50], days[30]},
		FilterSet: "default",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(grade.SkippedAnchors) != 1 {
		t.Fatalf("skipped anchors = %+v, want exactly the too-recent one", grade.SkippedAnchors)
	}
	skipped := grade.SkippedAnchors[0]
	if skipped.AsOf != days[50].Format("2006-01-02") || !strings.Contains(skipped.Reason, "post-anchor trading days") {
		t.Fatalf("skipped anchor = %+v, want day50 with a post-anchor reason", skipped)
	}
	if got := strings.Join(grade.ReplayDates, ","); got != days[0].Format("2006-01-02")+","+days[30].Format("2006-01-02") {
		t.Fatalf("replay dates = %q, want only the gradable anchors", got)
	}
}

// TestGradeScreen_NoneGradable — if every anchor is ungradable the run fails;
// an empty report would read as "no edge".
func TestGradeScreen_NoneGradable(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	engine := newGradeEngine(screener, prices, nil)

	_, err := engine.GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:   []time.Time{days[50]},
		FilterSet: "default",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

// TestGradeScreen_HoldoutViolation — crossing the seal is a hard error for the
// whole run, not a skipped anchor.
func TestGradeScreen_HoldoutViolation(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	engine := newGradeEngine(screener, prices, nil)
	seal := days[10]

	_, err := engine.GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:      []time.Time{days[0], days[30]},
		FilterSet:    "default",
		HoldoutStart: &seal,
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument for an anchor before the seal", err)
	}
	if screener.called {
		t.Fatal("the screen ran despite a holdout violation")
	}
}

// TestGradeScreen_OverlapFlag — anchors closer than the longest horizon are
// flagged, because their forward windows share price paths.
func TestGradeScreen_OverlapFlag(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	engine := newGradeEngine(screener, prices, nil)

	grade, err := engine.GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:   []time.Time{days[0], days[5]},
		FilterSet: "default",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(strings.Join(grade.FilterSets[0].Flags, "\n"), "overlap:") {
		t.Fatalf("anchors 5 trading days apart must be flagged overlapping: %v", grade.FilterSets[0].Flags)
	}
}

// TestGradeScreen_CompareSameSet — comparing a set with itself is a mistake, not
// a zero-delta comparison.
func TestGradeScreen_CompareSameSet(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	engine := newGradeEngine(screener, prices, nil)

	_, err := engine.GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:   []time.Time{days[0]},
		FilterSet: "default",
		Compare:   "default",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument when comparing a set with itself", err)
	}
}

// ─── Grade report writer ─────────────────────────────────────────────────────

// TestScreenGradeReport_JSONShape — the top-level Grade artifact and its per-set
// blocks carry the fields a reader needs, and the new drill-down fields survive
// a round trip.
func TestScreenGradeReport_JSONShape(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	grade, err := newGradeEngine(screener, prices, nil).GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:   []time.Time{days[0], days[30]},
		FilterSet: "default",
	})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	payload, err := json.Marshal(grade)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal object: %v", err)
	}
	for _, key := range []string{"kind", "as_of_start", "as_of_end", "replay_dates", "skipped_anchors", "filter_sets"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("JSON missing key %q; keys: %v", key, keysOf(decoded))
		}
	}
	if _, ok := decoded["comparison"]; ok {
		t.Fatalf("single-set grade should omit comparison; keys: %v", keysOf(decoded))
	}

	var set map[string]json.RawMessage
	var sets []json.RawMessage
	if err := json.Unmarshal(decoded["filter_sets"], &sets); err != nil {
		t.Fatalf("unmarshal filter_sets: %v", err)
	}
	if err := json.Unmarshal(sets[0], &set); err != nil {
		t.Fatalf("unmarshal set block: %v", err)
	}
	for _, key := range []string{"as_of_start", "as_of_end", "distinct_tickers", "per_date", "survivors", "horizons"} {
		if _, ok := set[key]; !ok {
			t.Fatalf("set block missing key %q; keys: %v", key, keysOf(set))
		}
	}
}

// TestScreenGradeReport_Artifact — a single-anchor Grade renders <as-of>_<set>,
// a comparison renders <as-of>_<A>_vs_<B>, and both markdown summaries carry the
// per-date table, comparison table, and per-set sections.
func TestScreenGradeReport_Artifact(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	grade, err := newGradeEngine(screener, prices, nil).GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:   []time.Time{days[0]},
		FilterSet: "default",
	})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	if artifact.Base != "2026-06-01_default" {
		t.Fatalf("base = %s, want 2026-06-01_default", artifact.Base)
	}

	var decoded ScreenGradeReport
	if err := json.Unmarshal(artifact.JSON, &decoded); err != nil {
		t.Fatalf("reparse json: %v", err)
	}
	if decoded.Kind != screenGradeKind || decoded.GitRevision != "testrev" {
		t.Fatalf("round-tripped grade = %+v", decoded)
	}

	md := string(artifact.Markdown)
	for _, want := range []string{
		"# Screen grade — default @ 2026-06-01",
		"## Per-date breakdown",
		"## Filter sets",
		"# Measurement report — default @ 2026-06-01",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q\n---\n%s", want, md)
		}
	}

	// The comparison base name carries both set names.
	comparison := measurementComparisonGrade(t)
	cartifact, err := comparison.Artifact()
	if err != nil {
		t.Fatalf("comparison Artifact: %v", err)
	}
	if cartifact.Base != "2026-06-01_momentum_vs_rsi" {
		t.Fatalf("comparison base = %s, want 2026-06-01_momentum_vs_rsi", cartifact.Base)
	}
	cmd := string(cartifact.Markdown)
	for _, want := range []string{"# Screen grade — momentum vs rsi", "## Comparison (Excess Delta)", "Excess Delta"} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("comparison markdown missing %q\n---\n%s", want, cmd)
		}
	}
}

// survivorRow finds a pooled survivor row by anchor and ticker.
func survivorRow(rows []TickerForwardReturns, asOf, ticker string) *TickerForwardReturns {
	for i := range rows {
		if rows[i].AsOf == asOf && rows[i].Ticker == ticker {
			return &rows[i]
		}
	}
	return nil
}
