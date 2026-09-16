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

// fakeScreenerSource is the screener's read source: the two hard-filter queries,
// the survivor price windows, and the anchor day. It records what the usecase
// asked for, which is how the tests assert the funnel's ordering (hard filters
// before any price read) and the parameters that reached SQL.
type fakeScreenerSource struct {
	universe   int
	candidates []repository.ScreenCandidate
	prices     map[string][]entity.DailyPrice
	latest     *time.Time

	universeErr   error
	candidatesErr error
	pricesErr     error

	gotUniverseWindow int
	gotAnchor         time.Time
	gotMinValue       int64
	gotSuspensionDays int
	gotTickers        []string
	gotPriceLimit     int
}

func (f *fakeScreenerSource) ScreenUniverse(db *sqlx.DB, anchor time.Time, window int) (int, error) {
	if f.universeErr != nil {
		return 0, f.universeErr
	}
	f.gotAnchor = anchor
	f.gotUniverseWindow = window
	return f.universe, nil
}

func (f *fakeScreenerSource) ScreenCandidates(db *sqlx.DB, anchor time.Time, minValue int64, suspensionDays int) ([]repository.ScreenCandidate, error) {
	if f.candidatesErr != nil {
		return nil, f.candidatesErr
	}
	f.gotMinValue = minValue
	f.gotSuspensionDays = suspensionDays
	return f.candidates, nil
}

func (f *fakeScreenerSource) FindByTickersUpTo(db *sqlx.DB, tickers []string, to time.Time, limit int) ([]entity.DailyPrice, error) {
	if f.pricesErr != nil {
		return nil, f.pricesErr
	}
	f.gotTickers = tickers
	f.gotPriceLimit = limit

	rows := make([]entity.DailyPrice, 0, len(tickers)*limit)
	for _, ticker := range tickers {
		series := f.prices[ticker]
		if len(series) > limit {
			series = series[len(series)-limit:]
		}
		rows = append(rows, series...)
	}
	return rows, nil
}

func (f *fakeScreenerSource) LatestTradingDayAll(db *sqlx.DB) (*time.Time, error) {
	return f.latest, nil
}

// screenSeries builds an ascending OHLCV series long enough for the default
// column set: a gentle uptrend on volume that fades day by day — the shipped
// default filter set's breakout-zone shape (price above its fast MA, high in its
// range, volume not above its own average), so a test that expects rows back is
// screening a shape the default filters actually admit.
func screenSeries(ticker string, days int) []entity.DailyPrice {
	return screenSeriesStep(ticker, days, 1)
}

// screenSeriesStep is screenSeries with a caller-chosen daily close step, so a
// test can give two tickers different momentum (and therefore different RSI)
// while keeping everything else identical. Volume always fades, and every series
// these helpers build is far shorter than 1000 rows, so it never reaches zero.
func screenSeriesStep(ticker string, days int, step float64) []entity.DailyPrice {
	rows := make([]entity.DailyPrice, 0, days)
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	for i := 0; i < days; i++ {
		close := 1000.0 + float64(i)*step
		rows = append(rows, entity.DailyPrice{
			Ticker:     ticker,
			TradingDay: day,
			High:       f64p(close + 10),
			Low:        f64p(close - 10),
			Close:      f64p(close),
			Volume:     i64p(1_000_000 - int64(i)*1000),
		})
		day = day.AddDate(0, 0, 1)
	}
	return rows
}

// screenCandidate is one survivor as the hard-filter query returns it.
func screenCandidate(ticker string, value int64) repository.ScreenCandidate {
	return repository.ScreenCandidate{Ticker: ticker, Value: i64p(value), Close: f64p(4850)}
}

// fakeScreenerFlow is the broker-flow seam: it returns a stored window (or an
// error) and records what the usecase asked for, so the tests can assert the
// flow read is scoped to survivors and carries the resolved window.
type fakeScreenerFlow struct {
	window *repository.ForeignNetWindow
	err    error

	gotTickers     []string
	gotAnchor      time.Time
	gotTradingDays int
}

func (f *fakeScreenerFlow) SumForeignNetByTickers(db *sqlx.DB, tickers []string, to time.Time, tradingDays int) (*repository.ForeignNetWindow, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.gotTickers = tickers
	f.gotAnchor = to
	f.gotTradingDays = tradingDays
	if f.window != nil {
		return f.window, nil
	}
	// The default: a window with the requested span and nothing stored — the
	// honest answer for a market whose broker population is anomaly-gated and
	// this test never seeded.
	return &repository.ForeignNetWindow{Rows: []repository.TickerForeignNet{}}, nil
}

// flowWindow builds a stored flow window over the standard anchor.
func flowWindow(tradeDays int, rows ...repository.TickerForeignNet) *repository.ForeignNetWindow {
	return &repository.ForeignNetWindow{
		From:      testAnchor().AddDate(0, 0, -(tradeDays - 1)),
		TradeDays: tradeDays,
		Rows:      rows,
	}
}

// newScreenTestUseCase wires the fake with a full-history series for every
// candidate and the standard anchor day. The flow seam is wired empty: tests
// that read the foreign-net column use newScreenTestUseCaseWithFlow.
func newScreenTestUseCase(source *fakeScreenerSource) *ScreenStocksUseCase {
	return newScreenTestUseCaseWithFlow(source, &fakeScreenerFlow{})
}

// newScreenTestUseCaseWithFlow is newScreenTestUseCase with a caller-supplied
// broker-flow seam.
func newScreenTestUseCaseWithFlow(source *fakeScreenerSource, flow *fakeScreenerFlow) *ScreenStocksUseCase {
	anchor := testAnchor()
	source.latest = &anchor
	return NewScreenStocksUseCase(nil, logrus.New(), source, flow)
}

// Validation must run before any read: with a nil DB and nil source these calls
// can only pass if they never reach the SQL seam.
func TestScreenStocks_PureValidation(t *testing.T) {
	uc := NewScreenStocksUseCase(nil, logrus.New(), nil, nil)

	cases := []struct {
		name    string
		req     ScreenStocksRequest
		wantErr error
	}{
		{
			name:    "window below the floor",
			req:     ScreenStocksRequest{Window: 1},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "window above the ceiling",
			req:     ScreenStocksRequest{Window: 501},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "limit above the ceiling",
			req:     ScreenStocksRequest{Limit: 51},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "negative limit",
			req:     ScreenStocksRequest{Limit: -3},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "negative min value",
			req:     ScreenStocksRequest{MinValue: i64p(-1)},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "min value above the ceiling",
			req:     ScreenStocksRequest{MinValue: i64p(1_000_000_000_000_001)},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "suspension window out of range",
			req:     ScreenStocksRequest{SuspensionWindowDays: 61},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "unknown indicator column",
			req:     ScreenStocksRequest{Indicators: []string{"smma:20"}},
			wantErr: ErrInvalidArgument,
		},
		{
			// A filter column is computed, so it is sortable; a column the call
			// computes nowhere is not.
			name:    "sort key that no column computes",
			req:     ScreenStocksRequest{Indicators: []string{"rsi:14"}, Sort: "bb_width:20"},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "sort key neither a column nor an indicator",
			req:     ScreenStocksRequest{Sort: "smart_money"},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "flow window above the retention ceiling",
			req:     ScreenStocksRequest{FlowWindowDays: 61},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "negative flow window",
			req:     ScreenStocksRequest{FlowWindowDays: -1},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "filter on an unknown indicator",
			req:     ScreenStocksRequest{Filters: &[]ScreenStocksFilter{filter("smma:20", filterOpGt, 0)}},
			wantErr: ErrInvalidArgument,
		},
		{
			// The broker-flow column is a stored fact, not a registry indicator:
			// thresholds on it are a filter kind v1 does not have, so the DSL
			// rejects it as an unknown indicator rather than quietly accepting it.
			name:    "filter on the broker-flow column",
			req:     ScreenStocksRequest{Filters: &[]ScreenStocksFilter{filter("foreign_net", filterOpGt, 0)}},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "filter with an unknown operator",
			req:     ScreenStocksRequest{Filters: &[]ScreenStocksFilter{filter("rsi:14", "above", 40)}},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "filter with a band on a scalar operator",
			req:     ScreenStocksRequest{Filters: &[]ScreenStocksFilter{filter("rsi:14", filterOpGt, 40, 60)}},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "filter band reversed",
			req:     ScreenStocksRequest{Filters: &[]ScreenStocksFilter{filter("range_position:60", filterOpBetween, 0.95, 0.70)}},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "unknown order",
			req:     ScreenStocksRequest{Order: "up"},
			wantErr: ErrInvalidArgument,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := uc.ScreenStocks(context.Background(), tc.req)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestScreenStocks_Defaults — a bare call screens with the shipped defaults and
// reports the funnel: the universe count, the hard-filter cut, and the effective
// parameters echoed back so the shortlist is reproducible.
func TestScreenStocks_Defaults(t *testing.T) {
	source := &fakeScreenerSource{
		universe:   900,
		candidates: []repository.ScreenCandidate{screenCandidate("BBRI", 812_000_000_000)},
		prices:     map[string][]entity.DailyPrice{"BBRI": screenSeries("BBRI", 250)},
	}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.AsOf != "2026-09-11" || res.Window != 250 {
		t.Fatalf("as_of/window = %s/%d, want 2026-09-11/250", res.AsOf, res.Window)
	}
	if res.MinValue != defaultScreenerMinValue {
		t.Fatalf("min_value = %d, want %d", res.MinValue, defaultScreenerMinValue)
	}
	if res.Limit != defaultScreenerLimit || res.SuspensionWindowDays != defaultSuspensionWindowDays {
		t.Fatalf("limit/suspension = %d/%d, want %d/%d",
			res.Limit, res.SuspensionWindowDays, defaultScreenerLimit, defaultSuspensionWindowDays)
	}
	if res.Sort != screenerSortValue || res.Order != sortOrderDesc {
		t.Fatalf("sort/order = %s/%s, want value/desc", res.Sort, res.Order)
	}
	if strings.Join(res.Indicators, ",") != strings.Join(ScreenerDefaultIndicators, ",") {
		t.Fatalf("indicators = %v, want the stage-1 default set %v", res.Indicators, ScreenerDefaultIndicators)
	}
	if res.Funnel.Universe != 900 || res.Funnel.AfterValue != 1 || res.Funnel.AfterStructure != 1 {
		t.Fatalf("funnel = %+v, want universe 900 / after_value 1 / after_structure 1", res.Funnel)
	}
	if len(res.Filters) != len(ScreenerDefaultFilters) {
		t.Fatalf("filters = %v, want the shipped default set echoed", res.Filters)
	}
	if res.TotalMatches != 1 || res.Count != 1 || len(res.Rows) != 1 {
		t.Fatalf("counts = total %d / count %d / rows %d, want 1/1/1", res.TotalMatches, res.Count, len(res.Rows))
	}

	row := res.Rows[0]
	if row.Ticker != "BBRI" || row.Value == nil || *row.Value != 812_000_000_000 {
		t.Fatalf("row = %+v, want BBRI with its anchor-day value", row)
	}
	if row.Close == nil || *row.Close != 4850 {
		t.Fatalf("row close = %v, want 4850", row.Close)
	}
	if len(row.Insufficient) != 0 {
		t.Fatalf("insufficient = %v, want none over a 250-day series", row.Insufficient)
	}
	for _, key := range ScreenerDefaultIndicators {
		if row.Values[key] == nil {
			t.Fatalf("value for %s is null over a 250-day series; row = %+v", key, row)
		}
	}
	if row.HistoryRows != 250 {
		t.Fatalf("history_rows = %d, want 250", row.HistoryRows)
	}
}

// TestScreenStocks_IndicatorsOnlyForSurvivors — the funnel's whole point: the
// price seam is asked for the survivors and nobody else, and the filter
// parameters reach it unchanged.
func TestScreenStocks_IndicatorsOnlyForSurvivors(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 900,
		candidates: []repository.ScreenCandidate{
			screenCandidate("BBRI", 812_000_000_000),
			screenCandidate("TLKM", 300_000_000_000),
		},
		prices: map[string][]entity.DailyPrice{
			"BBRI": screenSeries("BBRI", 250),
			"TLKM": screenSeries("TLKM", 250),
			// A liquid-looking name the hard filters dropped: its rows exist but
			// must never be read for a screen that discarded it.
			"GOTO": screenSeries("GOTO", 250),
		},
	}
	uc := newScreenTestUseCase(source)

	_, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{
		Window:               120,
		MinValue:             i64p(2_000_000_000),
		SuspensionWindowDays: 3,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Join(source.gotTickers, ",") != "BBRI,TLKM" {
		t.Fatalf("price read for %v, want exactly the two survivors", source.gotTickers)
	}
	if source.gotPriceLimit != 120 || source.gotUniverseWindow != 120 {
		t.Fatalf("windows = prices %d / universe %d, want 120/120", source.gotPriceLimit, source.gotUniverseWindow)
	}
	if source.gotMinValue != 2_000_000_000 || source.gotSuspensionDays != 3 {
		t.Fatalf("filters = min %d / suspension %d, want 2000000000/3", source.gotMinValue, source.gotSuspensionDays)
	}
	if !source.gotAnchor.Equal(testAnchor()) {
		t.Fatalf("anchor = %s, want %s", source.gotAnchor, testAnchor())
	}
}

// TestScreenStocks_RanksBySortKey — ranking follows the requested column and
// direction, and a row with no value for that column ranks last either way: it
// cannot compete in a ranking it has no number for. Structural filters are
// switched off here so the short-history row survives to be ranked.
func TestScreenStocks_RanksBySortKey(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 3,
		candidates: []repository.ScreenCandidate{
			screenCandidate("AAA", 900_000_000_000), // rsi observed
			screenCandidate("BBB", 500_000_000_000), // rsi observed
			screenCandidate("CCC", 700_000_000_000), // too short: rsi null
		},
		prices: map[string][]entity.DailyPrice{
			"AAA": screenSeriesStep("AAA", 250, 1),  // rising: high RSI
			"BBB": screenSeriesStep("BBB", 250, -1), // falling: low RSI
			"CCC": screenSeries("CCC", 3),
		},
	}
	uc := newScreenTestUseCase(source)

	desc, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Sort: "rsi:14", Filters: noFilters()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := rowTickers(desc.Rows); got != "AAA,BBB,CCC" {
		t.Fatalf("desc by rsi = %s, want AAA,BBB,CCC (unobserved last)", got)
	}

	asc, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Sort: "rsi:14", Order: "asc", Filters: noFilters()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := rowTickers(asc.Rows); got != "BBB,AAA,CCC" {
		t.Fatalf("asc by rsi = %s, want BBB,AAA,CCC (unobserved still last)", got)
	}
	if asc.Sort != "rsi:14" || asc.Order != sortOrderAsc {
		t.Fatalf("sort/order echoed = %s/%s, want rsi:14/asc", asc.Sort, asc.Order)
	}

	// The case-insensitive match resolves to the canonical registry key.
	upper, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Sort: "RSI:14", Filters: noFilters()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if upper.Sort != "rsi:14" {
		t.Fatalf("sort = %s, want the canonical key rsi:14", upper.Sort)
	}
}

// TestScreenStocks_TieBreakIsDeterministic — a tie on the sort key breaks by
// anchor-day value descending, then ticker ascending, so identical inputs never
// produce a different shortlist.
func TestScreenStocks_TieBreakIsDeterministic(t *testing.T) {
	// Equal rsi (identical series), so the sort key ties and the tie-break decides.
	source := &fakeScreenerSource{
		universe: 3,
		candidates: []repository.ScreenCandidate{
			screenCandidate("ZZZZ", 100_000_000_000),
			screenCandidate("AAAA", 400_000_000_000),
			screenCandidate("MMMM", 400_000_000_000),
		},
		prices: map[string][]entity.DailyPrice{
			"ZZZZ": screenSeries("ZZZZ", 250),
			"AAAA": screenSeries("AAAA", 250),
			"MMMM": screenSeries("MMMM", 250),
		},
	}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Sort: "rsi:14"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// AAAA and MMMM tie on rsi and on value, so ticker ascending splits them;
	// ZZZZ has the smallest value and comes last.
	if got := rowTickers(res.Rows); got != "AAAA,MMMM,ZZZZ" {
		t.Fatalf("rows = %s, want AAAA,MMMM,ZZZZ", got)
	}
}

// TestScreenStocks_CapsRowsAndReportsTheCut — total_matches is the pre-cap
// count, so a capped shortlist never hides how many tickers passed the filters.
func TestScreenStocks_CapsRowsAndReportsTheCut(t *testing.T) {
	candidates := make([]repository.ScreenCandidate, 0, 4)
	prices := make(map[string][]entity.DailyPrice, 4)
	for i, ticker := range []string{"AAAA", "BBBB", "CCCC", "DDDD"} {
		candidates = append(candidates, screenCandidate(ticker, int64(100-i)*1_000_000_000))
		prices[ticker] = screenSeries(ticker, 250)
	}
	source := &fakeScreenerSource{universe: 900, candidates: candidates, prices: prices}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Limit: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.TotalMatches != 4 || res.Count != 2 || len(res.Rows) != 2 {
		t.Fatalf("counts = total %d / count %d / rows %d, want 4/2/2", res.TotalMatches, res.Count, len(res.Rows))
	}
	if got := rowTickers(res.Rows); got != "AAAA,BBBB" {
		t.Fatalf("rows = %s, want the two largest values AAAA,BBBB", got)
	}
	if res.Funnel.AfterValue != 4 || res.Funnel.AfterStructure != 4 {
		t.Fatalf("funnel after_value/after_structure = %d/%d, want 4/4 (the cap is not a filter)",
			res.Funnel.AfterValue, res.Funnel.AfterStructure)
	}
	if res.Limit != 2 {
		t.Fatalf("limit echoed = %d, want 2", res.Limit)
	}
}

// TestScreenStocks_ShortHistoryIsFlaggedNotFiltered — a survivor whose stored
// history is shorter than a column's warm-up keeps its row, with a null value
// and the key named in insufficient: warm-up is a per-column flag, and it is the
// structural filters (not the hard filters) that drop such a row. The filter
// list is empty here so the row reaches the response for the flags to be read.
func TestScreenStocks_ShortHistoryIsFlaggedNotFiltered(t *testing.T) {
	source := &fakeScreenerSource{
		universe:   1,
		candidates: []repository.ScreenCandidate{screenCandidate("NEWIPO", 90_000_000_000)},
		prices:     map[string][]entity.DailyPrice{"NEWIPO": screenSeries("NEWIPO", 3)},
	}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Filters: noFilters()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d, want the survivor kept", len(res.Rows))
	}

	row := res.Rows[0]
	if row.HistoryRows != 3 {
		t.Fatalf("history_rows = %d, want 3", row.HistoryRows)
	}
	if row.RequiredRows <= 0 {
		t.Fatalf("required_rows = %d, want the binding warm-up bar", row.RequiredRows)
	}
	if len(row.Insufficient) != len(ScreenerDefaultIndicators) {
		t.Fatalf("insufficient = %v, want every default column over 3 rows", row.Insufficient)
	}
	for _, key := range ScreenerDefaultIndicators {
		if row.Values[key] != nil {
			t.Fatalf("value for %s = %v, want null below its warm-up", key, *row.Values[key])
		}
	}
	// The flag list and the null values must agree, or a reader cannot tell
	// "no value" from "value zero".
	listed := make(map[string]bool, len(row.Insufficient))
	for _, key := range row.Insufficient {
		listed[key] = true
	}
	for key, value := range row.Values {
		if (value == nil) != listed[key] {
			t.Fatalf("key %s: null=%v but insufficient-listed=%v", key, value == nil, listed[key])
		}
	}
}

// TestScreenStocks_EmptyFunnel — a filter set that keeps nothing is a result,
// not an error: the funnel reports the cut and rows marshals as an empty list.
func TestScreenStocks_EmptyFunnel(t *testing.T) {
	source := &fakeScreenerSource{universe: 900, candidates: nil, prices: nil}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Funnel.Universe != 900 || res.Funnel.AfterValue != 0 {
		t.Fatalf("funnel = %+v, want universe 900 / after_value 0", res.Funnel)
	}
	if res.Count != 0 || res.TotalMatches != 0 {
		t.Fatalf("counts = %d/%d, want 0/0", res.Count, res.TotalMatches)
	}
	if res.Rows == nil {
		t.Fatal("rows is nil; an empty funnel must marshal as []")
	}
	if len(source.gotTickers) != 0 {
		t.Fatalf("price read for %v, want no read with no survivors", source.gotTickers)
	}
}

// TestScreenStocks_ReadErrorsPropagate — a database failure is an error, never a
// silently empty shortlist that would read as "nothing matched today".
func TestScreenStocks_ReadErrorsPropagate(t *testing.T) {
	boom := errors.New("db exploded")
	cases := []struct {
		name   string
		source *fakeScreenerSource
	}{
		{name: "universe", source: &fakeScreenerSource{universeErr: boom}},
		{name: "candidates", source: &fakeScreenerSource{candidatesErr: boom}},
		{name: "prices", source: &fakeScreenerSource{
			candidates: []repository.ScreenCandidate{screenCandidate("BBRI", 1)},
			pricesErr:  boom,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uc := newScreenTestUseCase(tc.source)
			_, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{})
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want the read failure", err)
			}
		})
	}
}

// TestScreenStocks_NoTradingDay — with nothing stored there is no anchor, and
// the call says so rather than screening an empty market.
func TestScreenStocks_NoTradingDay(t *testing.T) {
	uc := NewScreenStocksUseCase(nil, logrus.New(), &fakeScreenerSource{}, &fakeScreenerFlow{})

	_, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestScreenStocks_AsOfPinsTheAnchor — a caller-supplied as_of is the anchor,
// so a screen can be replayed for a past session.
func TestScreenStocks_AsOfPinsTheAnchor(t *testing.T) {
	asOf := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	source := &fakeScreenerSource{universe: 10, candidates: nil, prices: nil}
	source.latest = &asOf
	uc := NewScreenStocksUseCase(nil, logrus.New(), source, &fakeScreenerFlow{})

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{AsOf: &asOf})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.AsOf != "2026-06-30" {
		t.Fatalf("as_of = %s, want 2026-06-30", res.AsOf)
	}
}

// TestScreenStocks_ForeignNetColumnAndCoverage — the broker-flow column is
// joined for survivors, and a ticker the anomaly-gated broker population never
// covered reads as a null column with a false coverage flag, never as a zero
// net the caller would take for "no foreign activity". The window is echoed
// with the denominator its day counts read against.
func TestScreenStocks_ForeignNetColumnAndCoverage(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 900,
		candidates: []repository.ScreenCandidate{
			screenCandidate("BBRI", 812_000_000_000),
			screenCandidate("TLKM", 300_000_000_000),
			screenCandidate("ASII", 120_000_000_000),
		},
		prices: map[string][]entity.DailyPrice{
			"BBRI": screenSeries("BBRI", 250),
			"TLKM": screenSeries("TLKM", 250),
			"ASII": screenSeries("ASII", 250),
		},
	}
	flow := &fakeScreenerFlow{window: flowWindow(20,
		repository.TickerForeignNet{Ticker: "BBRI", ForeignNet: 145_300_000_000, Days: 7},
		repository.TickerForeignNet{Ticker: "ASII", ForeignNet: -20_000_000_000, Days: 3},
	)}
	uc := newScreenTestUseCaseWithFlow(source, flow)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.FlowWindowDays != defaultFlowWindowDays || res.FlowDaysInWindow != 20 {
		t.Fatalf("window = %d over %d days, want %d over 20",
			res.FlowWindowDays, res.FlowDaysInWindow, defaultFlowWindowDays)
	}
	if flow.gotTradingDays != defaultFlowWindowDays {
		t.Fatalf("flow read asked for %d trading days, want the default %d",
			flow.gotTradingDays, defaultFlowWindowDays)
	}
	if strings.Join(flow.gotTickers, ",") != "BBRI,TLKM,ASII" {
		t.Fatalf("flow read for %v, want exactly the survivors", flow.gotTickers)
	}
	if !flow.gotAnchor.Equal(testAnchor()) {
		t.Fatalf("flow anchor = %s, want %s", flow.gotAnchor, testAnchor())
	}

	byTicker := map[string]ScreenStocksRow{}
	for _, row := range res.Rows {
		byTicker[row.Ticker] = row
	}

	if row := byTicker["BBRI"]; row.ForeignNet == nil || *row.ForeignNet != 145_300_000_000 {
		t.Fatalf("BBRI foreign_net = %v, want 145.3B", row.ForeignNet)
	}
	if got := byTicker["BBRI"].Coverage[coverageForeignNet]; !got.Observed || got.Days != 7 {
		t.Fatalf("BBRI coverage = %+v, want observed over 7 days", got)
	}

	// Observed and negative: a net seller is a fact, not a missing value.
	if row := byTicker["ASII"]; row.ForeignNet == nil || *row.ForeignNet != -20_000_000_000 {
		t.Fatalf("ASII foreign_net = %v, want −20B", row.ForeignNet)
	}

	// Uncovered: null, flagged, and never a zero.
	tlkm := byTicker["TLKM"]
	if tlkm.ForeignNet != nil {
		t.Fatalf("TLKM foreign_net = %d, want null for an uncovered ticker", *tlkm.ForeignNet)
	}
	if got := tlkm.Coverage[coverageForeignNet]; got.Observed || got.Days != 0 {
		t.Fatalf("TLKM coverage = %+v, want unobserved with no days", got)
	}
	if _, ok := tlkm.Coverage[coverageForeignNet]; !ok {
		t.Fatal("coverage is missing the foreign_net key; a reader cannot tell observed from missing")
	}

	// The flow column is not a registry indicator, so it must not appear among
	// the echoed columns — the registry stays the one place an indicator is
	// defined.
	for _, key := range res.Indicators {
		if key == coverageForeignNet {
			t.Fatalf("indicators = %v, want the flow column excluded", res.Indicators)
		}
	}
}

// TestScreenStocks_FlowWindowIsAParameter — the flow lookback is the caller's,
// and a window longer than the stored history reports the span it actually
// covered rather than the one asked for.
func TestScreenStocks_FlowWindowIsAParameter(t *testing.T) {
	source := &fakeScreenerSource{
		universe:   1,
		candidates: []repository.ScreenCandidate{screenCandidate("BBRI", 812_000_000_000)},
		prices:     map[string][]entity.DailyPrice{"BBRI": screenSeries("BBRI", 250)},
	}
	// Asked for 60 days, only 12 stored: the denominator says so.
	flow := &fakeScreenerFlow{window: flowWindow(12,
		repository.TickerForeignNet{Ticker: "BBRI", ForeignNet: 1_000_000_000, Days: 4},
	)}
	uc := newScreenTestUseCaseWithFlow(source, flow)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{FlowWindowDays: 60})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if flow.gotTradingDays != 60 {
		t.Fatalf("flow read asked for %d days, want 60", flow.gotTradingDays)
	}
	if res.FlowWindowDays != 60 || res.FlowDaysInWindow != 12 {
		t.Fatalf("window = %d over %d days, want 60 asked over 12 observed",
			res.FlowWindowDays, res.FlowDaysInWindow)
	}
	if got := res.Rows[0].Coverage[coverageForeignNet].Days; got != 4 {
		t.Fatalf("coverage days = %d, want the 4 observed days", got)
	}
}

// TestScreenStocks_RanksByForeignNet — the flow column is a sort key, and an
// uncovered row ranks last in either direction: a ticker with nothing stored
// has no claim to the top (or the bottom) of a ranking it cannot compete in.
// The tie-break still decides equal nets deterministically.
func TestScreenStocks_RanksByForeignNet(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 4,
		candidates: []repository.ScreenCandidate{
			screenCandidate("AAAA", 400_000_000_000), // net 5B
			screenCandidate("BBBB", 300_000_000_000), // uncovered
			screenCandidate("CCCC", 200_000_000_000), // net 5B (tie, smaller value)
			screenCandidate("DDDD", 600_000_000_000), // net −7B
		},
		prices: map[string][]entity.DailyPrice{
			"AAAA": screenSeries("AAAA", 250),
			"BBBB": screenSeries("BBBB", 250),
			"CCCC": screenSeries("CCCC", 250),
			"DDDD": screenSeries("DDDD", 250),
		},
	}
	flow := &fakeScreenerFlow{window: flowWindow(20,
		repository.TickerForeignNet{Ticker: "AAAA", ForeignNet: 5_000_000_000, Days: 9},
		repository.TickerForeignNet{Ticker: "CCCC", ForeignNet: 5_000_000_000, Days: 2},
		repository.TickerForeignNet{Ticker: "DDDD", ForeignNet: -7_000_000_000, Days: 6},
	)}
	uc := newScreenTestUseCaseWithFlow(source, flow)

	desc, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{
		Sort: coverageForeignNet, Filters: noFilters(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// AAAA and CCCC tie on 5B, so value descending breaks it; DDDD's −7B is last
	// among the observed and BBBB is last outright.
	if got := rowTickers(desc.Rows); got != "AAAA,CCCC,DDDD,BBBB" {
		t.Fatalf("desc by foreign_net = %s, want AAAA,CCCC,DDDD,BBBB (unobserved last)", got)
	}
	if desc.Sort != coverageForeignNet {
		t.Fatalf("sort echoed = %s, want %s", desc.Sort, coverageForeignNet)
	}

	// Ascending: the smallest net leads, and the uncovered row is still last.
	asc, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{
		Sort: coverageForeignNet, Order: "asc", Filters: noFilters(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := rowTickers(asc.Rows); got != "DDDD,AAAA,CCCC,BBBB" {
		t.Fatalf("asc by foreign_net = %s, want DDDD,AAAA,CCCC,BBBB (unobserved still last)", got)
	}

	// Case-insensitive, like every other sort key.
	upper, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Sort: "FOREIGN_NET"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if upper.Sort != coverageForeignNet {
		t.Fatalf("sort = %s, want the canonical key %s", upper.Sort, coverageForeignNet)
	}
}

// TestScreenStocks_FlowReadError — a broker read failure is an error, never a
// shortlist whose every row reads as "no foreign data".
func TestScreenStocks_FlowReadError(t *testing.T) {
	boom := errors.New("broker table exploded")
	source := &fakeScreenerSource{
		universe:   1,
		candidates: []repository.ScreenCandidate{screenCandidate("BBRI", 812_000_000_000)},
		prices:     map[string][]entity.DailyPrice{"BBRI": screenSeries("BBRI", 250)},
	}
	uc := newScreenTestUseCaseWithFlow(source, &fakeScreenerFlow{err: boom})

	_, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the flow read failure", err)
	}
}

// TestScreenStocks_FlowReadScopedToSurvivors — the flow read is asked for the
// survivors and nobody else, and a ticker the hard filters dropped is never
// read: the broker population is the expensive, sparsely covered one.
func TestScreenStocks_FlowReadScopedToSurvivors(t *testing.T) {
	source := &fakeScreenerSource{
		universe:   900,
		candidates: []repository.ScreenCandidate{screenCandidate("BBRI", 812_000_000_000)},
		prices:     map[string][]entity.DailyPrice{"BBRI": screenSeries("BBRI", 250)},
	}
	flow := &fakeScreenerFlow{}
	uc := newScreenTestUseCaseWithFlow(source, flow)

	if _, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{FlowWindowDays: 5}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Join(flow.gotTickers, ",") != "BBRI" {
		t.Fatalf("flow read for %v, want only the survivor", flow.gotTickers)
	}
	if flow.gotTradingDays != 5 {
		t.Fatalf("flow read asked for %d days, want 5", flow.gotTradingDays)
	}
}

// rowTickers renders the row order for assertions.
func rowTickers(rows []ScreenStocksRow) string {
	tickers := make([]string, 0, len(rows))
	for _, row := range rows {
		tickers = append(tickers, row.Ticker)
	}
	return strings.Join(tickers, ",")
}

// screenRisingVolumeSeries is screenSeries with volume climbing into the anchor
// day: the price shape clears the default filters, the volume-contraction bound
// does not, so a drop is attributable to the volume filter alone.
func screenRisingVolumeSeries(ticker string, days int) []entity.DailyPrice {
	rows := screenSeries(ticker, days)
	for i := range rows {
		rows[i].Volume = i64p(1_000_000 + int64(i)*1000)
	}
	return rows
}

// screenRangeSeries builds a series whose final close sits at a chosen position
// in a flat period range: every row spans 100..200, so range_position is exactly
// (lastClose-100)/100 and a band filter can be tested on its edge rather than
// near it.
func screenRangeSeries(ticker string, days int, lastClose float64) []entity.DailyPrice {
	rows := make([]entity.DailyPrice, 0, days)
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	for i := 0; i < days; i++ {
		close := 150.0
		if i == days-1 {
			close = lastClose
		}
		rows = append(rows, entity.DailyPrice{
			Ticker:     ticker,
			TradingDay: day,
			High:       f64p(200),
			Low:        f64p(100),
			Close:      f64p(close),
			Volume:     i64p(1_000_000 - int64(i)*1000),
		})
		day = day.AddDate(0, 0, 1)
	}
	return rows
}

// TestScreenStocks_DefaultFiltersShapeTheFunnel — stage 3 runs the shipped set
// unless told otherwise, and after_structure is what it removed. Each ticker
// here fails a different filter, so the single survivor is attributable.
func TestScreenStocks_DefaultFiltersShapeTheFunnel(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 900,
		candidates: []repository.ScreenCandidate{
			screenCandidate("BREAKOUT", 300_000_000_000), // passes all three
			screenCandidate("HEAVY", 200_000_000_000),    // volume above its average
			screenCandidate("FALLING", 100_000_000_000),  // below its MA, low in range
		},
		prices: map[string][]entity.DailyPrice{
			"BREAKOUT": screenSeries("BREAKOUT", 250),
			"HEAVY":    screenRisingVolumeSeries("HEAVY", 250),
			"FALLING":  screenSeriesStep("FALLING", 250, -1),
		},
	}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.Funnel.Universe != 900 || res.Funnel.AfterValue != 3 || res.Funnel.AfterStructure != 1 {
		t.Fatalf("funnel = %+v, want universe 900 / after_value 3 / after_structure 1", res.Funnel)
	}
	if got := rowTickers(res.Rows); got != "BREAKOUT" {
		t.Fatalf("rows = %s, want only the breakout shape", got)
	}
	// The cap never hides the cut: total_matches is the post-filter pre-cap count.
	if res.TotalMatches != 1 || res.Count != 1 {
		t.Fatalf("counts = %d/%d, want 1/1", res.TotalMatches, res.Count)
	}
}

// TestScreenStocks_EmptyFilterListIsAPassthrough — an explicitly empty filter
// list is a request for no structure, not for the defaults: stage 1 survives
// intact and after_structure equals after_value.
func TestScreenStocks_EmptyFilterListIsAPassthrough(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 900,
		candidates: []repository.ScreenCandidate{
			screenCandidate("BREAKOUT", 300_000_000_000),
			screenCandidate("FALLING", 100_000_000_000),
		},
		prices: map[string][]entity.DailyPrice{
			"BREAKOUT": screenSeries("BREAKOUT", 250),
			"FALLING":  screenSeriesStep("FALLING", 250, -1),
		},
	}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Filters: noFilters()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.Funnel.AfterValue != 2 || res.Funnel.AfterStructure != 2 {
		t.Fatalf("funnel = %+v, want after_value 2 / after_structure 2", res.Funnel)
	}
	if got := rowTickers(res.Rows); got != "BREAKOUT,FALLING" {
		t.Fatalf("rows = %s, want both survivors ranked by value desc", got)
	}
	if res.Filters == nil || len(res.Filters) != 0 {
		t.Fatalf("filters echoed = %v, want an empty list rather than the defaults", res.Filters)
	}
}

// TestScreenStocks_FilterIndicatorsJoinTheColumns — a filter's indicator is
// computed even when it was not requested as a column, the response echoes the
// union so `indicators` states what was actually computed, and a filter column
// is therefore sortable like any other.
func TestScreenStocks_FilterIndicatorsJoinTheColumns(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 2,
		candidates: []repository.ScreenCandidate{
			screenCandidate("HIGHER", 300_000_000_000),
			screenCandidate("LOWER", 100_000_000_000),
		},
		// Same price shape, different final close: identical ma_distance and
		// volume_ratio, different range_position — so the band ranks them.
		prices: map[string][]entity.DailyPrice{
			"HIGHER": screenSeries("HIGHER", 250),
			"LOWER":  screenSeries("LOWER", 250),
		},
	}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{
		Indicators: []string{"rsi:14"},
		Sort:       "range_position:60",
		Order:      "desc",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := "rsi:14,ma_distance:20:50,range_position:60,volume_ratio:20"
	if got := strings.Join(res.Indicators, ","); got != want {
		t.Fatalf("indicators = %s, want the requested column plus the filter columns %s", got, want)
	}
	if res.Sort != "range_position:60" {
		t.Fatalf("sort = %s, want the filter column to be sortable", res.Sort)
	}
	if len(res.Rows) == 0 {
		t.Fatal("no rows; want the breakout shape to survive")
	}
	for _, key := range []string{"rsi:14", "ma_distance:20:50", "range_position:60", "volume_ratio:20"} {
		if res.Rows[0].Values[key] == nil {
			t.Fatalf("value for %s is null; want every computed column present", key)
		}
	}
}

// TestScreenStocks_NullFilterValueDropsTheRow — a survivor whose filter column
// is below its warm-up is dropped by the structural stage, never assumed to
// pass. The hard filters kept it, so the drop is stage 3's and the funnel says so.
func TestScreenStocks_NullFilterValueDropsTheRow(t *testing.T) {
	source := &fakeScreenerSource{
		universe:   1,
		candidates: []repository.ScreenCandidate{screenCandidate("NEWIPO", 90_000_000_000)},
		prices:     map[string][]entity.DailyPrice{"NEWIPO": screenSeries("NEWIPO", 3)},
	}
	uc := newScreenTestUseCase(source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.Funnel.AfterValue != 1 || res.Funnel.AfterStructure != 0 {
		t.Fatalf("funnel = %+v, want after_value 1 / after_structure 0", res.Funnel)
	}
	if res.TotalMatches != 0 || res.Count != 0 {
		t.Fatalf("counts = %d/%d, want 0/0", res.TotalMatches, res.Count)
	}
	if res.Rows == nil {
		t.Fatal("rows is nil; a funnel that filters everything must still marshal as []")
	}
}

// TestScreenStocks_FilterBandEdgesAreInclusive — a value exactly on a band edge
// passes; a hair outside does not. The fixture pins range_position exactly, so
// this is the boundary and not a value near it.
func TestScreenStocks_BandEdgesAreInclusive(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 4,
		candidates: []repository.ScreenCandidate{
			screenCandidate("ATLOW", 400_000_000_000),  // position exactly 0.70
			screenCandidate("ATHIGH", 300_000_000_000), // position exactly 0.95
			screenCandidate("BELOW", 200_000_000_000),  // 0.69
			screenCandidate("ABOVE", 100_000_000_000),  // 0.96
		},
		prices: map[string][]entity.DailyPrice{
			"ATLOW":  screenRangeSeries("ATLOW", 60, 170),
			"ATHIGH": screenRangeSeries("ATHIGH", 60, 195),
			"BELOW":  screenRangeSeries("BELOW", 60, 169),
			"ABOVE":  screenRangeSeries("ABOVE", 60, 196),
		},
	}
	uc := newScreenTestUseCase(source)

	band := []ScreenStocksFilter{filter("range_position:60", filterOpBetween, 0.70, 0.95)}
	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Filters: &band})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.Funnel.AfterValue != 4 || res.Funnel.AfterStructure != 2 {
		t.Fatalf("funnel = %+v, want after_value 4 / after_structure 2 (both edges inclusive)", res.Funnel)
	}
	if got := rowTickers(res.Rows); got != "ATLOW,ATHIGH" {
		t.Fatalf("rows = %s, want ATLOW,ATHIGH (the values exactly on the edges)", got)
	}

	// The band's own number is visible per row, so the cut is explicable.
	for _, row := range res.Rows {
		position := row.Values["range_position:60"]
		if position == nil {
			t.Fatalf("row %s carries no range_position; want the filter column returned", row.Ticker)
		}
	}
}

// TestScreenStocks_CustomFiltersReplaceTheDefaults — a populated filter list is
// the whole structural stage: the defaults do not also run.
func TestScreenStocks_CustomFiltersReplaceTheDefaults(t *testing.T) {
	source := &fakeScreenerSource{
		universe: 2,
		candidates: []repository.ScreenCandidate{
			screenCandidate("BREAKOUT", 300_000_000_000),
			screenCandidate("FALLING", 100_000_000_000),
		},
		prices: map[string][]entity.DailyPrice{
			"BREAKOUT": screenSeries("BREAKOUT", 250),
			"FALLING":  screenSeriesStep("FALLING", 250, -1),
		},
	}
	uc := newScreenTestUseCase(source)

	// A permissive filter the defaults would never allow: both shapes pass.
	permissive := []ScreenStocksFilter{filter("rsi:14", filterOpGte, 0)}
	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Filters: &permissive})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Funnel.AfterStructure != 2 {
		t.Fatalf("after_structure = %d, want 2 (the defaults must not also run)", res.Funnel.AfterStructure)
	}
	if len(res.Filters) != 1 || res.Filters[0].Indicator != "rsi:14" || res.Filters[0].Op != filterOpGte {
		t.Fatalf("filters echoed = %+v, want exactly the caller's filter", res.Filters)
	}
}
