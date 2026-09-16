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
// column set: a gentle uptrend with a volume that grows with the price, so every
// default indicator has a value.
func screenSeries(ticker string, days int) []entity.DailyPrice {
	return screenSeriesStep(ticker, days, 1)
}

// screenSeriesStep is screenSeries with a caller-chosen daily close step, so a
// test can give two tickers different momentum (and therefore different RSI)
// while keeping everything else identical.
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
			Volume:     i64p(1_000_000 + int64(i)*1000),
		})
		day = day.AddDate(0, 0, 1)
	}
	return rows
}

// screenCandidate is one survivor as the hard-filter query returns it.
func screenCandidate(ticker string, value int64) repository.ScreenCandidate {
	return repository.ScreenCandidate{Ticker: ticker, Value: i64p(value), Close: f64p(4850)}
}

// newScreenTestUseCase wires the fake with a full-history series for every
// candidate and the standard anchor day.
func newScreenTestUseCase(source *fakeScreenerSource) *ScreenStocksUseCase {
	anchor := testAnchor()
	source.latest = &anchor
	return NewScreenStocksUseCase(nil, logrus.New(), source)
}

// Validation must run before any read: with a nil DB and nil source these calls
// can only pass if they never reach the SQL seam.
func TestScreenStocks_PureValidation(t *testing.T) {
	uc := NewScreenStocksUseCase(nil, logrus.New(), nil)

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
			name:    "sort key that is not a requested column",
			req:     ScreenStocksRequest{Indicators: []string{"rsi:14"}, Sort: "ma_distance:20:50"},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "unknown sort key",
			req:     ScreenStocksRequest{Sort: "foreign_net"},
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
	if res.Funnel.Universe != 900 || res.Funnel.AfterValue != 1 {
		t.Fatalf("funnel = %+v, want universe 900 / after_value 1", res.Funnel)
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
// cannot compete in a ranking it has no number for.
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

	desc, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Sort: "rsi:14"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := rowTickers(desc.Rows); got != "AAA,BBB,CCC" {
		t.Fatalf("desc by rsi = %s, want AAA,BBB,CCC (unobserved last)", got)
	}

	asc, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Sort: "rsi:14", Order: "asc"})
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
	upper, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{Sort: "RSI:14"})
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
	if res.Funnel.AfterValue != 4 {
		t.Fatalf("after_value = %d, want 4 (the cap is not a filter)", res.Funnel.AfterValue)
	}
	if res.Limit != 2 {
		t.Fatalf("limit echoed = %d, want 2", res.Limit)
	}
}

// TestScreenStocks_ShortHistoryIsFlaggedNotFiltered — a survivor whose stored
// history is shorter than a column's warm-up keeps its row, with a null value
// and the key named in insufficient: warm-up is a per-column flag, and ticket
// 05's structural filters (not the hard filters) are what drop such rows.
func TestScreenStocks_ShortHistoryIsFlaggedNotFiltered(t *testing.T) {
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
	uc := NewScreenStocksUseCase(nil, logrus.New(), &fakeScreenerSource{})

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
	uc := NewScreenStocksUseCase(nil, logrus.New(), source)

	res, err := uc.ScreenStocks(context.Background(), ScreenStocksRequest{AsOf: &asOf})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.AsOf != "2026-06-30" {
		t.Fatalf("as_of = %s, want 2026-06-30", res.AsOf)
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
