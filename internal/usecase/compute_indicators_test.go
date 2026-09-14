package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// fakePriceSeries is the stored-row seam: rows keyed by ticker, ordered as the
// real repository returns them (ascending by trading day).
type fakePriceSeries struct {
	rows   map[string][]entity.DailyPrice
	latest *time.Time
	err    error
}

func (f *fakePriceSeries) FindByTickerUpTo(db *sqlx.DB, ticker string, to time.Time, limit int) ([]entity.DailyPrice, error) {
	if f.err != nil {
		return nil, f.err
	}
	rows := f.rows[ticker]
	if len(rows) > limit {
		rows = rows[len(rows)-limit:]
	}
	return rows, nil
}

func (f *fakePriceSeries) LatestTradingDayAll(db *sqlx.DB) (*time.Time, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.latest, nil
}

// priceRows builds an ascending close series from closes.
func priceRows(closes []float64) []entity.DailyPrice {
	rows := make([]entity.DailyPrice, 0, len(closes))
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	for _, c := range closes {
		rows = append(rows, entity.DailyPrice{Ticker: "TEST", TradingDay: day, Close: f64p(c)})
		day = day.AddDate(0, 0, 1)
	}
	return rows
}

func newComputeTestUseCase(series *fakePriceSeries) *ComputeIndicatorsUseCase {
	return NewComputeIndicatorsUseCase(nil, logrus.New(), series)
}

func testAnchor() time.Time { return time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC) }

// Validation must run before any read: with a nil DB and nil repository these
// calls can only pass if they never reach the price seam.
func TestComputeIndicators_PureValidation(t *testing.T) {
	uc := NewComputeIndicatorsUseCase(nil, logrus.New(), nil)

	cases := []struct {
		name    string
		req     ComputeIndicatorsRequest
		wantErr error
	}{
		{
			name: "unknown indicator name",
			req: ComputeIndicatorsRequest{
				Tickers: []string{"BBRI"}, Indicators: []string{"smma:20"},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "bad period",
			req: ComputeIndicatorsRequest{
				Tickers: []string{"BBRI"}, Indicators: []string{"sma:0"},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "no indicators",
			req: ComputeIndicatorsRequest{
				Tickers: []string{"BBRI"},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "no tickers",
			req: ComputeIndicatorsRequest{
				Indicators: []string{"sma:20"},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "invalid ticker",
			req: ComputeIndicatorsRequest{
				Tickers: []string{"not a ticker"}, Indicators: []string{"sma:20"},
			},
			wantErr: ErrInvalidTicker,
		},
		{
			name: "window below the floor",
			req: ComputeIndicatorsRequest{
				Tickers: []string{"BBRI"}, Indicators: []string{"sma:20"}, Window: 1,
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "window above the ceiling",
			req: ComputeIndicatorsRequest{
				Tickers: []string{"BBRI"}, Indicators: []string{"sma:20"}, Window: 501,
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "series mode is not implemented yet",
			req: ComputeIndicatorsRequest{
				Mode: "series", Tickers: []string{"BBRI"}, Indicators: []string{"sma:20"},
			},
			wantErr: ErrInvalidArgument,
		},
		{
			name: "unknown mode",
			req: ComputeIndicatorsRequest{
				Mode: "scren", Tickers: []string{"BBRI"}, Indicators: []string{"sma:20"},
			},
			wantErr: ErrInvalidArgument,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := uc.ComputeIndicators(context.Background(), tc.req)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if resp != nil {
				t.Errorf("response = %+v, want nil on error", resp)
			}
		})
	}
}

// A typo has to be fixable from the error alone — the enumeration of valid
// names rides in the message.
func TestComputeIndicators_UnknownIndicatorEnumeratesValidNames(t *testing.T) {
	uc := NewComputeIndicatorsUseCase(nil, logrus.New(), nil)
	_, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers: []string{"BBRI"}, Indicators: []string{"bollinger"},
	})
	if err == nil {
		t.Fatal("expected an error for an unknown indicator")
	}
	for _, name := range []string{"sma", "ema", "rsi"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not enumerate valid name %q", err.Error(), name)
		}
	}
}

func TestComputeIndicators_ScreenModeRows(t *testing.T) {
	series := &fakePriceSeries{
		latest: f64pDay(testAnchor()),
		rows: map[string][]entity.DailyPrice{
			"BBRI": priceRows([]float64{1, 2, 3, 4, 5}),
			"TLKM": priceRows([]float64{10, 11, 12, 11, 13, 12}),
		},
	}
	uc := newComputeTestUseCase(series)

	resp, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers:    []string{"BBRI", "TLKM"},
		Indicators: []string{"sma:3", "rsi:3"},
		Window:     10,
	})
	if err != nil {
		t.Fatalf("ComputeIndicators error: %v", err)
	}

	if resp.Mode != computeModeScreen {
		t.Errorf("Mode = %q, want %q", resp.Mode, computeModeScreen)
	}
	if want := "2026-09-11"; resp.AsOf != want {
		t.Errorf("AsOf = %q, want %q", resp.AsOf, want)
	}
	if resp.Window != 10 {
		t.Errorf("Window = %d, want 10", resp.Window)
	}
	if resp.Count != 2 || len(resp.Rows) != 2 {
		t.Fatalf("Count = %d, rows = %d, want 2 and 2", resp.Count, len(resp.Rows))
	}
	if got, want := strings.Join(resp.Indicators, ","), "sma:3,rsi:3"; got != want {
		t.Errorf("Indicators = %q, want %q", got, want)
	}

	// BBRI: sma:3 over 1..5 = 4; rsi:3 over a strictly rising series = 100.
	bbri := resp.Rows[0]
	if bbri.Ticker != "BBRI" {
		t.Errorf("Rows[0].Ticker = %q, want BBRI (request order)", bbri.Ticker)
	}
	assertValue(t, bbri, "sma:3", 4)
	assertValue(t, bbri, "rsi:3", 100)
	if bbri.HistoryRows != 5 {
		t.Errorf("BBRI history_rows = %d, want 5", bbri.HistoryRows)
	}
	if bbri.RequiredRows != 4 { // rsi:3 needs period+1
		t.Errorf("BBRI required_rows = %d, want 4", bbri.RequiredRows)
	}
	if len(bbri.Insufficient) != 0 {
		t.Errorf("BBRI insufficient = %v, want empty", bbri.Insufficient)
	}

	// TLKM: sma:3 over the last three closes (11,13,12) = 12; rsi:3 is the
	// hand-computed 57.142857 fixture (registry test).
	tlkm := resp.Rows[1]
	assertValue(t, tlkm, "sma:3", 12)
	assertValue(t, tlkm, "rsi:3", 57.142857)
}

// A series shorter than an indicator's warm-up bar is null plus a flag for that
// indicator only — the other indicators in the same row still report values.
func TestComputeIndicators_InsufficientWarmupPerIndicator(t *testing.T) {
	series := &fakePriceSeries{
		latest: f64pDay(testAnchor()),
		rows: map[string][]entity.DailyPrice{
			"BBRI": priceRows([]float64{100, 101}),
		},
	}
	uc := newComputeTestUseCase(series)

	resp, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers:    []string{"BBRI"},
		Indicators: []string{"sma:2", "rsi:14"},
	})
	if err != nil {
		t.Fatalf("ComputeIndicators error: %v", err)
	}

	row := resp.Rows[0]
	assertValue(t, row, "sma:2", 100.5)
	if v := row.Values["rsi:14"]; v != nil {
		t.Errorf("rsi:14 = %v, want null (2 rows is short of 15)", *v)
	}
	if got, want := strings.Join(row.Insufficient, ","), "rsi:14"; got != want {
		t.Errorf("insufficient = %q, want %q", got, want)
	}
	if row.HistoryRows != 2 {
		t.Errorf("history_rows = %d, want 2", row.HistoryRows)
	}
	if row.RequiredRows != 15 { // rsi:14 is the binding constraint
		t.Errorf("required_rows = %d, want 15", row.RequiredRows)
	}
}

// A ticker with no stored rows is a row of nulls and flags, not an error: the
// caller asked about it and needs to see "no history", not a failed call.
func TestComputeIndicators_NoStoredRows(t *testing.T) {
	series := &fakePriceSeries{latest: f64pDay(testAnchor()), rows: map[string][]entity.DailyPrice{}}
	uc := newComputeTestUseCase(series)

	resp, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers: []string{"GHOST"}, Indicators: []string{"sma:20"},
	})
	if err != nil {
		t.Fatalf("ComputeIndicators error: %v", err)
	}
	row := resp.Rows[0]
	if row.Values["sma:20"] != nil {
		t.Errorf("sma:20 = %v, want null", *row.Values["sma:20"])
	}
	if row.HistoryRows != 0 {
		t.Errorf("history_rows = %d, want 0", row.HistoryRows)
	}
	if got, want := strings.Join(row.Insufficient, ","), "sma:20"; got != want {
		t.Errorf("insufficient = %q, want %q", got, want)
	}
}

// Rows with no stored close are dropped from the series rather than read as
// zero, and duplicate tickers cost one window, not two.
func TestComputeIndicators_UsableClosesAndDedupe(t *testing.T) {
	rows := priceRows([]float64{1, 2, 3, 4, 5})
	rows[2].Close = nil // a gap in the middle
	series := &fakePriceSeries{
		latest: f64pDay(testAnchor()),
		rows:   map[string][]entity.DailyPrice{"BBRI": rows},
	}
	uc := newComputeTestUseCase(series)

	resp, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers:    []string{"BBRI", "BBRI", "bbri.JK"},
		Indicators: []string{"sma:3", "sma:3"},
	})
	if err != nil {
		t.Fatalf("ComputeIndicators error: %v", err)
	}
	if resp.Count != 1 {
		t.Fatalf("Count = %d, want 1 (duplicates collapse)", resp.Count)
	}
	row := resp.Rows[0]
	if row.HistoryRows != 4 { // 5 rows, one without a close
		t.Errorf("history_rows = %d, want 4", row.HistoryRows)
	}
	// The series is 1,2,4,5 after the gap: sma:3 = (2+4+5)/3.
	assertValue(t, row, "sma:3", 11.0/3)
	if len(resp.Indicators) != 1 {
		t.Errorf("Indicators = %v, want one entry (duplicate spec collapses)", resp.Indicators)
	}
}

// The cap is stated in the tool description and enforced here: at the cap is
// fine, one over is a structured error naming the limit.
func TestComputeIndicators_TickerCap(t *testing.T) {
	series := &fakePriceSeries{latest: f64pDay(testAnchor()), rows: map[string][]entity.DailyPrice{}}
	uc := newComputeTestUseCase(series)

	atCap := make([]string, 0, maxScreenTickers)
	for i := 0; i < maxScreenTickers; i++ {
		atCap = append(atCap, fmt.Sprintf("AA%c", 'A'+i%26)+fmt.Sprintf("%c", 'A'+i/26))
	}
	if _, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers: atCap, Indicators: []string{"sma:20"},
	}); err != nil {
		t.Fatalf("call at the cap failed: %v", err)
	}

	_, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers:    append(atCap, "BBRI"),
		Indicators: []string{"sma:20"},
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("%d", maxScreenTickers)) {
		t.Errorf("error %q does not state the cap", err.Error())
	}
}

// The window is a count of stored trading days: no calendar padding, so the
// row's history_rows never overstates the rows the values rest on.
func TestComputeIndicators_WindowLimitsHistory(t *testing.T) {
	series := &fakePriceSeries{
		latest: f64pDay(testAnchor()),
		rows:   map[string][]entity.DailyPrice{"BBRI": priceRows([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})},
	}
	uc := newComputeTestUseCase(series)

	resp, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers: []string{"BBRI"}, Indicators: []string{"sma:3"}, Window: 4,
	})
	if err != nil {
		t.Fatalf("ComputeIndicators error: %v", err)
	}
	row := resp.Rows[0]
	if row.HistoryRows != 4 {
		t.Errorf("history_rows = %d, want 4", row.HistoryRows)
	}
	assertValue(t, row, "sma:3", 9) // (8+9+10)/3
}

// An explicit asOf anchors the window; the default anchor is the latest stored
// market-wide trading day.
func TestComputeIndicators_AsOfAnchor(t *testing.T) {
	series := &fakePriceSeries{
		rows: map[string][]entity.DailyPrice{"BBRI": priceRows([]float64{1, 2, 3})},
	}
	uc := newComputeTestUseCase(series)

	asOf := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	resp, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers: []string{"BBRI"}, Indicators: []string{"sma:3"}, AsOf: &asOf,
	})
	if err != nil {
		t.Fatalf("ComputeIndicators error: %v", err)
	}
	if want := "2026-09-04"; resp.AsOf != want {
		t.Errorf("AsOf = %q, want %q", resp.AsOf, want)
	}

	// No stored day at all: the latest-day read fails loudly rather than
	// anchoring on a zero time.
	_, err = uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers: []string{"BBRI"}, Indicators: []string{"sma:3"},
	})
	if err == nil {
		t.Error("expected an error when no anchor day is resolvable")
	}
}

func TestComputeIndicators_RepositoryErrorPropagates(t *testing.T) {
	series := &fakePriceSeries{
		latest: f64pDay(testAnchor()),
		err:    errors.New("db down"),
	}
	uc := newComputeTestUseCase(series)

	_, err := uc.ComputeIndicators(context.Background(), ComputeIndicatorsRequest{
		Tickers: []string{"BBRI"}, Indicators: []string{"sma:20"},
	})
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("error = %v, want the repository error wrapped", err)
	}
}

func assertValue(t *testing.T, row ComputeIndicatorsRow, key string, want float64) {
	t.Helper()
	got, ok := row.Values[key]
	if !ok || got == nil {
		t.Fatalf("%s missing from row %s (values %v)", key, row.Ticker, row.Values)
	}
	if diff := *got - want; diff > 1e-4 || diff < -1e-4 {
		t.Errorf("%s = %v, want %v", key, *got, want)
	}
}

func f64pDay(day time.Time) *time.Time { return &day }
