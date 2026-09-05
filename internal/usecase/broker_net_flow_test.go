package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Unit scale for these tests: arbitrary but consistent money-like values.
const (
	nfBuy10 = int64(10_000_000_000)
	nfBuy8  = int64(8_000_000_000)
	nfBuy2  = int64(2_000_000_000)
	nfSell6 = int64(6_000_000_000)
	nfSell5 = int64(5_000_000_000)
	nfSell4 = int64(4_000_000_000)
	nfSell3 = int64(3_000_000_000)
	nfSell7 = int64(7_000_000_000)
)

// nfRow builds one stored broker row for net-flow seeding.
func nfRow(ticker, broker, side string, day time.Time, val int64) entity.BrokerStockSummary {
	return entity.BrokerStockSummary{
		Ticker: ticker, BrokerCode: broker, Side: side, TradingDay: day,
		Lot: i64p(val), Value: i64p(val), AvgPrice: i64p(100), Rank: i32p(1),
	}
}

// seedNetFlowDay writes one ticker+day's stored rows (via the real repo path).
func seedNetFlowDay(t *testing.T, db *sqlx.DB, repo *repository.BrokerStockSummaryRepository, ticker string, day time.Time, rows ...entity.BrokerStockSummary) {
	t.Helper()
	totals := &entity.BrokerStockSummaryTotals{Ticker: ticker, TradingDay: day, TVal: i64p(0)}
	if err := repo.UpsertDay(db, rows, totals); err != nil {
		t.Fatalf("seedNetFlowDay %s %s: %v", ticker, day.Format("2006-01-02"), err)
	}
}

func cleanupNetFlowTicker(t *testing.T, db *sqlx.DB, tickers ...string) {
	t.Helper()
	t.Cleanup(func() {
		for _, tk := range tickers {
			db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
		}
	})
}

func findNetFlowRow(t *testing.T, rows []BrokerNetFlowRow, code string) BrokerNetFlowRow {
	t.Helper()
	for _, r := range rows {
		if r.BrokerCode == code {
			return r
		}
	}
	t.Fatalf("broker %q not in rows: %+v", code, rows)
	return BrokerNetFlowRow{}
}

// TestGetBrokerNetFlow_TickerMode — cumulative sums across multiple trading
// days: a broker listed 2 of 3 days (days_shown=2), the unlisted day's flow
// hidden inside others_net, coverage numbers from the daily_prices calendar.
func TestGetBrokerNetFlow_TickerMode(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newHistoryTestUC(t, db)

	ticker := "TESTN"
	d1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	d3 := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	cleanupNetFlowTicker(t, db, ticker)

	for _, d := range []time.Time{d1, d2, d3} {
		seedTickerAndPrice(t, db, ticker, d)
	}
	seedNetFlowDay(t, db, uc.Repo, ticker, d1, nfRow(ticker, "AK", "buy", d1, nfBuy10), nfRow(ticker, "XL", "sell", d1, nfSell6))
	seedNetFlowDay(t, db, uc.Repo, ticker, d2, nfRow(ticker, "AK", "buy", d2, nfBuy8), nfRow(ticker, "MN", "sell", d2, nfSell3))
	// d3: AK below top-10 — its flow lands in the day tail, not AK's row.
	seedNetFlowDay(t, db, uc.Repo, ticker, d3, nfRow(ticker, "MN", "buy", d3, nfBuy2), nfRow(ticker, "XL", "sell", d3, nfSell5))

	res, err := uc.GetBrokerNetFlow(context.Background(), &ticker, &d1, &d3)
	if err != nil {
		t.Fatalf("GetBrokerNetFlow: %v", err)
	}

	if res.Mode != "ticker" || res.From != "2026-08-10" || res.To != "2026-08-12" {
		t.Errorf("window = %s %s..%s, want ticker 2026-08-10..2026-08-12", res.Mode, res.From, res.To)
	}
	if res.TradeDaysInWindow != 3 || res.CoveredDays != 3 {
		t.Errorf("coverage = %d/%d, want 3/3", res.CoveredDays, res.TradeDaysInWindow)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("rows = %d, want 3: %+v", len(res.Rows), res.Rows)
	}
	// Sorted net desc: AK +18B, MN −1B, XL −11B.
	ak := findNetFlowRow(t, res.Rows, "AK")
	if ak.Buy != nfBuy10+nfBuy8 || ak.Sell != 0 || ak.Net != nfBuy10+nfBuy8 {
		t.Errorf("AK = %+v, want buy 18B net 18B", ak)
	}
	if ak.DaysShown != 2 {
		t.Errorf("AK days_shown = %d, want 2 (d3 below top-10)", ak.DaysShown)
	}
	mn := findNetFlowRow(t, res.Rows, "MN")
	if mn.Buy != nfBuy2 || mn.Sell != nfSell3 || mn.Net != nfBuy2-nfSell3 {
		t.Errorf("MN = %+v, want buy 2B sell 3B net −1B", mn)
	}
	if mn.DaysShown != 2 {
		t.Errorf("MN days_shown = %d, want 2", mn.DaysShown)
	}
	xl := findNetFlowRow(t, res.Rows, "XL")
	if xl.Buy != 0 || xl.Sell != nfSell6+nfSell5 || xl.Net != -(nfSell6+nfSell5) {
		t.Errorf("XL = %+v, want sell 11B net −11B", xl)
	}
	// Window tail = Σ listed sell − Σ listed buy = 14B − 20B = −6B.
	if res.OthersNet != (nfSell6+nfSell5+nfSell3)-(nfBuy10+nfBuy8+nfBuy2) {
		t.Errorf("others_net = %d, want −6B", res.OthersNet)
	}
	if res.Rows[0].BrokerCode != "AK" {
		t.Errorf("rows[0] = %s, want AK (top accumulator first)", res.Rows[0].BrokerCode)
	}
}

// TestGetBrokerNetFlow_MarketMode — per-broker net across tickers: sessions and
// tickers breadth, tickers_covered, market-level tail, coverage from the
// market-wide calendar.
func TestGetBrokerNetFlow_MarketMode(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newHistoryTestUC(t, db)

	tn, tm := "TESTN", "TESTM"
	d1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	cleanupNetFlowTicker(t, db, tn, tm)

	seedTickerAndPrice(t, db, tn, d1)
	seedTickerAndPrice(t, db, tn, d2) // extra market-wide trading day, no broker rows
	seedTickerAndPrice(t, db, tm, d1)
	seedNetFlowDay(t, db, uc.Repo, tn, d1, nfRow(tn, "AK", "buy", d1, nfBuy10), nfRow(tn, "XL", "sell", d1, nfSell6))
	seedNetFlowDay(t, db, uc.Repo, tm, d1, nfRow(tm, "AK", "sell", d1, nfSell4), nfRow(tm, "XL", "buy", d1, nfSell7))

	res, err := uc.GetBrokerNetFlow(context.Background(), nil, &d1, &d2)
	if err != nil {
		t.Fatalf("GetBrokerNetFlow: %v", err)
	}

	if res.Mode != "market" || res.Ticker != "" {
		t.Errorf("mode = %q ticker = %q, want market/empty", res.Mode, res.Ticker)
	}
	if res.TradeDaysInWindow != 2 || res.CoveredDays != 1 {
		t.Errorf("coverage = %d/%d, want 1/2 (d2 has prices but no broker rows)", res.CoveredDays, res.TradeDaysInWindow)
	}
	if res.TickersCovered != 2 {
		t.Errorf("tickers_covered = %d, want 2", res.TickersCovered)
	}
	ak := findNetFlowRow(t, res.Rows, "AK")
	if ak.Buy != nfBuy10 || ak.Sell != nfSell4 || ak.Net != nfBuy10-nfSell4 {
		t.Errorf("AK = %+v, want buy 10B sell 4B net 6B", ak)
	}
	if ak.Sessions != 2 || ak.Tickers != 2 {
		t.Errorf("AK breadth = %d sessions/%d tickers, want 2/2", ak.Sessions, ak.Tickers)
	}
	// Attribution: AK net-bought TESTN 10B but net-sold TESTM 4B — the
	// aggregate +6B alone would hide that split.
	if len(ak.ByTicker) != 2 {
		t.Fatalf("AK by_ticker = %d entries, want 2: %+v", len(ak.ByTicker), ak.ByTicker)
	}
	if ak.ByTicker[0].Ticker != tn || ak.ByTicker[0].Net != nfBuy10 || ak.ByTicker[0].DaysShown != 1 {
		t.Errorf("AK by_ticker[0] = %+v, want TESTN net 10B days 1 (top accumulator first)", ak.ByTicker[0])
	}
	if ak.ByTicker[1].Ticker != tm || ak.ByTicker[1].Net != -nfSell4 {
		t.Errorf("AK by_ticker[1] = %+v, want TESTM net −4B", ak.ByTicker[1])
	}
	xl := findNetFlowRow(t, res.Rows, "XL")
	if xl.Net != nfSell7-nfSell6 || xl.Sessions != 2 || xl.Tickers != 2 {
		t.Errorf("XL = %+v, want net 1B sessions 2 tickers 2", xl)
	}
	if len(xl.ByTicker) != 2 || xl.ByTicker[0].Ticker != tm || xl.ByTicker[0].Net != nfSell7 {
		t.Errorf("XL by_ticker = %+v, want TESTM net 7B first", xl.ByTicker)
	}
	// Market tail = (6+4)B − (10+7)B = −7B.
	if res.OthersNet != (nfSell6+nfSell4)-(nfBuy10+nfSell7) {
		t.Errorf("others_net = %d, want −7B", res.OthersNet)
	}
	if res.Rows[0].BrokerCode != "AK" {
		t.Errorf("rows[0] = %s, want AK", res.Rows[0].BrokerCode)
	}
}

// TestGetBrokerNetFlow_EmptyWindow — no stored rows: empty list + coverage 0,
// not an error (matches the history tool's empty-range contract).
func TestGetBrokerNetFlow_EmptyWindow(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newHistoryTestUC(t, db)

	ticker := "TESTN"
	d1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	cleanupNetFlowTicker(t, db, ticker)
	seedTickerAndPrice(t, db, ticker, d1)
	seedTickerAndPrice(t, db, ticker, d2)

	res, err := uc.GetBrokerNetFlow(context.Background(), &ticker, &d1, &d2)
	if err != nil {
		t.Fatalf("empty window must not error, got: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(res.Rows))
	}
	if res.CoveredDays != 0 || res.TradeDaysInWindow != 2 || res.OthersNet != 0 {
		t.Errorf("empty window = covered %d/%d others %d, want 0/2/0",
			res.CoveredDays, res.TradeDaysInWindow, res.OthersNet)
	}
}

// TestGetBrokerNetFlow_DefaultWindow — nil to resolves the latest trading day;
// nil from defaults 30 calendar days earlier.
func TestGetBrokerNetFlow_DefaultWindow(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newHistoryTestUC(t, db)

	ticker := "TESTN"
	d1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	cleanupNetFlowTicker(t, db, ticker)
	seedTickerAndPrice(t, db, ticker, d1)
	seedTickerAndPrice(t, db, ticker, d2)
	seedNetFlowDay(t, db, uc.Repo, ticker, d2, nfRow(ticker, "AK", "buy", d2, nfBuy10))

	// from given, to nil → to = latest trading day (d2).
	res, err := uc.GetBrokerNetFlow(context.Background(), &ticker, &d1, nil)
	if err != nil {
		t.Fatalf("GetBrokerNetFlow: %v", err)
	}
	if res.From != "2026-08-10" || res.To != "2026-08-12" {
		t.Errorf("window = %s..%s, want 2026-08-10..2026-08-12", res.From, res.To)
	}

	// both nil → to = d2, from = d2 − 30 days.
	res, err = uc.GetBrokerNetFlow(context.Background(), &ticker, nil, nil)
	if err != nil {
		t.Fatalf("GetBrokerNetFlow (full default): %v", err)
	}
	if res.To != "2026-08-12" || res.From != "2026-07-13" {
		t.Errorf("full default window = %s..%s, want 2026-07-13..2026-08-12", res.From, res.To)
	}
}

// Validation-path tests below touch no DB (the checks run before any query).
func TestGetBrokerNetFlow_InvalidTicker(t *testing.T) {
	uc := &BrokerStockSummaryUseCase{}
	bad := "bad ticker!"
	_, err := uc.GetBrokerNetFlow(context.Background(), &bad, nil, nil)
	if !errors.Is(err, ErrInvalidTicker) {
		t.Errorf("expected ErrInvalidTicker, got %v", err)
	}
}

func TestGetBrokerNetFlow_BackwardsRange(t *testing.T) {
	uc := &BrokerStockSummaryUseCase{}
	ticker := "BBCA"
	from := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	_, err := uc.GetBrokerNetFlow(context.Background(), &ticker, &from, &to)
	if !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange, got %v", err)
	}
}

func TestGetBrokerNetFlow_WindowTooLong(t *testing.T) {
	uc := &BrokerStockSummaryUseCase{}
	ticker := "BBCA"
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	_, err := uc.GetBrokerNetFlow(context.Background(), &ticker, &from, &to)
	if !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange (window > 180d), got %v", err)
	}
}
