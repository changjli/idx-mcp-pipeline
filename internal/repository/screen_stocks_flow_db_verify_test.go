package repository

import (
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// The screener's broker-flow column is one statement over
// broker_stock_summary_totals, and this is the only place it runs against a
// real Postgres: the window walk, the sparse-population honesty (an uncovered
// ticker is absent, never a zero), and the day count that makes the value
// readable either hold in SQL or the column silently lies. Skipped unless
// IDX_MCP_DB_DSN is set; cleanup is scoped to the TESTS* tickers only.
const (
	flowTickerCovered  = "TESTSA" // foreign net on 3 of the seeded days
	flowTickerPartial  = "TESTSB" // foreign net on the anchor day only
	flowTickerZero     = "TESTSC" // stored, but the window's net is exactly zero
	flowTickerAbsent   = "TESTSD" // no stored broker rows at all
	flowTickerNullNet  = "TESTSE" // stored footer rows whose f_nval is null
	flowTickerOutside  = "TESTSF" // stored, but older than the window
	flowTickerUniverse = "TESTSG" // price rows only, no broker rows
)

var flowTickers = []string{
	flowTickerCovered, flowTickerPartial, flowTickerZero,
	flowTickerAbsent, flowTickerNullNet, flowTickerOutside, flowTickerUniverse,
}

// TestSumForeignNetByTickers_WindowAndCoverage verifies the screener's flow
// read end to end against Postgres: the window walks stored trading days, only
// stored footer rows inside it count, and a ticker with nothing stored is
// absent from the result rather than present as a zero.
func TestSumForeignNetByTickers_WindowAndCoverage(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	priceRepo := NewDailyPriceRepository(logrus.New())
	brokerRepo := NewBrokerStockSummaryRepository(logrus.New())

	cleanup := func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = ANY($1)", flowTickers)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = ANY($1)", flowTickers)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = ANY($1)", flowTickers)
		db.MustExec("DELETE FROM tickers WHERE code = ANY($1)", flowTickers)
	}
	cleanup()
	t.Cleanup(cleanup)

	for _, code := range flowTickers {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", code, "Screener flow test "+code)
	}

	// 10 stored market-wide trading days, anchored on the last one, so "the last
	// N trading days" is exactly N days back.
	anchor := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	day := func(back int) time.Time { return anchor.AddDate(0, 0, -back) }

	for back := 0; back <= 9; back++ {
		if err := priceRepo.Upsert(db, &entity.DailyPrice{
			Ticker:     flowTickerUniverse,
			TradingDay: day(back),
			Open:       f64(100), High: f64(110), Low: f64(90), Close: f64(105),
			Volume: i64(1_000_000), Value: i64(10_000_000_000), Frequency: i32(1000),
			Source: "idx",
		}); err != nil {
			t.Fatalf("seed price %s: %v", day(back).Format("2006-01-02"), err)
		}
	}

	// Three days of foreign net inside a 5-day window, and two days outside it
	// (day 7 and day 9 back) that must not be summed in.
	seedFlowTotals(t, db, brokerRepo, flowTickerCovered, map[int]int64{
		0: 2_000_000_000, 2: -500_000_000, 4: 1_500_000_000, 7: 99_000_000_000, 9: 99_000_000_000,
	})
	// Only the anchor day is stored: observed, but thin.
	seedFlowTotals(t, db, brokerRepo, flowTickerPartial, map[int]int64{0: 750_000_000})
	// Stored and observed, net exactly zero — distinguishable from no data only
	// by the day count, which is the whole point of carrying it.
	seedFlowTotals(t, db, brokerRepo, flowTickerZero, map[int]int64{0: 400_000_000, 1: -400_000_000})
	// Footer rows with a null foreign net contribute no day and no value.
	seedFlowTotals(t, db, brokerRepo, flowTickerNullNet, map[int]int64{0: 0, 1: 0}, true)
	// Stored, but entirely outside the window.
	seedFlowTotals(t, db, brokerRepo, flowTickerOutside, map[int]int64{8: 5_000_000_000, 9: 5_000_000_000})

	window, err := brokerRepo.SumForeignNetByTickers(db, flowTickers, anchor, 5)
	if err != nil {
		t.Fatalf("SumForeignNetByTickers: %v", err)
	}

	// The window walks stored trading days: 5 back from the anchor is day 4.
	if got := window.From.Format("2006-01-02"); got != day(4).Format("2006-01-02") {
		t.Errorf("window start = %s, want %s", got, day(4).Format("2006-01-02"))
	}
	if window.TradeDays != 5 {
		t.Errorf("trade_days = %d, want 5", window.TradeDays)
	}

	got := map[string]TickerForeignNet{}
	for _, row := range window.Rows {
		got[row.Ticker] = row
	}

	// Summed over the window only: the day-7 and day-9 rows are out of range.
	if row := got[flowTickerCovered]; row.ForeignNet != 3_000_000_000 || row.Days != 3 {
		t.Errorf("%s = %d over %d days, want 3B over 3 days",
			flowTickerCovered, row.ForeignNet, row.Days)
	}
	// Observed and thin: 1 of 5 days, which reads only against the denominator.
	if row := got[flowTickerPartial]; row.ForeignNet != 750_000_000 || row.Days != 1 {
		t.Errorf("%s = %d over %d days, want 750M over 1 day",
			flowTickerPartial, row.ForeignNet, row.Days)
	}
	// A stored zero is observed, not missing.
	if row := got[flowTickerZero]; row.ForeignNet != 0 || row.Days != 2 {
		t.Errorf("%s = %d over %d days, want 0 over 2 days (observed, not missing)",
			flowTickerZero, row.ForeignNet, row.Days)
	}
	// A null foreign net is not a day: the row is absent entirely, so nothing
	// renders as a zero the caller never observed.
	if _, ok := got[flowTickerNullNet]; ok {
		t.Errorf("%s has only null foreign nets yet reported %+v, want absent",
			flowTickerNullNet, got[flowTickerNullNet])
	}
	// Stored, but outside: absent rather than a stale number.
	if _, ok := got[flowTickerOutside]; ok {
		t.Errorf("%s stored only outside the window yet reported %+v, want absent",
			flowTickerOutside, got[flowTickerOutside])
	}
	// Nothing stored at all: absent, never a zero.
	if _, ok := got[flowTickerAbsent]; ok {
		t.Errorf("%s has no stored broker rows yet reported %+v, want absent",
			flowTickerAbsent, got[flowTickerAbsent])
	}
	if _, ok := got[flowTickerUniverse]; ok {
		t.Errorf("%s has no broker rows yet reported %+v, want absent",
			flowTickerUniverse, got[flowTickerUniverse])
	}

	// A wider window reaches the days a narrow one excluded — this is what makes
	// the parameter meaningful rather than decorative.
	wide, err := brokerRepo.SumForeignNetByTickers(db, []string{flowTickerCovered}, anchor, 10)
	if err != nil {
		t.Fatalf("SumForeignNetByTickers at 10 days: %v", err)
	}
	if wide.TradeDays != 10 {
		t.Errorf("trade_days at 10 = %d, want 10", wide.TradeDays)
	}
	if len(wide.Rows) != 1 || wide.Rows[0].ForeignNet != 3_000_000_000+198_000_000_000 || wide.Rows[0].Days != 5 {
		t.Errorf("wide window = %+v, want 201B over 5 days", wide.Rows)
	}

	// A window longer than the stored history spans what exists, and the
	// denominator says so instead of pretending the ask was met.
	over, err := brokerRepo.SumForeignNetByTickers(db, []string{flowTickerCovered}, anchor, 30)
	if err != nil {
		t.Fatalf("SumForeignNetByTickers at 30 days: %v", err)
	}
	if over.TradeDays != 10 {
		t.Errorf("trade_days at an over-long window = %d, want the 10 stored days", over.TradeDays)
	}

	// No tickers: the window is still described, with no rows.
	none, err := brokerRepo.SumForeignNetByTickers(db, nil, anchor, 5)
	if err != nil {
		t.Fatalf("SumForeignNetByTickers with no tickers: %v", err)
	}
	if len(none.Rows) != 0 || none.TradeDays != 5 {
		t.Errorf("empty ticker list = %+v, want 0 rows and a 5-day window", none)
	}
}

// seedFlowTotals stores one footer totals row per `back` day before the anchor,
// with the given foreign net. nullNet stores the days with a null f_nval
// instead, which is a stored day without an observed foreign net.
func seedFlowTotals(t *testing.T, db *sqlx.DB, repo *BrokerStockSummaryRepository, ticker string, nets map[int]int64, nullNet ...bool) {
	t.Helper()
	anchor := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	nulls := len(nullNet) > 0 && nullNet[0]

	for back, net := range nets {
		var stored *int64
		if !nulls {
			stored = i64(net)
		}
		totals := &entity.BrokerStockSummaryTotals{
			Ticker:     ticker,
			TradingDay: anchor.AddDate(0, 0, -back),
			TVal:       i64(0),
			FNVal:      stored,
			TLot:       i64(0),
			Avg:        i64(0),
			OthersNet:  i64(0),
		}
		if err := repo.UpsertDay(db, nil, totals); err != nil {
			t.Fatalf("seed flow totals %s day-%d: %v", ticker, back, err)
		}
	}
}
