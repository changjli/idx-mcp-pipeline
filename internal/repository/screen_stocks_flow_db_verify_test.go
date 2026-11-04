package repository

import (
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// The screener's broker-flow column is one exact-cover solve over
// broker_stock_summary_totals, and this is the only place it runs against a real
// Postgres: the window walk, the exact-cover rule (a partial cover is never
// partially summed), the no-double-count rule for overlapping caches, and the
// sparse-population honesty (an uncovered ticker is absent, never a zero).
// Skipped unless IDX_MCP_DB_DSN is set; cleanup is scoped to the TESTS* tickers.
const (
	flowTickerCovered  = "TESTSA" // day rows exactly tile 0..9
	flowTickerPartial  = "TESTSB" // only the anchor day: not an exact cover
	flowTickerZero     = "TESTSC" // exact cover whose net is exactly zero
	flowTickerAbsent   = "TESTSD" // no stored broker rows at all
	flowTickerNullNet  = "TESTSE" // an exact cover whose f_nval is null
	flowTickerOutside  = "TESTSF" // stored, but entirely outside the window
	flowTickerUniverse = "TESTSG" // price rows only, no broker rows
	flowTickerOverlap  = "TESTSH" // a 10-day cache overlapping day rows 0..4
)

var flowTickers = []string{
	flowTickerCovered, flowTickerPartial, flowTickerZero,
	flowTickerAbsent, flowTickerNullNet, flowTickerOutside, flowTickerUniverse, flowTickerOverlap,
}

const flowAnchor = "2026-08-12"

// TestSumForeignNetByTickers_ExactCover verifies the screener's flow read end to
// end against Postgres.
func TestSumForeignNetByTickers_ExactCover(t *testing.T) {
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

	// An exact cover over the whole 10-day range, with the 5-day window's sum
	// (0..4) being 3B and the wide sum 201B.
	seedDayWindow(t, db, brokerRepo, flowTickerCovered, map[int]int64{
		0: 0, 1: 1_000_000_000, 2: 0, 3: 2_000_000_000, 4: 0,
		5: 0, 6: 0, 7: 99_000_000_000, 8: 0, 9: 99_000_000_000,
	})
	// One stored day cannot exactly cover a 5-day window.
	seedDayWindow(t, db, brokerRepo, flowTickerPartial, map[int]int64{0: 750_000_000})
	// An exact cover whose net is exactly zero: observed, distinguishable from
	// missing by the day count.
	seedDayWindow(t, db, brokerRepo, flowTickerZero, map[int]int64{
		0: 400_000_000, 1: -400_000_000, 2: 0, 3: 0, 4: 0,
	})
	// An exact cover whose every footer has a null foreign net: unobserved.
	seedDayWindow(t, db, brokerRepo, flowTickerNullNet, map[int]int64{0: 0, 1: 0, 2: 0, 3: 0, 4: 0}, true)
	// Stored, but entirely outside the window.
	seedDayWindow(t, db, brokerRepo, flowTickerOutside, map[int]int64{8: 5_000_000_000, 9: 5_000_000_000})
	// A 10-day answer cache overlapping day rows 0..4. Neither the 5-day read
	// (cache is wider than the request) nor the 10-day read (cache wins, day
	// rows skipped) may double-count.
	seedDayWindow(t, db, brokerRepo, flowTickerOverlap, map[int]int64{0: 0, 1: 1_000_000_000, 2: 0, 3: 2_000_000_000, 4: 0})
	seedWindowTotals(t, db, brokerRepo, flowTickerOverlap, day(9), anchor, 7_000_000_000)

	window, err := brokerRepo.SumForeignNetByTickers(db, flowTickers, anchor, 5)
	if err != nil {
		t.Fatalf("SumForeignNetByTickers: %v", err)
	}
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

	if row := got[flowTickerCovered]; row.ForeignNet != 3_000_000_000 || row.Days != 5 {
		t.Errorf("%s = %d over %d days, want 3B over 5 days", flowTickerCovered, row.ForeignNet, row.Days)
	}
	// Observed zero over a full cover.
	if row := got[flowTickerZero]; row.ForeignNet != 0 || row.Days != 5 {
		t.Errorf("%s = %d over %d days, want 0 over 5 days (observed, not missing)", flowTickerZero, row.ForeignNet, row.Days)
	}
	// The 5-day read uses the day rows: the 10-day cache cannot answer it.
	if row := got[flowTickerOverlap]; row.ForeignNet != 3_000_000_000 || row.Days != 5 {
		t.Errorf("%s = %d over %d days, want 3B over 5 days (cache is wider than the request)",
			flowTickerOverlap, row.ForeignNet, row.Days)
	}

	for _, ticker := range []string{flowTickerPartial, flowTickerNullNet, flowTickerOutside, flowTickerAbsent, flowTickerUniverse} {
		if row, ok := got[ticker]; ok {
			t.Errorf("%s should be unobserved (no exact cover), got %+v", ticker, row)
		}
	}

	// A wider window reaches the days a narrow one excluded.
	wide, err := brokerRepo.SumForeignNetByTickers(db, flowTickers, anchor, 10)
	if err != nil {
		t.Fatalf("SumForeignNetByTickers at 10 days: %v", err)
	}
	wideGot := map[string]TickerForeignNet{}
	for _, row := range wide.Rows {
		wideGot[row.Ticker] = row
	}
	if wide.TradeDays != 10 {
		t.Errorf("trade_days at 10 = %d, want 10", wide.TradeDays)
	}
	if row := wideGot[flowTickerCovered]; row.ForeignNet != 201_000_000_000 || row.Days != 10 {
		t.Errorf("wide %s = %d over %d days, want 201B over 10 days", flowTickerCovered, row.ForeignNet, row.Days)
	}
	// The 10-day cache answers the 10-day read; its overlapping day rows are
	// skipped, not added.
	if row := wideGot[flowTickerOverlap]; row.ForeignNet != 7_000_000_000 || row.Days != 10 {
		t.Errorf("wide %s = %d over %d days, want 7B over 10 days (cache wins, no double count)",
			flowTickerOverlap, row.ForeignNet, row.Days)
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
	if len(over.Rows) != 1 || over.Rows[0].ForeignNet != 201_000_000_000 {
		t.Errorf("over-long window = %+v, want the 10 stored days summed", over.Rows)
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

// seedDayWindow stores one one-day foreign window per `back` day before the
// anchor, with the given foreign net. nullNet stores null f_nval instead, which
// is a stored day without an observed foreign net.
func seedDayWindow(t *testing.T, db *sqlx.DB, repo *BrokerStockSummaryRepository, ticker string, nets map[int]int64, nullNet ...bool) {
	t.Helper()
	anchor, _ := time.Parse("2006-01-02", flowAnchor)
	nulls := len(nullNet) > 0 && nullNet[0]
	for back, net := range nets {
		day := anchor.AddDate(0, 0, -back)
		var stored *int64
		if !nulls {
			stored = i64(net)
		}
		totals := &entity.BrokerStockSummaryTotals{
			Ticker:     ticker,
			TradingDay: day,
			WinFrom:    day,
			Investor:   broker.InvestorForeign,
			Market:     broker.MarketRegular,
			FNVal:      stored,
			Source:     entity.BrokerSourceArchive,
		}
		if err := repo.UpsertWindow(db, nil, totals); err != nil {
			t.Fatalf("seed flow totals %s day-%d: %v", ticker, back, err)
		}
	}
}

// seedWindowTotals stores one multi-day window footer with a foreign net.
func seedWindowTotals(t *testing.T, db *sqlx.DB, repo *BrokerStockSummaryRepository, ticker string, from, to time.Time, net int64) {
	t.Helper()
	totals := &entity.BrokerStockSummaryTotals{
		Ticker:     ticker,
		TradingDay: to,
		WinFrom:    from,
		Investor:   broker.InvestorForeign,
		Market:     broker.MarketRegular,
		FNVal:      i64(net),
		Source:     entity.BrokerSourceWindow,
	}
	if err := repo.UpsertWindow(db, nil, totals); err != nil {
		t.Fatalf("seed window totals %s: %v", ticker, err)
	}
}
