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

// TestBrokerStockSummaryRepository_DateRange verifies the date-range read
// against a real Postgres: rows + totals for a ticker across two trading days,
// ordered by trading day then side/rank, with the range boundary respected.
// Skipped unless IDX_MCP_DB_DSN is set. Cleanup scoped to TESTR.
func TestBrokerStockSummaryRepository_DateRange(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewBrokerStockSummaryRepository(log)
	ticker := "TESTR"
	day1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	day3 := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	// Seed three days; day3 is outside the queried range.
	seed := func(day time.Time, val int64) {
		rows := []entity.BrokerStockSummary{
			{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, Lot: i64(100), Value: i64(val), AvgPrice: i64(100), Rank: i32(1)},
			{Ticker: ticker, BrokerCode: "XL", Side: "sell", TradingDay: day, Lot: i64(50), Value: i64(val / 2), AvgPrice: i64(99), Rank: i32(1)},
		}
		totals := &entity.BrokerStockSummaryTotals{
			Ticker: ticker, TradingDay: day,
			TVal: i64(val), FNVal: i64(val / 10), TLot: i64(150), Avg: i64(100),
		}
		if err := repo.UpsertDay(db, rows, totals); err != nil {
			t.Fatalf("UpsertDay %s: %v", day.Format("2006-01-02"), err)
		}
	}
	seed(day1, 1_000_000_000)
	seed(day2, 2_000_000_000)
	seed(day3, 3_000_000_000)

	// Range day1..day2 → 4 rows (2 per day), day3 excluded.
	rows, err := repo.FindByTickerAndDateRange(db, ticker, day1, day2)
	if err != nil {
		t.Fatalf("FindByTickerAndDateRange: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows in range, got %d", len(rows))
	}
	// Ordered by trading_day then side/rank: day1 buy, day1 sell, day2 buy, day2 sell.
	if rows[0].TradingDay.Format("2006-01-02") != "2026-08-10" || rows[0].Side != "buy" {
		t.Errorf("row[0] = %s/%s, want 2026-08-10/buy", rows[0].TradingDay.Format("2006-01-02"), rows[0].Side)
	}
	if rows[1].TradingDay.Format("2006-01-02") != "2026-08-10" || rows[1].Side != "sell" {
		t.Errorf("row[1] = %s/%s, want 2026-08-10/sell", rows[1].TradingDay.Format("2006-01-02"), rows[1].Side)
	}
	if rows[2].TradingDay.Format("2006-01-02") != "2026-08-11" || rows[2].Side != "buy" {
		t.Errorf("row[2] = %s/%s, want 2026-08-11/buy", rows[2].TradingDay.Format("2006-01-02"), rows[2].Side)
	}
	if rows[3].TradingDay.Format("2006-01-02") != "2026-08-11" || rows[3].Side != "sell" {
		t.Errorf("row[3] = %s/%s, want 2026-08-11/sell", rows[3].TradingDay.Format("2006-01-02"), rows[3].Side)
	}

	totals, err := repo.FindTotalsByTickerAndDateRange(db, ticker, day1, day2)
	if err != nil {
		t.Fatalf("FindTotalsByTickerAndDateRange: %v", err)
	}
	if len(totals) != 2 {
		t.Fatalf("expected 2 totals rows in range, got %d", len(totals))
	}
	if totals[0].TVal == nil || *totals[0].TVal != 1_000_000_000 {
		t.Errorf("totals[0].TVal = %v, want 1000000000", totals[0].TVal)
	}
	if totals[1].TVal == nil || *totals[1].TVal != 2_000_000_000 {
		t.Errorf("totals[1].TVal = %v, want 2000000000", totals[1].TVal)
	}

	// Empty range (no rows between) → empty slice, no error.
	empty, err := repo.FindByTickerAndDateRange(db, ticker, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FindByTickerAndDateRange empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 rows for empty range, got %d", len(empty))
	}
}
