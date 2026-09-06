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

// TestBrokerStockSummaryRepository_EndToEnd verifies the atomic upsert path
// against a real Postgres: rows land with the correct buy/sell split, the
// totals row is written, refetching the same day is idempotent, and brokers
// that drop out of the top-10 on a refetch are removed. Skipped unless
// IDX_MCP_DB_DSN is set. Cleanup is scoped to the TESTX ticker only.
func TestBrokerStockSummaryRepository_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewBrokerStockSummaryRepository(log)
	ticker := "TESTX"
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	// Clean slate.
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	rows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, Lot: i64(169544), Value: i64(15_000_000_000), AvgPrice: i64(883), Rank: i32(1)},
		{Ticker: ticker, BrokerCode: "XL", Side: "sell", TradingDay: day, Lot: i64(139188), Value: i64(12_300_000_000), AvgPrice: i64(881), Rank: i32(1)},
		{Ticker: ticker, BrokerCode: "YP", Side: "buy", TradingDay: day, Lot: i64(40722), Value: i64(3_600_000_000), AvgPrice: i64(878), Rank: i32(6)},
	}
	totals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day,
		TVal: i64(71_200_000_000), FNVal: i64(11_100_000_000), TLot: i64(808975), Avg: i64(880),
	}

	if err := repo.UpsertDay(db, rows, totals); err != nil {
		t.Fatalf("UpsertDay: %v", err)
	}

	// Verify buy/sell split and values.
	stored, err := repo.FindByTickerAndDay(db, ticker, day)
	if err != nil {
		t.Fatalf("FindByTickerAndDay: %v", err)
	}
	if len(stored) != 3 {
		t.Fatalf("expected 3 stored rows, got %d", len(stored))
	}
	// ORDER BY side, rank → all buys (rank asc) then all sells.
	if stored[0].Side != "buy" || stored[0].BrokerCode != "AK" {
		t.Errorf("row[0] = %s/%s, want buy/AK", stored[0].Side, stored[0].BrokerCode)
	}
	if stored[1].Side != "buy" || stored[1].BrokerCode != "YP" {
		t.Errorf("row[1] = %s/%s, want buy/YP", stored[1].Side, stored[1].BrokerCode)
	}
	if stored[2].Side != "sell" || stored[2].BrokerCode != "XL" {
		t.Errorf("row[2] = %s/%s, want sell/XL", stored[2].Side, stored[2].BrokerCode)
	}
	if stored[0].Value == nil || *stored[0].Value != 15_000_000_000 {
		t.Errorf("row[0].Value = %v, want 15000000000", stored[0].Value)
	}

	storedTotals, err := repo.FindTotalsByTickerAndDay(db, ticker, day)
	if err != nil {
		t.Fatalf("FindTotalsByTickerAndDay: %v", err)
	}
	if storedTotals.TVal == nil || *storedTotals.TVal != 71_200_000_000 {
		t.Errorf("totals.TVal = %v, want 71200000000", storedTotals.TVal)
	}

	// Refetch same day → idempotent, row count unchanged.
	if err := repo.UpsertDay(db, rows, totals); err != nil {
		t.Fatalf("second UpsertDay: %v", err)
	}
	stored2, err := repo.FindByTickerAndDay(db, ticker, day)
	if err != nil {
		t.Fatalf("FindByTickerAndDay after refetch: %v", err)
	}
	if len(stored2) != 3 {
		t.Errorf("refetch changed row count: got %d, want 3", len(stored2))
	}

	// Broker YP drops out of the top-10 on the next refetch → row removed.
	shrunk := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, Lot: i64(169544), Value: i64(15_000_000_000), AvgPrice: i64(883), Rank: i32(1)},
		{Ticker: ticker, BrokerCode: "XL", Side: "sell", TradingDay: day, Lot: i64(139188), Value: i64(12_300_000_000), AvgPrice: i64(881), Rank: i32(1)},
	}
	if err := repo.UpsertDay(db, shrunk, totals); err != nil {
		t.Fatalf("shrunk UpsertDay: %v", err)
	}
	stored3, err := repo.FindByTickerAndDay(db, ticker, day)
	if err != nil {
		t.Fatalf("FindByTickerAndDay after shrink: %v", err)
	}
	if len(stored3) != 2 {
		t.Errorf("expected 2 rows after top-10 shrink, got %d (stale row not removed)", len(stored3))
	}
	for _, r := range stored3 {
		if r.BrokerCode == "YP" {
			t.Errorf("stale row YP still present after it dropped out of the top-10")
		}
	}
}

// TestHasStoredDay verifies the sweep's skip-if-stored guard: true for a
// ticker+day with rows, false for a day with no rows, and scoped per ticker.
// Skipped unless IDX_MCP_DB_DSN is set.
func TestHasStoredDay(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := NewBrokerStockSummaryRepository(log)

	ticker := "TESTH"
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	// Empty ticker → no rows.
	ok, err := repo.HasStoredDay(db, ticker, day)
	if err != nil {
		t.Fatalf("HasStoredDay empty: %v", err)
	}
	if ok {
		t.Error("HasStoredDay = true for empty ticker, want false")
	}

	// After upsert → rows present.
	rows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, Lot: i64(1), Value: i64(2), AvgPrice: i64(3), Rank: i32(1)},
	}
	if err := repo.UpsertDay(db, rows, nil); err != nil {
		t.Fatalf("UpsertDay: %v", err)
	}
	ok, err = repo.HasStoredDay(db, ticker, day)
	if err != nil {
		t.Fatalf("HasStoredDay after upsert: %v", err)
	}
	if !ok {
		t.Error("HasStoredDay = false after upsert, want true")
	}

	// Different day → no rows.
	other := day.AddDate(0, 0, 1)
	ok, err = repo.HasStoredDay(db, ticker, other)
	if err != nil {
		t.Fatalf("HasStoredDay other day: %v", err)
	}
	if ok {
		t.Error("HasStoredDay = true for a day with no rows, want false")
	}
}

func i64(v int64) *int64 { return &v }
func i32(v int32) *int32 { return &v }
