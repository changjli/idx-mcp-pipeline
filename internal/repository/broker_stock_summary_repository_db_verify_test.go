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

// TestStoredForeignDayTickers verifies the refresher's skip-if-stored guard:
// only a foreign-lane one-day window counts, and the batch is scoped per
// ticker. An all-lane legacy day must not mask a missing foreign observation.
func TestStoredForeignDayTickers(t *testing.T) {
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

	// Empty ticker → absent.
	stored, err := repo.StoredForeignDayTickers(db, day, []string{ticker})
	if err != nil {
		t.Fatalf("StoredForeignDayTickers empty: %v", err)
	}
	if stored[ticker] {
		t.Error("stored = true for empty ticker, want false")
	}

	// An all-lane legacy day does not satisfy the foreign guard.
	legacy := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, Lot: i64(1), Value: i64(2), AvgPrice: i64(3), Rank: i32(1)},
	}
	if err := repo.UpsertDay(db, legacy, nil); err != nil {
		t.Fatalf("UpsertDay: %v", err)
	}
	stored, err = repo.StoredForeignDayTickers(db, day, []string{ticker})
	if err != nil {
		t.Fatalf("StoredForeignDayTickers all lane: %v", err)
	}
	if stored[ticker] {
		t.Error("all-lane day counted as a stored foreign day")
	}

	// A foreign one-day window counts.
	foreign := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, WinFrom: day,
			Investor: "f", Market: "RG", Lot: i64(1), Value: i64(2), AvgPrice: i64(3), Rank: i32(1),
			Source: entity.BrokerSourceSweep},
	}
	if err := repo.UpsertWindow(db, foreign, nil); err != nil {
		t.Fatalf("UpsertWindow: %v", err)
	}
	stored, err = repo.StoredForeignDayTickers(db, day, []string{ticker})
	if err != nil {
		t.Fatalf("StoredForeignDayTickers foreign: %v", err)
	}
	if !stored[ticker] {
		t.Error("foreign one-day window not reported as stored")
	}

	// A different day is still absent.
	stored, err = repo.StoredForeignDayTickers(db, day.AddDate(0, 0, 1), []string{ticker})
	if err != nil {
		t.Fatalf("StoredForeignDayTickers other day: %v", err)
	}
	if stored[ticker] {
		t.Error("stored = true for a different day")
	}
}

// TestBrokerStockSummaryRepository_WindowLaneCoexistence is ticket 10a's
// additive guarantee: an f window and an all-investor legacy day coexist for the
// same ticker, the upsert replaces only its own window+lane, and every old read
// stays pinned to the one-day all lane.
func TestBrokerStockSummaryRepository_WindowLaneCoexistence(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := NewBrokerStockSummaryRepository(log)

	ticker := "TESTW"
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	winFrom := day.AddDate(0, 0, -4)

	cleanup := func() {
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Legacy all-investor day.
	allRows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, Lot: i64(10), Value: i64(1000), AvgPrice: i64(100), Rank: i32(1)},
	}
	allTotals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day, TVal: i64(1000), TLot: i64(10), Avg: i64(100), Source: entity.BrokerSourceLegacyIPOT,
	}
	if err := repo.UpsertDay(db, allRows, allTotals); err != nil {
		t.Fatalf("UpsertDay(all): %v", err)
	}

	// Foreign day for the same ticker+day: same window key, different lane.
	fTotals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular,
		FNVal: i64(-100), Source: entity.BrokerSourceLive,
	}
	fRows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular, Lot: i64(5), Value: i64(500), AvgPrice: i64(100), Rank: i32(1), Source: entity.BrokerSourceLive},
	}
	if err := repo.UpsertWindow(db, fRows, fTotals); err != nil {
		t.Fatalf("UpsertWindow(f day): %v", err)
	}

	// Old readers see only the all lane.
	stored, err := repo.FindByTickerAndDay(db, ticker, day)
	if err != nil {
		t.Fatalf("FindByTickerAndDay: %v", err)
	}
	if len(stored) != 1 || stored[0].Investor != broker.InvestorAll || !stored[0].WinFrom.Equal(day) {
		t.Fatalf("old read returned %+v, want the single all-lane day row", stored)
	}
	oldTotals, err := repo.FindTotalsByTickerAndDay(db, ticker, day)
	if err != nil {
		t.Fatalf("FindTotalsByTickerAndDay: %v", err)
	}
	if oldTotals.Investor != broker.InvestorAll || oldTotals.TVal == nil || *oldTotals.TVal != 1000 {
		t.Fatalf("old totals = %+v, want the all-lane day totals", oldTotals)
	}

	// Refetching the f day replaces only the f day, not the all day.
	fRows = append(fRows, entity.BrokerStockSummary{
		Ticker: ticker, BrokerCode: "CC", Side: "sell", TradingDay: day, WinFrom: day,
		Investor: broker.InvestorForeign, Market: broker.MarketRegular,
		Lot: i64(5), Value: i64(400), AvgPrice: i64(80), Rank: i32(1), Source: entity.BrokerSourceLive,
	})
	if err := repo.UpsertWindow(db, fRows, fTotals); err != nil {
		t.Fatalf("UpsertWindow(f day refetch): %v", err)
	}

	// A foreign window ending on the same day, with a different win_from, is a
	// separate key and must not disturb either day row.
	winRows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "HD", Side: "buy", TradingDay: day, WinFrom: winFrom,
			Investor: broker.InvestorForeign, Market: broker.MarketRegular,
			Lot: i64(7), Value: i64(700), AvgPrice: i64(100), Rank: i32(1), Source: entity.BrokerSourceWindow},
	}
	winTotals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day, WinFrom: winFrom, Investor: broker.InvestorForeign,
		Market: broker.MarketRegular, FNVal: i64(700), Source: entity.BrokerSourceWindow,
	}
	if err := repo.UpsertWindow(db, winRows, winTotals); err != nil {
		t.Fatalf("UpsertWindow(f window): %v", err)
	}

	var byLane struct {
		All int `db:"all_rows"`
		F   int `db:"f_rows"`
	}
	if err := db.Get(&byLane, `SELECT
		COUNT(*) FILTER (WHERE investor = 'all') AS all_rows,
		COUNT(*) FILTER (WHERE investor = 'f') AS f_rows
		FROM broker_stock_summaries WHERE ticker = $1`, ticker); err != nil {
		t.Fatalf("count by lane: %v", err)
	}
	if byLane.All != 1 || byLane.F != 3 {
		t.Fatalf("rows by lane = all %d / f %d, want 1 / 3 (f day refetch replaced, f window separate)", byLane.All, byLane.F)
	}

	// The old range read also ignores both f windows.
	rangeRows, err := repo.FindByTickerAndDateRange(db, ticker, winFrom, day)
	if err != nil {
		t.Fatalf("FindByTickerAndDateRange: %v", err)
	}
	if len(rangeRows) != 1 || rangeRows[0].Investor != broker.InvestorAll {
		t.Fatalf("old range read returned %d rows, want the single all-lane day", len(rangeRows))
	}

	// The f totals row has no day totals, only f_nval.
	var fStored entity.BrokerStockSummaryTotals
	if err := db.Get(&fStored, `SELECT * FROM broker_stock_summary_totals
		WHERE ticker = $1 AND trading_day = $2 AND win_from = $2 AND investor = 'f'`, ticker, day); err != nil {
		t.Fatalf("read f totals: %v", err)
	}
	if fStored.TVal != nil || fStored.TLot != nil || fStored.Avg != nil {
		t.Errorf("f totals day columns = %v/%v/%v, want NULL", fStored.TVal, fStored.TLot, fStored.Avg)
	}
	if fStored.FNVal == nil || *fStored.FNVal != -100 {
		t.Errorf("f totals f_nval = %v, want -100", fStored.FNVal)
	}

	// A row set with mixed lanes is a programming error, not a silent write.
	bad := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular},
		{Ticker: ticker, BrokerCode: "CC", Side: "sell", TradingDay: day, WinFrom: day, Investor: broker.InvestorAll, Market: broker.MarketRegular},
	}
	if err := repo.UpsertWindow(db, bad, nil); err == nil {
		t.Fatal("UpsertWindow accepted a row set with mixed investor lanes")
	}
}

func i64(v int64) *int64 { return &v }
func i32(v int32) *int32 { return &v }
