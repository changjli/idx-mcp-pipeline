package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

func newHistoryTestUC(t *testing.T, db *sqlx.DB) *BrokerStockSummaryUseCase {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	// Fetcher is nil — the history path must never touch it.
	return NewBrokerStockSummaryUseCase(
		db, log, validator.New(), nil,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)
}

func seedHistoryDay(t *testing.T, db *sqlx.DB, repo *repository.BrokerStockSummaryRepository, ticker string, day time.Time, val int64) {
	t.Helper()
	rows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, Lot: i64p(100), Value: i64p(val), AvgPrice: i64p(100), Rank: i32p(1)},
		{Ticker: ticker, BrokerCode: "XL", Side: "sell", TradingDay: day, Lot: i64p(50), Value: i64p(val / 2), AvgPrice: i64p(99), Rank: i32p(1)},
	}
	totals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day,
		TVal: i64p(val), FNVal: i64p(val / 10), TLot: i64p(150), Avg: i64p(100),
	}
	if err := repo.UpsertDay(db, rows, totals); err != nil {
		t.Fatalf("UpsertDay %s: %v", day.Format("2006-01-02"), err)
	}
}

func TestGetStockBrokerSummaryHistory_ReadsStoredRange(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newHistoryTestUC(t, db)

	ticker := "TESTH"
	day1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	day3 := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	seedHistoryDay(t, db, uc.Repo, ticker, day1, 1_000_000_000)
	seedHistoryDay(t, db, uc.Repo, ticker, day2, 2_000_000_000)
	seedHistoryDay(t, db, uc.Repo, ticker, day3, 3_000_000_000)

	res, err := uc.GetStockBrokerSummaryHistory(context.Background(), ticker, day1, day2)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryHistory: %v", err)
	}

	if res.Ticker != ticker {
		t.Errorf("res.Ticker = %q, want %q", res.Ticker, ticker)
	}
	if len(res.Days) != 2 {
		t.Fatalf("expected 2 days in range, got %d", len(res.Days))
	}
	if res.Days[0].TradingDay != "2026-08-10" {
		t.Errorf("days[0].TradingDay = %q, want 2026-08-10", res.Days[0].TradingDay)
	}
	if len(res.Days[0].Buyers) != 1 || len(res.Days[0].Sellers) != 1 {
		t.Errorf("days[0] expected 1 buyer 1 seller, got %d/%d", len(res.Days[0].Buyers), len(res.Days[0].Sellers))
	}
	if res.Days[0].Buyers[0].BrokerCode != "AK" || res.Days[0].Buyers[0].Value != 1_000_000_000 {
		t.Errorf("days[0].buyers[0] = %+v, want AK/1000000000", res.Days[0].Buyers[0])
	}
	if res.Days[0].Totals.TVal != 1_000_000_000 {
		t.Errorf("days[0].Totals.TVal = %d, want 1000000000", res.Days[0].Totals.TVal)
	}
	// Issue 03: totals ride through; others_net recomputed from stored rows
	// (day1 = buy 1B / sell 0.5B → tail net = 0.5B − 1B = −0.5B).
	if res.Days[0].TotalBuyValue != 1_000_000_000 || res.Days[0].TotalSellValue != 1_000_000_000 {
		t.Errorf("days[0] totals = %d/%d, want 1000000000/1000000000",
			res.Days[0].TotalBuyValue, res.Days[0].TotalSellValue)
	}
	if res.Days[0].OthersNet != -500_000_000 {
		t.Errorf("days[0].OthersNet = %d, want -500000000 (sell 0.5B − buy 1B)", res.Days[0].OthersNet)
	}
	if res.Days[0].Totals.OthersNet != -500_000_000 {
		t.Errorf("days[0].Totals.OthersNet = %d, want -500000000", res.Days[0].Totals.OthersNet)
	}
	if res.Days[1].TradingDay != "2026-08-11" {
		t.Errorf("days[1].TradingDay = %q, want 2026-08-11", res.Days[1].TradingDay)
	}
}

func TestGetStockBrokerSummaryHistory_EmptyRange(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newHistoryTestUC(t, db)

	res, err := uc.GetStockBrokerSummaryHistory(
		context.Background(), "ZZZZ",
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("empty range should not error, got: %v", err)
	}
	if len(res.Days) != 0 {
		t.Errorf("expected 0 days for empty range, got %d", len(res.Days))
	}
}

func TestGetStockBrokerSummaryHistory_InvalidTicker(t *testing.T) {
	uc := &BrokerStockSummaryUseCase{}
	_, err := uc.GetStockBrokerSummaryHistory(
		context.Background(), "bad ticker!",
		time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidTicker) {
		t.Errorf("expected ErrInvalidTicker, got %v", err)
	}
}

func TestGetStockBrokerSummaryHistory_BackwardsRange(t *testing.T) {
	uc := &BrokerStockSummaryUseCase{}
	_, err := uc.GetStockBrokerSummaryHistory(
		context.Background(), "BBCA",
		time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange, got %v", err)
	}
}

func i32p(v int32) *int32 { return &v }
