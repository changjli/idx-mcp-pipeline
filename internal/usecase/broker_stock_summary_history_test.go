package usecase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
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

// seedHistoryDay stores one one-day foreign window.
func seedHistoryDay(t *testing.T, db *sqlx.DB, repo *repository.BrokerStockSummaryRepository, ticker string, day time.Time, val int64) {
	t.Helper()
	rows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular, Lot: i64p(100), Value: i64p(val), AvgPrice: i64p(100), Rank: i32p(1), Source: entity.BrokerSourceLive},
		{Ticker: ticker, BrokerCode: "XL", Side: "sell", TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular, Lot: i64p(50), Value: i64p(val / 2), AvgPrice: i64p(99), Rank: i32p(1), Source: entity.BrokerSourceLive},
	}
	totals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular,
		FNVal: i64p(val - val/2), Source: entity.BrokerSourceLive,
	}
	if err := repo.UpsertWindow(db, rows, totals); err != nil {
		t.Fatalf("UpsertWindow %s: %v", day.Format("2006-01-02"), err)
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
	if res.Grain != grainDay {
		t.Errorf("res.Grain = %q, want day", res.Grain)
	}
	if len(res.Windows) != 2 {
		t.Fatalf("expected 2 windows in range, got %d", len(res.Windows))
	}
	first := res.Windows[0]
	if first.From != "2026-08-10" || first.To != "2026-08-10" || first.Grain != grainDay {
		t.Errorf("windows[0] range = %+v, want 2026-08-10 day", first)
	}
	if len(first.Buyers) != 1 || len(first.Sellers) != 1 {
		t.Errorf("windows[0] expected 1 buyer 1 seller, got %d/%d", len(first.Buyers), len(first.Sellers))
	}
	if first.Buyers[0].BrokerCode != "AK" || first.Buyers[0].Value != 1_000_000_000 {
		t.Errorf("windows[0].buyers[0] = %+v, want AK/1000000000", first.Buyers[0])
	}
	if first.TotalBuyValue != 1_000_000_000 || first.TotalSellValue != 500_000_000 {
		t.Errorf("windows[0] gross = %d/%d, want 1B/0.5B", first.TotalBuyValue, first.TotalSellValue)
	}
	if first.Totals.TVal != nil {
		t.Errorf("windows[0].Totals.TVal = %v, want null in the foreign lane", *first.Totals.TVal)
	}
	if first.Totals.FNVal == nil || *first.Totals.FNVal != 500_000_000 {
		t.Errorf("windows[0].Totals.FNVal = %v, want 0.5B", first.Totals.FNVal)
	}
	if res.Windows[1].To != "2026-08-11" {
		t.Errorf("windows[1].To = %q, want 2026-08-11", res.Windows[1].To)
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
	if len(res.Windows) != 0 {
		t.Errorf("expected 0 windows for empty range, got %d", len(res.Windows))
	}
	if res.Grain != grainNone {
		t.Errorf("grain = %q, want none", res.Grain)
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

// TestGetStockBrokerSummaryHistory_MixedGrain — a table holding a day row and a
// month chunk reads as grain: "mixed", with each entry stating its own range.
func TestGetStockBrokerSummaryHistory_MixedGrain(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newHistoryTestUC(t, db)

	ticker := "TESTX"
	day := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	// A month chunk plus a day row in the same range.
	rows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: day, WinFrom: day.AddDate(0, 0, -20), Investor: broker.InvestorForeign, Market: broker.MarketRegular, Lot: i64p(100), Value: i64p(1_000_000), AvgPrice: i64p(100), Rank: i32p(1), Source: entity.BrokerSourceArchive},
	}
	totals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day, WinFrom: day.AddDate(0, 0, -20), Investor: broker.InvestorForeign, Market: broker.MarketRegular,
		FNVal: i64p(1_000_000), Source: entity.BrokerSourceArchive,
	}
	if err := uc.Repo.UpsertWindow(db, rows, totals); err != nil {
		t.Fatalf("UpsertWindow(chunk): %v", err)
	}
	seedHistoryDay(t, db, uc.Repo, ticker, day.AddDate(0, 0, -25), 2_000_000)

	res, err := uc.GetStockBrokerSummaryHistory(context.Background(), ticker, day.AddDate(0, 0, -30), day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryHistory: %v", err)
	}
	if len(res.Windows) != 2 {
		t.Fatalf("windows = %d, want 2 (a chunk and a day)", len(res.Windows))
	}
	if res.Grain != grainMixed {
		t.Errorf("grain = %q, want mixed", res.Grain)
	}
}

// TestGetStockBrokerSummaryHistory_ProjectsTopTen — the stored set is the full
// broker list, the response is the top ten per side, and coverage is measured
// over all of it before projection.
func TestGetStockBrokerSummaryHistory_ProjectsTopTen(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newHistoryTestUC(t, db)

	ticker := "TESTY"
	day := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	const n = 12
	rows := make([]entity.BrokerStockSummary, 0, 2*n)
	for i := 0; i < n; i++ {
		code := fmt.Sprintf("B%02d", i)
		rows = append(rows,
			entity.BrokerStockSummary{Ticker: ticker, BrokerCode: code, Side: "buy", TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular, Lot: i64p(100), Value: i64p(1_000_000_000), AvgPrice: i64p(100), Rank: i32p(int32(i + 1)), Source: entity.BrokerSourceLive},
			entity.BrokerStockSummary{Ticker: ticker, BrokerCode: code, Side: "sell", TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular, Lot: i64p(100), Value: i64p(900_000_000), AvgPrice: i64p(100), Rank: i32p(int32(i + 1)), Source: entity.BrokerSourceLive},
		)
	}
	totals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day, WinFrom: day, Investor: broker.InvestorForeign, Market: broker.MarketRegular,
		FNVal: i64p(n * 100_000_000), Source: entity.BrokerSourceLive,
	}
	if err := uc.Repo.UpsertWindow(db, rows, totals); err != nil {
		t.Fatalf("UpsertWindow: %v", err)
	}

	res, err := uc.GetStockBrokerSummaryHistory(context.Background(), ticker, day, day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryHistory: %v", err)
	}
	if len(res.Windows) != 1 {
		t.Fatalf("windows = %d, want 1", len(res.Windows))
	}
	got := res.Windows[0]
	if len(got.Buyers) != brokerSummaryTopN || len(got.Sellers) != brokerSummaryTopN {
		t.Errorf("projection = %d buyers/%d sellers, want %d per side", len(got.Buyers), len(got.Sellers), brokerSummaryTopN)
	}
	if got.Buyers[0].Rank != 1 || got.Buyers[len(got.Buyers)-1].Rank != brokerSummaryTopN {
		t.Errorf("buyer ranks = %d..%d, want 1..%d", got.Buyers[0].Rank, got.Buyers[len(got.Buyers)-1].Rank, brokerSummaryTopN)
	}
	if !got.Coverage.Complete || got.Coverage.Brokers != n || got.Coverage.Buyers != n || got.Coverage.Sellers != n {
		t.Errorf("coverage = %+v, want complete over all %d stored brokers", got.Coverage, n)
	}

	stored, err := uc.Repo.FindRowsInWindows(db, []string{ticker}, brokerReadLane, day, day)
	if err != nil {
		t.Fatalf("FindRowsInWindows: %v", err)
	}
	if len(stored) != 2*n {
		t.Errorf("stored rows = %d, want %d (full set)", len(stored), 2*n)
	}
}

func i32p(v int32) *int32 { return &v }
