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

func newDailyPriceTestUC(t *testing.T, db *sqlx.DB) *DailyPriceUseCase {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return NewDailyPriceUseCase(db, log, validator.New(), repository.NewDailyPriceRepository(log))
}

func seedDailyPrice(t *testing.T, db *sqlx.DB, repo *repository.DailyPriceRepository, ticker string, day time.Time, close float64) {
	t.Helper()
	price := &entity.DailyPrice{
		Ticker:     ticker,
		TradingDay: day,
		Open:       f64p(close - 10),
		High:       f64p(close + 5),
		Low:        f64p(close - 20),
		Close:      f64p(close),
		Volume:     i64p(1_000_000),
		Value:      i64p(1_000_000_000),
		Frequency:  i32p(1000),
		Source:     "idx",
	}
	if err := repo.Upsert(db, price); err != nil {
		t.Fatalf("Upsert %s: %v", day.Format("2006-01-02"), err)
	}
}

func TestGetDailyPrices_ReadsStoredRange(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newDailyPriceTestUC(t, db)

	ticker := "TESTP"
	day1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	day3 := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", ticker, "Test Ticker "+ticker)

	seedDailyPrice(t, db, uc.Repo, ticker, day1, 100)
	seedDailyPrice(t, db, uc.Repo, ticker, day2, 110)
	seedDailyPrice(t, db, uc.Repo, ticker, day3, 120)

	res, err := uc.GetDailyPrices(context.Background(), ticker, day1, day2)
	if err != nil {
		t.Fatalf("GetDailyPrices: %v", err)
	}

	if res.Ticker != ticker {
		t.Errorf("res.Ticker = %q, want %q", res.Ticker, ticker)
	}
	if res.From != "2026-08-10" || res.To != "2026-08-11" {
		t.Errorf("res.From/To = %q/%q, want 2026-08-10/2026-08-11", res.From, res.To)
	}
	if len(res.Prices) != 2 {
		t.Fatalf("expected 2 rows in range, got %d", len(res.Prices))
	}
	if res.Prices[0].TradingDay != "2026-08-10" {
		t.Errorf("prices[0].TradingDay = %q, want 2026-08-10", res.Prices[0].TradingDay)
	}
	if res.Prices[0].Close == nil || *res.Prices[0].Close != 100 {
		t.Errorf("prices[0].Close = %v, want 100", res.Prices[0].Close)
	}
	if res.Prices[1].TradingDay != "2026-08-11" {
		t.Errorf("prices[1].TradingDay = %q, want 2026-08-11", res.Prices[1].TradingDay)
	}
	if res.Prices[1].Close == nil || *res.Prices[1].Close != 110 {
		t.Errorf("prices[1].Close = %v, want 110", res.Prices[1].Close)
	}
}

func TestGetDailyPrices_EmptyRange(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newDailyPriceTestUC(t, db)

	res, err := uc.GetDailyPrices(
		context.Background(), "ZZZZ",
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("empty range should not error, got: %v", err)
	}
	if len(res.Prices) != 0 {
		t.Errorf("expected 0 rows for empty range, got %d", len(res.Prices))
	}
}

func TestGetDailyPrices_InvalidTicker(t *testing.T) {
	uc := &DailyPriceUseCase{}
	_, err := uc.GetDailyPrices(
		context.Background(), "bad ticker!",
		time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidTicker) {
		t.Errorf("expected ErrInvalidTicker, got %v", err)
	}
}

func TestGetDailyPrices_BackwardsRange(t *testing.T) {
	uc := &DailyPriceUseCase{}
	_, err := uc.GetDailyPrices(
		context.Background(), "BBCA",
		time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange, got %v", err)
	}
}

func f64p(v float64) *float64 { return &v }
