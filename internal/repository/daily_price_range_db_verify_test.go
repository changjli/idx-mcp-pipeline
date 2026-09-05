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

// TestDailyPriceRepository_DateRange verifies FindByTickerAndDateRange against
// a real Postgres: in-range rows returned ascending, out-of-range excluded,
// empty range returns an empty slice. Skipped unless IDX_MCP_DB_DSN is set.
// Cleanup is scoped to the TESTQ ticker only.
func TestDailyPriceRepository_DateRange(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewDailyPriceRepository(log)
	ticker := "TESTQ"
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

	for _, day := range []time.Time{day1, day2, day3} {
		price := &entity.DailyPrice{
			Ticker:     ticker,
			TradingDay: day,
			Open:       f64(100),
			High:       f64(110),
			Low:        f64(90),
			Close:      f64(105),
			Volume:     i64(1_000_000),
			Value:      i64(1_000_000_000),
			Frequency:  i32(1000),
			Source:     "idx",
		}
		if err := repo.Upsert(db, price); err != nil {
			t.Fatalf("Upsert %s: %v", day.Format("2006-01-02"), err)
		}
	}

	rows, err := repo.FindByTickerAndDateRange(db, ticker, day1, day2)
	if err != nil {
		t.Fatalf("FindByTickerAndDateRange: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows in range, got %d", len(rows))
	}
	if !rows[0].TradingDay.Equal(day1) || !rows[1].TradingDay.Equal(day2) {
		t.Errorf("rows not ascending: %v, %v", rows[0].TradingDay, rows[1].TradingDay)
	}
	if rows[0].Close == nil || *rows[0].Close != 105 {
		t.Errorf("rows[0].Close = %v, want 105", rows[0].Close)
	}

	empty, err := repo.FindByTickerAndDateRange(
		db, ticker,
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("empty range: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 rows for empty range, got %d", len(empty))
	}
}

// TestDailyPriceRepository_TradedTickersOnDate verifies the market-wide
// trading-day signal the sweep uses: distinct tickers with a daily_prices row
// on a day, empty on days nobody traded. Skipped unless IDX_MCP_DB_DSN is set.
func TestDailyPriceRepository_TradedTickersOnDate(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := NewDailyPriceRepository(log)

	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	tickers := []string{"TESTA", "TESTB", "TESTC"}
	for _, tk := range tickers {
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
		db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", tk, tk)
		db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)
			VALUES ($1, $2, 100, 101, 99, 100, 1000, 100000, 10, 'idx')`, tk, day)
	}
	t.Cleanup(func() {
		for _, tk := range tickers {
			db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
		}
	})

	got, err := repo.TradedTickersOnDate(db, day.Format("2006-01-02"))
	if err != nil {
		t.Fatalf("TradedTickersOnDate: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 traded tickers, got %v", got)
	}
	set := make(map[string]bool, len(got))
	for _, tk := range got {
		set[tk] = true
	}
	for _, tk := range tickers {
		if !set[tk] {
			t.Errorf("missing traded ticker %s", tk)
		}
	}

	// A day nobody traded → empty.
	empty, err := repo.TradedTickersOnDate(db, time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC).Format("2006-01-02"))
	if err != nil {
		t.Fatalf("TradedTickersOnDate empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 traded tickers on non-trading day, got %v", empty)
	}
}

func f64(v float64) *float64 { return &v }
