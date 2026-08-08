package tasks

import (
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestDetectAnomalies_EndToEnd verifies the full detection path against a real
// Postgres, including the ADTV liquidity filter. Skipped unless
// IDX_MCP_DB_DSN is set.
func TestDetectAnomalies_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	dailyRepo := repository.NewDailyPriceRepository(log)
	anomalyRepo := repository.NewAnomalyRepository(log)

	// Clean slate.
	for _, tk := range []string{"TESTA", "TESTB"} {
		db.MustExec("DELETE FROM anomalies WHERE ticker = $1", tk)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
		db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
	}
	t.Cleanup(func() {
		for _, tk := range []string{"TESTA", "TESTB"} {
			db.MustExec("DELETE FROM anomalies WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
		}
	})

	// TESTA: liquid (today value 6B > 5B ADTV threshold), 2.8x volume spike
	// (+180%), +10% close. Both anomalies expected.
	// TESTB: illiquid (today value 2M < 5B), same spike pattern. Filtered out.
	seedTicker(t, db, "TESTA")
	seedTicker(t, db, "TESTB")

	today := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	db.MustExec("INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source) VALUES ($1,$2,100,111,99,110,2800000,6000000000,2000,'idx')",
		"TESTA", today)
	db.MustExec("INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source) VALUES ($1,$2,100,111,99,110,2800000,2000000,2000,'idx')",
		"TESTB", today)

	written, err := detectAnomalies(db, dailyRepo, anomalyRepo, today, DefaultADTVMinValue, log)
	if err != nil {
		t.Fatalf("detectAnomalies: %v", err)
	}
	// Written includes real ingested tickers' anomalies too, so only assert
	// the synthetic ones are covered below per-ticker.
	_ = written

	rows, err := anomalyRepo.FindByTickerAndDate(db, "TESTA", "2026-08-07")
	if err != nil {
		t.Fatalf("FindByTickerAndDate: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 anomaly rows for TESTA, got %d", len(rows))
	}

	byType := map[string]bool{}
	for _, a := range rows {
		byType[a.Type] = true
		if a.Ticker != "TESTA" {
			t.Errorf("unexpected ticker %s", a.Ticker)
		}
		if a.MagnitudePct == nil {
			t.Errorf("%s anomaly missing magnitude", a.Type)
		}
	}
	if !byType["volume"] || !byType["price"] {
		t.Errorf("expected both volume and price anomalies, got %v", byType)
	}

	// Illiquid TESTB must have zero anomalies despite crossing both thresholds.
	testbRows, err := anomalyRepo.FindByTickerAndDate(db, "TESTB", "2026-08-07")
	if err != nil {
		t.Fatalf("FindByTickerAndDate(TESTB): %v", err)
	}
	if len(testbRows) != 0 {
		t.Errorf("expected 0 anomaly rows for illiquid TESTB, got %d", len(testbRows))
	}
}

// seedTicker inserts a ticker plus 20 days of flat history (volume 1M,
// value 100M, close 100) so volume/price thresholds are computable.
func seedTicker(t *testing.T, db *sqlx.DB, code string) {
	t.Helper()
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", code, "Test Ticker "+code)
	for i := 0; i < 20; i++ {
		d := time.Date(2026, 7, 10+i, 0, 0, 0, 0, time.UTC)
		db.MustExec("INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source) VALUES ($1,$2,100,101,99,100,1000000,100000000,1000,'idx')",
			code, d)
	}
}
