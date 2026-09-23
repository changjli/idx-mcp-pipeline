package repository

import (
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// dbVerifyTicker seeds a ticker row (the daily_prices FK) and returns a
// cleanup that removes every trace of it, scoped so a shared DB is untouched
// outside this ticker.
func dbVerifyTicker(t *testing.T, db *sqlx.DB, code string) {
	t.Helper()
	db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", code)
	db.MustExec("DELETE FROM tickers WHERE code = $1", code)
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", code, "Batch verify "+code)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", code)
		db.MustExec("DELETE FROM tickers WHERE code = $1", code)
	})
}

// TestDailyPriceRepository_UpsertBatch_EndToEnd verifies the chunked upsert
// against a real Postgres: 8 rows at chunk size 3 (a non-multiple, so the final
// chunk is short) round-trip as 8 stored rows, a second run with changed values
// updates in place rather than duplicating, and a row with no wire value stores
// NULL rather than a zero. Skipped unless IDX_MCP_DB_DSN is set.
func TestDailyPriceRepository_UpsertBatch_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewDailyPriceRepository(log)
	repo.ChunkSize = 3 // 8 rows → chunks of 3, 3, 2
	ticker := "TESTBTP"
	dbVerifyTicker(t, db, ticker)

	base := time.Date(2099, 6, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]entity.DailyPrice, 0, 8)
	for i := 0; i < 8; i++ {
		close := 100.0 + float64(i)
		rows = append(rows, entity.DailyPrice{
			Ticker:     ticker,
			TradingDay: base.AddDate(0, 0, i),
			Open:       f64(100),
			High:       f64(110),
			Low:        f64(90),
			Close:      &close,
			Volume:     i64(1_000_000),
			Value:      i64(1_000_000_000),
			Frequency:  i32(1000),
			Source:     "idx",
		})
	}
	// Last row carries no wire value — must land as NULL, not 0.
	rows[7].Close = nil
	rows[7].Open = nil

	written, err := repo.UpsertBatch(db, rows)
	if err != nil {
		t.Fatalf("UpsertBatch: %v", err)
	}
	if written != 8 {
		t.Fatalf("written = %d, want 8", written)
	}

	var count int
	db.Get(&count, "SELECT COUNT(*) FROM daily_prices WHERE ticker = $1", ticker)
	if count != 8 {
		t.Fatalf("stored rows = %d, want 8 (chunked insert lost or duplicated rows)", count)
	}

	stored, err := repo.FindByTickerAndDateRange(db, ticker, base, base.AddDate(0, 0, 7))
	if err != nil {
		t.Fatalf("FindByTickerAndDateRange: %v", err)
	}
	if len(stored) != 8 {
		t.Fatalf("read back %d rows, want 8", len(stored))
	}
	if stored[0].Close == nil || *stored[0].Close != 100 {
		t.Errorf("row 0 close = %v, want 100", stored[0].Close)
	}
	if stored[3].Close == nil || *stored[3].Close != 103 {
		t.Errorf("row 3 close = %v, want 103 (chunk boundary row)", stored[3].Close)
	}
	if stored[7].Close != nil || stored[7].Open != nil {
		t.Errorf("row 7 with no wire value should store NULL, got close=%v open=%v", stored[7].Close, stored[7].Open)
	}

	// ── Conflict path: same (ticker, trading_day), new values. In place, no
	// duplicate rows, every value column refreshed.
	updated := make([]entity.DailyPrice, 0, 8)
	for i := 0; i < 8; i++ {
		updated = append(updated, entity.DailyPrice{
			Ticker:     ticker,
			TradingDay: base.AddDate(0, 0, i),
			Open:       f64(200),
			High:       f64(210),
			Low:        f64(190),
			Close:      f64(205),
			Volume:     i64(2_000_000),
			Value:      i64(2_000_000_000),
			Frequency:  i32(2000),
			Source:     "idx",
		})
	}
	written2, err := repo.UpsertBatch(db, updated)
	if err != nil {
		t.Fatalf("second UpsertBatch: %v", err)
	}
	if written2 != 8 {
		t.Errorf("second run written = %d, want 8", written2)
	}

	var count2 int
	db.Get(&count2, "SELECT COUNT(*) FROM daily_prices WHERE ticker = $1", ticker)
	if count2 != 8 {
		t.Errorf("stored rows after re-run = %d, want 8 (conflict path inserted duplicates)", count2)
	}

	var closes []float64
	if err := db.Select(&closes,
		"SELECT close FROM daily_prices WHERE ticker = $1 ORDER BY trading_day", ticker); err != nil {
		t.Fatalf("select closes: %v", err)
	}
	if len(closes) != 8 {
		t.Fatalf("selected %d closes, want 8", len(closes))
	}
	for i, c := range closes {
		if c != 205 {
			t.Errorf("close[%d] = %v, want 205 (conflict update did not refresh the value)", i, c)
		}
	}

	// A re-run must also fill a value that was NULL before (nil → 205 above),
	// and the row count is unchanged, so the day is idempotent.
}

// TestDailyPriceRepository_UpsertBatchTiming measures a ~950-row day — the
// shape of one production date — over the pooler, so the ticket's before/after
// comparison has an "after" number measured the same way as the backfill's
// 50s-per-date figure. It writes and deletes its own rows; skipped unless
// IDX_MCP_DB_DSN is set.
func TestDailyPriceRepository_UpsertBatchTiming(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewDailyPriceRepository(log)
	ticker := "TESTBTP950"
	dbVerifyTicker(t, db, ticker)

	// 950 distinct trading days for one ticker: the row count of a real date
	// without needing 950 ticker rows.
	base := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	const dayCount = 950
	rows := make([]entity.DailyPrice, 0, dayCount)
	for i := 0; i < dayCount; i++ {
		rows = append(rows, entity.DailyPrice{
			Ticker:     ticker,
			TradingDay: base.AddDate(0, 0, i),
			Open:       f64(100),
			High:       f64(110),
			Low:        f64(90),
			Close:      f64(105),
			Volume:     i64(1_000_000),
			Value:      i64(1_000_000_000),
			Frequency:  i32(1000),
			Source:     "idx",
		})
	}

	start := time.Now()
	written, err := repo.UpsertBatch(db, rows)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("UpsertBatch(%d rows): %v", dayCount, err)
	}
	if written != dayCount {
		t.Fatalf("written = %d, want %d", written, dayCount)
	}

	var count int
	db.Get(&count, "SELECT COUNT(*) FROM daily_prices WHERE ticker = $1", ticker)
	if count != dayCount {
		t.Errorf("stored rows = %d, want %d", count, dayCount)
	}

	chunks := (dayCount + repo.ChunkSize - 1) / repo.ChunkSize
	t.Logf("batched upsert: %d rows in %d chunks (%d rows/chunk) in %s (%.1fms/row)",
		dayCount, chunks, repo.ChunkSize, elapsed.Round(time.Millisecond),
		float64(elapsed.Microseconds())/float64(dayCount)/1000)
	fmt.Printf("BATCH_TIMING rows=%d chunks=%d elapsed=%s\n", dayCount, chunks, elapsed.Round(time.Millisecond))
}
