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

// TestSuspensionRepository_EndToEnd verifies the suspensions upsert and range
// query against a real Postgres: rows land with the correct fields, refetching
// the same composite key is idempotent (update, not duplicate), and the range
// query orders by event date with an optional ticker filter. Skipped unless
// IDX_MCP_DB_DSN is set. Cleanup is scoped to the TESTSA/TESTSB tickers.
func TestSuspensionRepository_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewSuspensionRepository(log)

	cleanup := func() {
		db.MustExec("DELETE FROM suspensions WHERE ticker IN ('TESTSA', 'TESTSB')")
	}
	cleanup()
	t.Cleanup(cleanup)

	rows := []entity.Suspension{
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), Type: "SPT", Reason: "Penghentian Sementara TESTSA"},
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), Type: "UPT", Reason: "Pembukaan Kembali TESTSA"},
		{Ticker: "TESTSB", EventDate: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), Type: "UMA", Reason: "UMA atas Saham TESTSB"},
	}
	if err := repo.Upsert(db, rows); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)

	// No ticker → all 3, ordered by event_date.
	stored, err := repo.FindByDateRange(db, from, to, nil)
	if err != nil {
		t.Fatalf("FindByDateRange: %v", err)
	}
	if len(stored) != 3 {
		t.Fatalf("expected 3 stored rows, got %d", len(stored))
	}
	if stored[0].Ticker != "TESTSA" || stored[0].Type != "SPT" {
		t.Errorf("row[0] = %+v, want TESTSA/SPT (earliest first)", stored[0])
	}
	if stored[1].Type != "UMA" {
		t.Errorf("row[1].type = %q, want UMA", stored[1].Type)
	}
	if stored[2].Type != "UPT" {
		t.Errorf("row[2].type = %q, want UPT", stored[2].Type)
	}

	// Ticker filter → TESTSA rows only.
	ticker := "TESTSA"
	storedSA, err := repo.FindByDateRange(db, from, to, &ticker)
	if err != nil {
		t.Fatalf("FindByDateRange ticker: %v", err)
	}
	if len(storedSA) != 2 {
		t.Fatalf("expected 2 TESTSA rows, got %d", len(storedSA))
	}

	// Refetch same composite key with a changed reason → idempotent upsert:
	// the conflict key includes reason, so a DIFFERENT reason inserts a new row,
	// and an identical one just refreshes stored_at (no duplicate either way).
	newRows := []entity.Suspension{
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), Type: "SPT", Reason: "Penghentian Sementara TESTSA"},
	}
	if err := repo.Upsert(db, newRows); err != nil {
		t.Fatalf("second Upsert: %v", err)
	}
	stored2, err := repo.FindByDateRange(db, from, to, &ticker)
	if err != nil {
		t.Fatalf("FindByDateRange after refetch: %v", err)
	}
	if len(stored2) != 2 {
		t.Errorf("refetch changed row count: got %d, want 2", len(stored2))
	}

	// Out of range → empty.
	outFrom := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	outTo := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	outside, err := repo.FindByDateRange(db, outFrom, outTo, nil)
	if err != nil {
		t.Fatalf("FindByDateRange out-of-range: %v", err)
	}
	if len(outside) != 0 {
		t.Errorf("expected 0 rows out of range, got %d", len(outside))
	}

	// Empty upsert is a no-op.
	if err := repo.Upsert(db, nil); err != nil {
		t.Fatalf("empty Upsert: %v", err)
	}
}
