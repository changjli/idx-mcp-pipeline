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

// TestCorporateActionRepository_EndToEnd verifies the corporate-actions upsert
// and range query against a real Postgres: rows land with the correct detail
// JSONB, refetching the same id is idempotent (update, not duplicate), and the
// range query orders by event date with an optional ticker filter. Skipped
// unless IDX_MCP_DB_DSN is set. Cleanup is scoped to the TESTCA/TESTCB tickers.
func TestCorporateActionRepository_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewCorporateActionRepository(log)

	// Clean slate for the synthetic tickers (ids 9_000_001+ can't collide with
	// real GetIssuedHistory surrogates).
	cleanup := func() {
		db.MustExec("DELETE FROM corporate_actions WHERE ticker IN ('TESTCA', 'TESTCB')")
	}
	cleanup()
	t.Cleanup(cleanup)

	rows := []entity.CorporateAction{
		{Id: 9_000_001, Ticker: "TESTCA", EventDate: time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC), Type: "Waran", JumlahSaham: 1000, JumlahSahamSetelahTindakan: 2000},
		{Id: 9_000_002, Ticker: "TESTCA", EventDate: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), Type: "Stock Split", JumlahSaham: 2000, JumlahSahamSetelahTindakan: 4000},
		{Id: 9_000_003, Ticker: "TESTCB", EventDate: time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC), Type: "Rights Issue", JumlahSaham: 3000, JumlahSahamSetelahTindakan: 6000},
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
	if stored[0].EventDate.Year() != 2026 || stored[0].EventDate.Month() != 9 || stored[0].EventDate.Day() != 4 {
		t.Errorf("row[0] event_date = %v, want 2026-09-04", stored[0].EventDate)
	}
	if stored[1].Type != "Stock Split" {
		t.Errorf("row[1].type = %q, want Stock Split", stored[1].Type)
	}

	// Ticker filter → TESTCA rows only.
	ticker := "TESTCA"
	storedCA, err := repo.FindByDateRange(db, from, to, &ticker)
	if err != nil {
		t.Fatalf("FindByDateRange ticker: %v", err)
	}
	if len(storedCA) != 2 {
		t.Fatalf("expected 2 TESTCA rows, got %d", len(storedCA))
	}

	// Amount columns round-trip.
	if stored[0].JumlahSaham != 1000 || stored[0].JumlahSahamSetelahTindakan != 2000 {
		t.Errorf("stored amounts = %d/%d, want 1000/2000", stored[0].JumlahSaham, stored[0].JumlahSahamSetelahTindakan)
	}

	// Refetch same id with a changed type → idempotent upsert, still 3 rows.
	rows[0].Type = "Waran (updated)"
	if err := repo.Upsert(db, rows); err != nil {
		t.Fatalf("second Upsert: %v", err)
	}
	stored2, err := repo.FindByDateRange(db, from, to, nil)
	if err != nil {
		t.Fatalf("FindByDateRange after refetch: %v", err)
	}
	if len(stored2) != 3 {
		t.Errorf("refetch changed row count: got %d, want 3", len(stored2))
	}
	if stored2[0].Type != "Waran (updated)" {
		t.Errorf("row[0].type = %q after upsert, want Waran (updated)", stored2[0].Type)
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
