package usecase

import (
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// TestJournalSQLSource_ReadVerdicts_EndToEnd verifies the SQL source against a
// real Postgres: rows in a decision-date window come back whole (zones, session,
// committed, supersedes), ordered oldest-first, and the table's CHECK constraints
// reject a malformed Verdict. Skipped unless IDX_MCP_DB_DSN is set.
func TestJournalSQLSource_ReadVerdicts_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() })

	// A window no other data occupies, so the source's whole-window read is
	// scoped to this test without a ticker filter it does not have.
	base := time.Date(2031, 3, 3, 0, 0, 0, 0, time.UTC)
	cleanup := func() {
		if _, err := db.Exec("DELETE FROM measurement_journal WHERE decision_day >= $1", base.AddDate(0, 0, -1)); err != nil {
			t.Logf("cleanup measurement_journal: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	var firstID int64
	if err := db.Get(&firstID, `
		INSERT INTO measurement_journal
			(decision_day, ticker, verdict, entry_low, entry_high, cl_low, cl_high, tp_low, tp_high, rationale, session)
		VALUES ($1, 'TESTJS', 'SETUP', 100, 105, 90, 95, 120, 125, 'first', 's1')
		RETURNING id`, base); err != nil {
		t.Fatalf("insert setup: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO measurement_journal
			(decision_day, ticker, verdict, rationale, session, committed, supersedes, created_at)
		VALUES ($1, 'TESTJS', 'PASS', 'second', 's2', TRUE, $2, NOW() + INTERVAL '1 second')`,
		base.AddDate(0, 0, 1), firstID); err != nil {
		t.Fatalf("insert pass: %v", err)
	}

	window := base.AddDate(0, 0, -1)
	until := base.AddDate(0, 0, 2)
	verdicts, issues, err := NewJournalSQLSource(db).ReadVerdicts(&window, &until)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(issues) != 0 {
		t.Fatalf("issues = %+v, want none", issues)
	}
	if len(verdicts) != 2 {
		t.Fatalf("verdicts = %d, want 2", len(verdicts))
	}

	setup := verdicts[0]
	if setup.Ticker != "TESTJS" || setup.Verdict != verdictSetup || setup.ID != firstID ||
		setup.Session != "s1" || setup.Committed || setup.Supersedes != nil {
		t.Fatalf("setup row = %+v", setup)
	}
	if setup.Date != base.Format("2006-01-02") || setup.EntryLow == nil || *setup.EntryLow != 100 || setup.TPHigh == nil || *setup.TPHigh != 125 {
		t.Fatalf("setup fields = %+v", setup)
	}

	pass := verdicts[1]
	if pass.Verdict != verdictPass || pass.Session != "s2" || !pass.Committed ||
		pass.Supersedes == nil || *pass.Supersedes != firstID || pass.EntryLow != nil {
		t.Fatalf("pass row = %+v", pass)
	}

	// The table itself rejects a SETUP without its zones.
	if _, err := db.Exec(`
		INSERT INTO measurement_journal (decision_day, ticker, verdict, rationale, session)
		VALUES ($1, 'TESTJS', 'SETUP', 'bad', 's3')`, base); err == nil {
		t.Fatalf("expected the SETUP zone CHECK to reject a zone-less SETUP")
	}
}
