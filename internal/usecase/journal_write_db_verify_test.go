package usecase

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// TestJournalSQLWriter_LogVerdict_EndToEnd verifies the writer against a real
// Postgres: the append persists zones, supersedes implies committed, committing
// moves the single committed row (the partial unique index never sees two), and
// a divergent cross-session write surfaces a Verdict Conflict. Skipped unless
// IDX_MCP_DB_DSN is set.
func TestJournalSQLWriter_LogVerdict_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() })

	base := "2031-04-07"
	day, _ := time.Parse("2006-01-02", base)
	cleanup := func() {
		if _, err := db.Exec("DELETE FROM measurement_journal WHERE decision_day = $1::date", base); err != nil {
			t.Logf("cleanup measurement_journal: %v", err)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	ctx := context.Background()
	w := NewJournalSQLWriter(db)

	// One SETUP from session s1.
	if _, err := LogVerdict(ctx, w, LogVerdictInput{
		Session: "s1", Date: base, Ticker: "TESTJW", Verdict: "SETUP",
		EntryLow: f64(100), EntryHigh: f64(105), CLLow: f64(90), CLHigh: f64(95),
		TPLow: f64(130), TPHigh: f64(135), Rationale: "first",
	}); err != nil {
		t.Fatalf("log setup: %v", err)
	}

	// A divergent PASS from another session is a conflict, not a resolution.
	res, err := LogVerdict(ctx, w, LogVerdictInput{
		Session: "s2", Date: base, Ticker: "TESTJW", Verdict: "PASS", Rationale: "disagree",
	})
	if err != nil {
		t.Fatalf("log pass: %v", err)
	}
	if res.Conflict == nil {
		t.Fatalf("expected a Verdict Conflict, got %+v", res)
	}

	// Committing a third-row correction resolves it and is the only committed row.
	res, err = LogVerdict(ctx, w, LogVerdictInput{
		Session: "s3", Date: base, Ticker: "TESTJW", Verdict: "SETUP", Committed: true,
		EntryLow: f64(101), EntryHigh: f64(106), CLLow: f64(91), CLHigh: f64(96),
		TPLow: f64(131), TPHigh: f64(136), Rationale: "committed",
	})
	if err != nil {
		t.Fatalf("log committed: %v", err)
	}
	if res.Conflict != nil {
		t.Fatalf("committed write left a conflict: %+v", res.Conflict)
	}

	var committedIDs []int64
	if err := db.Select(&committedIDs,
		"SELECT id FROM measurement_journal WHERE decision_day = $1::date AND ticker = 'TESTJW' AND committed", base); err != nil {
		t.Fatalf("select committed: %v", err)
	}
	if len(committedIDs) != 1 || committedIDs[0] != res.ID {
		t.Fatalf("committed ids = %v, want exactly [%d]", committedIDs, res.ID)
	}

	rows, err := w.ReadKey(ctx, day, "TESTJW")
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[0].EntryLow == nil || *rows[0].EntryLow != 100 {
		t.Fatalf("first row lost its zones: %+v", rows[0])
	}
}
