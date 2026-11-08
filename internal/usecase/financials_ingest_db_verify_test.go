package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// fakeSnapshotFetcher records calls and returns the configured snapshot.
type fakeSnapshotFetcher struct {
	calls    int
	year     int
	month    int
	snapshot *financials.Snapshot
	err      error
}

func (f *fakeSnapshotFetcher) FetchFinancialSnapshot(ctx context.Context, year, month int) (*financials.Snapshot, error) {
	f.calls++
	f.year, f.month = year, month
	if f.err != nil {
		return nil, f.err
	}
	snap := *f.snapshot
	snap.Month = time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	return &snap, nil
}

func ucStatement(ticker string, assets int64) financials.Statement {
	period := time.Date(2091, time.June, 30, 0, 0, 0, 0, time.UTC)
	return financials.Statement{
		Ticker:         ticker,
		PeriodEnd:      period,
		FiscalYearEnd:  time.Date(2091, time.December, 31, 0, 0, 0, 0, time.UTC),
		DurationMonths: 6,
		Assets:         &assets,
		Source:         financials.SourceRatioSnapshot,
		ValueBasis:     financials.ValueBasisIDX,
		SourceFSDate:   period,
	}
}

func ucSnapshot(month time.Time, status string, st *financials.Statement) *financials.Snapshot {
	share := 1.0
	snap := &financials.Snapshot{
		Month:           month,
		Status:          status,
		RowCount:        1,
		DistinctPeriods: 0,
		QuarterEndShare: &share,
		ContentHash:     "payload",
		Statements:      []financials.Statement{},
	}
	if st != nil {
		snap.Statements = append(snap.Statements, *st)
		snap.DistinctPeriods = 1
	}
	if status == financials.StatusEmpty {
		snap.QuarterEndShare = nil
		snap.RowCount = 0
	}
	return snap
}

// TestEnsureLatest_DailyCheck drives the scheduling contract against a real
// Postgres: the previous calendar month is fetched until its ledger row is
// ok/suspect; an empty month is retried; a suspect month is terminal. Skipped
// unless IDX_MCP_DB_DSN is set. Test months are in 2091 so production rows are
// never touched.
func TestEnsureLatest_DailyCheck(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := repository.NewFinancialsRepository(log)

	cleanup := func() {
		db.MustExec(`DELETE FROM financial_statement_versions WHERE ticker = 'TESTUC'`)
		db.MustExec(`DELETE FROM financial_statements WHERE ticker = 'TESTUC'`)
		db.MustExec(`DELETE FROM financial_snapshot_runs WHERE snapshot_month >= '2091-01-01' AND snapshot_month < '2092-01-01'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	okSnap := ucSnapshot(time.Time{}, financials.StatusOK, ptrUCStatement(ucStatement("TESTUC", 123)))
	fetcher := &fakeSnapshotFetcher{snapshot: okSnap}
	uc := NewFinancialsIngestUseCase(db, log, fetcher, repo)

	// 2091-11-08 targets 2091-10 and ingests it.
	res, err := uc.EnsureLatest(context.Background(), time.Date(2091, 11, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("first EnsureLatest: %v", err)
	}
	if fetcher.calls != 1 || fetcher.year != 2091 || fetcher.month != 10 {
		t.Fatalf("fetched %d times (%d-%d), want 1 (2091-10)", fetcher.calls, fetcher.year, fetcher.month)
	}
	if res.Skipped || res.Status != financials.StatusOK {
		t.Errorf("result = %+v, want a fresh ok run", res)
	}

	// The next day is a no-op: the run ledger is terminal.
	before := fetcher.calls
	res, err = uc.EnsureLatest(context.Background(), time.Date(2091, 11, 9, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("second EnsureLatest: %v", err)
	}
	if fetcher.calls != before {
		t.Errorf("fetcher called again despite an ok run")
	}
	if !res.Skipped {
		t.Errorf("result = %+v, want skipped", res)
	}

	// An unpublished month is recorded empty and retried the next day.
	fetcher.snapshot = ucSnapshot(time.Time{}, financials.StatusEmpty, nil)
	res, err = uc.EnsureLatest(context.Background(), time.Date(2091, 12, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("empty EnsureLatest: %v", err)
	}
	if res.Status != financials.StatusEmpty {
		t.Fatalf("status = %q, want empty", res.Status)
	}
	run, err := repo.FindRun(db, time.Date(2091, 11, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || run == nil || run.Status != financials.StatusEmpty {
		t.Fatalf("ledger = %+v, err %v; want empty", run, err)
	}
	before = fetcher.calls
	if _, err := uc.EnsureLatest(context.Background(), time.Date(2091, 12, 2, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("empty retry: %v", err)
	}
	if fetcher.calls != before+1 {
		t.Errorf("empty month was not retried")
	}

	// A suspect month derives nothing and is terminal.
	fetcher.snapshot = ucSnapshot(time.Time{}, financials.StatusSuspect, nil)
	res, err = uc.EnsureLatest(context.Background(), time.Date(2092, 1, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("suspect EnsureLatest: %v", err)
	}
	if res.Status != financials.StatusSuspect {
		t.Fatalf("status = %q, want suspect", res.Status)
	}
	before = fetcher.calls
	res, err = uc.EnsureLatest(context.Background(), time.Date(2092, 1, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("suspect recheck: %v", err)
	}
	if fetcher.calls != before || !res.Skipped {
		t.Errorf("suspect month was retried, want terminal skip")
	}
}

// TestEnsureLatest_MalformedRecordsFailure checks a malformed payload is
// recorded as failed (and therefore retried), not silently accepted.
func TestEnsureLatest_MalformedRecordsFailure(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := repository.NewFinancialsRepository(log)

	cleanup := func() {
		db.MustExec(`DELETE FROM financial_snapshot_runs WHERE snapshot_month >= '2092-01-01' AND snapshot_month < '2093-01-01'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetcher := &fakeSnapshotFetcher{err: client.ErrMalformedSnapshot}
	uc := NewFinancialsIngestUseCase(db, log, fetcher, repo)

	_, err := uc.EnsureLatest(context.Background(), time.Date(2092, 6, 1, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, client.ErrMalformedSnapshot) {
		t.Fatalf("err = %v, want ErrMalformedSnapshot", err)
	}
	run, runErr := repo.FindRun(db, time.Date(2092, 5, 1, 0, 0, 0, 0, time.UTC))
	if runErr != nil {
		t.Fatalf("FindRun: %v", runErr)
	}
	if run == nil || run.Status != financials.StatusFailed {
		t.Fatalf("ledger = %+v, want failed", run)
	}
	if run.Error == nil || *run.Error == "" {
		t.Error("failed run has no error text")
	}
}

func TestPreviousMonth(t *testing.T) {
	tests := []struct {
		in   time.Time
		want string
	}{
		{time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC), "2026-10"},
		{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "2025-12"},
		{time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC), "2026-02"},
	}
	for _, tc := range tests {
		if got := PreviousMonth(tc.in).Format("2006-01"); got != tc.want {
			t.Errorf("PreviousMonth(%s) = %s, want %s", tc.in.Format("2006-01-02"), got, tc.want)
		}
	}
}

func ptrUCStatement(st financials.Statement) *financials.Statement { return &st }
