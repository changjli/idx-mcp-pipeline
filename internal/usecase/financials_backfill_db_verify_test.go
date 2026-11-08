package usecase

import (
	"context"
	"fmt"
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

// monthSnapshotFetcher serves a different snapshot per (year, month), so a
// backfill walk can be driven across a range. A month absent from snaps is an
// empty (unpublished) snapshot; a month in errs fails the fetch.
type monthSnapshotFetcher struct {
	calls map[string]int
	snaps map[string]*financials.Snapshot
	errs  map[string]error
}

func (f *monthSnapshotFetcher) FetchFinancialSnapshot(ctx context.Context, year, month int) (*financials.Snapshot, error) {
	key := fmt.Sprintf("%04d-%02d", year, month)
	if f.calls == nil {
		f.calls = make(map[string]int)
	}
	f.calls[key]++
	if err := f.errs[key]; err != nil {
		return nil, err
	}
	snap, ok := f.snaps[key]
	if !ok {
		snap = &financials.Snapshot{Status: financials.StatusEmpty, Statements: []financials.Statement{}}
	}
	cp := *snap
	cp.Month = bfTime(year, time.Month(month), 1)
	return &cp, nil
}

func (f *monthSnapshotFetcher) totalCalls() int {
	total := 0
	for _, n := range f.calls {
		total += n
	}
	return total
}

func bfOKSnapshot(sts ...financials.Statement) *financials.Snapshot {
	share := 1.0
	periods := make(map[string]struct{}, len(sts))
	for _, st := range sts {
		periods[st.PeriodEnd.Format("2006-01-02")] = struct{}{}
	}
	return &financials.Snapshot{
		Status:          financials.StatusOK,
		RowCount:        len(sts),
		DistinctPeriods: len(periods),
		QuarterEndShare: &share,
		ContentHash:     "payload",
		Statements:      sts,
	}
}

func bfSuspectSnapshot() *financials.Snapshot {
	share := 0.0
	return &financials.Snapshot{
		Status:          financials.StatusSuspect,
		RowCount:        3,
		QuarterEndShare: &share,
		ContentHash:     "payload",
		Statements:      []financials.Statement{},
	}
}

func bfStatement(ticker string, period time.Time, duration int, assets int64) financials.Statement {
	return financials.Statement{
		Ticker:         ticker,
		PeriodEnd:      period,
		FiscalYearEnd:  bfTime(period.Year(), time.December, 31),
		DurationMonths: duration,
		Assets:         &assets,
		Source:         financials.SourceRatioSnapshot,
		ValueBasis:     financials.ValueBasisIDX,
		SourceFSDate:   period,
	}
}

func bfDB(t *testing.T) (*sqlx.DB, *logrus.Logger, *repository.FinancialsRepository) {
	t.Helper()
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() })
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return db, log, repository.NewFinancialsRepository(log)
}

// bfCleanup removes the test tickers and the 2093–2095 run ledger so the
// backfill tests never touch production rows.
func bfCleanup(t *testing.T, db *sqlx.DB) {
	t.Helper()
	cleanup := func() {
		db.MustExec(`DELETE FROM financial_statement_versions WHERE ticker IN ('TESTBF', 'TESTBG')`)
		db.MustExec(`DELETE FROM financial_statements WHERE ticker IN ('TESTBF', 'TESTBG')`)
		db.MustExec(`DELETE FROM financial_snapshot_runs WHERE snapshot_month >= '2093-01-01' AND snapshot_month < '2096-01-01'`)
	}
	cleanup()
	t.Cleanup(cleanup)
}

// TestFinancialsBackfill_ContinuousSeriesAndResume drives a full backward walk
// over one fiscal year against a real Postgres: Q1/Q2/Q3/FY each appear once,
// and a second run skips every ok month without touching a row.
func TestFinancialsBackfill_ContinuousSeriesAndResume(t *testing.T) {
	db, log, repo := bfDB(t)
	bfCleanup(t, db)

	fy := bfStatement("TESTBF", bfTime(2093, time.December, 31), 12, 1000)
	q1 := bfStatement("TESTBF", bfTime(2094, time.March, 31), 3, 1100)
	q2 := bfStatement("TESTBF", bfTime(2094, time.June, 30), 6, 1200)
	q3 := bfStatement("TESTBF", bfTime(2094, time.September, 30), 9, 1300)

	fetcher := &monthSnapshotFetcher{snaps: map[string]*financials.Snapshot{
		"2094-01": bfOKSnapshot(fy),
		"2094-02": bfOKSnapshot(fy),
		"2094-03": bfOKSnapshot(fy),
		"2094-04": bfOKSnapshot(q1),
		"2094-05": bfOKSnapshot(q1),
		"2094-06": bfOKSnapshot(q1),
		"2094-07": bfOKSnapshot(q2),
		"2094-08": bfOKSnapshot(q2),
		"2094-09": bfOKSnapshot(q2),
		"2094-10": bfOKSnapshot(q3),
		"2094-11": bfOKSnapshot(q3),
		"2094-12": bfOKSnapshot(q3),
	}}
	uc := NewFinancialsIngestUseCase(db, log, fetcher, repo)
	opts := FinancialsBackfillOptions{
		From:    bfTime(2094, time.December, 1),
		To:      bfTime(2094, time.January, 1),
		RunDate: bfTime(2095, time.January, 15),
	}

	res, err := uc.Backfill(context.Background(), opts)
	if err != nil {
		t.Fatalf("first backfill: %v", err)
	}
	if res.Direction != "backward" || res.MonthsTotal != 12 || res.MonthsProcessed != 12 {
		t.Fatalf("result = %+v, want a 12-month backward walk", res)
	}
	if res.MonthsOK != 12 || res.MonthsFailed != 0 || res.MonthsSkipped != 0 {
		t.Errorf("status counts = %+v, want ok 12", res)
	}
	if fetcher.totalCalls() != 12 {
		t.Errorf("fetches = %d, want 12", fetcher.totalCalls())
	}
	// Walking backwards, the first month of a period inserts; the older repeats
	// are ordered-upsert skips, so four statements are written once each.
	if res.RowsWritten != 4 {
		t.Errorf("rows written = %d, want 4", res.RowsWritten)
	}

	rows, err := repo.FindByTicker(db, "TESTBF")
	if err != nil {
		t.Fatalf("FindByTicker: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("statements = %d, want 4 (Q1/Q2/Q3/FY once each)", len(rows))
	}
	wantPeriods := map[string]time.Time{
		"2093-12-31": bfTime(2094, time.March, 1),     // FY appeared in Jan–Mar
		"2094-03-31": bfTime(2094, time.June, 1),      // Q1 appeared in Apr–Jun
		"2094-06-30": bfTime(2094, time.September, 1), // Q2 appeared in Jul–Sep
		"2094-09-30": bfTime(2094, time.December, 1),  // Q3 appeared in Oct–Dec
	}
	for _, row := range rows {
		key := row.PeriodEnd.Format("2006-01-02")
		wantSeen, ok := wantPeriods[key]
		if !ok {
			t.Errorf("unexpected period %s", key)
			continue
		}
		delete(wantPeriods, key)
		if !row.LastSeenMonth.Equal(wantSeen) {
			t.Errorf("%s last_seen = %s, want %s", key,
				row.LastSeenMonth.Format("2006-01"), wantSeen.Format("2006-01"))
		}
	}
	if len(wantPeriods) != 0 {
		t.Errorf("missing periods %v", wantPeriods)
	}

	// Every month is settled in the ledger.
	for m := 1; m <= 12; m++ {
		run, err := repo.FindRun(db, bfTime(2094, time.Month(m), 1))
		if err != nil {
			t.Fatalf("FindRun 2094-%02d: %v", m, err)
		}
		if run == nil || run.Status != financials.StatusOK {
			t.Errorf("ledger 2094-%02d = %+v, want ok", m, run)
		}
	}

	// A second full run is a no-op: every month is skipped, nothing is fetched.
	res, err = uc.Backfill(context.Background(), opts)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if res.MonthsSkipped != 12 || res.MonthsProcessed != 0 || res.RowsWritten != 0 {
		t.Errorf("second run = %+v, want 12 skipped / 0 processed / 0 written", res)
	}
	if fetcher.totalCalls() != 12 {
		t.Errorf("fetches after re-run = %d, want still 12", fetcher.totalCalls())
	}
	rows, _ = repo.FindByTicker(db, "TESTBF")
	if len(rows) != 4 {
		t.Errorf("statements after re-run = %d, want 4", len(rows))
	}
	for _, row := range rows {
		if row.RevisionCount != 0 {
			t.Errorf("%s revision after re-run = %d, want 0", row.PeriodEnd.Format("2006-01-02"), row.RevisionCount)
		}
	}
}

// TestFinancialsBackfill_EmptyAndFailedDoNotAbort proves an empty month is
// recorded and a malformed month is recorded failed, and the walk continues to
// the older bound in both cases.
func TestFinancialsBackfill_EmptyAndFailedDoNotAbort(t *testing.T) {
	db, log, repo := bfDB(t)
	bfCleanup(t, db)

	q1 := bfStatement("TESTBG", bfTime(2094, time.March, 31), 3, 500)
	fy := bfStatement("TESTBG", bfTime(2093, time.December, 31), 12, 400)
	fetcher := &monthSnapshotFetcher{
		snaps: map[string]*financials.Snapshot{
			"2094-05": bfOKSnapshot(q1),
			"2094-04": {Status: financials.StatusEmpty, Statements: []financials.Statement{}},
			"2094-02": bfOKSnapshot(fy),
		},
		errs: map[string]error{"2094-03": client.ErrMalformedSnapshot},
	}
	uc := NewFinancialsIngestUseCase(db, log, fetcher, repo)

	res, err := uc.Backfill(context.Background(), FinancialsBackfillOptions{
		From:    bfTime(2094, time.May, 1),
		To:      bfTime(2094, time.February, 1),
		RunDate: bfTime(2094, time.June, 15),
	})
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if res.MonthsProcessed != 4 || res.MonthsOK != 2 || res.MonthsEmpty != 1 || res.MonthsFailed != 1 {
		t.Fatalf("result = %+v, want processed 4 / ok 2 / empty 1 / failed 1", res)
	}
	if fetcher.totalCalls() != 4 {
		t.Errorf("fetches = %d, want 4 (the walk did not abort)", fetcher.totalCalls())
	}

	rows, err := repo.FindByTicker(db, "TESTBG")
	if err != nil {
		t.Fatalf("FindByTicker: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("statements = %d, want 2", len(rows))
	}

	emptyRun, _ := repo.FindRun(db, bfTime(2094, time.April, 1))
	if emptyRun == nil || emptyRun.Status != financials.StatusEmpty {
		t.Errorf("2094-04 ledger = %+v, want empty", emptyRun)
	}
	failedRun, _ := repo.FindRun(db, bfTime(2094, time.March, 1))
	if failedRun == nil || failedRun.Status != financials.StatusFailed {
		t.Fatalf("2094-03 ledger = %+v, want failed", failedRun)
	}
	if failedRun.Error == nil || *failedRun.Error == "" {
		t.Error("failed run has no error text")
	}
}

// TestFinancialsBackfill_ForceReprocesses pins --force: an ok or suspect month
// is fetched again, without duplicating rows or bumping a revision.
func TestFinancialsBackfill_ForceReprocesses(t *testing.T) {
	db, log, repo := bfDB(t)
	bfCleanup(t, db)

	st := bfStatement("TESTBF", bfTime(2093, time.December, 31), 12, 777)
	fetcher := &monthSnapshotFetcher{snaps: map[string]*financials.Snapshot{
		"2094-05": bfOKSnapshot(st),
		"2094-04": bfSuspectSnapshot(),
	}}
	uc := NewFinancialsIngestUseCase(db, log, fetcher, repo)
	opts := FinancialsBackfillOptions{
		From:    bfTime(2094, time.May, 1),
		To:      bfTime(2094, time.April, 1),
		RunDate: bfTime(2094, time.June, 15),
	}

	if _, err := uc.Backfill(context.Background(), opts); err != nil {
		t.Fatalf("first backfill: %v", err)
	}
	if fetcher.totalCalls() != 2 {
		t.Fatalf("first run fetches = %d, want 2", fetcher.totalCalls())
	}

	// Without --force, ok and suspect are both terminal.
	res, err := uc.Backfill(context.Background(), opts)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if res.MonthsSkipped != 2 || fetcher.totalCalls() != 2 {
		t.Errorf("second run = %+v / fetches %d, want 2 skipped and no new fetch", res, fetcher.totalCalls())
	}

	// With --force, both are re-fetched; the unchanged ok statement writes no
	// new version and the suspect month still derives nothing.
	opts.Force = true
	res, err = uc.Backfill(context.Background(), opts)
	if err != nil {
		t.Fatalf("forced backfill: %v", err)
	}
	if res.MonthsProcessed != 2 || fetcher.totalCalls() != 4 {
		t.Errorf("forced run = %+v / fetches %d, want 2 processed and 4 total", res, fetcher.totalCalls())
	}
	if res.RowsWritten != 0 {
		t.Errorf("forced re-run wrote %d rows, want 0", res.RowsWritten)
	}

	rows, _ := repo.FindByTicker(db, "TESTBF")
	if len(rows) != 1 || rows[0].RevisionCount != 0 {
		t.Errorf("statements = %+v, want one row with revision 0", rows)
	}
	versions, _ := repo.FindVersions(db, "TESTBF", bfTime(2093, time.December, 31))
	if len(versions) != 1 {
		t.Errorf("versions = %d, want still 1", len(versions))
	}
	suspectRun, _ := repo.FindRun(db, bfTime(2094, time.April, 1))
	if suspectRun == nil || suspectRun.Status != financials.StatusSuspect {
		t.Errorf("2094-04 ledger = %+v, want suspect", suspectRun)
	}
}

// TestFinancialsBackfill_OlderMonthCannotOverwriteNewer exercises the ticket-01
// ordered upsert through the backfill path: a later start date walking into an
// older month must not replace the newer observation.
func TestFinancialsBackfill_OlderMonthCannotOverwriteNewer(t *testing.T) {
	db, log, repo := bfDB(t)
	bfCleanup(t, db)

	period := bfTime(2093, time.December, 31)
	newer := bfStatement("TESTBF", period, 12, 999)
	older := bfStatement("TESTBF", period, 12, 1)
	fetcher := &monthSnapshotFetcher{snaps: map[string]*financials.Snapshot{
		"2094-09": bfOKSnapshot(newer),
		"2094-08": bfOKSnapshot(older),
	}}
	uc := NewFinancialsIngestUseCase(db, log, fetcher, repo)

	// The newer observation lands first (the run starts at the newest month).
	if _, err := uc.Backfill(context.Background(), FinancialsBackfillOptions{
		From:    bfTime(2094, time.September, 1),
		To:      bfTime(2094, time.September, 1),
		RunDate: bfTime(2094, time.October, 15),
	}); err != nil {
		t.Fatalf("newer backfill: %v", err)
	}

	// A later run walks into the older month with a conflicting value.
	res, err := uc.Backfill(context.Background(), FinancialsBackfillOptions{
		From:    bfTime(2094, time.August, 1),
		To:      bfTime(2094, time.August, 1),
		RunDate: bfTime(2094, time.October, 15),
	})
	if err != nil {
		t.Fatalf("older backfill: %v", err)
	}
	if res.Months[0].Stats.Skipped != 1 || res.RowsWritten != 0 {
		t.Errorf("older run = %+v, want one ordered-upsert skip and no write", res.Months[0])
	}

	rows, _ := repo.FindByTicker(db, "TESTBF")
	if len(rows) != 1 {
		t.Fatalf("statements = %d, want 1", len(rows))
	}
	if rows[0].Assets == nil || *rows[0].Assets != 999 {
		t.Errorf("assets = %v, want the newer 999 (older month overwrote it)", rows[0].Assets)
	}
	if !rows[0].LastSeenMonth.Equal(bfTime(2094, time.September, 1)) {
		t.Errorf("last_seen = %s, want 2094-09", rows[0].LastSeenMonth.Format("2006-01"))
	}
	if rows[0].RevisionCount != 0 {
		t.Errorf("revision = %d, want 0", rows[0].RevisionCount)
	}
	versions, _ := repo.FindVersions(db, "TESTBF", period)
	if len(versions) != 1 {
		t.Errorf("versions = %d, want 1 (an older snapshot appends none)", len(versions))
	}
}
