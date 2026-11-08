package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// FinancialsSource is the source_status key the monthly financial snapshot
// writes. It is deliberately not the asynq task type (idx:financials): the
// freshness surface is the source, not the task. The task handler records
// through this same constant.
const FinancialsSource = "idx_financials"

// FinancialSnapshotFetcher fetches one validated snapshot month. *client.Client
// satisfies this; tests use a fake.
type FinancialSnapshotFetcher interface {
	FetchFinancialSnapshot(ctx context.Context, year, month int) (*financials.Snapshot, error)
}

// FinancialsIngestUseCase owns the monthly snapshot write: fetch, validate
// (the adapter already applied both guards), persist statements + versions +
// the run ledger. It is the core ticket 01's daily task and ticket 02's
// backfill both drive.
type FinancialsIngestUseCase struct {
	DB      *sqlx.DB
	Log     *logrus.Logger
	Fetcher FinancialSnapshotFetcher
	Repo    *repository.FinancialsRepository
}

// NewFinancialsIngestUseCase wires the ingest core.
func NewFinancialsIngestUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	fetcher FinancialSnapshotFetcher,
	repo *repository.FinancialsRepository,
) *FinancialsIngestUseCase {
	return &FinancialsIngestUseCase{DB: db, Log: log, Fetcher: fetcher, Repo: repo}
}

// FinancialsIngestResult is one month's outcome. Skipped means the run ledger
// already held a terminal status (ok/suspect) and no fetch was made.
type FinancialsIngestResult struct {
	SnapshotMonth          string
	Status                 string
	RowCount               int
	DistinctPeriods        int
	QuarterEndShare        *float64
	Rejected               int
	DefaultedFiscalYearEnd int
	Skipped                bool
	Stats                  repository.FinancialsIngestStats
}

// PreviousMonth returns the first day of the month before d. The newest
// snapshot the endpoint can expose is the previous calendar month: periodMonth
// M is frozen at the close of M, so the current month is not published yet.
func PreviousMonth(d time.Time) time.Time {
	first := financials.FirstOfMonth(d)
	return first.AddDate(0, -1, 0)
}

// EnsureLatest is the cheap daily check: try the previous month, skip when its
// ledger row is already ok or suspect, otherwise ingest. A month that is not
// published yet is recorded as empty and retried tomorrow; a suspect month is
// terminal because re-fetching it derives nothing new.
func (uc *FinancialsIngestUseCase) EnsureLatest(ctx context.Context, runDate time.Time) (*FinancialsIngestResult, error) {
	target := PreviousMonth(runDate)
	run, err := uc.Repo.FindRun(uc.DB, target)
	if err != nil {
		return nil, err
	}
	if run != nil && (run.Status == financials.StatusOK || run.Status == financials.StatusSuspect) {
		uc.Log.Infof("financials: %s already recorded as %s, nothing to ingest",
			target.Format("2006-01"), run.Status)
		return resultFromRun(run, true), nil
	}
	return uc.Ingest(ctx, target)
}

// Ingest fetches and persists one snapshot month. Empty and suspect months are
// recorded in the ledger but derive no statements. A fetch or malformed-payload
// error is recorded as failed (best effort) and returned so asynq retries.
func (uc *FinancialsIngestUseCase) Ingest(ctx context.Context, month time.Time) (*FinancialsIngestResult, error) {
	month = financials.FirstOfMonth(month)
	uc.Log.Infof("financials: ingesting snapshot %s", month.Format("2006-01"))

	snap, err := uc.Fetcher.FetchFinancialSnapshot(ctx, month.Year(), int(month.Month()))
	if err != nil {
		// Record the failure so a malformed month is visible in the ledger;
		// a later successful retry overwrites it.
		errText := err.Error()
		failed := &entity.FinancialSnapshotRun{
			SnapshotMonth: month,
			Source:        financials.SourceRatioSnapshot,
			Status:        financials.StatusFailed,
			Error:         &errText,
		}
		if recErr := uc.Repo.UpsertRun(uc.DB, failed); recErr != nil {
			uc.Log.Errorf("financials: record failed run %s: %v", month.Format("2006-01"), recErr)
		}
		return nil, fmt.Errorf("financials ingest %s: %w", month.Format("2006-01"), err)
	}

	run := &entity.FinancialSnapshotRun{
		SnapshotMonth:   month,
		Source:          financials.SourceRatioSnapshot,
		Status:          snap.Status,
		RowCount:        snap.RowCount,
		DistinctPeriods: snap.DistinctPeriods,
		QuarterEndShare: snap.QuarterEndShare,
	}
	if snap.ContentHash != "" {
		hash := snap.ContentHash
		run.ContentHash = &hash
	}
	// A suspect or empty month derives nothing; only ok months have statements.
	if snap.Status != financials.StatusOK {
		if err := uc.Repo.UpsertRun(uc.DB, run); err != nil {
			return nil, err
		}
	}

	var stats repository.FinancialsIngestStats
	if snap.Status == financials.StatusOK {
		st, ingestErr := uc.Repo.IngestSnapshot(uc.DB, snap, run)
		if ingestErr != nil {
			return nil, ingestErr
		}
		stats = *st
	}

	if snap.DefaultedFiscalYearEnd > 0 {
		uc.Log.Infof("financials: %s: %d row(s) had no fiscalYearEnd and defaulted to December",
			month.Format("2006-01"), snap.DefaultedFiscalYearEnd)
	}
	for _, rej := range snap.Rejected {
		uc.Log.Warnf("financials: %s: rejected %s: %s", month.Format("2006-01"), rej.Ticker, rej.Reason)
	}

	switch snap.Status {
	case financials.StatusEmpty:
		uc.Log.Infof("financials: %s returned no rows (not published yet); will retry", month.Format("2006-01"))
	case financials.StatusSuspect:
		uc.Log.Warnf("financials: %s recorded as suspect (quarter-end share below %.0f%%), no statements derived",
			month.Format("2006-01"), financials.SuspectQuarterEndShareFloor*100)
	case financials.StatusOK:
		uc.Log.Infof("financials: %s ingested rows=%d distinct_periods=%d rejected=%d inserted=%d updated=%d unchanged=%d skipped=%d versions=%d",
			month.Format("2006-01"), snap.RowCount, snap.DistinctPeriods, len(snap.Rejected),
			stats.Inserted, stats.Updated, stats.Unchanged, stats.Skipped, stats.Versions)
	}

	return &FinancialsIngestResult{
		SnapshotMonth:          month.Format("2006-01"),
		Status:                 snap.Status,
		RowCount:               snap.RowCount,
		DistinctPeriods:        snap.DistinctPeriods,
		QuarterEndShare:        snap.QuarterEndShare,
		Rejected:               len(snap.Rejected),
		DefaultedFiscalYearEnd: snap.DefaultedFiscalYearEnd,
		Stats:                  stats,
	}, nil
}

// resultFromRun adapts an existing ledger row to the ingest result.
func resultFromRun(run *entity.FinancialSnapshotRun, skipped bool) *FinancialsIngestResult {
	return &FinancialsIngestResult{
		SnapshotMonth:   run.SnapshotMonth.Format("2006-01"),
		Status:          run.Status,
		RowCount:        run.RowCount,
		DistinctPeriods: run.DistinctPeriods,
		QuarterEndShare: run.QuarterEndShare,
		Skipped:         skipped,
	}
}
