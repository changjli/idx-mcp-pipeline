package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// FinancialsBackfillFloorMonth returns the first day of the backfill floor
// month: 2021-01, the first month with full-market coverage. 2018–2020 returns
// only 4–11 straggler rows and has a gap at 2020-12, so "walk back until the
// source returns nothing" would stop too early or too late. The floor is
// recorded explicitly rather than inferred.
func FinancialsBackfillFloorMonth() time.Time {
	return time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)
}

// FinancialsBackfillOptions is one manual backfill run. From and To are month
// bounds; either may be zero to take the default. RunDate decides "the newest
// available month" (periodMonth M is frozen at the close of M, so the newest
// possible month is the previous calendar month). Force re-processes months
// whose ledger row is already ok or suspect.
type FinancialsBackfillOptions struct {
	From    time.Time // month bound; zero = PreviousMonth(RunDate)
	To      time.Time // month bound; zero = FinancialsBackfillFloorMonth()
	RunDate time.Time // zero = time.Now()
	Force   bool
}

// FinancialsBackfillMonth is one month's outcome in a backfill run. Skipped
// means the ledger already held a terminal status (ok/suspect) and no fetch was
// made. Err is set only for a failed month.
type FinancialsBackfillMonth struct {
	SnapshotMonth   string
	Status          string
	RowCount        int
	DistinctPeriods int
	Rejected        int
	Skipped         bool
	Stats           repository.FinancialsIngestStats
	Err             error
}

// FinancialsBackfillResult summarizes a walk over a month range. MonthsProcessed
// counts attempted months (including failures); MonthsSkipped counts months the
// ledger already settled. RowsWritten is Inserted+Updated across the walk —
// unchanged repeats and skipped months write nothing.
type FinancialsBackfillResult struct {
	From            string
	To              string
	Floor           string
	Direction       string // "backward" or "forward"
	MonthsTotal     int
	MonthsProcessed int
	MonthsSkipped   int
	MonthsOK        int
	MonthsEmpty     int
	MonthsSuspect   int
	MonthsFailed    int
	RowsWritten     int
	RowsRejected    int
	Months          []FinancialsBackfillMonth
}

// Backfill walks a month range and ingests every month that is not already
// settled in the run ledger. The walk is resumable and order-safe: an ok or
// suspect month is skipped unless Force is set, empty months are recorded and
// the walk continues, and the 01 ordered upsert guarantees an older month can
// never overwrite a newer observation for the same statement.
//
// A failed fetch or malformed payload is recorded in the ledger by Ingest and
// the walk continues. The caller decides whether an all-failed run is an error
// (cmd/enqueue-daily exits non-zero only when every attempted month failed).
func (uc *FinancialsIngestUseCase) Backfill(ctx context.Context, opts FinancialsBackfillOptions) (*FinancialsBackfillResult, error) {
	from, to, err := resolveFinancialsBackfillBounds(opts.From, opts.To, opts.RunDate)
	if err != nil {
		return nil, err
	}
	months := financialsMonthsBetween(from, to)
	if len(months) == 0 {
		return nil, fmt.Errorf("financials backfill: empty month range %s to %s",
			from.Format("2006-01"), to.Format("2006-01"))
	}

	res := &FinancialsBackfillResult{
		From:        from.Format("2006-01"),
		To:          to.Format("2006-01"),
		Floor:       FinancialsBackfillFloorMonth().Format("2006-01"),
		Direction:   "forward",
		MonthsTotal: len(months),
		Months:      make([]FinancialsBackfillMonth, 0, len(months)),
	}
	if from.After(to) {
		res.Direction = "backward"
	}

	uc.Log.Infof("financials backfill: %d month(s) %s to %s %s, floor %s, force=%t",
		res.MonthsTotal, res.From, res.To, res.Direction, res.Floor, opts.Force)

	for i, month := range months {
		monthKey := month.Format("2006-01")
		remaining := len(months) - i - 1

		run, err := uc.Repo.FindRun(uc.DB, month)
		if err != nil {
			return nil, err
		}
		if run != nil && !opts.Force && (run.Status == financials.StatusOK || run.Status == financials.StatusSuspect) {
			res.MonthsSkipped++
			res.Months = append(res.Months, FinancialsBackfillMonth{
				SnapshotMonth:   monthKey,
				Status:          run.Status,
				RowCount:        run.RowCount,
				DistinctPeriods: run.DistinctPeriods,
				Skipped:         true,
			})
			uc.Log.Infof("financials backfill: %s skipped (ledger %s) [%d/%d done, %d remaining]",
				monthKey, run.Status, i+1, len(months), remaining)
			continue
		}

		ingestRes, ingestErr := uc.Ingest(ctx, month)
		res.MonthsProcessed++
		if ingestErr != nil {
			res.MonthsFailed++
			res.Months = append(res.Months, FinancialsBackfillMonth{
				SnapshotMonth: monthKey,
				Status:        financials.StatusFailed,
				Err:           ingestErr,
			})
			uc.Log.Errorf("financials backfill: %s failed: %v [%d/%d done, %d remaining]",
				monthKey, ingestErr, i+1, len(months), remaining)
			continue
		}

		switch ingestRes.Status {
		case financials.StatusOK:
			res.MonthsOK++
		case financials.StatusEmpty:
			res.MonthsEmpty++
		case financials.StatusSuspect:
			res.MonthsSuspect++
		}
		res.RowsWritten += ingestRes.Stats.Inserted + ingestRes.Stats.Updated
		res.RowsRejected += ingestRes.Rejected
		res.Months = append(res.Months, FinancialsBackfillMonth{
			SnapshotMonth:   monthKey,
			Status:          ingestRes.Status,
			RowCount:        ingestRes.RowCount,
			DistinctPeriods: ingestRes.DistinctPeriods,
			Rejected:        ingestRes.Rejected,
			Stats:           ingestRes.Stats,
		})
		uc.Log.Infof("financials backfill: %s status=%s rows=%d distinct_periods=%d rejected=%d written=%d [%d/%d done, %d remaining] totals rows_written=%d rows_rejected=%d",
			monthKey, ingestRes.Status, ingestRes.RowCount, ingestRes.DistinctPeriods,
			ingestRes.Rejected, ingestRes.Stats.Inserted+ingestRes.Stats.Updated,
			i+1, len(months), remaining, res.RowsWritten, res.RowsRejected)
	}

	uc.Log.Infof("financials backfill complete: %s to %s %s, floor %s — ok=%d suspect=%d empty=%d failed=%d skipped=%d, rows_written=%d rows_rejected=%d",
		res.From, res.To, res.Direction, res.Floor,
		res.MonthsOK, res.MonthsSuspect, res.MonthsEmpty, res.MonthsFailed, res.MonthsSkipped,
		res.RowsWritten, res.RowsRejected)
	return res, nil
}

// resolveFinancialsBackfillBounds applies the defaults and validates both
// bounds. A month is never allowed above the newest available month (the
// previous calendar month) or below the 2021-01 floor.
func resolveFinancialsBackfillBounds(from, to, runDate time.Time) (time.Time, time.Time, error) {
	if runDate.IsZero() {
		runDate = time.Now()
	}
	newest := PreviousMonth(runDate)
	floor := FinancialsBackfillFloorMonth()

	if from.IsZero() {
		from = newest
	} else {
		from = financials.FirstOfMonth(from)
	}
	if to.IsZero() {
		to = floor
	} else {
		to = financials.FirstOfMonth(to)
	}

	if newest.Before(floor) {
		return time.Time{}, time.Time{}, fmt.Errorf(
			"financials backfill: no available month at or above the %s floor yet",
			floor.Format("2006-01"))
	}
	for _, bound := range []time.Time{from, to} {
		if bound.Before(floor) {
			return time.Time{}, time.Time{}, fmt.Errorf(
				"financials backfill: month %s is before the %s floor",
				bound.Format("2006-01"), floor.Format("2006-01"))
		}
		if bound.After(newest) {
			return time.Time{}, time.Time{}, fmt.Errorf(
				"financials backfill: month %s is not available yet (newest is %s)",
				bound.Format("2006-01"), newest.Format("2006-01"))
		}
	}
	return from, to, nil
}

// financialsMonthsBetween returns every month from from to to inclusive, with
// the direction following the bounds: from after to walks backwards.
func financialsMonthsBetween(from, to time.Time) []time.Time {
	step := 1
	if from.After(to) {
		step = -1
	}
	var months []time.Time
	for m := from; ; m = m.AddDate(0, step, 0) {
		months = append(months, m)
		if m.Equal(to) {
			break
		}
	}
	return months
}
