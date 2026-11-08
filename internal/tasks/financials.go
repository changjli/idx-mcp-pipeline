package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

const (
	// FinancialsMaxAgeSeconds is the source_status max age for idx_financials
	// (~45 days). The source is monthly plus a publication lag: a snapshot for
	// month M appears only after M closes, so 45 days keeps the envelope fresh
	// across one skipped publication while making a two-month gap visible.
	FinancialsMaxAgeSeconds int32 = 45 * 86400
)

// FinancialsPayload is the payload for an idx:financials task. Date is the
// run date; the handler derives the target snapshot month (the previous
// calendar month) from it.
type FinancialsPayload struct {
	Date string `json:"date"` // YYYY-MM-DD run date
}

// NewFinancialsHandler returns an asynq handler for the idx:financials task
// type. It is a cheap daily "ensure the newest available month" check: the
// previous calendar month is fetched once, and the run ledger makes later
// fires a no-op after the first success. A 0-row response means the month is
// not published yet and is retried the next day. The MCP read path will read
// the stored statements (issue 03) — the request path never touches IDX
// (Heroku H12 constraint, ADR-0009).
func NewFinancialsHandler(
	log *logrus.Logger,
	uc *usecase.FinancialsIngestUseCase,
	recorder *pipeline.SourceStatusRecorder,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[FinancialsPayload](t)
		if err != nil {
			return err
		}
		runDate, err := pipeline.ParseTaskDay(p.Date)
		if err != nil {
			return err
		}

		res, ingestErr := uc.EnsureLatest(ctx, runDate)
		if ingestErr != nil {
			recorder.Failure(usecase.FinancialsSource, FinancialsMaxAgeSeconds, p.Date, ingestErr)
			return ingestErr
		}

		// Only a newly ingested snapshot touches source_status. A skipped run is
		// a no-op, and an empty response means the month is not published yet —
		// neither is a data success, so the freshness clock keeps running from
		// the last month that actually landed. That is what makes the 45-day
		// window detect a publication outage instead of being refreshed by the
		// daily check itself.
		if !res.Skipped && (res.Status == financials.StatusOK || res.Status == financials.StatusSuspect) {
			month, parseErr := time.Parse("2006-01", res.SnapshotMonth)
			if parseErr == nil {
				recorder.Success(usecase.FinancialsSource, FinancialsMaxAgeSeconds, &month)
			}
		}

		if res.Skipped {
			log.Infof("financials: %s already %s, no fetch", res.SnapshotMonth, res.Status)
		} else {
			log.Infof("financials: %s status=%s rows=%d periods=%d rejected=%d",
				res.SnapshotMonth, res.Status, res.RowCount, res.DistinctPeriods, res.Rejected)
		}
		return nil
	}
}
