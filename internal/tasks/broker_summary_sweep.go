package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// sweepTaskTimeout is the task-level asynq timeout for the full-market sweep.
// A fresh sweep fetches ~hundreds of traded tickers at the IPOT client's shared
// 2s pacing, so it routinely exceeds asynq's 30m default; the date-keyed TaskID
// dedup means a re-run (self-heal or manual) skips already-stored tickers.
const sweepTaskTimeout = 2 * time.Hour

// BrokerSummarySweepPayload is the payload for an idx:broker_stock_summary_sweep
// task: one date, the whole market.
type BrokerSummarySweepPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// EnqueueBrokerSummarySweep enqueues an idx:broker_stock_summary_sweep task for
// a date. The TaskID is date-keyed, so a duplicate sweep for the same day is
// deduped (ErrTaskIDConflict) — at most one outstanding full-market sweep per
// day.
func EnqueueBrokerSummarySweep(enq pipeline.Enqueuer, date time.Time) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeBrokerStockSummarySweep).Enqueue(enq, date, nil)
}

// NewBrokerSummarySweepHandler returns an asynq handler for the
// idx:broker_stock_summary_sweep task type: enumerate active tickers, run the
// shared sweep usecase (skip-if-stored + fetch+persist, paced by the IPOT
// client), and record source_status. A full-market failure (every traded
// ticker failed) returns an error so asynq retries; partial failures are
// isolated per ticker inside the usecase.
func NewBrokerSummarySweepHandler(
	log *logrus.Logger,
	db *sqlx.DB,
	uc *usecase.BrokerStockSummaryUseCase,
	tickerRepo *repository.TickerRepository,
	recorder *pipeline.SourceStatusRecorder,
) asynq.HandlerFunc {
	stage := pipeline.NewIngestStage(TypeBrokerStockSummarySweep, log, nil, 3)
	return func(ctx context.Context, t *asynq.Task) error {
		// The daily scheduler fires this task with a nil payload; the sweep
		// defaults to today server-side, same convention as pipeline:daily.
		date := time.Now()
		if len(t.Payload()) > 0 {
			p, err := pipeline.DecodeTask[BrokerSummarySweepPayload](t)
			if err != nil {
				return err
			}
			date, err = pipeline.ParseTaskDay(p.Date)
			if err != nil {
				return err
			}
		}
		dateStr := date.Format("2006-01-02")

		tickers, err := tickerRepo.FindAll(db)
		if err != nil {
			return fmt.Errorf("list active tickers: %w", err)
		}
		codes := make([]string, 0, len(tickers))
		for _, tk := range tickers {
			codes = append(codes, tk.Code)
		}

		taskID := pipeline.TaskID(ctx)
		f := stage.StartFetch(taskID, "sweeping market broker summaries",
			logrus.Fields{"date": dateStr, "universe": len(codes)})

		res, err := uc.SweepStockBrokerSummaries(ctx, codes, date)
		if err != nil {
			f.Fail("broker summary sweep failed", err, logrus.Fields{"date": dateStr})
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		// Every traded ticker failed (nothing stored, nothing already present) →
		// indistinguishable from an outage; return an error so asynq retries.
		if res.Total > 0 && res.Fetched == 0 && res.Skipped == 0 && res.Failed > 0 {
			err := fmt.Errorf("broker summary sweep %s: all %d traded tickers failed", dateStr, res.Failed)
			f.Fail("broker summary sweep total failure", err, logrus.Fields{"date": dateStr})
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		// Advance the watermark only when the day actually produced/confirmed
		// data; a non-trading-day no-op carries the prior watermark forward.
		var lastGood *time.Time
		if res.Fetched > 0 || res.Skipped > 0 {
			lastGood = &date
		}
		recorder.Success(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, lastGood)
		f.Ok("broker summary sweep complete",
			logrus.Fields{"date": dateStr, "total": res.Total, "not_traded": res.NotTraded, "skipped": res.Skipped, "fetched": res.Fetched, "empty": res.Empty, "failed": res.Failed})
		return nil
	}
}
