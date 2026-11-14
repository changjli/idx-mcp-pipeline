package tasks

import (
	"time"

	"github.com/hibiken/asynq"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// EnqueueFinancialDepth enqueues one depth task. An empty ticker runs the
// monthly sweep; a ticker runs the on-demand single-ticker path (issue 07).
// month is YYYY-MM or empty to let the handler derive it. force re-runs a
// ticker whose ledger row is already terminal — the manual/quarterly retry
// path for a not_covered ticker. The TaskID dedupes a repeated on-demand
// request for the same ticker/month and a repeated monthly sweep.
func EnqueueFinancialDepth(enq pipeline.Enqueuer, day time.Time, ticker, month string, force bool, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	dateKey := day.Format("2006-01-02")
	payload := FinancialDepthPayload{Date: dateKey, Ticker: ticker, Month: month, Force: force}

	dedup := TypeFinancialsDepth + ":" + dateKey
	if month != "" {
		dedup = TypeFinancialsDepth + ":" + month
	}
	if ticker != "" {
		dedup += ":" + ticker
	}

	stage := pipeline.NewIngestStage(TypeFinancialsDepth, nil, enq, 3)
	return stage.EnqueueWithOpts(dedup, payload, opts...)
}

// financialDepthEnqueuer adapts the read path's on-demand seam to the task
// helper (issue 07). It keys the task on the current month so the ledger row
// and the TaskID are per-ticker-per-month, matching the handler's on-demand
// month derivation.
type financialDepthEnqueuer struct {
	enq pipeline.Enqueuer
	now func() time.Time
}

func (e financialDepthEnqueuer) EnqueueFinancialDepth(ticker string) error {
	now := e.now()
	_, err := EnqueueFinancialDepth(e.enq, now, ticker, now.Format("2006-01"), false)
	return err
}

// NewFinancialDepthEnqueuer returns the read path's on-demand depth seam over
// the given pipeline.Enqueuer (the asynq client in production). A repeat
// enqueue for the same ticker surfaces as asynq.ErrTaskIDConflict, which the
// usecase maps to the same pending answer.
func NewFinancialDepthEnqueuer(enq pipeline.Enqueuer) usecase.FinancialDepthEnqueuer {
	return financialDepthEnqueuer{enq: enq, now: time.Now}
}
