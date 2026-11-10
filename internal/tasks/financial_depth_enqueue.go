package tasks

import (
	"time"

	"github.com/hibiken/asynq"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
)

// EnqueueFinancialDepth enqueues one depth task. An empty ticker runs the
// monthly sweep; a ticker runs the on-demand single-ticker path. month is
// YYYY-MM or empty to let the handler derive it. The TaskID dedupes a repeated
// on-demand request for the same ticker/month and a repeated monthly sweep.
func EnqueueFinancialDepth(enq pipeline.Enqueuer, day time.Time, ticker, month string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	dateKey := day.Format("2006-01-02")
	payload := FinancialDepthPayload{Date: dateKey, Ticker: ticker, Month: month}

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
