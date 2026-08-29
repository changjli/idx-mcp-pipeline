package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// brokerStockSummaryMaxAgeSeconds is the source_status freshness window for
// the per-stock broker summary source (~1 trading day).
const brokerStockSummaryMaxAgeSeconds int32 = 86400

// BrokerStockSummaryPayload is the payload for an idx:broker_stock_summary task.
type BrokerStockSummaryPayload struct {
	Ticker string `json:"ticker"`
	Date   string `json:"date"` // YYYY-MM-DD
}

// EnqueueBrokerStockSummary enqueues an idx:broker_stock_summary task for a
// ticker+day. The TaskID is keyed by ticker+date, so a signal for a ticker+day
// that already has a pending/active task is deduped (ErrTaskIDConflict) — at
// most one outstanding fetch per (ticker, trading_day). The IPOT client's 1h
// cache additionally makes a re-run after completion a no-op upstream.
func EnqueueBrokerStockSummary(enq pipeline.Enqueuer, ticker string, date time.Time) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeBrokerStockSummary, ticker+":"+dateKey)
	stage := pipeline.NewIngestStage(TypeBrokerStockSummary, nil, enq, 3)
	return stage.Enqueue(taskKey, BrokerStockSummaryPayload{Ticker: ticker, Date: dateKey})
}

// NewBrokerStockSummaryHandler returns an asynq handler for the
// idx:broker_stock_summary task type. It delegates to the shared
// fetch+parse+persist usecase (the same core the MCP tool uses) and updates
// source_status / alerts on success or failure via the shared recorder.
func NewBrokerStockSummaryHandler(
	log *logrus.Logger,
	uc *usecase.BrokerStockSummaryUseCase,
	recorder *pipeline.SourceStatusRecorder,
) asynq.HandlerFunc {
	stage := pipeline.NewIngestStage(TypeBrokerStockSummary, log, nil, 3)
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[BrokerStockSummaryPayload](t)
		if err != nil {
			return err
		}
		date, err := pipeline.ParseTaskDay(p.Date)
		if err != nil {
			return err
		}

		taskID := pipeline.TaskID(ctx)
		f := stage.StartFetch(taskID, "fetching broker stock summary",
			logrus.Fields{"ticker": p.Ticker, "date": p.Date})

		if _, err := uc.GetStockBrokerSummary(ctx, p.Ticker, &date); err != nil {
			f.Fail("broker stock summary fetch failed", err,
				logrus.Fields{"ticker": p.Ticker, "date": p.Date})
			recorder.Failure(TypeBrokerStockSummary, brokerStockSummaryMaxAgeSeconds, p.Date, err)
			return err
		}

		recorder.Success(TypeBrokerStockSummary, brokerStockSummaryMaxAgeSeconds, nil)
		f.Ok("broker stock summary stored",
			logrus.Fields{"ticker": p.Ticker, "date": p.Date})
		return nil
	}
}
