package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
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
func EnqueueBrokerStockSummary(client *asynq.Client, ticker string, date time.Time) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeBrokerStockSummary, ticker+":"+dateKey)
	payload := BrokerStockSummaryPayload{Ticker: ticker, Date: dateKey}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal broker_stock_summary payload: %w", err)
	}

	task := asynq.NewTask(TypeBrokerStockSummary, raw)
	return client.Enqueue(task,
		asynq.TaskID(taskKey),
		asynq.Queue("ingest"),
		asynq.MaxRetry(3),
		asynq.Retention(24*time.Hour),
	)
}

// NewBrokerStockSummaryHandler returns an asynq handler for the
// idx:broker_stock_summary task type. It delegates to the shared
// fetch+parse+persist usecase (the same core the MCP tool uses) and updates
// source_status / alerts on success or failure.
func NewBrokerStockSummaryHandler(
	log *logrus.Logger,
	db *sqlx.DB,
	uc *usecase.BrokerStockSummaryUseCase,
	sourceStatusRepo *repository.SourceStatusRepository,
	alertRepo *repository.AlertRepository,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var p BrokerStockSummaryPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		date, err := time.Parse("2006-01-02", p.Date)
		if err != nil {
			return fmt.Errorf("invalid date %q: %w", p.Date, err)
		}

		log.Infof("broker_stock_summary: fetching %s for %s", p.Ticker, p.Date)

		if _, err := uc.GetStockBrokerSummary(ctx, p.Ticker, &date); err != nil {
			log.Errorf("broker_stock_summary: %s %s failed: %v", p.Ticker, p.Date, err)
			recordSourceFailure(db, sourceStatusRepo, alertRepo, TypeBrokerStockSummary, brokerStockSummaryMaxAgeSeconds, p.Date, err, log)
			return err
		}

		recordSourceSuccess(db, sourceStatusRepo, TypeBrokerStockSummary, brokerStockSummaryMaxAgeSeconds, nil, log)
		log.Infof("broker_stock_summary: stored %s for %s", p.Ticker, p.Date)
		return nil
	}
}
