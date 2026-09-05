package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// BrokerStockSummaryMaxAgeSeconds is the source_status freshness window for
// the per-stock broker summary source (~1 trading day).
const BrokerStockSummaryMaxAgeSeconds int32 = 86400

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
	return Graph.Node(TypeBrokerStockSummary).Enqueue(enq, date, []string{"ticker=" + ticker})
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
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, p.Date, err)
			return err
		}

		recorder.Success(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, nil)
		f.Ok("broker stock summary stored",
			logrus.Fields{"ticker": p.Ticker, "date": p.Date})
		return nil
	}
}

// BrokerStockSummaryRangePayload is the payload for an
// idx:broker_stock_summary_range task (issue 12): one ticker over a date range.
type BrokerStockSummaryRangePayload struct {
	Ticker string `json:"ticker"`
	From   string `json:"from"` // YYYY-MM-DD
	To     string `json:"to"`   // YYYY-MM-DD
}

// EnqueueBrokerStockSummaryRange enqueues an idx:broker_stock_summary_range task
// for a ticker over a date range. The TaskID is keyed by ticker+from+to, so a
// duplicate backfill signal for the same range is deduped (ErrTaskIDConflict) —
// at most one outstanding range backfill per (ticker, from, to).
func EnqueueBrokerStockSummaryRange(enq pipeline.Enqueuer, ticker string, from, to time.Time) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeBrokerStockSummaryRange).Enqueue(enq, time.Time{}, []string{
		"ticker=" + ticker,
		"from=" + from.Format("2006-01-02"),
		"to=" + to.Format("2006-01-02"),
	})
}

// brokerStockSummaryRangeEnqueuer adapts EnqueueBrokerStockSummaryRange to the
// usecase's async seam (issue 12): enqueue a range backfill task for one ticker
// over a date range. The per-range TaskID dedup surfaces as
// asynq.ErrTaskIDConflict, which the usecase maps to the pending envelope.
type brokerStockSummaryRangeEnqueuer struct{ enq pipeline.Enqueuer }

func (e brokerStockSummaryRangeEnqueuer) EnqueueBrokerStockSummaryRange(ticker string, from, to time.Time) error {
	_, err := EnqueueBrokerStockSummaryRange(e.enq, ticker, from, to)
	return err
}

// NewBrokerStockSummaryRangeEnqueuer returns the usecase's async enqueue seam
// over the given pipeline.Enqueuer (the asynq client in production).
func NewBrokerStockSummaryRangeEnqueuer(enq pipeline.Enqueuer) usecase.BrokerStockSummaryRangeEnqueuer {
	return brokerStockSummaryRangeEnqueuer{enq: enq}
}

// NewBrokerStockSummaryRangeHandler returns an asynq handler for the
// idx:broker_stock_summary_range task type. It delegates to the shared
// GetStockBrokerSummaryRange usecase (the same core the CLI bulk mode and the
// MCP tool use) and updates source_status under the per-stock broker summary
// source — the backfill refreshes the same data the read tools report on.
func NewBrokerStockSummaryRangeHandler(
	log *logrus.Logger,
	uc *usecase.BrokerStockSummaryUseCase,
	recorder *pipeline.SourceStatusRecorder,
) asynq.HandlerFunc {
	stage := pipeline.NewIngestStage(TypeBrokerStockSummaryRange, log, nil, 3)
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[BrokerStockSummaryRangePayload](t)
		if err != nil {
			return err
		}
		from, err := pipeline.ParseTaskDay(p.From)
		if err != nil {
			return err
		}
		to, err := pipeline.ParseTaskDay(p.To)
		if err != nil {
			return err
		}

		taskID := pipeline.TaskID(ctx)
		f := stage.StartFetch(taskID, "backfilling broker stock summary range",
			logrus.Fields{"ticker": p.Ticker, "from": p.From, "to": p.To})

		resp, err := uc.GetStockBrokerSummaryRange(ctx, p.Ticker, from, to)
		if err != nil {
			f.Fail("broker stock summary range backfill failed", err,
				logrus.Fields{"ticker": p.Ticker, "from": p.From, "to": p.To})
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, p.To, err)
			return err
		}

		var lastGood *time.Time
		if len(resp.Days) > 0 {
			d, _ := time.Parse("2006-01-02", resp.Days[len(resp.Days)-1].TradingDay)
			lastGood = &d
		}
		recorder.Success(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, lastGood)
		f.Ok("broker stock summary range stored",
			logrus.Fields{"ticker": p.Ticker, "from": p.From, "to": p.To, "fetched": resp.Fetched, "failed": resp.Failed, "empty": resp.Empty})
		return nil
	}
}
