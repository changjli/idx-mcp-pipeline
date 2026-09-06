package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// BrokerStockSummaryRangeEnqueuer is the async backfill seam: enqueue an
// idx:broker_stock_summary_range task for one ticker over a date range.
// Implemented by the asynq wiring in cmd/mcp-server (adapter over
// tasks.EnqueueBrokerStockSummaryRange). The worker owns the fetch+persist loop
// (GetStockBrokerSummaryRange); the tool returns a pending envelope and the
// client polls get_stock_broker_summary_history for completion.
type BrokerStockSummaryRangeEnqueuer interface {
	EnqueueBrokerStockSummaryRange(ticker string, from, to time.Time) error
}

// BrokerSummaryBackfillUseCase is the MCP backfill tool's usecase (issue 12).
// It is deliberately thin: validate the range, enqueue the async task, return
// the pending envelope. The actual fetch+persist work happens in the worker —
// a synchronous range call would blow the Heroku 30s router limit (N IPOT
// fetches per request). The operator path (CLI bulk mode) calls
// GetStockBrokerSummaryRange directly and is not routed through here.
type BrokerSummaryBackfillUseCase struct {
	Log      *logrus.Logger
	Validate *validator.Validate
	Enqueuer BrokerStockSummaryRangeEnqueuer
}

func NewBrokerSummaryBackfillUseCase(
	log *logrus.Logger,
	validate *validator.Validate,
	enqueuer BrokerStockSummaryRangeEnqueuer,
) *BrokerSummaryBackfillUseCase {
	return &BrokerSummaryBackfillUseCase{
		Log:      log,
		Validate: validate,
		Enqueuer: enqueuer,
	}
}

// BrokerSummaryBackfillData is a backfill_stock_broker_summary response. Status
// is always "pending" on success — the task is enqueued, the worker owns the
// fetch+persist, and the client polls get_stock_broker_summary_history until
// the range's days are covered.
type BrokerSummaryBackfillData struct {
	Ticker string `json:"ticker"`
	From   string `json:"from"`
	To     string `json:"to"`
	Status string `json:"status"`
}

// BackfillStockBrokerSummary validates the range and enqueues the async
// backfill task. A duplicate enqueue for the same (ticker, from, to) surfaces
// as asynq.ErrTaskIDConflict and maps to the same pending envelope — idempotent.
func (uc *BrokerSummaryBackfillUseCase) BackfillStockBrokerSummary(ctx context.Context, ticker string, from, to time.Time) (*BrokerSummaryBackfillData, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}
	if from.After(to) {
		return nil, ErrInvalidRange
	}
	if uc.Enqueuer == nil {
		return nil, fmt.Errorf("backfill enqueuer not configured")
	}

	if err := uc.Enqueuer.EnqueueBrokerStockSummaryRange(ticker, from, to); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			// Already queued/running for this range — same pending envelope,
			// idempotent (the TaskID dedup is the guard).
			return pendingBackfillEnvelope(ticker, from, to), nil
		}
		return nil, fmt.Errorf("enqueue broker stock summary range: %w", err)
	}
	return pendingBackfillEnvelope(ticker, from, to), nil
}

// pendingBackfillEnvelope is the async backfill response: the task is enqueued,
// the worker owns the transition to stored rows. The client polls
// get_stock_broker_summary_history.
func pendingBackfillEnvelope(ticker string, from, to time.Time) *BrokerSummaryBackfillData {
	return &BrokerSummaryBackfillData{
		Ticker: ticker,
		From:   from.Format("2006-01-02"),
		To:     to.Format("2006-01-02"),
		Status: "pending",
	}
}
