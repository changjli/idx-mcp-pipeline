package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeRangeEnqueuer records the enqueue call and returns a canned error.
type fakeRangeEnqueuer struct {
	err    error
	ticker string
	from   time.Time
	to     time.Time
	called bool
}

func (f *fakeRangeEnqueuer) EnqueueBrokerStockSummaryRange(ticker string, from, to time.Time) error {
	f.called = true
	f.ticker = ticker
	f.from = from
	f.to = to
	return f.err
}

// backfillTestServer builds a Server wired to a real backfill usecase over a
// fake enqueuer — no DB, no asynq (the enqueuer seam is faked).
func backfillTestServer(enq *fakeRangeEnqueuer) *Server {
	uc := usecase.NewBrokerSummaryBackfillUseCase(
		logrus.New(), validator.New(), enq,
	)
	return &Server{
		brokerSummaryBackfillUC: uc,
		tickers:                 NewTickerValidator(nil, nil, logrus.New()),
	}
}

// TestHandleBackfillStockBrokerSummaryInvalidArgs covers the parse path: ticker
// normalization and date parsing return the envelope before the enqueuer is
// touched (TickerValidator over the bundled list, no DB needed).
func TestHandleBackfillStockBrokerSummaryInvalidArgs(t *testing.T) {
	s := backfillTestServer(&fakeRangeEnqueuer{})

	cases := []struct {
		name string
		args map[string]any
		want mcp.ErrorCode
	}{
		{"invalid ticker", map[string]any{"ticker": "ZZZZ", "from": "2026-01-01", "to": "2026-01-31"}, mcp.ErrorCodeInvalidTicker},
		{"empty ticker", map[string]any{"ticker": "  ", "from": "2026-01-01", "to": "2026-01-31"}, mcp.ErrorCodeInvalidTicker},
		{"invalid from", map[string]any{"ticker": "BBCA", "from": "abc", "to": "2026-01-31"}, mcp.ErrorCodeInvalidArgument},
		{"invalid to", map[string]any{"ticker": "BBCA", "from": "2026-01-01", "to": "2026-13-40"}, mcp.ErrorCodeInvalidArgument},
		{"from after to", map[string]any{"ticker": "BBCA", "from": "2026-01-31", "to": "2026-01-01"}, mcp.ErrorCodeInvalidArgument},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
				Name:      "backfill_stock_broker_summary",
				Arguments: tc.args,
			}}
			res, err := s.handleBackfillStockBrokerSummary(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !res.IsError {
				t.Fatal("result must be an error")
			}
			text, ok := res.Content[0].(mcpgo.TextContent)
			if !ok {
				t.Fatalf("content type = %T, want text", res.Content[0])
			}
			var got mcp.ErrorEnvelope
			if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			if got.Error.Code != tc.want {
				t.Fatalf("code = %q, want %q", got.Error.Code, tc.want)
			}
		})
	}
}

// TestHandleBackfillStockBrokerSummarySuccess — valid range: the enqueuer is
// called with the normalized ticker and the pending envelope is returned.
func TestHandleBackfillStockBrokerSummarySuccess(t *testing.T) {
	enq := &fakeRangeEnqueuer{}
	s := backfillTestServer(enq)

	req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name: "backfill_stock_broker_summary",
		Arguments: map[string]any{
			"ticker": "bbca",
			"from":   "2026-01-01",
			"to":     "2026-01-31",
		},
	}}
	res, err := s.handleBackfillStockBrokerSummary(context.Background(), req)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.IsError {
		text, _ := res.Content[0].(mcpgo.TextContent)
		t.Fatalf("handler returned error: %s", text.Text)
	}
	if !enq.called {
		t.Fatal("enqueuer not called")
	}
	if enq.ticker != "BBCA" {
		t.Errorf("enqueued ticker = %q, want BBCA (normalized)", enq.ticker)
	}
	if enq.from.Format("2006-01-02") != "2026-01-01" || enq.to.Format("2006-01-02") != "2026-01-31" {
		t.Errorf("enqueued range = %s..%s, want 2026-01-01..2026-01-31", enq.from, enq.to)
	}

	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want text", res.Content[0])
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if got["status"] != "pending" {
		t.Errorf("status = %v, want pending", got["status"])
	}
	if got["ticker"] != "BBCA" || got["from"] != "2026-01-01" || got["to"] != "2026-01-31" {
		t.Errorf("envelope = %v, want ticker/from/to echoed", got)
	}
}

// TestHandleBackfillStockBrokerSummaryEnqueueError — the enqueuer fails: the
// error envelope is returned, not a success body.
func TestHandleBackfillStockBrokerSummaryEnqueueError(t *testing.T) {
	enq := &fakeRangeEnqueuer{err: errors.New("redis down")}
	s := backfillTestServer(enq)

	req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name: "backfill_stock_broker_summary",
		Arguments: map[string]any{
			"ticker": "BBCA",
			"from":   "2026-01-01",
			"to":     "2026-01-31",
		},
	}}
	res, err := s.handleBackfillStockBrokerSummary(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("result must be an error")
	}
	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want text", res.Content[0])
	}
	var got mcp.ErrorEnvelope
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if got.Error.Code != mcp.ErrorCodeInternal {
		t.Fatalf("code = %q, want %q", got.Error.Code, mcp.ErrorCodeInternal)
	}
}
