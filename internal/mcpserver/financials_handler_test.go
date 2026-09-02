package mcpserver

import (
	"context"
	"encoding/json"
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeFinancialFetcher implements usecase.FinancialsFetcher for handler tests.
type fakeFinancialFetcher struct {
	lastView ipot.FinancialView
}

func (f *fakeFinancialFetcher) FetchFinancial(ctx context.Context, ticker string, view ipot.FinancialView) (*ipot.Financials, error) {
	f.lastView = view
	rev := 37.2e12
	return &ipot.Financials{
		Ticker:   ticker,
		Currency: "IDR",
		Periods: []ipot.FinancialStatement{
			{Label: "3M 2026", PeriodEnd: "2026-03-31", DurationMonths: 3, Revenue: &rev},
			{Label: "3M 2025", PeriodEnd: "2025-03-31", DurationMonths: 3},
		},
	}, nil
}

// newFinancialsTestServer builds a Server with only the pieces the
// get_financials handler touches: the usecase over a fake fetcher and a
// ticker validator backed by the bundled list (nil DB).
func newFinancialsTestServer() *Server {
	uc := usecase.NewFinancialsUseCase(nil, logrus.New(), &fakeFinancialFetcher{})
	return &Server{
		log:          logrus.New(),
		financialsUC: uc,
		tickers:      NewTickerValidator(nil, nil, logrus.New()),
	}
}

// TestHandleGetFinancialsSuccess checks the happy path: normalized ticker,
// default quarterly view, last_good_date from the newest statement period.
func TestHandleGetFinancialsSuccess(t *testing.T) {
	s := newFinancialsTestServer()
	req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "get_financials",
		Arguments: map[string]any{"ticker": "tlkm"},
	}}
	res, err := s.handleGetFinancials(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error result: %v", res.Content)
	}
	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T", res.Content[0])
	}
	var got financialsResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, text.Text)
	}
	if got.Ticker != "TLKM" {
		t.Errorf("ticker = %q, want TLKM", got.Ticker)
	}
	if got.Period != "recent" {
		t.Errorf("period = %q, want recent (default)", got.Period)
	}
	if got.LastGoodDate != "2026-03-31" {
		t.Errorf("last_good_date = %q, want 2026-03-31", got.LastGoodDate)
	}
	if got.DataStale {
		t.Error("data_stale must be false for a live fetch")
	}
	if len(got.Statements) != 2 {
		t.Errorf("statements = %d, want 2", len(got.Statements))
	}
}

// TestHandleGetFinancialsErrors checks the envelope paths: invalid ticker and
// invalid period.
func TestHandleGetFinancialsErrors(t *testing.T) {
	s := newFinancialsTestServer()
	cases := []struct {
		name     string
		args     map[string]any
		wantCode mcp.ErrorCode
	}{
		{"unknown ticker", map[string]any{"ticker": "ZZZZ"}, mcp.ErrorCodeInvalidTicker},
		{"bad period", map[string]any{"ticker": "TLKM", "period": "monthly"}, mcp.ErrorCodeInvalidArgument},
		{"missing ticker", map[string]any{}, mcp.ErrorCodeInvalidTicker},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
				Name:      "get_financials",
				Arguments: tc.args,
			}}
			res, err := s.handleGetFinancials(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !res.IsError {
				t.Fatal("result must be an error")
			}
			text, ok := res.Content[0].(mcpgo.TextContent)
			if !ok {
				t.Fatalf("content type = %T", res.Content[0])
			}
			var got mcp.ErrorEnvelope
			if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			if got.Error.Code != tc.wantCode {
				t.Errorf("code = %q, want %q", got.Error.Code, tc.wantCode)
			}
		})
	}
}
