package mcpserver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeFinancialStatements implements usecase.FinancialStatementsReader.
type fakeFinancialStatements struct {
	rows []entity.FinancialStatement
}

func (f *fakeFinancialStatements) FindByTicker(_ *sqlx.DB, _ string) ([]entity.FinancialStatement, error) {
	return f.rows, nil
}

// fakeFinancialPrices implements usecase.FinancialPriceReader with no prices:
// the handler test only cares about the statement projection and envelope.
type fakeFinancialPrices struct{}

func (f *fakeFinancialPrices) FindCloseOnOrBefore(_ *sqlx.DB, _ string, _ time.Time) (*entity.DailyPrice, error) {
	return nil, nil
}

func (f *fakeFinancialPrices) FindLatestClose(_ *sqlx.DB, _ string) (*entity.DailyPrice, error) {
	return nil, nil
}

// newFinancialsTestServer builds a Server with only the pieces the
// get_financials handler touches: the read usecase over fakes and a ticker
// validator backed by the bundled list (nil DB). stalenessFor reports stale on
// nil wiring, which the handler ORs with the ticker-level signal.
func newFinancialsTestServer() *Server {
	period, _ := time.Parse("2006-01-02", "2026-03-31")
	older, _ := time.Parse("2006-01-02", "2025-03-31")
	revenue := int64(37_200_000_000_000)
	uc := usecase.NewFinancialsUseCase(
		nil,
		logrus.New(),
		&fakeFinancialStatements{rows: []entity.FinancialStatement{
			{Ticker: "TLKM", PeriodEnd: period, FiscalYearEnd: period, DurationMonths: 3, Revenue: &revenue},
			{Ticker: "TLKM", PeriodEnd: older, FiscalYearEnd: older, DurationMonths: 3, Revenue: &revenue},
		}},
		&fakeFinancialPrices{},
	)
	uc.Now = func() time.Time { return time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC) }
	return &Server{
		log:          logrus.New(),
		financialsUC: uc,
		tickers:      NewTickerValidator(nil, nil, logrus.New()),
	}
}

// TestHandleGetFinancialsSuccess checks the happy path: normalized ticker,
// default recent view, last_good_date from the newest stored period, the
// per-ticker stale signal, and the stored statements.
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
	// nil DB wiring makes the source-level signal stale; the handler must not
	// hide it behind the ticker's own fresh period.
	if !got.DataStale {
		t.Error("data_stale = false, want true on nil wiring (no source_status)")
	}
	if len(got.Statements) != 2 {
		t.Errorf("statements = %d, want 2", len(got.Statements))
	}
	if got.Coverage["revenue"].Status != "observed" {
		t.Errorf("revenue coverage = %+v, want observed", got.Coverage["revenue"])
	}
	if got.Coverage["cash"].Status != "unknown" {
		t.Errorf("cash coverage = %+v, want unknown", got.Coverage["cash"])
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
