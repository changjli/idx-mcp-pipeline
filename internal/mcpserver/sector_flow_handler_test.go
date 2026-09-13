package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeSectorFlowReader implements usecase.SectorFlowReader for handler tests;
// it records the request it was called with.
type fakeSectorFlowReader struct {
	data *usecase.SectorFlowResponse
	err  error
	got  usecase.SectorFlowRequest
}

func (f *fakeSectorFlowReader) GetSectorFlow(ctx context.Context, req usecase.SectorFlowRequest) (*usecase.SectorFlowResponse, error) {
	f.got = req
	return f.data, f.err
}

// newSectorFlowTestServer builds a Server with a fake sector-flow reader and no
// DB (stalenessFor reports stale on nil wiring).
func newSectorFlowTestServer(reader usecase.SectorFlowReader) *Server {
	return &Server{
		log:          logrus.New(),
		sectorFlowUC: reader,
		tickers:      NewTickerValidator(nil, nil, logrus.New()),
	}
}

func sectorFlowReq(args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "get_sector_flow",
		Arguments: args,
	}}
}

// TestHandleGetSectorFlowSuccess checks the happy path: dates parse, the
// sector/group_by arguments pass through to the usecase, and the response
// comes back inside the staleness envelope.
func TestHandleGetSectorFlowSuccess(t *testing.T) {
	reader := &fakeSectorFlowReader{data: &usecase.SectorFlowResponse{
		From: "2026-08-01", To: "2026-08-31", GroupBy: "sektor", Sector: "Financials",
		TradeDaysInWindow: 20, CoveredDays: 6, TickersCovered: 12,
		Rows: []usecase.SectorFlowRow{{
			Sector: "Financials", Buy: 30_000_000_000, Sell: 12_000_000_000,
			Net: 18_500_000_000, OthersNet: 500_000_000, ForeignNet: -1_000_000_000,
			TickersCovered: 12, DaysCovered: 6, BrokersCount: 7,
			TopBrokers: []usecase.SectorFlowBrokerRow{{BrokerCode: "AK", Buy: 20_000_000_000, Net: 20_000_000_000}},
		}},
	}}
	s := newSectorFlowTestServer(reader)

	res, err := s.handleGetSectorFlow(context.Background(), sectorFlowReq(map[string]any{
		"from": "2026-08-01", "to": "2026-08-31", "sector": "financials", "group_by": "sektor", "breakdown": "ticker",
	}))
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
	var got sectorFlowResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, text.Text)
	}
	if got.GroupBy != "sektor" || got.Sector != "Financials" || len(got.Rows) != 1 {
		t.Fatalf("response = %+v, want sektor/Financials with one row", got)
	}
	if got.Rows[0].Net != 18_500_000_000 || got.Rows[0].TopBrokers[0].BrokerCode != "AK" {
		t.Errorf("row = %+v, want net 18.5B with AK top broker", got.Rows[0])
	}
	if !got.DataStale {
		t.Error("data_stale must be true on nil wiring (no source_status)")
	}

	if reader.got.From == nil || reader.got.From.Format("2006-01-02") != "2026-08-01" {
		t.Errorf("from = %v, want 2026-08-01", reader.got.From)
	}
	if reader.got.To == nil || reader.got.To.Format("2006-01-02") != "2026-08-31" {
		t.Errorf("to = %v, want 2026-08-31", reader.got.To)
	}
	if reader.got.Sector == nil || *reader.got.Sector != "financials" {
		t.Errorf("sector = %v, want the raw argument passed through", reader.got.Sector)
	}
	if reader.got.GroupBy != "sektor" {
		t.Errorf("group_by = %q, want sektor", reader.got.GroupBy)
	}
	if reader.got.Breakdown != "ticker" {
		t.Errorf("breakdown = %q, want ticker", reader.got.Breakdown)
	}
}

// TestHandleGetSectorFlowDefaults checks that omitted arguments reach the
// usecase as nil/empty so its window and grouping defaults apply.
func TestHandleGetSectorFlowDefaults(t *testing.T) {
	reader := &fakeSectorFlowReader{data: &usecase.SectorFlowResponse{
		GroupBy: "sektor", Rows: []usecase.SectorFlowRow{},
	}}
	s := newSectorFlowTestServer(reader)

	res, err := s.handleGetSectorFlow(context.Background(), sectorFlowReq(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error result: %v", res.Content)
	}
	if reader.got.From != nil || reader.got.To != nil || reader.got.Sector != nil || reader.got.GroupBy != "" || reader.got.Breakdown != "" {
		t.Errorf("request = %+v, want all defaults (nil/nil/nil/empty/empty)", reader.got)
	}
}

// TestHandleGetSectorFlowErrors checks the envelope paths: bad date formats at
// the handler, an invalid group_by and a DB failure through
// exceptionToEnvelope.
func TestHandleGetSectorFlowErrors(t *testing.T) {
	cases := []struct {
		name     string
		args     map[string]any
		reader   usecase.SectorFlowReader
		wantCode mcp.ErrorCode
	}{
		{"invalid from", map[string]any{"from": "not-a-date"}, &fakeSectorFlowReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid to", map[string]any{"to": "31-08-2026"}, &fakeSectorFlowReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid group_by", map[string]any{"group_by": "ticker"}, &fakeSectorFlowReader{err: usecase.ErrInvalidArgument}, mcp.ErrorCodeInvalidArgument},
		{"invalid breakdown", map[string]any{"breakdown": "sector"}, &fakeSectorFlowReader{err: usecase.ErrInvalidArgument}, mcp.ErrorCodeInvalidArgument},
		{"invalid range", map[string]any{"from": "2026-08-31", "to": "2026-08-01"}, &fakeSectorFlowReader{err: usecase.ErrInvalidRange}, mcp.ErrorCodeInvalidArgument},
		{"no trading day", map[string]any{}, &fakeSectorFlowReader{err: usecase.ErrNoTradingDay}, mcp.ErrorCodeNotFound},
		{"internal", map[string]any{}, &fakeSectorFlowReader{err: errors.New("boom")}, mcp.ErrorCodeInternal},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSectorFlowTestServer(tc.reader)
			res, err := s.handleGetSectorFlow(context.Background(), sectorFlowReq(tc.args))
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
