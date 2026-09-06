package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeShareholderCompositionReader implements
// usecase.ShareholderCompositionReader for handler tests; it records the
// ticker/time arguments it was called with.
type fakeShareholderCompositionReader struct {
	data      *usecase.ShareholderCompositionResponse
	err       error
	gotTicker string
	gotFrom   time.Time
	gotTo     time.Time
}

func (f *fakeShareholderCompositionReader) GetShareholderComposition(ctx context.Context, ticker string, from, to time.Time) (*usecase.ShareholderCompositionResponse, error) {
	f.gotTicker = ticker
	f.gotFrom = from
	f.gotTo = to
	return f.data, f.err
}

// newShareholderCompositionTestServer builds a Server with a fake
// shareholder-composition reader, no DB, and a nil-wired ticker validator
// (stalenessFor reports stale on nil wiring; Normalize pattern-checks
// without a DB).
func newShareholderCompositionTestServer(reader usecase.ShareholderCompositionReader) *Server {
	return &Server{
		log:                      logrus.New(),
		shareholderCompositionUC: reader,
		tickers:                  NewTickerValidator(nil, nil, logrus.New()),
	}
}

func shareholderCompositionReq(args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "get_shareholder_composition",
		Arguments: args,
	}}
}

// TestHandleGetShareholderCompositionSuccess checks the happy path: required
// args parse through and the rows + staleness envelope come back.
func TestHandleGetShareholderCompositionSuccess(t *testing.T) {
	reader := &fakeShareholderCompositionReader{data: &usecase.ShareholderCompositionResponse{
		Ticker: "BBCA",
		From:   "2026-01-01",
		To:     "2026-08-31",
		Count:  1,
		Rows: []usecase.ShareholderCompositionRow{
			{
				PositionDate: "2026-08-31",
				SecNum:       123_275_050_000,
				Local:        usecase.CompositionBreakdown{ID: 11_009_825_687},
				LocalTotal:   16_135_449_399,
				Foreign:      usecase.CompositionBreakdown{CP: 4_993_879_259},
				ForeignTotal: 36_318_241_721,
				Total:        52_453_691_120,
			},
		},
	}}
	s := newShareholderCompositionTestServer(reader)

	res, err := s.handleGetShareholderComposition(context.Background(), shareholderCompositionReq(map[string]any{
		"ticker":    "bbca.jk",
		"date_from": "2026-01-01",
		"date_to":   "2026-08-31",
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
	var got shareholderCompositionResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, text.Text)
	}
	if got.Count != 1 || len(got.Rows) != 1 {
		t.Fatalf("count = %d rows = %d, want 1", got.Count, len(got.Rows))
	}
	if got.Ticker != "BBCA" {
		t.Errorf("ticker = %q, want BBCA", got.Ticker)
	}
	if got.Rows[0].Total != 52_453_691_120 {
		t.Errorf("rows[0].total = %d, want 52453691120", got.Rows[0].Total)
	}
	if got.Rows[0].Local.ID != 11_009_825_687 {
		t.Errorf("rows[0].local.id = %d, want 11009825687", got.Rows[0].Local.ID)
	}
	if !got.DataStale {
		t.Error("data_stale must be true on nil wiring (no source_status)")
	}
	if reader.gotTicker != "BBCA" {
		t.Errorf("ticker = %q, want BBCA (normalized)", reader.gotTicker)
	}
	if reader.gotFrom.Format("2006-01-02") != "2026-01-01" || reader.gotTo.Format("2006-01-02") != "2026-08-31" {
		t.Errorf("passed range = %v..%v, want 2026-01-01..2026-08-31", reader.gotFrom, reader.gotTo)
	}
}

// TestHandleGetShareholderCompositionErrors checks the envelope paths:
// missing/invalid args (handler-level) and usecase errors through
// exceptionToEnvelope.
func TestHandleGetShareholderCompositionErrors(t *testing.T) {
	cases := []struct {
		name     string
		args     map[string]any
		reader   usecase.ShareholderCompositionReader
		wantCode mcp.ErrorCode
	}{
		{"missing ticker", map[string]any{"date_from": "2026-01-01", "date_to": "2026-08-31"}, &fakeShareholderCompositionReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid ticker", map[string]any{"ticker": "ZZZZ", "date_from": "2026-01-01", "date_to": "2026-08-31"}, &fakeShareholderCompositionReader{}, mcp.ErrorCodeInvalidTicker},
		{"invalid date_from", map[string]any{"ticker": "BBCA", "date_from": "abc", "date_to": "2026-08-31"}, &fakeShareholderCompositionReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid date_to", map[string]any{"ticker": "BBCA", "date_from": "2026-01-01", "date_to": "2026-13-40"}, &fakeShareholderCompositionReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid range", map[string]any{"ticker": "BBCA", "date_from": "2026-08-31", "date_to": "2026-01-01"}, &fakeShareholderCompositionReader{err: usecase.ErrInvalidRange}, mcp.ErrorCodeInvalidArgument},
		{"internal", map[string]any{"ticker": "BBCA", "date_from": "2026-01-01", "date_to": "2026-08-31"}, &fakeShareholderCompositionReader{err: errors.New("boom")}, mcp.ErrorCodeInternal},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newShareholderCompositionTestServer(tc.reader)
			res, err := s.handleGetShareholderComposition(context.Background(), shareholderCompositionReq(tc.args))
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
