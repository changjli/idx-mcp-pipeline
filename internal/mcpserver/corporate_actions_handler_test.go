package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeCorporateActionsReader implements usecase.CorporateActionsReader for
// handler tests; it records the ticker/time arguments it was called with.
type fakeCorporateActionsReader struct {
	data      *usecase.CorporateActionsResponse
	err       error
	gotTicker *string
	gotFrom   time.Time
	gotTo     time.Time
}

func (f *fakeCorporateActionsReader) GetCorporateActions(ctx context.Context, from, to time.Time, ticker *string) (*usecase.CorporateActionsResponse, error) {
	f.gotFrom = from
	f.gotTo = to
	f.gotTicker = ticker
	return f.data, f.err
}

// newCorporateActionsTestServer builds a Server with a fake corporate-actions
// reader, no DB, and a nil-wired ticker validator (stalenessFor reports stale
// on nil wiring; Normalize pattern-checks without a DB).
func newCorporateActionsTestServer(reader usecase.CorporateActionsReader) *Server {
	return &Server{
		log:                logrus.New(),
		corporateActionsUC: reader,
		tickers:            NewTickerValidator(nil, nil, logrus.New()),
	}
}

func corporateActionsReq(args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "get_corporate_actions",
		Arguments: args,
	}}
}

// TestHandleGetCorporateActionsSuccess checks the happy path: required dates
// parse through and the events + staleness envelope come back.
func TestHandleGetCorporateActionsSuccess(t *testing.T) {
	reader := &fakeCorporateActionsReader{data: &usecase.CorporateActionsResponse{
		From:  "2026-09-01",
		To:    "2026-09-06",
		Count: 2,
		Events: []usecase.CorporateActionEvent{
			{EventDate: "2026-09-04", Ticker: "PJHB", Type: "Waran", Detail: client.CorporateActionDetail{JumlahSaham: 11661, JumlahSahamSetelahTindakan: 1920917693}},
			{EventDate: "2026-09-04", Ticker: "PACK", Type: "Obligasi Wajib Konversi", Detail: client.CorporateActionDetail{}},
		},
	}}
	s := newCorporateActionsTestServer(reader)

	res, err := s.handleGetCorporateActions(context.Background(), corporateActionsReq(map[string]any{
		"date_from": "2026-09-01",
		"date_to":   "2026-09-06",
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
	var got corporateActionsResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, text.Text)
	}
	if got.Count != 2 || len(got.Events) != 2 {
		t.Fatalf("count = %d events = %d, want 2", got.Count, len(got.Events))
	}
	if got.Events[0].Ticker != "PJHB" || got.Events[0].Type != "Waran" {
		t.Errorf("events[0] = %+v, want PJHB / Waran", got.Events[0])
	}
	if got.Events[0].Detail.JumlahSaham != 11661 {
		t.Errorf("events[0].detail = %+v, want jumlah_saham 11661", got.Events[0].Detail)
	}
	if !got.DataStale {
		t.Error("data_stale must be true on nil wiring (no source_status)")
	}
	if reader.gotFrom.Format("2006-01-02") != "2026-09-01" || reader.gotTo.Format("2006-01-02") != "2026-09-06" {
		t.Errorf("passed range = %v..%v, want 2026-09-01..2026-09-06", reader.gotFrom, reader.gotTo)
	}
	if reader.gotTicker != nil {
		t.Errorf("ticker = %v, want nil (omitted)", *reader.gotTicker)
	}

	// Optional ticker passes through normalized.
	reader2 := &fakeCorporateActionsReader{data: &usecase.CorporateActionsResponse{From: "2026-09-01", To: "2026-09-06", Events: []usecase.CorporateActionEvent{}}}
	s2 := newCorporateActionsTestServer(reader2)
	res2, err := s2.handleGetCorporateActions(context.Background(), corporateActionsReq(map[string]any{
		"date_from": "2026-09-01",
		"date_to":   "2026-09-06",
		"ticker":    "bbri.jk",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res2.IsError {
		t.Fatalf("unexpected error result: %v", res2.Content)
	}
	if reader2.gotTicker == nil || *reader2.gotTicker != "BBRI" {
		t.Errorf("ticker = %v, want BBRI (normalized)", reader2.gotTicker)
	}
}

// TestHandleGetCorporateActionsEmpty checks the empty result: a valid call with
// no events returns an empty events array, not an error.
func TestHandleGetCorporateActionsEmpty(t *testing.T) {
	reader := &fakeCorporateActionsReader{data: &usecase.CorporateActionsResponse{
		From:   "2026-09-01",
		To:     "2026-09-06",
		Events: []usecase.CorporateActionEvent{},
	}}
	s := newCorporateActionsTestServer(reader)

	res, err := s.handleGetCorporateActions(context.Background(), corporateActionsReq(map[string]any{
		"date_from": "2026-09-01",
		"date_to":   "2026-09-06",
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
	var got corporateActionsResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Events == nil || len(got.Events) != 0 {
		t.Fatalf("events = %v, want empty non-nil array", got.Events)
	}
	if got.Count != 0 {
		t.Errorf("count = %d, want 0", got.Count)
	}
}

// TestHandleGetCorporateActionsErrors checks the envelope paths: invalid dates
// and invalid ticker (handler-level), and usecase errors through
// exceptionToEnvelope.
func TestHandleGetCorporateActionsErrors(t *testing.T) {
	cases := []struct {
		name     string
		args     map[string]any
		reader   usecase.CorporateActionsReader
		wantCode mcp.ErrorCode
	}{
		{"invalid date_from", map[string]any{"date_from": "abc", "date_to": "2026-09-06"}, &fakeCorporateActionsReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid date_to", map[string]any{"date_from": "2026-09-01", "date_to": "2026-13-40"}, &fakeCorporateActionsReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid ticker", map[string]any{"date_from": "2026-09-01", "date_to": "2026-09-06", "ticker": "ZZZZ"}, &fakeCorporateActionsReader{}, mcp.ErrorCodeInvalidTicker},
		{"invalid range", map[string]any{"date_from": "2026-09-01", "date_to": "2026-09-06"}, &fakeCorporateActionsReader{err: usecase.ErrInvalidRange}, mcp.ErrorCodeInvalidArgument},
		{"internal", map[string]any{"date_from": "2026-09-01", "date_to": "2026-09-06"}, &fakeCorporateActionsReader{err: errors.New("boom")}, mcp.ErrorCodeInternal},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newCorporateActionsTestServer(tc.reader)
			res, err := s.handleGetCorporateActions(context.Background(), corporateActionsReq(tc.args))
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

// TestHandleGetCorporateActionsNilServerParseErrors verifies the date-parse
// guards fire before any server field is touched (nil *Server receiver is
// safe on the parse path).
func TestHandleGetCorporateActionsNilServerParseErrors(t *testing.T) {
	var s *Server
	for _, args := range []map[string]any{
		{"date_from": "not-a-date", "date_to": "2026-09-06"},
		{"date_from": "2026-09-01", "date_to": ""},
	} {
		res, err := s.handleGetCorporateActions(context.Background(), corporateActionsReq(args))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !res.IsError {
			t.Fatal("result must be an error")
		}
	}
}
