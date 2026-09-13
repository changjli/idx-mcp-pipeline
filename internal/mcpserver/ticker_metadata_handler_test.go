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

// fakeTickerMetadataReader implements usecase.TickerMetadataReader for
// handler tests; it records the ticker/date arguments it was called with.
type fakeTickerMetadataReader struct {
	data      *usecase.TickerMetadataResponse
	err       error
	gotTick   *string
	gotAsOf   *time.Time
	asOfCalls int
}

func (f *fakeTickerMetadataReader) GetTickerMetadata(ctx context.Context, ticker *string, asOf *time.Time) (*usecase.TickerMetadataResponse, error) {
	f.gotTick = ticker
	if asOf != nil {
		f.gotAsOf = asOf
	}
	f.asOfCalls++
	return f.data, f.err
}

// newTickerMetadataTestServer builds a Server with a fake ticker-metadata
// reader, no DB, and a nil-wired ticker validator (stalenessFor reports stale
// on nil wiring; Normalize pattern-checks without a DB).
func newTickerMetadataTestServer(reader usecase.TickerMetadataReader) *Server {
	return &Server{
		log:              logrus.New(),
		tickerMetadataUC: reader,
		tickers:          NewTickerValidator(nil, nil, logrus.New()),
	}
}

func tickerMetadataReq(args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "get_ticker_metadata",
		Arguments: args,
	}}
}

// TestHandleGetTickerMetadataSuccess checks the happy path: the single-ticker
// query normalizes through (BBRI.JK → BBRI), the response + staleness envelope
// come back, and the full-universe query passes a nil ticker through.
func TestHandleGetTickerMetadataSuccess(t *testing.T) {
	reader := &fakeTickerMetadataReader{data: &usecase.TickerMetadataResponse{
		Count:         1,
		EffectiveDate: "2026-09-09",
		Tickers: []usecase.TickerMetadata{{
			Ticker:          "BBRI",
			Sector:          strPtrMcp("Financials"),
			Industry:        strPtrMcp("Banks"),
			SubSector:       strPtrMcp("Banks"),
			SubIndustry:     strPtrMcp("Banks"),
			SubIndustryCode: strPtrMcp("G111"),
			Indices:         []string{"COMPOSITE", "LQ45"},
		}},
	}}
	s := newTickerMetadataTestServer(reader)

	res, err := s.handleGetTickerMetadata(context.Background(), tickerMetadataReq(map[string]any{
		"ticker": "bbri.jk",
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
	var got tickerMetadataResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, text.Text)
	}
	if got.Count != 1 || len(got.Tickers) != 1 {
		t.Fatalf("count = %d tickers = %d, want 1", got.Count, len(got.Tickers))
	}
	m := got.Tickers[0]
	if m.Ticker != "BBRI" || m.Sector == nil || *m.Sector != "Financials" {
		t.Errorf("tickers[0] = %+v, want BBRI / Financials", m)
	}
	if len(m.Indices) != 2 || m.Indices[0] != "COMPOSITE" {
		t.Errorf("indices = %v, want [COMPOSITE LQ45]", m.Indices)
	}
	if got.EffectiveDate != "2026-09-09" {
		t.Errorf("effective_date = %q, want 2026-09-09", got.EffectiveDate)
	}
	if !got.DataStale {
		t.Error("data_stale must be true on nil wiring (no source_status)")
	}
	if reader.gotTick == nil || *reader.gotTick != "BBRI" {
		t.Errorf("ticker = %v, want BBRI (normalized)", reader.gotTick)
	}
	if reader.gotAsOf != nil {
		t.Errorf("asOf = %v, want nil (date omitted → latest)", reader.gotAsOf)
	}

	// Optional date pins the snapshot; universe query passes nil through.
	reader2 := &fakeTickerMetadataReader{data: &usecase.TickerMetadataResponse{Count: 0, EffectiveDate: "", Tickers: []usecase.TickerMetadata{}}}
	s2 := newTickerMetadataTestServer(reader2)
	res2, err := s2.handleGetTickerMetadata(context.Background(), tickerMetadataReq(map[string]any{
		"date": "2026-02-06",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res2.IsError {
		t.Fatalf("unexpected error result: %v", res2.Content)
	}
	if reader2.gotTick != nil {
		t.Errorf("ticker = %v, want nil (omitted → universe)", reader2.gotTick)
	}
	if reader2.gotAsOf == nil || reader2.gotAsOf.Format("2006-01-02") != "2026-02-06" {
		t.Errorf("asOf = %v, want 2026-02-06", reader2.gotAsOf)
	}
}

// TestHandleGetTickerMetadataEmpty checks the empty result: an unknown ticker
// with no metadata returns a count-0 body with a non-nil tickers array, not an
// error.
func TestHandleGetTickerMetadataEmpty(t *testing.T) {
	reader := &fakeTickerMetadataReader{data: &usecase.TickerMetadataResponse{
		Count:   0,
		Tickers: []usecase.TickerMetadata{},
	}}
	s := newTickerMetadataTestServer(reader)

	res, err := s.handleGetTickerMetadata(context.Background(), tickerMetadataReq(map[string]any{"ticker": "BBRI"}))
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
	var got tickerMetadataResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Tickers == nil || len(got.Tickers) != 0 {
		t.Fatalf("tickers = %v, want empty non-nil array", got.Tickers)
	}
	if got.Count != 0 {
		t.Errorf("count = %d, want 0", got.Count)
	}
}

// TestHandleGetTickerMetadataErrors checks the envelope paths: invalid ticker
// (handler-level Normalize), invalid date (handler-level), and usecase errors
// through exceptionToEnvelope.
func TestHandleGetTickerMetadataErrors(t *testing.T) {
	cases := []struct {
		name     string
		args     map[string]any
		reader   usecase.TickerMetadataReader
		wantCode mcp.ErrorCode
	}{
		{"invalid ticker", map[string]any{"ticker": "ZZZZ"}, &fakeTickerMetadataReader{}, mcp.ErrorCodeInvalidTicker},
		{"invalid date", map[string]any{"date": "not-a-date"}, &fakeTickerMetadataReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid ticker usecase", map[string]any{"ticker": "test!"}, &fakeTickerMetadataReader{}, mcp.ErrorCodeInvalidTicker},
		{"internal", map[string]any{"ticker": "BBRI"}, &fakeTickerMetadataReader{err: errors.New("boom")}, mcp.ErrorCodeInternal},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTickerMetadataTestServer(tc.reader)
			res, err := s.handleGetTickerMetadata(context.Background(), tickerMetadataReq(tc.args))
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

func strPtrMcp(s string) *string { return &s }
