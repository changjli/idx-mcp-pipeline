package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/embed"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeComputeIndicatorsReader implements usecase.ComputeIndicatorsReader for
// handler tests; it records the request the handler built.
type fakeComputeIndicatorsReader struct {
	data *usecase.ComputeIndicatorsResponse
	err  error
	got  usecase.ComputeIndicatorsRequest
}

func (f *fakeComputeIndicatorsReader) ComputeIndicators(ctx context.Context, req usecase.ComputeIndicatorsRequest) (*usecase.ComputeIndicatorsResponse, error) {
	f.got = req
	return f.data, f.err
}

func newComputeIndicatorsTestServer(reader usecase.ComputeIndicatorsReader) *Server {
	return &Server{
		log:                 logrus.New(),
		computeIndicatorsUC: reader,
		tickers:             NewTickerValidator(nil, nil, logrus.New()),
	}
}

func computeIndicatorsReq(args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "compute_indicators",
		Arguments: args,
	}}
}

// TestHandleComputeIndicatorsSuccess checks the happy path: tickers normalize
// (bbri.jk → BBRI) and keep caller order, the indicator spec list and window
// pass through, and the response carries rows plus the staleness envelope.
func TestHandleComputeIndicatorsSuccess(t *testing.T) {
	value := 4850.0
	reader := &fakeComputeIndicatorsReader{data: &usecase.ComputeIndicatorsResponse{
		Mode:       "screen",
		AsOf:       "2026-09-11",
		Window:     250,
		Indicators: []string{"sma:20", "rsi:14"},
		Count:      1,
		Rows: []usecase.ComputeIndicatorsRow{{
			Ticker:       "BBRI",
			Values:       map[string]*float64{"sma:20": &value, "rsi:14": nil},
			Insufficient: []string{"rsi:14"},
			HistoryRows:  20,
			RequiredRows: 20,
		}},
	}}
	s := newComputeIndicatorsTestServer(reader)

	res, err := s.handleComputeIndicators(context.Background(), computeIndicatorsReq(map[string]any{
		"tickers":    []any{"bbri.jk", "TLKM"},
		"indicators": []any{"sma:20", "rsi:14"},
		"window":     float64(120),
		"as_of":      "2026-09-11",
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
	var got computeIndicatorsResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, text.Text)
	}
	if got.AsOf != "2026-09-11" || got.Count != 1 || len(got.Rows) != 1 {
		t.Fatalf("envelope = %+v, want as_of 2026-09-11 / count 1", got)
	}
	if got.Rows[0].Values["sma:20"] == nil || *got.Rows[0].Values["sma:20"] != 4850 {
		t.Errorf("sma:20 = %v, want 4850", got.Rows[0].Values["sma:20"])
	}
	if got.Rows[0].Insufficient[0] != "rsi:14" {
		t.Errorf("insufficient = %v, want [rsi:14]", got.Rows[0].Insufficient)
	}
	if !got.DataStale {
		t.Error("data_stale must be true on nil wiring (no source_status)")
	}

	if len(reader.got.Tickers) != 2 || reader.got.Tickers[0] != "BBRI" || reader.got.Tickers[1] != "TLKM" {
		t.Errorf("tickers = %v, want [BBRI TLKM] (normalized, caller order)", reader.got.Tickers)
	}
	if len(reader.got.Indicators) != 2 || reader.got.Indicators[0] != "sma:20" {
		t.Errorf("indicators = %v, want [sma:20 rsi:14]", reader.got.Indicators)
	}
	if reader.got.Window != 120 {
		t.Errorf("window = %d, want 120", reader.got.Window)
	}
	if reader.got.AsOf == nil || reader.got.AsOf.Format("2006-01-02") != "2026-09-11" {
		t.Errorf("asOf = %v, want 2026-09-11", reader.got.AsOf)
	}
	if reader.got.Mode != "" {
		t.Errorf("mode = %q, want empty (omitted → the usecase default)", reader.got.Mode)
	}
}

// TestHandleComputeIndicatorsErrors covers the envelope paths: handler-level
// argument failures and usecase errors through exceptionToEnvelope.
func TestHandleComputeIndicatorsErrors(t *testing.T) {
	cases := []struct {
		name     string
		args     map[string]any
		reader   usecase.ComputeIndicatorsReader
		wantCode mcp.ErrorCode
	}{
		{
			name:     "no tickers",
			args:     map[string]any{"indicators": []any{"sma:20"}},
			reader:   &fakeComputeIndicatorsReader{},
			wantCode: mcp.ErrorCodeInvalidArgument,
		},
		{
			name:     "tickers wrong type",
			args:     map[string]any{"tickers": "BBRI", "indicators": []any{"sma:20"}},
			reader:   &fakeComputeIndicatorsReader{},
			wantCode: mcp.ErrorCodeInvalidArgument,
		},
		{
			name:     "invalid ticker in the list",
			args:     map[string]any{"tickers": []any{"BBRI", "ZZZZ"}, "indicators": []any{"sma:20"}},
			reader:   &fakeComputeIndicatorsReader{},
			wantCode: mcp.ErrorCodeInvalidTicker,
		},
		{
			name:     "invalid as_of",
			args:     map[string]any{"tickers": []any{"BBRI"}, "indicators": []any{"sma:20"}, "as_of": "not-a-date"},
			reader:   &fakeComputeIndicatorsReader{},
			wantCode: mcp.ErrorCodeInvalidArgument,
		},
		{
			name:     "usecase invalid argument",
			args:     map[string]any{"tickers": []any{"BBRI"}, "indicators": []any{"nope:20"}},
			reader:   &fakeComputeIndicatorsReader{err: fmt.Errorf("%w: unknown indicator \"nope\"", usecase.ErrInvalidArgument)},
			wantCode: mcp.ErrorCodeInvalidArgument,
		},
		{
			name:     "internal",
			args:     map[string]any{"tickers": []any{"BBRI"}, "indicators": []any{"sma:20"}},
			reader:   &fakeComputeIndicatorsReader{err: errors.New("boom")},
			wantCode: mcp.ErrorCodeInternal,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newComputeIndicatorsTestServer(tc.reader)
			res, err := s.handleComputeIndicators(context.Background(), computeIndicatorsReq(tc.args))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !res.IsError {
				t.Fatalf("expected an error result, got %v", res.Content)
			}
			text, ok := res.Content[0].(mcpgo.TextContent)
			if !ok {
				t.Fatalf("content type = %T", res.Content[0])
			}
			var env mcp.ErrorEnvelope
			if err := json.Unmarshal([]byte(text.Text), &env); err != nil {
				t.Fatalf("unmarshal envelope: %v\n%s", err, text.Text)
			}
			if env.Error.Code != tc.wantCode {
				t.Errorf("code = %s, want %s (%s)", env.Error.Code, tc.wantCode, env.Error.Message)
			}
		})
	}
}

// TestHandleComputeIndicatorsUsecaseValidation covers the seam end to end with
// the real usecase: the registry's own name enumeration reaches the caller
// through the INVALID_ARGUMENT envelope, unwrapped from the usecase's wrap.
func TestHandleComputeIndicatorsUsecaseValidation(t *testing.T) {
	uc := usecase.NewComputeIndicatorsUseCase(nil, logrus.New(), nil)
	s := newComputeIndicatorsTestServer(uc)

	res, err := s.handleComputeIndicators(context.Background(), computeIndicatorsReq(map[string]any{
		"tickers":    []any{"BBRI"},
		"indicators": []any{"bollinger"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected an error result, got %v", res.Content)
	}
	text := res.Content[0].(mcpgo.TextContent)
	var env mcp.ErrorEnvelope
	if err := json.Unmarshal([]byte(text.Text), &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Error.Code != mcp.ErrorCodeInvalidArgument {
		t.Errorf("code = %s, want %s", env.Error.Code, mcp.ErrorCodeInvalidArgument)
	}
	if !strings.Contains(env.Error.Message, "sma") || !strings.Contains(env.Error.Message, "ema") || !strings.Contains(env.Error.Message, "rsi") {
		t.Errorf("message %q must enumerate the registry's valid names", env.Error.Message)
	}
}

// TestHandleComputeIndicatorsCapEnforced is the seam check for the cap the tool
// description states: 50 tickers per call pass through, 51 is rejected.
func TestHandleComputeIndicatorsCapEnforced(t *testing.T) {
	// Real codes from the bundled ticker list, so the handler's ticker
	// validation passes and the cap is what decides the outcome. The cap counts
	// requested entries (not unique tickers), so cycling the list is enough.
	entries, err := embed.LoadTickers()
	if err != nil {
		t.Fatalf("load tickers: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("bundled ticker list is empty")
	}
	atCap := make([]any, 0, 50)
	for i := 0; i < 50; i++ {
		atCap = append(atCap, entries[i%len(entries)].Code)
	}

	reader := &fakeComputeIndicatorsReader{data: &usecase.ComputeIndicatorsResponse{Mode: "screen", Rows: []usecase.ComputeIndicatorsRow{}}}
	s := newComputeIndicatorsTestServer(reader)
	res, err := s.handleComputeIndicators(context.Background(), computeIndicatorsReq(map[string]any{
		"tickers":    atCap,
		"indicators": []any{"sma:20"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("a call at the cap must pass, got %v", res.Content)
	}
	if len(reader.got.Tickers) != 50 {
		t.Errorf("tickers forwarded = %d, want 50", len(reader.got.Tickers))
	}

	// The cap itself lives in the usecase (one rule for every caller), and it
	// runs before any read — so the real usecase can enforce it with no DB.
	overCap := append(atCap, entries[0].Code)
	s2 := newComputeIndicatorsTestServer(usecase.NewComputeIndicatorsUseCase(nil, logrus.New(), nil))
	res, err = s2.handleComputeIndicators(context.Background(), computeIndicatorsReq(map[string]any{
		"tickers":    overCap,
		"indicators": []any{"sma:20"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an over-cap error result")
	}
	text := res.Content[0].(mcpgo.TextContent)
	var env mcp.ErrorEnvelope
	if err := json.Unmarshal([]byte(text.Text), &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Error.Code != mcp.ErrorCodeInvalidArgument {
		t.Errorf("code = %s, want %s", env.Error.Code, mcp.ErrorCodeInvalidArgument)
	}
	if !strings.Contains(env.Error.Message, "50") {
		t.Errorf("message %q must state the cap", env.Error.Message)
	}
}
