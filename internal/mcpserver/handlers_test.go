package mcpserver

import (
	"context"
	"encoding/json"
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

func TestArgLimit(t *testing.T) {
	cases := []struct {
		name string
		args map[string]any
		want int
	}{
		{"missing", map[string]any{}, defaultLimit},
		{"zero", map[string]any{"limit": float64(0)}, defaultLimit},
		{"negative", map[string]any{"limit": float64(-3)}, defaultLimit},
		{"in range", map[string]any{"limit": float64(5)}, 5},
		{"capped", map[string]any{"limit": float64(500)}, maxLimit},
		{"non-number", map[string]any{"limit": "20"}, defaultLimit},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := argLimit(tc.args); got != tc.want {
				t.Fatalf("argLimit(%v) = %d, want %d", tc.args, got, tc.want)
			}
		})
	}
}

func TestTextResult(t *testing.T) {
	res := textResult(map[string]any{"a": 1})
	if res.IsError {
		t.Fatal("success result must not be an error")
	}
	if len(res.Content) != 1 {
		t.Fatalf("content length = %d, want 1", len(res.Content))
	}
	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want text", res.Content[0])
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if got["a"] != float64(1) {
		t.Fatalf("result = %v, want a=1", got)
	}
}

func TestEnvelopeResult(t *testing.T) {
	env := mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: NOPE", false)
	res := envelopeResult(env)
	if !res.IsError {
		t.Fatal("error result must set IsError")
	}
	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want text", res.Content[0])
	}
	var got mcp.ErrorEnvelope
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if got.Error.Code != mcp.ErrorCodeInvalidTicker || got.Error.Retryable {
		t.Fatalf("envelope = %+v, want INVALID_TICKER non-retryable", got)
	}
}

// TestHandleReadIdxDisclosureInvalidID covers the parse path, which returns
// INVALID_ARGUMENT before touching any dependency (safe on a nil *Server).
func TestHandleReadIdxDisclosureInvalidID(t *testing.T) {
	var s *Server
	for _, bad := range []any{"", "abc", "-1", "12.5", "99999999999999999999", 0, -5, 1.5, 3.14159} {
		req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
			Name:      "read_idx_disclosure",
			Arguments: map[string]any{"disclosure_id": bad},
		}}
		res, err := s.handleReadIdxDisclosure(context.Background(), req)
		if err != nil {
			t.Fatalf("disclosure_id=%v: unexpected error: %v", bad, err)
		}
		if !res.IsError {
			t.Fatalf("disclosure_id=%v: result must be an error", bad)
		}
		text, ok := res.Content[0].(mcpgo.TextContent)
		if !ok {
			t.Fatalf("disclosure_id=%v: content type = %T", bad, res.Content[0])
		}
		var got mcp.ErrorEnvelope
		if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
			t.Fatalf("disclosure_id=%v: unmarshal envelope: %v", bad, err)
		}
		if got.Error.Code != mcp.ErrorCodeInvalidArgument {
			t.Fatalf("disclosure_id=%v: code = %q, want INVALID_ARGUMENT", bad, got.Error.Code)
		}
	}
}

// TestHandleFetchDisclosurePDFInvalidID covers the parse path, which returns
// INVALID_ARGUMENT before touching any dependency (safe on a nil *Server).
func TestHandleFetchDisclosurePDFInvalidID(t *testing.T) {
	var s *Server
	for _, bad := range []any{"", "abc", "-1", "12.5", "99999999999999999999", 0, -5, 1.5, 3.14159} {
		req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
			Name:      "fetch_disclosure_pdf",
			Arguments: map[string]any{"disclosure_id": bad},
		}}
		res, err := s.handleFetchDisclosurePDF(context.Background(), req)
		if err != nil {
			t.Fatalf("disclosure_id=%v: unexpected error: %v", bad, err)
		}
		if !res.IsError {
			t.Fatalf("disclosure_id=%v: result must be an error", bad)
		}
		text, ok := res.Content[0].(mcpgo.TextContent)
		if !ok {
			t.Fatalf("disclosure_id=%v: content type = %T", bad, res.Content[0])
		}
		var got mcp.ErrorEnvelope
		if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
			t.Fatalf("disclosure_id=%v: unmarshal envelope: %v", bad, err)
		}
		if got.Error.Code != mcp.ErrorCodeInvalidArgument {
			t.Fatalf("disclosure_id=%v: code = %q, want INVALID_ARGUMENT", bad, got.Error.Code)
		}
	}
}

// TestHandleGetDailyPricesInvalidArgs covers the parse path, which returns
// INVALID_TICKER / INVALID_ARGUMENT before touching the usecase. Uses a real
// TickerValidator over the bundled list (no DB needed).
func TestHandleGetDailyPricesInvalidArgs(t *testing.T) {
	s := &Server{tickers: NewTickerValidator(nil, nil, logrus.New())}

	cases := []struct {
		name string
		args map[string]any
		want mcp.ErrorCode
	}{
		{"invalid ticker", map[string]any{"ticker": "NOPE", "from": "2026-01-01", "to": "2026-01-31"}, mcp.ErrorCodeInvalidTicker},
		{"empty ticker", map[string]any{"ticker": "", "from": "2026-01-01", "to": "2026-01-31"}, mcp.ErrorCodeInvalidTicker},
		{"invalid from", map[string]any{"ticker": "BBCA", "from": "abc", "to": "2026-01-31"}, mcp.ErrorCodeInvalidArgument},
		{"invalid to", map[string]any{"ticker": "BBCA", "from": "2026-01-01", "to": "2026-13-40"}, mcp.ErrorCodeInvalidArgument},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
				Name:      "get_daily_prices",
				Arguments: tc.args,
			}}
			res, err := s.handleGetDailyPrices(context.Background(), req)
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

// TestDisclosureIDArg — the string form is the documented input; the numeric
// form matches what get_market_anomalies emits in disclosure_ids.
func TestDisclosureIDArg(t *testing.T) {
	cases := []struct {
		name string
		arg  any
		want int64
		ok   bool
	}{
		{"string", "12345", 12345, true},
		{"string with spaces", " 12345 ", 12345, true},
		{"number", float64(12345), 12345, true},
		{"number as json float", float64(42), 42, true},
		{"empty string", "", 0, false},
		{"non-numeric", "abc", 0, false},
		{"negative string", "-1", 0, false},
		{"zero", float64(0), 0, false},
		{"fractional", 12.5, 0, false},
		{"overflow", float64(1e19), 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := disclosureIDArg(map[string]any{"disclosure_id": tc.arg})
			if ok != tc.ok || got != tc.want {
				t.Fatalf("disclosureIDArg(%v) = (%d, %v), want (%d, %v)", tc.arg, got, ok, tc.want, tc.ok)
			}
		})
	}
}
