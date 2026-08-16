package mcpserver

import (
	"encoding/json"
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

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
