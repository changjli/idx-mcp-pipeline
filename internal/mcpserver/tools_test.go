package mcpserver

import (
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// allTools is the full tool registry. The contract test below asserts every
// tool carries the read-only annotations and the required-argument shape the
// ticket mandates — a cheap surface-wide guard when tools are added.
var allTools = []mcpgo.Tool{
	toolGetMarketAnomalies,
	toolGetTickerNews,
	toolGetBrokerSummary,
	toolListIdxDisclosures,
	toolGetPipelineStatus,
	toolGetStockBrokerSummary,
	toolGetStockBrokerSummaryHistory,
}

func TestToolAnnotationsContract(t *testing.T) {
	for _, tool := range allTools {
		t.Run(tool.Name, func(t *testing.T) {
			a := tool.Annotations
			if a.ReadOnlyHint == nil || !*a.ReadOnlyHint {
				t.Error("readOnlyHint must be true")
			}
			if a.DestructiveHint == nil || *a.DestructiveHint {
				t.Error("destructiveHint must be false")
			}
			if a.OpenWorldHint == nil || !*a.OpenWorldHint {
				t.Error("openWorldHint must be true")
			}
			if tool.Description == "" {
				t.Error("description must not be empty")
			}
		})
	}
}

func TestToolRequiredArguments(t *testing.T) {
	required := map[string][]string{
		"get_market_anomalies":             {},
		"get_ticker_news":                  {"ticker"},
		"get_broker_summary":               {},
		"list_idx_disclosures":             {"ticker"},
		"get_pipeline_status":              {},
		"get_stock_broker_summary":         {"ticker"},
		"get_stock_broker_summary_history": {"ticker", "from", "to"},
	}

	for _, tool := range allTools {
		t.Run(tool.Name, func(t *testing.T) {
			got := tool.InputSchema.Required
			want := required[tool.Name]
			if len(got) != len(want) {
				t.Fatalf("required = %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("required = %v, want %v", got, want)
				}
			}
		})
	}
}

func TestToolLimitDefaults(t *testing.T) {
	for _, name := range []string{"get_ticker_news", "list_idx_disclosures"} {
		tool := toolByName(t, name)
		prop, ok := tool.InputSchema.Properties["limit"]
		if !ok {
			t.Fatalf("%s: limit property missing", name)
		}
		pm, ok := prop.(map[string]any)
		if !ok {
			t.Fatalf("%s: limit property not a map", name)
		}
		if pm["default"] != float64(20) && pm["default"] != 20 {
			t.Fatalf("%s: limit default = %v, want 20", name, pm["default"])
		}
	}
}

func toolByName(t *testing.T, name string) mcpgo.Tool {
	t.Helper()
	for _, tool := range allTools {
		if tool.Name == name {
			return tool
		}
	}
	t.Fatalf("tool %q not registered", name)
	return mcpgo.Tool{}
}
