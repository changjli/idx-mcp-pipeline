package mcpserver

import (
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// allTools is the read-only tool registry. The contract test below asserts
// every read tool carries the read-only annotations and the required-argument
// shape the ticket mandates — a cheap surface-wide guard when tools are added.
var allTools = []mcpgo.Tool{
	toolGetMarketAnomalies,
	toolGetTickerNews,
	toolGetBrokerSummary,
	toolListIdxDisclosures,
	toolSearchDisclosures,
	toolReadIdxDisclosure,
	toolFetchDisclosurePDF,
	toolGetPipelineStatus,
	toolGetStockBrokerSummary,
	toolGetStockBrokerSummaryHistory,
	toolGetBrokerNetFlow,
	toolGetDailyPrices,
}

// writeTools is the write-tool registry (issue 12): tools that mutate state
// must declare readOnlyHint=false so clients prompt for confirmation.
var writeTools = []mcpgo.Tool{
	toolBackfillStockBrokerSummary,
}

// everyTool is the full registry (read + write) for the required-argument
// shape test.
var everyTool = append(append([]mcpgo.Tool{}, allTools...), writeTools...)

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

// TestWriteToolAnnotationsContract — write tools must NOT declare read-only
// (the spec's rule: a write tool must not silently declare destructive=false
// via the read-only helper). DestructiveHint stays false — the backfill is an
// idempotent upsert, not a delete.
func TestWriteToolAnnotationsContract(t *testing.T) {
	for _, tool := range writeTools {
		t.Run(tool.Name, func(t *testing.T) {
			a := tool.Annotations
			if a.ReadOnlyHint == nil || *a.ReadOnlyHint {
				t.Error("readOnlyHint must be false for a write tool")
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
		"search_disclosures":               {"query"},
		"read_idx_disclosure":              {"disclosure_id"},
		"fetch_disclosure_pdf":             {"disclosure_id"},
		"get_pipeline_status":              {},
		"get_stock_broker_summary":         {"ticker"},
		"get_stock_broker_summary_history": {"ticker", "from", "to"},
		"backfill_stock_broker_summary":    {"ticker", "from", "to"},
		"get_broker_net_flow":              {},
		"get_daily_prices":                 {"ticker", "from", "to"},
	}

	for _, tool := range everyTool {
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
	for _, name := range []string{"get_ticker_news", "list_idx_disclosures", "search_disclosures"} {
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
