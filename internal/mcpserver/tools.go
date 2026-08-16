package mcpserver

import (
	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// instructions is the server-level instructions string (draft from spec §11,
// trimmed to the tools actually wired — read_idx_disclosure is ticket 12).
const instructions = "IDX Market Analyzer co-pilot. Tools read a daily-persisted IDX pipeline (no live network fetch in V1, except get_stock_broker_summary which fetches per-stock broker data on demand). get_market_anomalies — volume/price anomalies for a trading day, each with disclosure_ids to follow into list_idx_disclosures. list_idx_disclosures — browse a ticker's filings (metadata only). get_ticker_news — RSS headlines tagged to a ticker. get_broker_summary — aggregate per-broker activity for a date. get_stock_broker_summary — per-stock top buyers/sellers for a ticker+day (fetches + persists). get_stock_broker_summary_history — stored per-stock broker history over a date range. get_pipeline_status — pipeline health / staleness. All outputs carry data_stale + last_good_date; if stale, note it to the user."

// readOnlyAnnotations marks a tool read-only: clients skip confirmation
// prompts. Every tool declares readOnlyHint=true, destructiveHint=false,
// openWorldHint=true (ticket 10).
func readOnlyAnnotations() mcpgo.ToolOption {
	return mcpgo.WithToolAnnotation(mcpgo.ToolAnnotation{
		ReadOnlyHint:    boolPtr(true),
		DestructiveHint: boolPtr(false),
		OpenWorldHint:   boolPtr(true),
	})
}

func boolPtr(v bool) *bool { return &v }

// toolGetMarketAnomalies — anomalies for a trading day with derived
// disclosure_ids.
var toolGetMarketAnomalies = mcpgo.NewTool("get_market_anomalies",
	mcpgo.WithDescription("Volume/price anomalies for a trading day, each with disclosure_ids derived from the disclosures that passed the filter for the same ticker. Defaults to the most recent trading day."),
	mcpgo.WithString("date", mcpgo.Description("Trading day, YYYY-MM-DD. Defaults to the most recent trading day.")),
	mcpgo.WithString("ticker", mcpgo.Description("Optional ticker filter (e.g. RAJA or RAJA.JK).")),
	readOnlyAnnotations(),
)

// toolGetTickerNews — RSS headlines tagged to a ticker.
var toolGetTickerNews = mcpgo.NewTool("get_ticker_news",
	mcpgo.WithDescription("RSS headlines tagged to a ticker, newest first, each with the match_method that linked it."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("since", mcpgo.Description("Only items published on or after this date, YYYY-MM-DD.")),
	mcpgo.WithNumber("limit", mcpgo.Description("Max items to return. Defaults to 20."), mcpgo.DefaultNumber(20)),
	readOnlyAnnotations(),
)

// toolGetBrokerSummary — aggregate per-broker activity for a date.
var toolGetBrokerSummary = mcpgo.NewTool("get_broker_summary",
	mcpgo.WithDescription("Aggregate per-broker activity for a trading day. Defaults to the most recent trading day."),
	mcpgo.WithString("date", mcpgo.Description("Trading day, YYYY-MM-DD. Defaults to the most recent trading day.")),
	readOnlyAnnotations(),
)

// toolListIdxDisclosures — a ticker's disclosure metadata.
var toolListIdxDisclosures = mcpgo.NewTool("list_idx_disclosures",
	mcpgo.WithDescription("A ticker's disclosure metadata (no extracted text), newest first."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("date", mcpgo.Description("Only disclosures announced on this date, YYYY-MM-DD.")),
	mcpgo.WithNumber("limit", mcpgo.Description("Max disclosures to return. Defaults to 20."), mcpgo.DefaultNumber(20)),
	readOnlyAnnotations(),
)

// toolGetPipelineStatus — per-source pipeline health.
var toolGetPipelineStatus = mcpgo.NewTool("get_pipeline_status",
	mcpgo.WithDescription("Pipeline health: per-source staleness from source_status plus recent alerts."),
	readOnlyAnnotations(),
)

// toolGetStockBrokerSummary — per-stock broker summary via IPOT (the one tool
// that makes an upstream call).
var toolGetStockBrokerSummary = mcpgo.NewTool("get_stock_broker_summary",
	mcpgo.WithDescription("Per-stock top buyers/sellers for a ticker+day, fetched from IPOT on demand and persisted. Defaults to the ticker's latest trading day."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("date", mcpgo.Description("Trading day, YYYY-MM-DD. Defaults to the ticker's latest stored trading day.")),
	readOnlyAnnotations(),
)

// toolGetStockBrokerSummaryHistory — stored per-stock broker history.
var toolGetStockBrokerSummaryHistory = mcpgo.NewTool("get_stock_broker_summary_history",
	mcpgo.WithDescription("Stored per-stock broker history over a date range, grouped by trading day. Pure DB read — no upstream call. Empty range returns an empty list."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD."), mcpgo.Required()),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD."), mcpgo.Required()),
	readOnlyAnnotations(),
)
