package mcpserver

import (
	mcpgo "github.com/mark3labs/mcp-go/mcp"
)

// instructions is the server-level instructions string (draft from spec §11,
// trimmed to the tools actually wired — read_idx_disclosure is ticket 12).
const instructions = "IDX Market Analyzer co-pilot. Tools read a daily-persisted IDX pipeline (no live network fetch in V1, except get_stock_broker_summary and fetch_disclosure_pdf which fetch on demand, and get_financials which fetches financial statements live from IPOT — nothing persisted). get_market_anomalies — volume/price anomalies for a trading day, each with disclosure_ids to follow into list_idx_disclosures or read_idx_disclosure. list_idx_disclosures — browse a ticker's filings (metadata only). search_disclosures — cross-ticker disclosure search by keyword over titles and categories, with date range (e.g. which issuers announced a dividend this week). read_idx_disclosure — one disclosure's metadata plus pre-extracted text (truncated to 64KB); check its status field (ok/pending/failed/evicted) before assuming text is present. fetch_disclosure_pdf — on-demand PDF extraction. Cached text → return it. Otherwise enqueue an async extraction job and return pending; poll read_idx_disclosure until status leaves pending (ok | failed). get_ticker_news — RSS headlines tagged to a ticker. get_broker_summary — aggregate per-broker activity for a date. get_stock_broker_summary — per-stock top buyers/sellers for a ticker+day (fetches + persists). get_stock_broker_summary_history — stored per-stock broker history over a date range. backfill_stock_broker_summary — enqueue a per-stock broker summary backfill over a date range (async; poll get_stock_broker_summary_history for completion). get_broker_net_flow — per-broker cumulative net flow over a window (omit ticker for market-wide stance); rows read from stored history, coverage declared. get_daily_prices — stored Daily Price (OHLCV) series for a ticker over a date range. get_financials — normalized financial statements fetched live; periods are cumulative YTD, use the same-duration columns for comparison. get_pipeline_status — pipeline health / staleness. All outputs carry data_stale + last_good_date; if stale, note it to the user."

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

// writeAnnotations marks a tool as a write: readOnlyHint=false so clients
// prompt for confirmation. destructiveHint stays false — the backfill is an
// idempotent upsert, not a delete. The spec's rule (issue 12): a write tool
// must not silently declare destructive=false via the read-only helper.
func writeAnnotations() mcpgo.ToolOption {
	return mcpgo.WithToolAnnotation(mcpgo.ToolAnnotation{
		ReadOnlyHint:    boolPtr(false),
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

// toolSearchDisclosures — cross-ticker disclosure search by keyword + range.
var toolSearchDisclosures = mcpgo.NewTool("search_disclosures",
	mcpgo.WithDescription("Cross-ticker disclosure search by keyword and announcement date range, newest first. Answers questions like \"which issuers announced a dividend this week\" in one call instead of per-ticker list_idx_disclosures. query is matched case-insensitively against disclosure titles and categories (e.g. \"dividen\", \"rups\", \"right issue\"). Unfiltered — returns disclosures the filter pipeline never categorized too; each row carries passed_filter so you can see the filter status."),
	mcpgo.WithString("query", mcpgo.Description("Search keyword, matched case-insensitively against disclosure titles and categories (e.g. \"dividen\", \"rups\", \"right issue\")."), mcpgo.Required()),
	mcpgo.WithString("date_from", mcpgo.Description("Range start, YYYY-MM-DD. Inclusive.")),
	mcpgo.WithString("date_to", mcpgo.Description("Range end, YYYY-MM-DD. Inclusive.")),
	mcpgo.WithNumber("limit", mcpgo.Description("Max disclosures to return. Defaults to 20."), mcpgo.DefaultNumber(20)),
	readOnlyAnnotations(),
)

// toolReadIdxDisclosure — one disclosure's metadata plus extracted text.
var toolReadIdxDisclosure = mcpgo.NewTool("read_idx_disclosure",
	mcpgo.WithDescription("A disclosure's metadata plus its pre-extracted text, truncated to 64KB. The status field tells you whether text is present: ok, pending (not yet processed), failed (extraction errored), or evicted (text past 90-day retention; metadata still served)."),
	mcpgo.WithString("disclosure_id", mcpgo.Description("Postgres surrogate ID from get_market_anomalies.disclosure_ids or list_idx_disclosures."), mcpgo.Required()),
	readOnlyAnnotations(),
)

// toolFetchDisclosurePDF — on-demand PDF extraction, async live path (issue
// 05b): cached text is served immediately; otherwise an extract:disclosure job
// is enqueued and the pending envelope returned — the client polls
// read_idx_disclosure until the status leaves pending.
var toolFetchDisclosurePDF = mcpgo.NewTool("fetch_disclosure_pdf",
	mcpgo.WithDescription("Fetches and extracts a single Disclosure's PDF on demand. If the text is already cached (extraction status ok), returns it immediately. Otherwise enqueues an async extraction job and returns immediately with status pending and text null. Poll read_idx_disclosure with the same disclosure_id every few seconds: status stays pending while the job runs (self-retries twice, 30s and 2m), then returns ok with text, or failed with error. Use when read_idx_disclosure reports pending/failed/evicted or the text is missing."),
	mcpgo.WithString("disclosure_id", mcpgo.Description("Postgres surrogate ID from get_market_anomalies.disclosure_ids or list_idx_disclosures."), mcpgo.Required()),
	// A write: enqueues an extraction job whose worker persists raw_files and
	// updates the disclosure's extraction status. Declared with
	// writeAnnotations, not read-only.
	writeAnnotations(),
)

// toolGetPipelineStatus — per-source pipeline health.
var toolGetPipelineStatus = mcpgo.NewTool("get_pipeline_status",
	mcpgo.WithDescription("Pipeline health: per-source staleness from source_status plus recent alerts."),
	readOnlyAnnotations(),
)

// toolGetStockBrokerSummary — per-stock broker summary via IPOT (the one tool
// that makes an upstream call). IPOT lists only the top-10 each side; the
// response's total_buy_value / total_sell_value / others_net cover the whole
// market incl. the non-listed tail (issue 03).
var toolGetStockBrokerSummary = mcpgo.NewTool("get_stock_broker_summary",
	mcpgo.WithDescription("Per-stock top buyers/sellers for a ticker+day, fetched from IPOT on demand and persisted. IPOT shows only the top-10 per side, so total_buy_value, total_sell_value, and others_net (= the unlisted tail's net) are included to keep sums market-accurate. Defaults to the ticker's latest trading day."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("date", mcpgo.Description("Trading day, YYYY-MM-DD. Defaults to the ticker's latest stored trading day.")),
	// A write: fetches from IPOT AND persists the day's rows. Declared with
	// writeAnnotations, not read-only (the persist is the tool's contract).
	writeAnnotations(),
)

// toolGetStockBrokerSummaryHistory — stored per-stock broker history.
var toolGetStockBrokerSummaryHistory = mcpgo.NewTool("get_stock_broker_summary_history",
	mcpgo.WithDescription("Stored per-stock broker history over a date range, grouped by trading day. Pure DB read — no upstream call. Empty range returns an empty list."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD."), mcpgo.Required()),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD."), mcpgo.Required()),
	readOnlyAnnotations(),
)

// toolGetBrokerNetFlow — per-broker cumulative net flow over a window (issue
// 04). Rows are aggregated from stored per-day top-10 lists; a broker below
// top-10 on a day is not inferred (its flow sits in others_net), and coverage
// (trade_days_in_window vs covered_days) declares how much of the window the
// stored rows actually observe.
var toolGetBrokerNetFlow = mcpgo.NewTool("get_broker_net_flow",
	mcpgo.WithDescription("Per-broker cumulative net flow over a window, aggregated from stored per-stock broker summaries. Pass ticker for one stock's accumulation over the range; omit ticker for market-wide stance (every broker's net across all tickers with stored rows — population is anomaly-gated, tickers_covered declares it, and each broker row carries a by_ticker breakdown so you can see which stocks a broker accumulated). Each row carries buy/sell/net (net = buy − sell, positive = accumulation) plus days_shown in ticker mode or sessions/tickers/by_ticker in market mode. A broker below the top-10 on a day is never inferred — its flow sits in the window others_net tail. Coverage fields trade_days_in_window vs covered_days show how much of the window has stored rows; empty data returns empty rows with coverage 0, not an error. from/to default to the last 30 calendar days ending at the latest trading day; windows over 180 days are rejected."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. BBRI or BBRI.JK). Omit for market-wide mode.")),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD. Defaults to 30 calendar days before to.")),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD. Defaults to the latest trading day.")),
	readOnlyAnnotations(),
)

// toolGetDailyPrices — stored OHLCV price history over a date range.
var toolGetDailyPrices = mcpgo.NewTool("get_daily_prices",
	mcpgo.WithDescription("Stored Daily Price (OHLCV) series for a ticker over a date range, ascending by trading day. Pure DB read — no upstream call. Empty range returns an empty list."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD."), mcpgo.Required()),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD."), mcpgo.Required()),
	readOnlyAnnotations(),
)

// toolBackfillStockBrokerSummary — on-demand per-stock broker summary backfill
// over a date range (issue 12). Async: enqueues an idx:broker_stock_summary_range
// task and returns a pending envelope; the worker owns the fetch+persist loop
// and the client polls get_stock_broker_summary_history until the range's days
// are covered. A write tool — declared with writeAnnotations, not read-only.
var toolBackfillStockBrokerSummary = mcpgo.NewTool("backfill_stock_broker_summary",
	mcpgo.WithDescription("Backfill per-stock broker summaries for a ticker over a date range. Enqueues an async backfill task and returns immediately with status pending; the worker fetches + persists each trading day in the range (IPOT on-demand). Poll get_stock_broker_summary_history with the same ticker/from/to until the days are covered. Use to fill gaps get_broker_net_flow or get_stock_broker_summary_history reveal (e.g. days the anomaly gate missed, or pre-others_net rows)."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD."), mcpgo.Required()),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD."), mcpgo.Required()),
	writeAnnotations(),
)

// toolGetFinancials — live financial statements from IPOT (temporary route,
// issue 07: nothing persisted; the persisted pipeline is issue 07b).
var toolGetFinancials = mcpgo.NewTool("get_financials",
	mcpgo.WithDescription("Normalized financial statements for a ticker, fetched live from IPOT on demand — nothing is persisted. Periods are cumulative year-to-date as IDX reports them (3M = Jan–Mar, 6M = Jan–Jun, 9M = Jan–Sep, 12M = full year): compare only columns of the same duration. period selects which columns: \"recent\" (default) — the latest ~2 years, one column per report type, plus the analyst-consensus forecast (is_forecast) and the latest unaudited interim report (is_interim); \"quarterly\" — reported Q1 (Jan–Mar) columns for ~6 years; \"annual\" — audited full-year columns for ~6 years. YoY comparison for Q2/Q3 individually is not available — derive direction from recent or annual. Money values are raw IDR; ratios keep the source's units (ROE/ROA in percent, PER/PBV/DebtToEquity as plain multiples); line items without a dedicated field ride in extra keyed by the source label."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. BBRI or TLKM)."), mcpgo.Required()),
	mcpgo.WithString("period", mcpgo.Description("\"recent\" (default), \"quarterly\", or \"annual\".")),
	readOnlyAnnotations(),
)
