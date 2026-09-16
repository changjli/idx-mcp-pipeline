package mcpserver

import (
	"fmt"
	"strings"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/indicator"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// instructions is the server-level instructions string (draft from spec §11,
// trimmed to the tools actually wired — read_idx_disclosure is ticket 12).
const instructions = "IDX Market Analyzer co-pilot. Tools read a daily-persisted IDX pipeline (no live network fetch in V1, except get_stock_broker_summary and fetch_disclosure_pdf which fetch on demand, and get_financials which fetches financial statements live from IPOT — nothing persisted). get_market_anomalies — volume/price anomalies for a trading day, each with disclosure_ids to follow into list_idx_disclosures or read_idx_disclosure. list_idx_disclosures — browse a ticker's filings (metadata only). search_disclosures — cross-ticker disclosure search by keyword over titles and categories, with date range (e.g. which issuers announced a dividend this week). read_idx_disclosure — one disclosure's metadata plus pre-extracted text (truncated to 64KB); check its status field (ok/pending/failed/evicted) before assuming text is present. fetch_disclosure_pdf — on-demand PDF extraction. Cached text → return it. Otherwise enqueue an async extraction job and return pending; poll read_idx_disclosure until status leaves pending (ok | failed). get_ticker_news — RSS headlines tagged to a ticker. get_broker_summary — aggregate per-broker activity for a date. get_stock_broker_summary — per-stock top buyers/sellers for a ticker+day (fetches + persists). get_stock_broker_summary_history — stored per-stock broker history over a date range. backfill_stock_broker_summary — enqueue a per-stock broker summary backfill over a date range (async; poll get_stock_broker_summary_history for completion). get_broker_net_flow — per-broker cumulative net flow over a window (omit ticker for market-wide stance); rows read from stored history, coverage declared. get_sector_flow — the same stored flow aggregated to sector level (group_by sektor/sub_sector/industry), with per-sector listed net, unlisted tail, foreign net, coverage and top brokers. get_daily_prices — stored Daily Price (OHLCV) series for a ticker over a date range. get_financials — normalized financial statements fetched live; periods are cumulative YTD, use the same-duration columns for comparison. get_corporate_actions — stored corporate-actions calendar for a date range (dividends, stock splits, rights issues, warrants, ...), refreshed daily by the pipeline. get_ticker_metadata — stored sector/industry taxonomy + point-in-time index membership per ticker (or the full active universe when ticker is omitted); refreshed ~6-monthly by the sector/index seeder. get_suspensions — stored BEI UMA/suspension events for a date range (type SPT/UPT/UMA; a ticker's presence on a date = suspended or UMA'd that day), refreshed daily by the pipeline. get_shareholder_composition — stored monthly KSEI shareholder composition for one ticker (local/foreign × investor-type share counts, month-end positions, refreshed monthly by the pipeline); for named ≥5% holders use search_disclosures + fetch_disclosure_pdf on demand. get_pipeline_status — pipeline health / staleness. compute_indicators — registered indicators (sma/ema/rsi/... , each with a period) computed on demand from stored Daily Price rows: screen mode returns the latest value per ticker for up to 50 tickers, series mode returns one ticker's full per-day arrays for reading trajectory; insufficient warm-up history is flagged, never short-computed. screen_stocks — whole-universe funnel screener: SQL hard filters over stored Daily Price rows (still trading on the anchor day, at least min_value rupiah of anchor-day transaction value, no suspension/UMA event in the last suspension_window_days trading days), then the requested indicators computed on the survivors only; returns a ranked, capped shortlist with a funnel block (universe, after_value) and total_matches reporting the pre-cap cut. All outputs carry data_stale + last_good_date; if stale, note it to the user."

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

// toolGetSectorFlow — per-sector net flow over a window (issue 17). Read-time
// aggregation over the stored per-stock broker rows joined to the ticker
// taxonomy: no new ingestion, read-only annotations are honest.
var toolGetSectorFlow = mcpgo.NewTool("get_sector_flow",
	mcpgo.WithDescription("Per-sector net flow over a window — where money is rotating, aggregated from the stored per-stock broker summaries joined to the sector taxonomy (IDX stock screener labels). Pure DB read — no upstream call. Each sector row carries buy/sell (the listed top-10 rows), others_net (the sector's unlisted tail, Σ per-ticker footer others_net), net (= buy − sell + others_net, the sector's true net flow; positive = accumulation), foreign_net (Σ per-ticker foreign net; positive = foreign buying), coverage (tickers_covered, days_covered — the population is anomaly-gated + on-demand fetches, so thin sectors are declared, not hidden), brokers_count and the top_brokers breakdown (up to 5 biggest accumulators, net desc). group_by picks the taxonomy dimension: sektor (default), sub_sector or industry. sector filters to one group label (case-insensitive exact match); a ticker with no classification buckets as UNCLASSIFIED so sums still reconcile with get_broker_net_flow over the same window. breakdown adds nested detail per sector row: \"broker\" gives each top broker its per-ticker split, \"ticker\" gives the sector's per-ticker rows (with the per-ticker tail and foreign net a broker row cannot carry). from/to default to the last 30 calendar days ending at the latest trading day; windows over 180 days are rejected."),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD. Defaults to 30 calendar days before to.")),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD. Defaults to the latest trading day.")),
	mcpgo.WithString("sector", mcpgo.Description("Filter to one group label, e.g. Financials (case-insensitive exact match on the group_by dimension). Omit for every sector.")),
	mcpgo.WithString("group_by", mcpgo.Description("\"sektor\" (default), \"sub_sector\", or \"industry\".")),
	mcpgo.WithString("breakdown", mcpgo.Description("Nested detail per sector row: omit for sector totals only; \"broker\" adds each top broker's per-ticker detail (by_ticker + tickers_count); \"ticker\" adds the sector's per-ticker rows (top_tickers) with per-ticker others_net/foreign_net and each ticker's top brokers.")),
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

// toolGetCorporateActions — corporate-actions calendar for a date range (issue
// 09). Pure DB read over the corporate_actions table, which the daily
// idx:corporate_actions task refreshes (one GetIssuedHistory request per day —
// the MCP request path never touches the nodriver sidecar, Heroku H12
// constraint ADR-0009). Read-only annotations are honest here: no fetch, no
// persist on the tool path.
var toolGetCorporateActions = mcpgo.NewTool("get_corporate_actions",
	mcpgo.WithDescription("Stored corporate-actions calendar for an inclusive date range (dividends, stock splits, rights issues, warrants, ...), refreshed daily by the pipeline. Pure DB read — no upstream call. Event dates are the listing/action dates; coverage is bounded by the daily fetch window (recent past through ~90 days ahead), so events outside it return empty. Pass ticker (e.g. BBRI or BBRI.JK) for one issuer; omit for all. Each event carries event_date, ticker, type (the IDX Indonesian label, e.g. Waran, Stock Split), and detail with jumlah_saham and jumlah_saham_setelah_tindakan — either may be 0. Events ascend by event date; source coverage shows in data_stale / last_good_date."),
	mcpgo.WithString("date_from", mcpgo.Description("Range start, YYYY-MM-DD. Inclusive."), mcpgo.Required()),
	mcpgo.WithString("date_to", mcpgo.Description("Range end, YYYY-MM-DD. Inclusive."), mcpgo.Required()),
	mcpgo.WithString("ticker", mcpgo.Description("Optional ticker filter (e.g. BBRI or BBRI.JK).")),
	readOnlyAnnotations(),
)

// toolGetSuspensions — BEI UMA/suspension list for a date range (issue 10).
// Pure DB read over the suspensions table, which the daily idx:suspensions
// task refreshes (two IDX requests per day — GetSuspension + GetUma — the MCP
// request path never touches the nodriver sidecar, Heroku H12 constraint
// ADR-0009). Read-only annotations are honest here: no fetch, no persist on the
// tool path.
var toolGetSuspensions = mcpgo.NewTool("get_suspensions",
	mcpgo.WithDescription("Stored BEI UMA/suspension events for an inclusive date range, refreshed daily by the pipeline. Pure DB read — no upstream call. type is the raw IDX discriminator: SPT (suspension), UPT (trading resumed after suspension), or UMA (unusual market activity warning). A ticker present on a given date means it was suspended or UMA'd that day — the source of truth instead of RSS headline matches. Pass ticker (e.g. BBRI or BBRI.JK) for one issuer; omit for all. Each event carries event_date, ticker, type, and reason (the announcement title). Events ascend by event date; source coverage shows in data_stale / last_good_date."),
	mcpgo.WithString("date_from", mcpgo.Description("Range start, YYYY-MM-DD. Inclusive."), mcpgo.Required()),
	mcpgo.WithString("date_to", mcpgo.Description("Range end, YYYY-MM-DD. Inclusive."), mcpgo.Required()),
	mcpgo.WithString("ticker", mcpgo.Description("Optional ticker filter (e.g. BBRI or BBRI.JK).")),
	readOnlyAnnotations(),
)

// toolGetShareholderComposition — KSEI monthly shareholder composition for one
// ticker (issue 08). Pure DB read over the shareholder_composition table,
// which the ksei:balancepos task refreshes (one zip download per month — the
// MCP request path never touches an upstream host, Heroku H12 constraint
// ADR-0009). Read-only annotations are honest here: no fetch, no persist on
// the tool path.
var toolGetShareholderComposition = mcpgo.NewTool("get_shareholder_composition",
	mcpgo.WithDescription("Stored monthly shareholder composition for one ticker from KSEI's balance-position archive (Lokal-Asing), ingested monthly by the pipeline. Pure DB read — no upstream call. Each row is a month-end position: sec_num (registered shares), price, and local/foreign breakdowns across nine KSEI investor-type codes (is=Insurance, cp=Corporate, pf=Pension Fund, ib=Financial Institution, id=Individu, mf=Mutual Fund, sc=Securities Company, fd=Foundation, ot=Others) plus local_total/foreign_total/total share counts. Counts are scripless (SID-held) positions — large registered (warkat) blocks are absent, so local totals can understate vs a company's LBE report; foreign holdings are scripless in practice and match it. Rows ascend by position date; source coverage shows in data_stale / last_good_date. For named ≥5% holders or pengendali, use search_disclosures (title 'Laporan Bulanan Registrasi Pemegang Efek') then fetch_disclosure_pdf and read the extracted text on demand."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. BBCA or BBCA.JK)."), mcpgo.Required()),
	mcpgo.WithString("date_from", mcpgo.Description("Range start, YYYY-MM-DD. Inclusive."), mcpgo.Required()),
	mcpgo.WithString("date_to", mcpgo.Description("Range end, YYYY-MM-DD. Inclusive."), mcpgo.Required()),
	readOnlyAnnotations(),
)

// toolComputeIndicators — the registry-driven indicator engine, screen and
// series modes (tickets 01 and 03). Pure DB read over daily_prices: indicators
// are computed on demand from stored rows, nothing is precomputed or persisted,
// and the MCP request path never touches the nodriver sidecar (Heroku H12
// constraint, ADR-0009). The registry list in the description is generated from
// internal/indicator, so growing the registry (ticket 02) never edits this
// definition.
var toolComputeIndicators = mcpgo.NewTool("compute_indicators",
	mcpgo.WithDescription(fmt.Sprintf("Registered-indicator values for one or more tickers, computed on demand from stored Daily Price rows. Pure DB read — no upstream call. mode is explicit: screen (default) returns one row per ticker carrying the latest value per requested indicator, for comparing many tickers; series returns one ticker's full per-day arrays over the window, for reading trajectory (is RSI rising, was the volume spike one day or three) — a series call naming more than one ticker is rejected. tickers takes up to 50 codes in screen mode and exactly one in series mode; an over-cap call is rejected, never silently truncated. indicators takes registry specs: a bare name uses the entry's default periods as listed below, and a name carrying periods overrides them — \"sma:50\" for a single-period entry, \"ma_distance:10:30\" for a fast-then-slow pair; periods are 2..400, and an entry with no period (macd, obv) rejects one. Registry — %s. Values are null whenever an indicator has no value: its stored history is shorter than its warm-up (a 200-period MA over 42 stored rows), a day's row did not store a column it reads (high, low, or volume — never a zero substituted for a missing one), or the formula itself yields no number (a flat high-low range). An indicator below its warm-up is never computed on a short window, which would read as fully warmed when it is not: in screen mode it is null and keyed in that row's insufficient array, in series mode its whole array is null and its insufficient flag is true. window is the number of stored trading days loaded per ticker (default 250, max 500); history_rows reports how many usable closes the values rest on, and required_rows the warm-up bar. as_of pins the anchor trading day (default: the latest stored market-wide trading day, so every row in a multi-ticker call is same-day). Screen rows follow the requested ticker order; series arrays are ascending by trading day and parallel to the response's dates array, with each indicator declaring its own required_rows and observed_rows. Staleness shows in data_stale / last_good_date.", indicator.Catalog())),
	mcpgo.WithString("mode", mcpgo.Description("\"screen\" (default) — latest values, one row per ticker, up to 50 tickers. \"series\" — one ticker's full per-day arrays over the window, ascending by trading day.")),
	mcpgo.WithArray("tickers", mcpgo.WithStringItems(), mcpgo.Description("Ticker codes (e.g. BBRI or BBRI.JK). Up to 50 in screen mode, exactly one in series mode; over-cap is rejected."), mcpgo.Required()),
	mcpgo.WithArray("indicators", mcpgo.WithStringItems(), mcpgo.Description("Registry specs, e.g. [\"sma:20\", \"rsi\"]. A bare name uses the entry's default period. Unknown names or bad periods return a structured error listing the valid names."), mcpgo.Required()),
	mcpgo.WithString("as_of", mcpgo.Description("Anchor trading day, YYYY-MM-DD. Default: the latest stored market-wide trading day.")),
	mcpgo.WithNumber("window", mcpgo.Description("Stored trading days loaded per ticker (default 250, max 500). Warm-up is counted in stored rows, so weekends and holidays never pad it.")),
	readOnlyAnnotations(),
)

// toolScreenStocks — the whole-universe funnel screener (screener ticket 04),
// the deterministic stage 1 of the daily flow. Pure DB read over daily_prices
// and suspensions: the hard filters are one SQL statement, indicators are
// computed in-process on the survivors only, and the MCP request path never
// touches the nodriver sidecar (Heroku H12 constraint, ADR-0009). The default
// column set is read from the usecase so the description and the computation
// cannot drift apart.
var toolScreenStocks = mcpgo.NewTool("screen_stocks",
	mcpgo.WithDescription(fmt.Sprintf("Whole-universe funnel screener over stored Daily Price rows: one call turns the persisted market into a ranked shortlist. Pure DB read — no upstream call. The funnel runs in stages and every response reports them. Stage 1, the SQL hard filters, keeps a ticker that (a) has a stored row ON the anchor trading day — a halted or delisted name's latest row is older, so it drops out, (b) traded at least min_value rupiah on that day, and (c) has no suspension-related event (SPT suspend, UPT resume, or UMA warning) in the last suspension_window_days trading days. Stage 2 computes the requested indicators for the survivors only — the universe's price history is never loaded to answer a screen that discards it. No structural filter runs yet, so this is a liquid, alive, unsuspended shortlist with indicator columns. The funnel block reports universe (tickers with a row inside the window) and after_value (survivors of the hard filters), so a filter set that keeps everything or nothing is visible instead of guessed at. Each row carries the ticker, its anchor-day value and close, and one column per requested indicator; a value is null whenever the stored history is shorter than that indicator's warm-up, and the key is listed in the row's insufficient array — never a short-window number that would read as fully warmed, and never a zero standing in for \"no value\". Default columns: %s. sort names the ranking key: %q (the anchor day's transaction value, the default) or any requested indicator key; order is \"desc\" (default) or \"asc\". A row whose sort-key value is unobserved ranks last in either direction; ties break by anchor-day value descending, then ticker ascending, so identical inputs always produce identical order. window is the lookback in stored trading days — the population the funnel counts and the history each survivor's indicators rest on (default 250, max 500); as_of pins the anchor trading day (default: the latest stored market-wide trading day, so every row is same-day). limit caps the returned rows (default 30, max 50, an over-cap call is rejected, never silently truncated) and total_matches reports how many tickers passed every filter before that cap — the difference is the cut. Staleness shows in data_stale / last_good_date.",
		strings.Join(usecase.ScreenerDefaultIndicators, ", "), "value")),
	mcpgo.WithNumber("min_value", mcpgo.Description("Minimum anchor-day transaction value in rupiah. Defaults to 5000000000 (Rp 5bn — the same liquidity floor the pipeline's anomaly detector uses). 0 removes the floor; a ticker whose row stored no value never passes at any floor.")),
	mcpgo.WithNumber("suspension_window_days", mcpgo.Description("Trading days back that a suspension-related event disqualifies a ticker. Default 5, max 60. Counted in stored trading days, so weekends and holidays never shorten it.")),
	mcpgo.WithNumber("window", mcpgo.Description("Lookback in stored trading days: the population the funnel counts and the history each survivor's indicators rest on. Default 250, max 500.")),
	mcpgo.WithNumber("limit", mcpgo.Description("Maximum rows returned, after ranking. Default 30, max 50; an over-cap value is rejected, not truncated. total_matches reports the pre-cap count.")),
	mcpgo.WithArray("indicators", mcpgo.WithStringItems(), mcpgo.Description("Registry specs for the row columns, e.g. [\"rsi:14\", \"ma_distance:10:30\"]. Empty uses the default stage-1 set. Unknown names or bad periods return a structured error listing the valid names.")),
	mcpgo.WithString("sort", mcpgo.Description("\"value\" (default, the anchor day's transaction value) or any requested indicator key, e.g. \"range_position:60\".")),
	mcpgo.WithString("order", mcpgo.Description("\"desc\" (default) or \"asc\".")),
	mcpgo.WithString("as_of", mcpgo.Description("Anchor trading day, YYYY-MM-DD. Default: the latest stored market-wide trading day.")),
	readOnlyAnnotations(),
)

// toolGetTickerMetadata — sector/industry taxonomy + point-in-time index
// membership (issue 16). Pure DB read over the tickers sector columns and the
// ticker_indices table, both owned by the 15b sector/index seeder (6-monthly
// Feb+Jul rebalance cadence — the MCP request path never touches the nodriver
// sidecar, Heroku H12 constraint ADR-0009). Read-only annotations are honest
// here: no fetch, no persist on the tool path. Index membership is
// point-in-time; the sector columns are current state.
var toolGetTickerMetadata = mcpgo.NewTool("get_ticker_metadata",
	mcpgo.WithDescription("Stored ticker metadata: sector/industry taxonomy (IDX stock screener's canonical English labels — sector, industry, sub_sector, sub_industry, sub_industry_code) plus point-in-time index membership (e.g. LQ45, IDX30, COMPOSITE). Pure DB read — no upstream call. Refreshed by the pipeline's sector/index seeder on the Feb+Jul index-rebalance cadence. Pass ticker (e.g. BBRI or BBRI.JK) for one ticker; omit for the full active universe. Pass date (YYYY-MM-DD) to pin the index-membership snapshot (e.g. a historical screen); default is the latest snapshot, whose date is returned in effective_date. Sector columns are current state — not dated by effective_date. Staleness shows in data_stale / last_good_date."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. BBRI or BBRI.JK). Omit for the full active universe.")),
	mcpgo.WithString("date", mcpgo.Description("Effective date for index membership, YYYY-MM-DD. Default: latest snapshot.")),
	readOnlyAnnotations(),
)
