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
const instructions = "IDX Market Analyzer co-pilot. Tools read a daily-persisted IDX pipeline (no live network fetch in V1, except get_stock_broker_summary, screen_stocks, backfill_stock_broker_summary and fetch_disclosure_pdf, which fetch and persist on demand; get_financials is a pure DB read over the persisted monthly statement store — never live; Tier 2 depth columns are filled by a monthly Yahoo enrichment and are null/unknown until then). The broker tools are FOREIGN-EXECUTED flow: buy/sell are foreign gross, per-broker net is foreign net, and f_nval is null when unobserved, never zero. get_market_anomalies — volume/price anomalies for a trading day, each with disclosure_ids to follow into list_idx_disclosures or read_idx_disclosure. list_idx_disclosures — browse a ticker's filings (metadata only). search_disclosures — cross-ticker disclosure search by keyword over titles and categories, with date range (e.g. which issuers announced a dividend this week). read_idx_disclosure — one disclosure's metadata plus pre-extracted text (truncated to 64KB); check its status field (ok/pending/failed/evicted) before assuming text is present. fetch_disclosure_pdf — on-demand PDF extraction. Cached text → return it. Otherwise enqueue an async extraction job and return pending; poll read_idx_disclosure until status leaves pending (ok | failed). get_ticker_news — RSS headlines tagged to a ticker. get_broker_summary — aggregate per-broker activity for a date. get_stock_broker_summary — per-stock foreign-executed flow for a ticker+day: an exact stored window costs nothing, a miss costs one unit and persists; served_from says which. get_stock_broker_summary_history — stored per-stock broker windows over a date range, each with its own from/to/grain; a mixed table reads grain=mixed. backfill_stock_broker_summary — enqueue a per-stock broker summary backfill over a date range (one window call per ticker; async — poll get_stock_broker_summary_history for completion). get_broker_net_flow — per-broker foreign net over a window (omit ticker for market-wide stance); rows read from a disjoint selection of stored windows, coverage declared. get_sector_flow — the same stored flow aggregated to sector level (group_by sektor/sub_sector/industry), with FOREIGN gross buy/sell, foreign net, coverage and top brokers — every figure is foreign-executed. get_daily_prices — stored Daily Price (OHLCV) series for a ticker over a date range. get_financials — stored normalized financial statements (monthly IDX snapshot, pure DB read); periods are cumulative YTD, compare same-duration columns, ratios are TTM as reported, and coverage declares each field observed / unknown / not_applicable (depth fields are unknown until the monthly Tier-2 enrichment fills them, never zero; a bank/insurer's absent gross profit is not_applicable). get_corporate_actions — stored corporate-actions calendar for a date range (dividends, stock splits, rights issues, warrants, ...), refreshed daily by the pipeline. get_ticker_metadata — stored sector/industry taxonomy + point-in-time index membership per ticker (or the full active universe when ticker is omitted); refreshed ~6-monthly by the sector/index seeder. get_suspensions — stored BEI UMA/suspension events for a date range (type SPT/UPT/UMA; a ticker's presence on a date = suspended or UMA'd that day), refreshed daily by the pipeline. get_shareholder_composition — stored monthly KSEI shareholder composition for one ticker (local/foreign × investor-type share counts, month-end positions, refreshed monthly by the pipeline); for named ≥5% holders use search_disclosures + fetch_disclosure_pdf on demand. get_pipeline_status — pipeline health / staleness (the broker source is fresh when its newest stored window reaches the last closed month's last trading day; stale_trading_days reports how far behind it is). compute_indicators — registered indicators (sma/ema/rsi/... , each with a period) computed on demand from stored Daily Price rows: screen mode returns the latest value per ticker for up to 50 tickers, series mode returns one ticker's full per-day arrays for reading trajectory; insufficient warm-up history is flagged, never short-computed. screen_stocks — whole-universe funnel screener: SQL hard filters over stored Daily Price rows (still trading on the anchor day, at least min_value rupiah of anchor-day transaction value, no suspension/UMA event in the last suspension_window_days trading days), then the requested indicators computed on the survivors only, then the structural filter set ({indicator, op, value} threshold comparisons on registered indicators, AND-combined — omit to run the shipped default breakout-zone set, pass [] for none); returns a ranked, capped shortlist with a funnel block (universe, after_value, after_structure) and total_matches reporting the pre-cap cut; every row also carries the broker-flow column foreign_net; the tool enriches at most its first 10 rows (ranked without foreign_net) from a shared 10-unit daily pool, and every other row is a stored-only read — an uncovered value is null and flagged, never zero. It is sortable via sort=foreign_net, but a name outside the enriched ten cannot enter on fresh flow. All outputs carry data_stale + last_good_date; if stale, note it to the user. Measurement Harness (ADR-0017): measure_screen / measure_anomalies / measure_flow / measure_disclosures / measure_journal replay past trading days and return the report body (read-only — they grade, they do not seal a report to R2, and they never spend broker quota: a window that stored data does not exactly cover reads unobserved); log_verdict appends one Verdict to the Decision Journal at decision time; get_screener_config / put_screener_config read and publish the Active Filter Set; list_measurement_reports / get_measurement_report read the stored Grade reports."

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

// toolGetStockBrokerSummary — per-stock broker summary via Index Alpha (the
// one tool that makes an upstream call). Every broker is stored; the response
// projects the top ten per side and declares coverage against t_val.
var toolGetStockBrokerSummary = mcpgo.NewTool("get_stock_broker_summary",
	mcpgo.WithDescription("Per-stock foreign-executed broker flow for a ticker+day. An exact stored window answers for zero quota; a miss spends exactly one unit on that day's foreign lane, persists it, and answers (served_from says which: store | upstream). Every broker is stored; the response returns at most ten buyers and ten sellers per side. total_buy_value / total_sell_value are foreign gross buy/sell and totals.f_nval is the foreign net (Σbuy − Σsell). In the foreign lane totals.t_val / t_lot / avg are null — an f payload has no all-investor totals — and coverage.complete is structural (the vendor returns the full set), not a t_val reconciliation; reconciling requires a separate all-investor fetch. The response states its range (from/to, grain=day). Defaults to the ticker's latest trading day; an empty day falls back over stored prior days only, never an upstream probe."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("date", mcpgo.Description("Trading day, YYYY-MM-DD. Defaults to the ticker's latest stored trading day.")),
	// A write: fetches from Index Alpha AND persists the day's window. Declared
	// with writeAnnotations, not read-only (the persist is the tool's contract).
	writeAnnotations(),
)

// toolGetStockBrokerSummaryHistory — stored per-stock broker history.
var toolGetStockBrokerSummaryHistory = mcpgo.NewTool("get_stock_broker_summary_history",
	mcpgo.WithDescription("Stored per-stock broker windows intersecting a date range, each with its own from/to and grain (day or window). Pure DB read — no upstream call, no quota. A table holding day rows and aggregated windows returns grain \"mixed\", so a window is never presented as a daily series; each entry still shows at most ten buyers and ten sellers projected from its full stored set. Every figure is foreign-executed flow: total_buy_value / total_sell_value are foreign gross and totals.f_nval is the window's foreign net; t_val / t_lot / avg are null in the foreign lane. Legacy top-10 rows are never read. Empty range returns an empty list."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD."), mcpgo.Required()),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD."), mcpgo.Required()),
	readOnlyAnnotations(),
)

// toolGetBrokerNetFlow — per-broker foreign net flow over a window.
var toolGetBrokerNetFlow = mcpgo.NewTool("get_broker_net_flow",
	mcpgo.WithDescription("Per-broker cumulative foreign net flow over a window, aggregated from stored foreign-lane broker windows. Pass ticker for one stock's accumulation over the range; omit ticker for market-wide stance (every broker's foreign net across all tickers with stored windows — tickers_covered declares the population, and each broker row carries a by_ticker breakdown so you can see which stocks a broker accumulated). Each row carries buy/sell (foreign gross) and net (foreign net = buy − sell, positive = accumulation), plus days_shown in ticker mode or sessions/tickers/by_ticker in market mode. Only a disjoint selection of stored windows is summed, so overlapping answer caches never double-count; an all-investor aggregate would be structurally zero, so net exists only per broker. Coverage fields trade_days_in_window vs covered_days show how much of the window has stored windows; an uncovered window returns empty rows with coverage 0, not an error. from/to default to the last 30 calendar days ending at the latest trading day; windows over 180 days are rejected."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. BBRI or BBRI.JK). Omit for market-wide mode.")),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD. Defaults to 30 calendar days before to.")),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD. Defaults to the latest trading day.")),
	readOnlyAnnotations(),
)

// toolGetSectorFlow — per-sector flow over a window (issue 17). Read-time
// aggregation over the stored per-stock broker rows joined to the ticker
// taxonomy: no new ingestion, read-only annotations are honest.
var toolGetSectorFlow = mcpgo.NewTool("get_sector_flow",
	mcpgo.WithDescription("Per-sector foreign flow over a window — where foreign money is rotating, aggregated from stored foreign-lane broker windows joined to the sector taxonomy (IDX stock screener labels). Pure DB read — no upstream call. Every monetary figure is FOREIGN-EXECUTED flow: buy/sell are foreign gross over the full stored broker set (not all-investor gross), and each broker's net is that broker's foreign net. foreign_net (Σ the selected windows' f_nval; null when no window in scope observed it, never zero) comes with foreign_net_days (the trading days it rests on) and coverage (tickers_covered, days_covered — only a disjoint selection of stored windows is summed, so an overlapping cache never double-counts; thin sectors are declared, not hidden), brokers_count and the top_brokers breakdown (up to 5 brokers by foreign net desc). There is deliberately no aggregate net: over the full broker set an all-investor sector net is structurally zero, so net exists only per broker. Rows are ordered foreign_net desc (unobserved last), then foreign gross buy desc, then label. group_by picks the taxonomy dimension: sektor (default), sub_sector or industry. sector filters to one group label (case-insensitive exact match); a ticker with no classification buckets as UNCLASSIFIED so sums still reconcile with get_broker_net_flow over the same window. breakdown adds nested detail per sector row: \"broker\" gives each top broker its per-ticker split, \"ticker\" gives the sector's per-ticker rows (with the ticker's foreign net) and each ticker's top brokers. from/to default to the last 30 calendar days ending at the latest trading day; windows over 180 days are rejected."),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD. Defaults to 30 calendar days before to.")),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD. Defaults to the latest trading day.")),
	mcpgo.WithString("sector", mcpgo.Description("Filter to one group label, e.g. Financials (case-insensitive exact match on the group_by dimension). Omit for every sector.")),
	mcpgo.WithString("group_by", mcpgo.Description("\"sektor\" (default), \"sub_sector\", or \"industry\".")),
	mcpgo.WithString("breakdown", mcpgo.Description("Nested detail per sector row: omit for sector totals only; \"broker\" adds each top broker's per-ticker detail (by_ticker + tickers_count); \"ticker\" adds the sector's per-ticker rows (top_tickers) with per-ticker foreign_net and each ticker's top brokers.")),
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
	mcpgo.WithDescription("Backfill per-stock broker summaries for a ticker over a date range. Enqueues an async backfill task and returns immediately with status pending; the worker answers from an exact cover of stored windows for zero units, otherwise fetches the WHOLE requested range in one call (one unit, regardless of span) and stores it as a window cache — it never walks trading days. Poll get_stock_broker_summary_history with the same ticker/from/to until the range is covered; entries may be window-grain rather than a daily series. Use to fill gaps get_broker_net_flow or get_stock_broker_summary_history reveal."),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("from", mcpgo.Description("Range start, YYYY-MM-DD."), mcpgo.Required()),
	mcpgo.WithString("to", mcpgo.Description("Range end, YYYY-MM-DD."), mcpgo.Required()),
	writeAnnotations(),
)

// toolGetFinancials — stored financial statements (financials-v2 issue 03).
// Pure DB read over financial_statements, populated by the monthly IDX ratio
// snapshot and its history backfill; the request path never touches IPOT or
// IDX. The response declares per-field coverage because Tier 1 does not carry
// every line item.
var toolGetFinancials = mcpgo.NewTool("get_financials",
	mcpgo.WithDescription("Stored financial statements for a ticker, read from the monthly IDX ratio-snapshot store — pure DB read, no live fetch. period selects the view: \"recent\" (default) — the latest ~2 years, all durations; \"quarterly\" — every interim report (3M, 6M and 9M) across full stored history (not Q1 only; 6M and 9M periods are included), newest first; \"annual\" — every 12M report across full stored history. Periods are cumulative year-to-date (3M = Jan–Mar, 6M = Jan–Jun, 9M = Jan–Sep, 12M = full year): compare only rows with the same duration_months, carrying the period_label (e.g. \"9M 2026\"). Money is raw rupiah after IDX's undisclosed conversion of USD reporters (value_basis says so); ratios are IDX's TTM values (eps, roe, roa, npm) and are never recomputed from the YTD columns. net_profit is attributable to owners; net_profit_incl_nci adds non-controlling interests. per and pbv are calculated at read time from the last stored close on or before period_end (price_as_of); they are null when no close covers the period or the denominator is zero, and negative values are returned as-is. coverage declares each field observed / unknown / not_applicable with observed_rows vs returned_rows: cash, gross profit, operating profit, EBITDA, interest expense and total debt are filled from the monthly Tier-2 depth enrichment when it has run; an unenriched field is unknown, never zero, and a bank/insurer's absent gross profit is not_applicable. debt_to_equity is calculated from Tier-2 total debt over Tier-1 equity when both exist (liabilities_to_equity remains the snapshot's der, which measures something else); is_forecast is always false. A ticker with no stored statements returns an empty statements list, not an error. data_stale + last_good_date reflect the newest stored period end (a 2019 statement reads stale)."),
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
	mcpgo.WithDescription(fmt.Sprintf("Whole-universe funnel screener over stored Daily Price rows: one call turns the persisted market into a ranked shortlist. The price stages are a pure DB read; the broker-flow column reads the stored partition first and enriches at most the first 10 rows (ranked without foreign_net) from a shared 10-unit daily pool, then re-ranks the returned set. It never fetches the survivor set and never fetches an eleventh name. The funnel runs in stages and every response reports them. Stage 1, the SQL hard filters, keeps a ticker that (a) has a stored row ON the anchor trading day — a halted or delisted name's latest row is older, so it drops out, (b) traded at least min_value rupiah on that day, and (c) has no suspension-related event (SPT suspend, UPT resume, or UMA warning) in the last suspension_window_days trading days. Stage 2 computes the requested indicators for the survivors only — the universe's price history is never loaded to answer a screen that discards it. Stage 3 applies the structural filter set: threshold comparisons on single registered indicator values, AND-combined, so \"price above MA20\" is ma_distance > 0 and the sideways-breakout-zone shape is that plus a range_position band. Omitting filters runs the shipped default set (%s); passing [] runs no structural filters; a non-empty list replaces the default set. A filter indicator is computed even when it was not requested as a column, and a row whose filter value is null is dropped rather than assumed to pass. The funnel block reports universe (tickers with a row inside the window), after_value (survivors of the hard filters), and after_structure (survivors of the filters), so a filter set that keeps everything or nothing is visible instead of guessed at. Each row carries the ticker, its anchor-day value and close, and one column per computed indicator; a value is null whenever the stored history is shorter than that indicator's warm-up, and the key is listed in the row's insufficient array — never a short-window number that would read as fully warmed, and never a zero standing in for \"no value\". Default columns: %s. Every row also carries foreign_net: the stored foreign net (Σ f_nval from the broker footer totals, covering the whole window's foreign flow) over the last flow_window_days stored trading days, joined for the survivors only. A ticker is observed only when stored windows exactly cover that window — a partial cover is unobserved, never a partial sum, and overlapping answer caches are never summed. Shares an exact stored trailing window with the Flow Grade. On a cover miss inside the enriched ten, the tool fetches that ticker's whole trailing window once and caches it while the shared daily pool and pool.Available() allow, so flow_fetched counts the units spent this call, flow_skipped counts eligible rows left alone, flow_enrich_limit says how many rows were eligible, and flow_budget_remaining is the shared daily pool left; the column is always accompanied by a per-row coverage block keyed by column name — coverage.foreign_net.observed says whether an exact cover was found and coverage.foreign_net.days how many of the window's trading days it rests on — and an uncovered row carries a null foreign_net, never a zero standing in for \"no data\". The response echoes flow_window_days and flow_days_in_window (the trading days the window actually spans), so a row's day count reads against its denominator. sort names the ranking key: %q (the anchor day's transaction value, the default), \"foreign_net\", or any computed indicator key; order is \"desc\" (default) or \"asc\". A row whose sort-key value is unobserved ranks last in either direction; ties break by anchor-day value descending, then ticker ascending, so identical inputs always produce identical order. window is the lookback in stored trading days — the population the funnel counts and the history each survivor's indicators rest on (default 250, max 500); as_of pins the anchor trading day (default: the latest stored market-wide trading day, so every row is same-day). limit caps the returned rows (default 30, max 50, an over-cap call is rejected, never silently truncated) and total_matches reports how many tickers passed every filter before that cap — the difference is the cut. Staleness shows in data_stale / last_good_date.",
		usecase.ScreenerDefaultFiltersDoc(), strings.Join(usecase.ScreenerDefaultIndicators, ", "), "value")),
	mcpgo.WithNumber("min_value", mcpgo.Description("Minimum anchor-day transaction value in rupiah. Defaults to 5000000000 (Rp 5bn — the same liquidity floor the pipeline's anomaly detector uses). 0 removes the floor; a ticker whose row stored no value never passes at any floor.")),
	mcpgo.WithNumber("suspension_window_days", mcpgo.Description("Trading days back that a suspension-related event disqualifies a ticker. Default 5, max 60. Counted in stored trading days, so weekends and holidays never shorten it.")),
	mcpgo.WithNumber("window", mcpgo.Description("Lookback in stored trading days: the population the funnel counts and the history each survivor's indicators rest on. Default 250, max 500.")),
	mcpgo.WithNumber("flow_window_days", mcpgo.Description("Stored trading days the broker-flow column reaches back over, ending on the anchor day. Default 20, max 60 — broker rows are retained 60 calendar days, so a longer window can never be observed and is rejected rather than returned mostly empty. Independent of `window`.")),
	mcpgo.WithNumber("limit", mcpgo.Description("Maximum rows returned, after ranking. Default 30, max 50; an over-cap value is rejected, not truncated. total_matches reports the pre-cap count.")),
	mcpgo.WithArray("indicators", mcpgo.WithStringItems(), mcpgo.Description("Registry specs for the row columns, e.g. [\"rsi:14\", \"ma_distance:10:30\"]. Empty uses the default stage-1 set. Unknown names or bad periods return a structured error listing the valid names.")),
	mcpgo.WithArray("filters", mcpgo.Items(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"indicator": map[string]any{
				"type":        "string",
				"description": "A registry spec, e.g. \"ma_distance:20:50\", or a bare name for the entry's default periods.",
			},
			"op": map[string]any{
				"type":        "string",
				"enum":        []string{"lt", "gt", "lte", "gte", "between"},
				"description": "Threshold comparison applied to the indicator's latest value.",
			},
			"value": map[string]any{
				"description": "One number for lt/gt/lte/gte; [low, high] for between, inclusive at both ends.",
				"anyOf": []map[string]any{
					{"type": "number"},
					{
						"type":     "array",
						"items":    map[string]any{"type": "number"},
						"minItems": 2,
						"maxItems": 2,
					},
				},
			},
		},
		"required": []string{"indicator", "op", "value"},
	}), mcpgo.Description("Structural filters, AND-combined, each a threshold on ONE registered indicator's latest value. Omit to run the shipped default set (listed above); pass [] to run no structural filters at all; a non-empty list replaces the default set rather than adding to it. Filter indicators join the columns even when not requested, so a row shows the number it was judged on and sort may name it. A row whose filter value is null — the indicator below its warm-up — is dropped, never assumed to pass. Direct indicator-to-indicator comparison belongs in a derived indicator (ma_distance for close-vs-MA), not here; crossovers and streaks are out of scope and belong to compute_indicators series mode.")),
	mcpgo.WithString("sort", mcpgo.Description("\"value\" (default, the anchor day's transaction value) or any requested indicator key, e.g. \"range_position:60\".")),
	mcpgo.WithString("order", mcpgo.Description("\"desc\" (default) or \"asc\".")),
	mcpgo.WithString("as_of", mcpgo.Description("Anchor trading day, YYYY-MM-DD. Default: the latest stored market-wide trading day.")),
	// A write when the broker-flow column has to fetch: it persists the
	// trailing-window cache. Declared write so a client prompts honestly.
	writeAnnotations(),
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

// ─── Measurement Harness (ticket 08, ADR-0017) ─────────────────────────────
//
// The five grader tools reuse the same usecases the local `cmd/measure` CLI
// runs, so the two interfaces can never grade differently. They are read-only:
// each computes a report and returns its body; it does NOT seal the report to
// R2 (uploading stays `cmd/measure`'s job, ADR-0016). The journal is the one
// read/write area — measure_journal reads, log_verdict writes.

// toolMeasureScreen — the screen grader: survivors vs the Liquidity Baseline.
var toolMeasureScreen = mcpgo.NewTool("measure_screen",
	mcpgo.WithDescription("Replay the whole-universe screen as of past trading days and score its survivors against the Liquidity Baseline at 5/10/20-day Forward Returns — the harness's answer to \"are my screen picks any good?\". Reuses the exact screener the daily `screen_stocks` tool runs, so a Grade can never drift from what runs live. Pass one or more as_of anchors (20 post-anchor trading days of stored prices are required; too-recent anchors are skipped and reported). filters names a filter set; default is the Active Filter Set. compare names a second set for an Excess Delta read. Returns the same report body `measure grade screen` renders. Read-only: it does not seal a report to R2."),
	mcpgo.WithArray("as_of", mcpgo.WithStringItems(), mcpgo.Description("Replay anchor trading days, YYYY-MM-DD (one or more)."), mcpgo.Required()),
	mcpgo.WithString("filters", mcpgo.Description("Named filter set to grade. Default: the Active Filter Set.")),
	mcpgo.WithString("compare", mcpgo.Description("Optional second named filter set to compare against (Excess Delta).")),
	mcpgo.WithString("holdout_start", mcpgo.Description("Seal date, YYYY-MM-DD: restrict anchors to on/after it (a Holdout validation run).")),
	readOnlyAnnotations(),
)

// toolMeasureAnomalies — the anomaly grader, split by direction/confluence.
var toolMeasureAnomalies = mcpgo.NewTool("measure_anomalies",
	mcpgo.WithDescription("Score market Anomalies: each anomaly ticker-day's Forward Returns against the Liquidity Baseline, split by Direction Bucket (up / down / volume_spike) and by Anomaly Confluence (broker confirmation × passed-filter disclosure) — the harness's answer to \"which anomaly population deserves attention?\". Read-only: it does not seal a report to R2."),
	mcpgo.WithArray("as_of", mcpgo.WithStringItems(), mcpgo.Description("Replay anchor trading days, YYYY-MM-DD (one or more)."), mcpgo.Required()),
	mcpgo.WithNumber("flow_window_days", mcpgo.Description("Flow Lead-in Window length in trading days (default 20).")),
	mcpgo.WithString("holdout_start", mcpgo.Description("Seal date, YYYY-MM-DD: restrict anchors to on/after it.")),
	readOnlyAnnotations(),
)

// toolMeasureFlow — the foreign-net grader.
var toolMeasureFlow = mcpgo.NewTool("measure_flow",
	mcpgo.WithDescription("Score the Flow Column's claim — that foreign accumulation precedes outperformance. It buckets the Liquidity Baseline by stored foreign_net (sign, and a signed-magnitude tercile) and measures each bucket's Forward Returns against that same baseline; unobserved rows are their own bucket, never zero-imputed. Read-only: it does not seal a report to R2."),
	mcpgo.WithArray("as_of", mcpgo.WithStringItems(), mcpgo.Description("Replay anchor trading days, YYYY-MM-DD (one or more)."), mcpgo.Required()),
	mcpgo.WithNumber("flow_window_days", mcpgo.Description("Flow Column window length in trading days (default 20).")),
	mcpgo.WithString("holdout_start", mcpgo.Description("Seal date, YYYY-MM-DD: restrict anchors to on/after it.")),
	readOnlyAnnotations(),
)

// toolMeasureDisclosures — the disclosure-precision grader.
var toolMeasureDisclosures = mcpgo.NewTool("measure_disclosures",
	mcpgo.WithDescription("Score the 3-layer disclosure filter's precision: each passed-filter Disclosure's ticker Forward Returns from its Filing Day against the Liquidity Baseline, split by whitelist category, plus a deterministic manual Reject Sample for the false-negative check. Read-only: it does not seal a report to R2."),
	mcpgo.WithArray("as_of", mcpgo.WithStringItems(), mcpgo.Description("Disclosure Filing Days (announcement dates), YYYY-MM-DD (one or more)."), mcpgo.Required()),
	mcpgo.WithNumber("reject_sample", mcpgo.Description("Manual Reject Sample size in rejected filings (default 20).")),
	mcpgo.WithString("holdout_start", mcpgo.Description("Seal date, YYYY-MM-DD: restrict filing days to on/after it.")),
	readOnlyAnnotations(),
)

// toolMeasureJournal — the Decision Journal grader.
var toolMeasureJournal = mcpgo.NewTool("measure_journal",
	mcpgo.WithDescription("Grade the AI's logged Verdicts: resolve every SETUP's entry zone and TP/CL race against stored prices (win / loss / expired / unfilled / unresolved) and report win rate, Expectancy, Breakeven Win Rate, Calibration and any Verdict Conflicts. Reads the measurement_journal table and stored prices; replays nothing. Read-only: it does not seal a report to R2."),
	mcpgo.WithString("from", mcpgo.Description("Earliest decision date to grade, YYYY-MM-DD (inclusive). Optional.")),
	mcpgo.WithString("until", mcpgo.Description("Latest decision date to grade, YYYY-MM-DD (inclusive). Optional.")),
	readOnlyAnnotations(),
)

// toolLogVerdict — the Decision Journal writer.
var toolLogVerdict = mcpgo.NewTool("log_verdict",
	mcpgo.WithDescription("Append one Verdict to the Decision Journal (measurement_journal) at decision time: SETUP (a trade plan exists, carrying the three zones), WATCH (interesting, no plan), or PASS (rejected). Every deep-dive logs exactly one Verdict, not only trades taken. session identifies this agent run. date is the last stored trading day the plan reads. supersedes names the row a deliberate correction replaces and implies committed (the Grader scores the committed Verdict when a key holds more than one). A same-session duplicate is last-wins; a cross-session disagreement is a Verdict Conflict — appended, never auto-resolved, and surfaced in the response. Appends only: it never touches the pipeline."),
	mcpgo.WithString("session", mcpgo.Description("Identifier of this agent run (e.g. a UUID or a descriptive run id). Required."), mcpgo.Required()),
	mcpgo.WithString("date", mcpgo.Description("Decision day — the last stored trading day whose close the plan reads, YYYY-MM-DD."), mcpgo.Required()),
	mcpgo.WithString("ticker", mcpgo.Description("Ticker code (e.g. RAJA or RAJA.JK)."), mcpgo.Required()),
	mcpgo.WithString("verdict", mcpgo.Description("SETUP, WATCH or PASS."), mcpgo.Required()),
	mcpgo.WithNumber("entryLow", mcpgo.Description("Entry zone low (SETUP only).")),
	mcpgo.WithNumber("entryHigh", mcpgo.Description("Entry zone high (SETUP only).")),
	mcpgo.WithNumber("clLow", mcpgo.Description("Cut-loss zone low (SETUP only).")),
	mcpgo.WithNumber("clHigh", mcpgo.Description("Cut-loss zone high (SETUP only).")),
	mcpgo.WithNumber("tpLow", mcpgo.Description("Target zone low (SETUP only).")),
	mcpgo.WithNumber("tpHigh", mcpgo.Description("Target zone high (SETUP only).")),
	mcpgo.WithString("rationale", mcpgo.Description("One-line rationale."), mcpgo.Required()),
	mcpgo.WithNumber("supersedes", mcpgo.Description("Row id this Verdict deliberately corrects (provenance; implies committed).")),
	mcpgo.WithBoolean("committed", mcpgo.Description("Mark this the Committed Verdict the Grader scores when the key holds more than one.")),
	writeAnnotations(),
)

// toolGetScreenerConfig — read the Active Filter Set's Config Document.
var toolGetScreenerConfig = mcpgo.NewTool("get_screener_config",
	mcpgo.WithDescription("Read the Active Filter Set: the Config Document ({active, sets}) the screen Grades run, plus its Config Hash. The document is one R2 object holding every named filter set and the active pointer; a Grade pins this hash so it stays reproducible after the set moves (ADR-0016). Read-only."),
	readOnlyAnnotations(),
)

// toolPutScreenerConfig — publish a new Config Document version.
var toolPutScreenerConfig = mcpgo.NewTool("put_screener_config",
	mcpgo.WithDescription("Publish the Active Filter Set: writes a new immutable, content-addressed Config Document version and repoints the active pointer, returning the new Config Hash. Reads whole and writes whole — get_screener_config first, edit, then put. Pass expected_hash (the hash you read) to reject the write if another writer has moved the pointer; omit to overwrite unconditionally. This is the loop's tuning write; Holdout gating of writes is a separate ticket."),
	mcpgo.WithString("active", mcpgo.Description("Name of the filter set in force."), mcpgo.Required()),
	mcpgo.WithObject("sets", mcpgo.Description("Named filter sets, keyed by name. Each value is a filter-set document (description, window, min_value, suspension_window_days, flow_window_days, indicators, filters, sort, order) — the same shape as measurement/filtersets/<name>.json."), mcpgo.Required()),
	mcpgo.WithString("expected_hash", mcpgo.Description("Optional Config Hash you read; the write is rejected if the active pointer has since moved.")),
	writeAnnotations(),
)

// toolListMeasurementReports — the stored Grade reports.
var toolListMeasurementReports = mcpgo.NewTool("list_measurement_reports",
	mcpgo.WithDescription("List the stored Grade report base names on R2, newest-first (e.g. 2026-07-01_to_2026-09-07_volume-tolerant — the range and filter-set name). Each is the latest copy of a report; sealed versions are audit and are not listed. Read-only."),
	readOnlyAnnotations(),
)

// toolGetMeasurementReport — one stored Grade report.
var toolGetMeasurementReport = mcpgo.NewTool("get_measurement_report",
	mcpgo.WithDescription("Read one stored Grade report by its base name (from list_measurement_reports): the report JSON body plus the human markdown summary. Returns the latest copy; the body carries its own Config Hash and git revision. Read-only."),
	mcpgo.WithString("base", mcpgo.Description("Report base name, e.g. 2026-07-01_to_2026-09-07_volume-tolerant."), mcpgo.Required()),
	readOnlyAnnotations(),
)
