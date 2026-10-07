# IDX Market Analyzer & MCP Co-pilot

A daily-persisted Indonesian stock market pipeline (EOD prices, RSS news, IDX disclosure PDFs, broker summary) that surfaces anomalies + disclosures to an AI assistant via MCP. Glossary of the project's ubiquitous language.

## Language

**Ticker**:
An IDX-listed company's canonical 4-letter code (e.g. `BBCA`, `TLKM`); the `.JK` suffix is added only at the Yahoo boundary, never stored.
_Avoid_: symbol, stock code (as stored identifiers)

**Trading Day**:
A date on which the IDX market actually traded (a date with an EOD row in `daily_prices`). Weekends/holidays have no row — data presence is the trading-day signal, no separate calendar.
_Avoid_: business day, calendar day

**Daily Price**:
End-of-day OHLCV for one ticker on one trading day. Primary source: IDX `GetStockSummary`; Yahoo is bootstrap-only history.
_Avoid_: quote, candle, bar

**Anomaly**:
A ticker-day that crossed a threshold — volume >2× the trailing 20-trading-day average, or price move ≥5% vs prior close. Direction (up/down) is recorded.
_Avoid_: alert, signal, event

**Disclosure**:
An IDX material announcement filing (PDF + metadata) for a ticker on a date. `disclosure_id` is the internal Postgres surrogate; the natural key is `pdf_url`. `passed_filter` means it cleared the 3-layer filter (anomaly-gate ∩ keyword whitelist).
_Avoid_: filing, report, announcement (use Disclosure for the row; "announcement" for the IDX source item)

**Extraction Status**:
State of a Disclosure's pre-extracted PDF text: `pending` (extraction in flight — queued, running, or retrying; text not yet available; poll `read_idx_disclosure`), `ok` (text on R2), `failed` (errored / exceeded caps), `evicted` (text R2 object past 90-day retention, metadata kept). A body status on a successful response, not an error. `queued` is deliberately not a concept — it collapses into `pending`.
_Avoid_: parse state, queued

**Broker Summary**:
Per-broker aggregate trading activity across all stocks for a date (V1). No ticker dimension and no buy/sell split in V1; per-symbol bandarmology is a fast-follow.
_Avoid_: broker report, orderbook

**Broker Stock Summary**:
Per-ticker, per-day broker table: top-10 buyers and top-10 sellers by value for one ticker on one day, plus footer totals (`t_val`, `f_nval`) and `others_net`. IPOT lists only the top-10 per side — brokers below the cut appear only inside `others_net`. Persisted for anomaly-passing days and on-demand `get_stock_broker_summary` fetches; this sparse coverage is the norm, never an error.
_Avoid_: bandarmologi, broker table (use for the raw IPOT artifact)

**Others Net**:
The unlisted tail's net in one stock-day's broker table: `Σ listed sellers − Σ listed buyers` = the tail's net in buy−sell convention — positive = the quiet tail net-bought, negative = net-sold. Same sign convention as Net Flow (positive = accumulation). A single aggregate; the tail cannot be attributed to specific brokers. Computed by the pipeline, and recomputed by the history reader from stored rows (always correct for pre-000013 rows).
_Avoid_: tail net value, residual

**Net Flow**:
Per-broker cumulative net over a `(ticker, from, to)` window: `Σ listed-day buy values − Σ listed-day sell values`, positive = net accumulation. Computed from stored top-10 rows; a broker below top-10 on a day contributes only to that day's Others Net and is never inferred from it.
_Avoid_: flow, net value (ambiguous with a single day)

**Coverage**:
How much of a window a Net Flow answer actually observes: `covered_days` (distinct trading days with stored broker rows) vs `trade_days_in_window` (distinct EOD days in `daily_prices`). Reported so partial data reads as partial, never as silence.
_Avoid_: completeness, freshness (owned by Stale)

**Market-Wide Stance**:
`get_broker_net_flow`'s ticker-less mode: per-broker Net Flow summed across all tickers with stored broker rows in the window, each broker row carrying a per-ticker breakdown (`by_ticker`) so the stance is attributable — which stocks a broker accumulated, not just how much. Population is anomaly-gated (skews to busy stocks); the response declares `tickers_covered` so the bias is visible, not hidden.
_Avoid_: market sentiment, foreign flow (out of scope)

**Financial Statement**:
A ticker's normalized income-statement, balance-sheet and ratio line items for one reported period, in raw IDR. Fetched live on request from the IPOT fundamental source — not persisted, no pipeline stage (the persisted pipeline is a deferred fast-follow). IDX periods are cumulative year-to-date (3M, 6M, 9M, 12M columns) — statements are comparable only within the same duration. Analyst-forecast and interim columns ride along tagged (`is_forecast` / `is_interim`), not excluded; line items without a dedicated field land in `extra`.
_Avoid_: fundamentals, financials (as stored entity — nothing is stored)

**News Item**:
A parsed RSS article matched to ≥1 ticker (by code or company name). Unmatched feed items are not stored; raw feed XML is claim-checked for 30 days as the re-parse safety net.
_Avoid_: article, headline (use for the display row; News Item for the stored entity)

**Raw File**:
A claim-check pointer (in `raw_files`) to a binary blob on R2/volume — a disclosure PDF, raw RSS XML, or extracted disclosure text. The `raw_files` row survives object eviction (`deleted_at`) for audit.
_Avoid_: blob, attachment

**Source Status**:
Singleton-per-source state: last success, last attempt, consecutive failures, staleness flag, and the incremental high-water mark. The freshness signal is time-based: stale when `now - last_success_at > max_age`. `consecutive_failures` is tracked for alerting only — it never drives the staleness flag. The stored `stale` column is sampled at write time; read paths (`get_pipeline_status`, per-tool `data_stale`) recompute the time-based rule live.
_Avoid_: health, status (too generic)

**Stale**:
A source whose data is older than its freshness window (`now - last_success_at > max_age`, ~1 trading day). Every MCP tool response carries `data_stale` + `last_good_date` so the consumer never silently reasons over old data. Distinct from a *failed* fetch.
_Avoid_: outdated, expired

**Pipeline**:
The daily asynq job graph: Wave 1 ingestion (stock summary, announcements, broker summary, RSS, cleanup) → detect anomalies → filter disclosures → extract per disclosure. Owned by the asynq Scheduler inside `mcp-server`. The graph is declarative data in `internal/tasks/graph.go`, consumed by the scheduler, enqueue-daily, and self-heal — not emergent from handler chains.
_Avoid_: cron job, batch

## Screening & indicators

**Screen**:
One `screen_stocks` call: the whole persisted universe run through the funnel and returned as a ranked, capped shortlist. The universe is the caller's market, never a ticker list they name — that is what makes stage 1 of the daily flow deterministic and cheap.
_Avoid_: scan, query, search

**Funnel Stage**:
One filter layer of a Screen, each reporting its own survivor count: `universe` (tickers with a row inside the window) → `after_value` (survivors of the SQL hard filters: trading on the anchor day, at or above the value floor, no recent suspension/UMA) → `after_structure` (survivors of the Structural Filters). The counts are mandatory in every response, so a filter set that keeps everything or nothing is visible rather than inferred from a shortlist.
_Avoid_: step, phase, tier

**Structural Filter**:
One `{indicator, op, value}` threshold comparison on a **single** registered indicator's latest value. Filters are AND-combined, and the set is a call parameter that replaces the shipped default set outright. Comparing two indicators is not a filter — it is a derived indicator (`ma_distance` for close-vs-MA, `ma_spread` for fast-MA-vs-slow-MA), which collapses the comparison into one filterable number. Temporal logic (crossovers, streaks, state changes) is not expressible and belongs to series reads of `compute_indicators`.
_Avoid_: rule, condition, criteria, screen

**Threshold Operator**:
The comparison a Structural Filter applies: `lt`, `gt`, `lte`, `gte`, or `between`. `between` takes `[low, high]`, inclusive at both ends.
_Avoid_: comparator, predicate

**Indicator Registry**:
`internal/indicator`'s map of name → formula, default periods, and warm-up bar. The single place an indicator is defined: a Screen's default columns, an ad-hoc `compute_indicators` call, and a Structural Filter all resolve through it, so adding an indicator is one registry entry and the MCP tool definitions never change.
_Avoid_: indicator library, formula set

**Warm-up Bar**:
The number of usable stored rows an indicator needs before it reports a value at all (`required_rows`). Below it the value is null and the key is flagged (`insufficient` on a screen row) — never a short-window number that would read as fully warmed. A missing column narrows the series rather than counting as a zero. Warm-up is a **flag** at the indicator stage and a **drop** at the structural-filter stage: a short-history ticker keeps its row when no filters run, and is removed by the default set when they do.
_Avoid_: lookback period, minimum history

**Flow Column**:
A screen row's `foreign_net` — the stored foreign net (Σ `f_nval`) from the Broker Stock Summary footer totals over `flow_window_days` stored trading days ending on the anchor day. It is joined on every call (never a toggle: the column is flagged per row, not switched per request) and read for the survivors only. The footer grain is what makes it complete — `f_nval` covers brokers below IPOT's top-10 that the listed rows alone cannot attribute. It is **not** a registry indicator and **not** a Structural Filter target: the registry computes from price series, this is a stored broker fact. It is a sort key (`sort: foreign_net`), where an unobserved row ranks last in either direction.
_Avoid_: foreign flow, bandarmology, smart money

**Column Coverage**:
A screen row's per-column declaration of what an optional column actually observes: `{observed, days}`, keyed by column name so a later column lands without a schema change. The response's `flow_days_in_window` is the denominator the day count reads against — "7 of 20". It is what keeps a missing number from reading as a zero: an uncovered row's `foreign_net` is null with `observed: false`, never 0. Broker coverage is sparse by design (the population is anomaly-gated), so unobserved is the normal case, not an error.
_Avoid_: completeness, data quality, null flag

## Measurement

**Measurement Harness**:
The offline grader for the pipeline's claims — a read-only CLI that Replays Screens as of past Trading Days and scores survivors, Anomalies, flow signals, Disclosures, and journal verdicts against a Baseline using Forward Returns. The tool computes, the AI interprets, the user tunes. _Avoid_: backtester, bot, monitor

**Grader**:
One claim-scorer in the Measurement Harness — the screen, Anomaly, flow, Disclosure, or journal Grader. Each Grader reuses the shared engine and stands alone: it can land without the others. _Avoid_: monitor, test, benchmark

**Grade**:
One Grader's run against a named filter set (or two, compared) over one or more Replay Dates, producing a single report. The observations of every Replay Date are pooled into one population, and the report states the distinct-ticker count and any skipped dates, so a Grade never reads as more evidence than it holds. _Avoid_: backtest, score, run

**Replay**:
A Screen executed with a past anchor Trading Day. The Screen is deterministic, so a Replay reproduces exactly what the tool would have said that day; a Replay that needs post-anchor prices (Forward Returns) is bounded to dates with enough stored history after them. _Avoid_: backtest run, simulation

**Replay Date**:
One anchor Trading Day a Replay runs on. A Grade pools the observations of all its Replay Dates; the pooled count is reported beside the number of distinct tickers it rests on, because repeated tickers and overlapping forward windows make it an observation count, not an independent-sample count. _Avoid_: as-of date, sample date

**Forward Return**:
Close-to-close price change over 5, 10, or 20 trading days after an anchor day. The only outcome measure the Harness uses. _Avoid_: future return, PnL

**Baseline**:
The counterfactual a measurement is compared against — edge is always relative. Two kinds: the **Liquidity Baseline** (survivors of the hard-filter stage alone — traded on the anchor day, value floor, suspension/UMA window — before structural filters and ranking) and the **Big-Cap Basket** (the fixed 8-ticker IHSG proxy, a secondary reference line). _Avoid_: market average, benchmark

**Excess**:
Survivor median Forward Return minus Baseline median Forward Return at the same horizon. A positive Excess is the only sense in which a filter set "works." _Avoid_: alpha, outperformance

**Excess Delta**:
At one horizon, the difference between two filter sets' Excess, each measured against its own Liquidity Baseline. It is how a tuning decision is read: positive means the first set's structural filters add more over their own hard-filter floor. When the two sets differ in hard-filter parameters, the comparison is flagged not-like-for-like. _Avoid_: alpha delta, outperformance spread

**Holdout**:
The most recent ~3 months of Trading Days, sealed: filter changes are tuned on older data and must pass exactly one validation on Holdout data before adoption. Prevents tuning that memorizes the past. _Avoid_: validation set, test window

**Decision Journal**:
The append-only log of the AI's tradable verdicts — date, ticker, verdict, entry, TP, SL, rationale — written at decision time so history can grade them later. Reasoning is subjective; the logged verdict is falsifiable. _Avoid_: trade log, diary

## Fetch transport

**Fetch Mode**:
The IDX client's transport: `nodriver` — headless Chrome sidecar + rotating proxy pool, the only fetch route (ADR-0007). Every fetch — stock summary, announcements, and disclosure PDF streams (`GetStream`) — goes through the sidecar; direct GETs on the StaticData host 403 on the Cloudflare JS-execution gate (issue 01, resolved). Fetches are coarse (one request per source per day) and uncached/unthrottled by design. Proxy pool source: `nodriver.proxies`.
_Avoid_: transport mode, proxy mode

**Proxy Pool**:
The configured list of egress proxies the nodriver sidecar rotates through. Rotation is sticky-until-dead: the proxy behind the last successful fetch is reused for later fetches — the Cloudflare clearance cookie is IP-bound, so the egress IP must be held for the sidecar's warm Chrome to skip re-solving the challenge — until it dies (ban), flunks out transiently, or reaches `nodriver.max_sticky_successes` consecutive successes (deliberate rotation, default 10). Scope is the client instance, not the resource: one `Client` per process serves every source (announcements, suspensions, corporate actions, index summary, screener, profiles, PDFs), so they share one sticky proxy — and the success cap, which counts across all of them.
_Avoid_: proxy list (use for the raw artifact)

**Dead Proxy**:
A proxy marked unusable after a failed fetch (challenge 403, FlareSolverr error, or timeout); skipped until `dead_retry_after` elapses. Distinct from a *burned* proxy, which is the specific case where idx's Cloudflare rejects the proxy's IP.
_Avoid_: bad proxy, burned proxy (burned is the specific Cloudflare-IP case)

**FlareSolverr Session**:
A proxy-bound browser context inside FlareSolverr that retains the solved Cloudflare cookies. Reused for many requests through the same proxy (one challenge solve per proxy); never portable across proxies or outside FlareSolverr.
_Avoid_: cookie, session cookie

## Relationships

- **Ticker → Daily Price / Anomaly / Disclosure / News Item**: a ticker owns its price history, anomalies, disclosures, and tagged news (FK by `code`).
- **Anomaly → Disclosure**: derived at read time (no stored link) — a disclosure belongs to an anomaly's `(ticker, trading_day)` if `passed_filter=true`.
- **Disclosure → Raw File**: a disclosure's extracted text is a `raw_files` row pointing to an R2 object; the PDF itself is another `raw_files` row.
- **News Item → Ticker**: many-to-many via `news_tickers` (one article can match multiple tickers).
- **Source Status → Pipeline**: each Wave-1 source has one `source_status` row; `get_pipeline_status` is its MCP-readable face.
- **Structural Filter → Indicator Registry**: a filter names a registry spec, and the filter's key and the row column's key are the same canonical string (`ma_distance:20:50`) — which is why a filter's own number appears in the row it judged, and why the filter's indicator is computed even when it was not requested as a column.
- **Flow Column → Broker Stock Summary**: a screen row's `foreign_net` is Σ `f_nval` over the flow window, read from the same stored footer totals `get_sector_flow` aggregates — one source, so a ticker's flow reads the same in a sector breakdown and in a shortlist row.
- **Column Coverage → Flow Column**: every row flags whether its optional column was observed and over how many days, against the response's `flow_days_in_window` denominator (the same covered-vs-spanning shape Coverage names for a Net Flow window).