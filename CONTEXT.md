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
State of a Disclosure's pre-extracted PDF text: `pending` (not yet processed), `ok` (text on R2), `failed` (errored / exceeded caps), `evicted` (text R2 object past 90-day retention, metadata kept). A body status on a successful response, not an error.
_Avoid_: parse state

**Broker Summary**:
Per-broker aggregate trading activity across all stocks for a date (V1). No ticker dimension and no buy/sell split in V1; per-symbol bandarmology is a fast-follow.
_Avoid_: broker report, orderbook

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

## Fetch transport

**Fetch Mode**:
The IDX client's transport: `nodriver` — headless Chrome sidecar + rotating proxy pool, the only fetch route (ADR-0007). Every fetch — stock summary, announcements, and disclosure PDF streams (`GetStream`) — goes through the sidecar; direct GETs on the StaticData host 403 on the Cloudflare JS-execution gate (issue 01, resolved). Fetches are coarse (one request per source per day) and uncached/unthrottled by design. Proxy pool source: `nodriver.proxies`.
_Avoid_: transport mode, proxy mode

**Proxy Pool**:
The configured list of egress proxies FlareSolverr rotates through. A proxy is tried until one works, then stuck with for the run; failures mark it dead and advance the rotation.
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