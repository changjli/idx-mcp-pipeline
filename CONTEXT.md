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

**News Item**:
A parsed RSS article matched to ≥1 ticker (by code or company name). Unmatched feed items are not stored; raw feed XML is claim-checked for 30 days as the re-parse safety net.
_Avoid_: article, headline (use for the display row; News Item for the stored entity)

**Raw File**:
A claim-check pointer (in `raw_files`) to a binary blob on R2/volume — a disclosure PDF, raw RSS XML, or extracted disclosure text. The `raw_files` row survives object eviction (`deleted_at`) for audit.
_Avoid_: blob, attachment

**Source Status**:
Singleton-per-source state: last success, last attempt, consecutive failures, staleness flag, and the incremental high-water mark. The freshness signal is time-based: stale when `now - last_success_at > max_age`.
_Avoid_: health, status (too generic)

**Stale**:
A source whose data is older than its freshness window (`now - last_success_at > max_age`, ~1 trading day). Every MCP tool response carries `data_stale` + `last_good_date` so the consumer never silently reasons over old data. Distinct from a *failed* fetch.
_Avoid_: outdated, expired

**Pipeline**:
The daily asynq job graph: Wave 1 ingestion (stock summary, announcements, broker summary, RSS, cleanup) → detect anomalies → filter disclosures → extract per disclosure. Owned by the asynq Scheduler inside `mcp-server`.
_Avoid_: cron job, batch

## Relationships

- **Ticker → Daily Price / Anomaly / Disclosure / News Item**: a ticker owns its price history, anomalies, disclosures, and tagged news (FK by `code`).
- **Anomaly → Disclosure**: derived at read time (no stored link) — a disclosure belongs to an anomaly's `(ticker, trading_day)` if `passed_filter=true`.
- **Disclosure → Raw File**: a disclosure's extracted text is a `raw_files` row pointing to an R2 object; the PDF itself is another `raw_files` row.
- **News Item → Ticker**: many-to-many via `news_tickers` (one article can match multiple tickers).
- **Source Status → Pipeline**: each Wave-1 source has one `source_status` row; `get_pipeline_status` is its MCP-readable face.