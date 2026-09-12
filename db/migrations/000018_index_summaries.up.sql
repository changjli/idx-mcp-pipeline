-- Index/sector summary (issue 18), sourced from the IDX GetIndexSummary
-- endpoint (one GET, length=9999: all 45 indices — 34 main + 11 sector).
-- One row per index per trading day. The index's date is the wire Date (the
-- trading day the summary describes), not the task run date — a holiday fetch
-- that returns Friday's rows keys them as Friday, and a same-day re-run is
-- idempotent.
--
-- OHLC-ish index points carry 3 wire decimals (Close 6678.201) — wider than
-- daily_prices' NUMERIC(18,2), so the source is stored without truncation.
-- Volume/Value/Frequency/MarketCap are large integral IDX aggregates
-- (MarketCapital reaches ~1.2e16 IDR) — kept as BIGINT like daily_prices'
-- value/volume, with the wire float truncated at ingest.
CREATE TABLE index_summaries (
    index_code      TEXT NOT NULL,      -- e.g. COMPOSITE, LQ45, IDXENERGY
    date            DATE NOT NULL,      -- the trading day the summary describes
    previous        NUMERIC(18,4),     -- prior-day close (index points)
    high            NUMERIC(18,4),
    low             NUMERIC(18,4),
    close           NUMERIC(18,4),
    change          NUMERIC(18,4),      -- close - previous (index points)
    number_of_stock INT,                -- constituent count; doubles as membership-count validation vs ticker_indices
    volume          BIGINT,             -- shares traded across the index
    value           BIGINT,             -- rupiah value traded
    frequency       BIGINT,             -- trade count
    market_cap      BIGINT,             -- aggregate market capitalization (IDR)
    stored_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (index_code, date)
);

-- Stage-0 sector-rotation reads: per-index time series by day.
CREATE INDEX idx_index_summaries_date ON index_summaries (date);
-- Market-regime reads: all indices for one day.
CREATE INDEX idx_index_summaries_index_code ON index_summaries (index_code);
