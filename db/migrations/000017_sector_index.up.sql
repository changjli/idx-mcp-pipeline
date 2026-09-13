-- Sector/industry classification + index membership (issue 15b), sourced from
-- the IDX stock-screener endpoint (stock-screener/get, one call, ~960 rows).
--
-- tickers gains the screener's 4-level sector taxonomy. sektor/industri already
-- exist (Indonesian labels from GetCompanyProfiles); 15b owns them now and
-- overwrites with the screener's canonical English labels. sub_sektor /
-- sub_industry / sub_industry_code are the new deeper levels; sub_industry_code
-- is the canonical IDX code (e.g. A121, D232). All nullable — the screener
-- emits null industry/subIndustry/subIndustryCode for a few dirty rows, and
-- KETR's "No Sector" maps to NULL at ingest.
ALTER TABLE tickers
    ADD COLUMN sub_sektor TEXT,
    ADD COLUMN sub_industry TEXT,
    ADD COLUMN sub_industry_code TEXT;

-- Index membership is many-to-many and point-in-time: one row per
-- (ticker, index, effective_date) snapshot. Each seeder run writes a fresh
-- snapshot dated with the run date, so historical screens never read today's
-- constituents. The PK (ticker + index + effective_date) makes a same-day
-- re-run idempotent; ReplaceMembership deletes the day's rows first so a
-- ticker that dropped out of an index is removed, not orphaned.
CREATE TABLE ticker_indices (
    ticker_code    TEXT NOT NULL,      -- KodeEmiten, unpadded
    index_code     TEXT NOT NULL,      -- e.g. LQ45, COMPOSITE, IDX30
    effective_date DATE NOT NULL,      -- snapshot date (the seeder run date)
    stored_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker_code, index_code, effective_date)
);

-- Point-in-time reads: "who was in LQ45 on date X" scans by effective_date.
CREATE INDEX idx_ticker_indices_effective_date ON ticker_indices (effective_date);
-- Membership reads: "which indices is BBRI in" scans by ticker.
CREATE INDEX idx_ticker_indices_ticker_code ON ticker_indices (ticker_code);
