-- On-demand Tier-2 depth trigger (financials-v2 issue 07).
--
-- get_financials is a pure DB read, but when a ticker's depth is still missing
-- it enqueues ONE async idx:financials_depth task and returns depth_status
-- pending; the client polls and the worker does the Yahoo fetch. This table is
-- the per-ticker cooldown clock: at most one on-demand request per cooldown
-- window, so a client polling the tool cannot hammer Yahoo through repeated
-- enqueues.
--
-- It is deliberately separate from ticker_financial_taxonomy: that row cannot
-- exist before the worker has classified the ticker (taxonomy is NOT NULL), and
-- a request is a read-path event, not a classification.
CREATE TABLE financial_depth_requests (
    ticker       TEXT PRIMARY KEY,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ops visibility: the most recently requested tickers.
CREATE INDEX idx_financial_depth_requests_requested_at
    ON financial_depth_requests (requested_at DESC);
