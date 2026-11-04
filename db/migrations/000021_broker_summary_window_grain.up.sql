-- Broker Summary v2 (ticket 10a): the storage grain becomes the window.
--
-- A row set is [win_from, trading_day] for one investor lane and market. A
-- single day is the degenerate window win_from == trading_day, which is how
-- every legacy row is backfilled. The primary keys gain win_from and investor
-- so an all-investor legacy day and a new foreign-only day (or window) can
-- coexist for the same ticker and window end.
--
-- The source enum gains the two window-grain writers: archive (monthly chunk)
-- and window (cached answer). Existing writers keep live/sweep/backfill.

-- ── rows ─────────────────────────────────────────────────────────────────────
ALTER TABLE broker_stock_summaries ADD COLUMN win_from DATE;
ALTER TABLE broker_stock_summaries ADD COLUMN investor TEXT;
ALTER TABLE broker_stock_summaries ADD COLUMN market TEXT;

UPDATE broker_stock_summaries
SET win_from = trading_day,
    investor = 'all',
    market = 'RG';

ALTER TABLE broker_stock_summaries ALTER COLUMN win_from SET NOT NULL;
ALTER TABLE broker_stock_summaries ALTER COLUMN investor SET NOT NULL;
ALTER TABLE broker_stock_summaries ALTER COLUMN market SET NOT NULL;

ALTER TABLE broker_stock_summaries
    ADD CONSTRAINT broker_stock_summaries_window_check CHECK (win_from <= trading_day);
ALTER TABLE broker_stock_summaries
    ADD CONSTRAINT broker_stock_summaries_investor_check CHECK (investor IN ('all', 'f'));
ALTER TABLE broker_stock_summaries
    ADD CONSTRAINT broker_stock_summaries_market_check CHECK (market = 'RG');

ALTER TABLE broker_stock_summaries DROP CONSTRAINT broker_stock_summaries_pkey;
ALTER TABLE broker_stock_summaries
    ADD PRIMARY KEY (ticker, trading_day, win_from, investor, broker_code, side);

ALTER TABLE broker_stock_summaries DROP CONSTRAINT broker_stock_summaries_source_check;
ALTER TABLE broker_stock_summaries
    ADD CONSTRAINT broker_stock_summaries_source_check
    CHECK (source IN ('legacy_ipot', 'live', 'sweep', 'backfill', 'archive', 'window'));

-- ── totals ───────────────────────────────────────────────────────────────────
ALTER TABLE broker_stock_summary_totals ADD COLUMN win_from DATE;
ALTER TABLE broker_stock_summary_totals ADD COLUMN investor TEXT;
ALTER TABLE broker_stock_summary_totals ADD COLUMN market TEXT;

UPDATE broker_stock_summary_totals
SET win_from = trading_day,
    investor = 'all',
    market = 'RG';

ALTER TABLE broker_stock_summary_totals ALTER COLUMN win_from SET NOT NULL;
ALTER TABLE broker_stock_summary_totals ALTER COLUMN investor SET NOT NULL;
ALTER TABLE broker_stock_summary_totals ALTER COLUMN market SET NOT NULL;

ALTER TABLE broker_stock_summary_totals
    ADD CONSTRAINT broker_stock_summary_totals_window_check CHECK (win_from <= trading_day);
ALTER TABLE broker_stock_summary_totals
    ADD CONSTRAINT broker_stock_summary_totals_investor_check CHECK (investor IN ('all', 'f'));
ALTER TABLE broker_stock_summary_totals
    ADD CONSTRAINT broker_stock_summary_totals_market_check CHECK (market = 'RG');

ALTER TABLE broker_stock_summary_totals DROP CONSTRAINT broker_stock_summary_totals_pkey;
ALTER TABLE broker_stock_summary_totals
    ADD PRIMARY KEY (ticker, trading_day, win_from, investor);

ALTER TABLE broker_stock_summary_totals DROP CONSTRAINT broker_stock_summary_totals_source_check;
ALTER TABLE broker_stock_summary_totals
    ADD CONSTRAINT broker_stock_summary_totals_source_check
    CHECK (source IN ('legacy_ipot', 'live', 'sweep', 'backfill', 'archive', 'window'));
