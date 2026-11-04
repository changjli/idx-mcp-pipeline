-- Reverse the window-grain extension of 000020. Window rows and the f lane
-- cannot be represented in the old schema, so they are deleted rather than
-- silently collapsed onto the day/all grain. archive/window provenance is
-- downgraded to sweep so the old source check accepts it.
DELETE FROM broker_stock_summaries WHERE investor <> 'all' OR win_from <> trading_day;
DELETE FROM broker_stock_summary_totals WHERE investor <> 'all' OR win_from <> trading_day;

UPDATE broker_stock_summaries SET source = 'sweep' WHERE source IN ('archive', 'window');
UPDATE broker_stock_summary_totals SET source = 'sweep' WHERE source IN ('archive', 'window');

ALTER TABLE broker_stock_summaries DROP CONSTRAINT broker_stock_summaries_pkey;
ALTER TABLE broker_stock_summaries
    ADD PRIMARY KEY (ticker, trading_day, broker_code, side);
ALTER TABLE broker_stock_summaries DROP CONSTRAINT broker_stock_summaries_source_check;
ALTER TABLE broker_stock_summaries
    ADD CONSTRAINT broker_stock_summaries_source_check
    CHECK (source IN ('legacy_ipot', 'live', 'sweep', 'backfill'));
ALTER TABLE broker_stock_summaries DROP COLUMN win_from;
ALTER TABLE broker_stock_summaries DROP COLUMN investor;
ALTER TABLE broker_stock_summaries DROP COLUMN market;

ALTER TABLE broker_stock_summary_totals DROP CONSTRAINT broker_stock_summary_totals_pkey;
ALTER TABLE broker_stock_summary_totals
    ADD PRIMARY KEY (ticker, trading_day);
ALTER TABLE broker_stock_summary_totals DROP CONSTRAINT broker_stock_summary_totals_source_check;
ALTER TABLE broker_stock_summary_totals
    ADD CONSTRAINT broker_stock_summary_totals_source_check
    CHECK (source IN ('legacy_ipot', 'live', 'sweep', 'backfill'));
ALTER TABLE broker_stock_summary_totals DROP COLUMN win_from;
ALTER TABLE broker_stock_summary_totals DROP COLUMN investor;
ALTER TABLE broker_stock_summary_totals DROP COLUMN market;
