-- Issue 03: persist the unlisted-tail net (top-10 table bias) for a ticker+day.
-- Null on rows written before this migration; history recomputes from stored rows,
-- and a refetch regenerates the value.
ALTER TABLE broker_stock_summary_totals ADD COLUMN others_net BIGINT;
