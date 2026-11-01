-- Reverse the structural parts of 000020. The legacy f_nval values cannot be
-- restored (the migration intentionally discarded them as unreliable), and any
-- others_net recreated here starts NULL rather than being recomputed.
ALTER TABLE broker_stock_summaries DROP COLUMN source;
ALTER TABLE broker_stock_summary_totals DROP COLUMN source;
ALTER TABLE broker_stock_summary_totals ADD COLUMN others_net BIGINT;
