DROP TABLE IF EXISTS ticker_indices;

ALTER TABLE tickers
    DROP COLUMN IF EXISTS sub_sektor,
    DROP COLUMN IF EXISTS sub_industry,
    DROP COLUMN IF EXISTS sub_industry_code;
