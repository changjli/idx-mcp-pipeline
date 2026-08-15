-- Per-stock broker summary (IPOT source): one row per broker per side per day.
-- Grain: (ticker, trading_day, broker_code, side). Upsert is idempotent on
-- this PK — refetching the same day overwrites, never duplicates.
CREATE TABLE broker_stock_summaries (
    ticker        TEXT NOT NULL,
    broker_code   TEXT NOT NULL,   -- 2-letter IPOT broker code
    side          TEXT NOT NULL,   -- 'buy' | 'sell'
    trading_day   DATE NOT NULL,
    lot           BIGINT,          -- B.Lot / S.Lot
    value         BIGINT,          -- B.Val / S.Val in rupiah
    avg_price     INT,             -- B.Avg / S.Avg
    rank          INT,             -- 1..10 within side, by value
    PRIMARY KEY (ticker, trading_day, broker_code, side)
);

CREATE INDEX idx_broker_stock_summaries_day ON broker_stock_summaries (trading_day);

-- Footer summary line per ticker+day, kept separate so the top-N table stays
-- one-row-per-broker-per-side.
CREATE TABLE broker_stock_summary_totals (
    ticker        TEXT NOT NULL,
    trading_day   DATE NOT NULL,
    t_val         BIGINT,          -- total value (rupiah)
    f_nval        BIGINT,          -- foreign net value (rupiah)
    t_lot         BIGINT,          -- total lots
    avg           INT,             -- average price
    PRIMARY KEY (ticker, trading_day)
);
