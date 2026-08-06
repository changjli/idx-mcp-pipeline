CREATE TABLE daily_prices (
    ticker TEXT NOT NULL REFERENCES tickers(code),
    trading_day DATE NOT NULL,
    open NUMERIC(18,2),
    high NUMERIC(18,2),
    low NUMERIC(18,2),
    close NUMERIC(18,2),
    volume BIGINT,
    value BIGINT,
    frequency INT,
    source TEXT NOT NULL CHECK (source IN ('idx', 'yahoo_bootstrap')),
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, trading_day)
);

CREATE INDEX idx_daily_prices_ticker_day_desc ON daily_prices (ticker, trading_day DESC);
