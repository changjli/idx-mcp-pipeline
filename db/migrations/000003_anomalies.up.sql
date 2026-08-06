CREATE TABLE anomalies (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL REFERENCES tickers(code),
    trading_day DATE NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('volume', 'price')),
    direction TEXT NOT NULL CHECK (direction IN ('up', 'down')),
    magnitude_pct NUMERIC(8,2),
    baseline_ref NUMERIC(18,4),
    observed_value NUMERIC(18,4),
    prior_value NUMERIC(18,4),
    UNIQUE (ticker, trading_day, type)
);

CREATE INDEX idx_anomalies_trading_day ON anomalies (trading_day);
