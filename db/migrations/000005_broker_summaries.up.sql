CREATE TABLE broker_summaries (
    broker_code TEXT NOT NULL,
    trading_day DATE NOT NULL,
    firm_name TEXT,
    volume BIGINT,
    value BIGINT,
    frequency INT,
    PRIMARY KEY (broker_code, trading_day)
);

CREATE INDEX idx_broker_summaries_day ON broker_summaries (trading_day);
