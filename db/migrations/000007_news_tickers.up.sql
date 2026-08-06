CREATE TABLE news_tickers (
    news_id BIGINT NOT NULL REFERENCES news_items(id),
    ticker TEXT NOT NULL REFERENCES tickers(code),
    match_method TEXT NOT NULL CHECK (match_method IN ('code', 'name')),
    PRIMARY KEY (news_id, ticker)
);

CREATE INDEX idx_news_tickers_ticker ON news_tickers (ticker, news_id);
