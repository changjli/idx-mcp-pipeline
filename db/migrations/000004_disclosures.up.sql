CREATE TABLE disclosures (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT REFERENCES tickers(code),
    announcement_date DATE NOT NULL,
    title TEXT NOT NULL,
    pdf_url TEXT NOT NULL UNIQUE,
    attachment_idx INT NOT NULL DEFAULT 0,
    categories TEXT[],
    passed_filter BOOLEAN NOT NULL DEFAULT false,
    extraction_status TEXT NOT NULL DEFAULT 'pending' CHECK (extraction_status IN ('pending', 'ok', 'failed', 'evicted')),
    text_r2_key TEXT,
    extraction_error TEXT,
    extracted_at TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_disclosures_ticker_date ON disclosures (ticker, announcement_date);
CREATE INDEX idx_disclosures_filter_date ON disclosures (passed_filter, announcement_date);
