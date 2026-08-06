CREATE TABLE tickers (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    listing_date DATE,
    shares BIGINT,
    listing_board TEXT,
    sektor TEXT,
    industri TEXT,
    active BOOLEAN NOT NULL DEFAULT true,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
