CREATE TABLE raw_files (
    id BIGSERIAL PRIMARY KEY,
    storage_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('pdf', 'rss_xml', 'disclosure_text')),
    source_ref TEXT,
    size_bytes BIGINT,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    retention_days INT NOT NULL DEFAULT 30
);

CREATE INDEX idx_raw_files_kind_stored ON raw_files (kind, stored_at);
