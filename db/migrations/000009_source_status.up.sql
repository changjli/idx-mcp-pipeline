CREATE TABLE source_status (
    source TEXT PRIMARY KEY,
    last_success_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    last_error TEXT,
    consecutive_failures INT NOT NULL DEFAULT 0,
    stale BOOLEAN NOT NULL DEFAULT true,
    max_age_seconds INT NOT NULL DEFAULT 86400,
    high_water_mark TIMESTAMPTZ
);
