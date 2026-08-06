CREATE TABLE alerts (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    message TEXT NOT NULL,
    raised_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX idx_alerts_raised_at ON alerts (raised_at);
