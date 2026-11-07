-- Ticket 10c: per-feature daily fetch budgets.
--
-- The broker quota is monthly per key, but the pipeline also caps how many
-- units a feature may spend on a single trading day. The screener has a shared
-- 10-unit pool across every call; the anomaly refresher has 30. Both are
-- policies, not vendor truth, so they live in Postgres and survive restarts.
CREATE TABLE fetch_usage (
    usage_day  DATE        NOT NULL,
    feature    TEXT        NOT NULL,
    units      INTEGER     NOT NULL DEFAULT 0 CHECK (units >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (usage_day, feature)
);
