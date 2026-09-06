-- UMA/suspension list (IDX GetSuspension + GetUma): one row per per-ticker
-- announcement event. id is a local BIGSERIAL surrogate — suspension rows have
-- no stable IDX id (UMA rows have UMAID, but the composite key below covers
-- both sources uniformly). ticker is Kode (GetSuspension) / CompanyID (GetUma),
-- unpadded, always present. event_date is Date / UMADate. type is the raw IDX
-- discriminator: SPT (suspend), UPT (unsuspend), or UMA (unusual market
-- activity warning) — no CHECK, keeps pace with IDX. reason is the Judul
-- (announcement title). No FK to tickers: the endpoints emit issuers that may
-- not be in the tickers table (universe lag), and a hard FK would fail the
-- persist. The ">1 Kode" aggregate rows are skipped client-side (their detail
-- lives only in the PDF, which is not extracted — the per-ticker rows carry
-- the actionable data).
CREATE TABLE suspensions (
    id         BIGSERIAL PRIMARY KEY,
    ticker     TEXT NOT NULL,               -- Kode / CompanyID, unpadded
    event_date DATE NOT NULL,               -- Date / UMADate
    type       TEXT NOT NULL,               -- SPT (suspend), UPT (unsuspend), UMA
    reason     TEXT NOT NULL,               -- Judul (announcement title)
    stored_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ticker, event_date, type, reason)  -- idempotent upsert key
);

CREATE INDEX idx_suspensions_event_date ON suspensions (event_date);
CREATE INDEX idx_suspensions_ticker_event_date ON suspensions (ticker, event_date);
