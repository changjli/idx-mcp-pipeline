-- Corporate-actions calendar (IDX GetIssuedHistory): one row per corporate
-- action event. id is the stable IDX surrogate (the DataTables row's id).
-- ticker is KodeEmiten, unpadded, always present. event_date is
-- TanggalPencatatan. type is the free-form Indonesian label (Waran, Stock
-- Split, Obligasi Wajib Konversi, ...) — no CHECK, keeps pace with IDX.
-- jumlah_saham / jumlah_saham_setelah_tindakan are the per-event amounts
-- (either may be 0, e.g. Obligasi Wajib Konversi). No FK to tickers:
-- GetIssuedHistory emits issuers that may not be in the tickers table
-- (delisted / universe lag), and a hard FK would fail the persist.
CREATE TABLE corporate_actions (
    id                         BIGINT PRIMARY KEY,  -- IDX surrogate id
    ticker                     TEXT NOT NULL,      -- KodeEmiten, unpadded
    event_date                 DATE NOT NULL,      -- TanggalPencatatan
    type                       TEXT NOT NULL,      -- JenisTindakan
    jumlah_saham               BIGINT NOT NULL DEFAULT 0,
    jumlah_saham_setelah_tindakan BIGINT NOT NULL DEFAULT 0,
    stored_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_corporate_actions_event_date ON corporate_actions (event_date);
CREATE INDEX idx_corporate_actions_ticker_event_date ON corporate_actions (ticker, event_date);
