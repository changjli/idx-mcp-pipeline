-- Shareholder composition from KSEI's monthly "Kepemilikan Efek
-- (Lokal-Asing)" balance-position archive (issue 08). One row per ticker per
-- month-end position date. The 18 per-type columns keep KSEI's own investor
-- codes verbatim: IS=Insurance, CP=Corporate, PF=Pension Fund,
-- IB=Financial Institution, ID=Individu, MF=Mutual Fund, SC=Securities
-- Company, FD=Foundation, OT=Others (per KSEI's official investor-type
-- guide). All counts are scripless (SID-held) positions — large registered
-- (warkat) blocks are absent, which is why local totals can understate vs an
-- LBE report; foreign holdings are scripless in practice and match it.
-- sec_num is the file's "Sec. Num" (registered securities / listed shares);
-- price is the file's price column (unverified semantics — stored verbatim).
-- No FK to tickers: KSEI emits securities that may not be in the tickers
-- table (delisted / universe lag), and a hard FK would fail the persist.
CREATE TABLE shareholder_composition (
    ticker         TEXT NOT NULL,
    position_date  DATE NOT NULL,
    sec_num        BIGINT NOT NULL DEFAULT 0,
    price          BIGINT NOT NULL DEFAULT 0,
    local_is       BIGINT NOT NULL DEFAULT 0,
    local_cp       BIGINT NOT NULL DEFAULT 0,
    local_pf       BIGINT NOT NULL DEFAULT 0,
    local_ib       BIGINT NOT NULL DEFAULT 0,
    local_id       BIGINT NOT NULL DEFAULT 0,
    local_mf       BIGINT NOT NULL DEFAULT 0,
    local_sc       BIGINT NOT NULL DEFAULT 0,
    local_fd       BIGINT NOT NULL DEFAULT 0,
    local_ot       BIGINT NOT NULL DEFAULT 0,
    local_total    BIGINT NOT NULL DEFAULT 0,
    foreign_is     BIGINT NOT NULL DEFAULT 0,
    foreign_cp     BIGINT NOT NULL DEFAULT 0,
    foreign_pf     BIGINT NOT NULL DEFAULT 0,
    foreign_ib     BIGINT NOT NULL DEFAULT 0,
    foreign_id     BIGINT NOT NULL DEFAULT 0,
    foreign_mf     BIGINT NOT NULL DEFAULT 0,
    foreign_sc     BIGINT NOT NULL DEFAULT 0,
    foreign_fd     BIGINT NOT NULL DEFAULT 0,
    foreign_ot     BIGINT NOT NULL DEFAULT 0,
    foreign_total  BIGINT NOT NULL DEFAULT 0,
    total          BIGINT NOT NULL DEFAULT 0,  -- local_total + foreign_total
    stored_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, position_date)
);

CREATE INDEX idx_shareholder_composition_position_date ON shareholder_composition (position_date);