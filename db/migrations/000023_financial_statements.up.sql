-- Financial statements Tier 1 (financials-v2 issue 01): the canonical
-- statement store, its point-in-time version trail, and the monthly snapshot
-- run ledger.
--
-- The key is the STATEMENT PERIOD (ticker, period_end), not the snapshot month.
-- The monthly sweep returns each ticker's latest known statement, so the same
-- (ticker, 2026-06-30) row repeats every month until the next filing; a
-- primary key on the period collapses that repetition to one row. At most four
-- rows per ticker per fiscal year fall out of the key plus the duration CHECK —
-- deliberately no quarter counter, which breaks on fiscal-year-end changes.
--
-- No FK to tickers: IDX emits issuers outside the universe (delisted / lag),
-- and a hard FK would fail the persist. Same convention as corporate_actions
-- and shareholder_composition.
CREATE TABLE financial_statements (
    ticker             TEXT     NOT NULL,
    period_end         DATE     NOT NULL,

    -- fiscal position of this period, derived at ingestion
    fiscal_year_end    DATE     NOT NULL,              -- e.g. 2026-12-31
    duration_months    SMALLINT NOT NULL,              -- 3 | 6 | 9 | 12

    -- absolute values, RUPIAH (IDX snapshot is billions; stored x 1e9)
    assets             NUMERIC(24,0),
    liabilities        NUMERIC(24,0),
    equity             NUMERIC(24,0),
    revenue            NUMERIC(24,0),                  -- snapshot sales, cumulative YTD
    ebt                NUMERIC(24,0),                  -- cumulative YTD
    net_profit         NUMERIC(24,0),                  -- snapshot profitPeriod (incl. NCI)
    net_profit_parent  NUMERIC(24,0),                  -- snapshot profitAttrOwner

    -- per-share + TTM ratios exactly as IDX reports them
    eps                NUMERIC(18,4),
    book_value         NUMERIC(18,4),
    roa                NUMERIC(12,4),
    roe                NUMERIC(12,4),
    npm                NUMERIC(12,4),                  -- TTM profit / YTD sales, IDX's own formula
    der                NUMERIC(12,4),                  -- liabilities/equity, NOT total-debt/equity

    -- assurance / provenance
    audit_status       TEXT,                           -- U | A | LR | NULL
    opinion            TEXT,                           -- normalised from IDX's dirty enum
    source             TEXT NOT NULL DEFAULT 'idx_ratio_snapshot',
    value_basis        TEXT NOT NULL DEFAULT 'IDR (IDX-converted at an undisclosed rate)',
    source_fs_date     DATE NOT NULL,                  -- raw wire value, for debugging

    -- idempotency / revision bookkeeping
    first_seen_month   DATE NOT NULL,                  -- first day of the snapshot month
    last_seen_month    DATE NOT NULL,
    revision_count     INT  NOT NULL DEFAULT 0,
    content_hash       TEXT NOT NULL,
    stored_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (ticker, period_end),
    CHECK (EXTRACT(DAY FROM (period_end + INTERVAL '1 day')) = 1),   -- month end
    CHECK (duration_months IN (3, 6, 9, 12)),
    CHECK (EXTRACT(DAY FROM first_seen_month) = 1),
    CHECK (EXTRACT(DAY FROM last_seen_month) = 1)
);

CREATE INDEX idx_financial_statements_latest
    ON financial_statements (ticker, period_end DESC);
CREATE INDEX idx_financial_statements_period
    ON financial_statements (period_end);

-- Append-only point-in-time + restatement trail. One row on first sight and on
-- every value change, not every monthly sweep: the statement repeated in a
-- later snapshot is a no-op and writes nothing here.
CREATE TABLE financial_statement_versions (
    ticker           TEXT NOT NULL,
    period_end       DATE NOT NULL,
    seen_month       DATE NOT NULL,        -- snapshot month that carried this version
    -- same value columns as financial_statements (assets ... opinion)
    fiscal_year_end  DATE NOT NULL,
    duration_months  SMALLINT NOT NULL,
    assets           NUMERIC(24,0),
    liabilities      NUMERIC(24,0),
    equity           NUMERIC(24,0),
    revenue          NUMERIC(24,0),
    ebt              NUMERIC(24,0),
    net_profit       NUMERIC(24,0),
    net_profit_parent NUMERIC(24,0),
    eps              NUMERIC(18,4),
    book_value       NUMERIC(18,4),
    roa              NUMERIC(12,4),
    roe              NUMERIC(12,4),
    npm              NUMERIC(12,4),
    der              NUMERIC(12,4),
    audit_status     TEXT,
    opinion          TEXT,
    content_hash     TEXT NOT NULL,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, period_end, seen_month),
    CHECK (EXTRACT(DAY FROM (period_end + INTERVAL '1 day')) = 1),
    CHECK (EXTRACT(DAY FROM seen_month) = 1),
    CHECK (duration_months IN (3, 6, 9, 12))
);

-- Ingestion ledger. Makes a run resumable and idempotent: a month recorded
-- 'ok' is skipped on re-run (unless forced); 'suspect' (2025-12) and 'empty'
-- (2020-12) are recorded instead of silently retried forever.
CREATE TABLE financial_snapshot_runs (
    snapshot_month    DATE PRIMARY KEY,    -- first day of the month
    source            TEXT NOT NULL DEFAULT 'idx_ratio_snapshot',
    status            TEXT NOT NULL CHECK (status IN ('ok','suspect','empty','failed')),
    row_count         INT  NOT NULL DEFAULT 0,
    distinct_periods  INT  NOT NULL DEFAULT 0,
    quarter_end_share NUMERIC(5,4),
    content_hash      TEXT,
    error             TEXT,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (EXTRACT(DAY FROM snapshot_month) = 1),
    CHECK (quarter_end_share IS NULL OR (quarter_end_share >= 0 AND quarter_end_share <= 1))
);
