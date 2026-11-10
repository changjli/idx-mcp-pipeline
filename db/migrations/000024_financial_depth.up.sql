-- Financial statements Tier 2 (financials-v2 issue 05): monthly depth
-- enrichment from Yahoo/Morningstar into the same (ticker, period_end) rows
-- created by 000023.
--
-- The key does not change: every column here is nullable and is filled by an
-- UPDATE on the existing Tier-1 row. A depth failure leaves the Tier-1 values
-- untouched and only marks the depth columns unobserved.
--
-- Money is stored in IDR for the existing read contract. The issuer currency,
-- the rate and the rate date used for the conversion are recorded next to the
-- values, and the original reporting-currency values are kept in depth_raw for
-- audit. The main columns are cumulative YTD (3M/6M/9M rows) or 12M (annual);
-- the *_q columns keep the discrete quarter and *_ttm the trailing-twelve-month
-- view, so no extra duration_months row is ever created.
ALTER TABLE financial_statements
    ADD COLUMN cash              NUMERIC(24,4),
    ADD COLUMN gross_profit      NUMERIC(24,4),
    ADD COLUMN operating_profit  NUMERIC(24,4),
    ADD COLUMN ebitda            NUMERIC(24,4),
    ADD COLUMN cfo               NUMERIC(24,4),
    ADD COLUMN capex             NUMERIC(24,4),
    ADD COLUMN fcf               NUMERIC(24,4),
    ADD COLUMN total_debt        NUMERIC(24,4),
    ADD COLUMN interest_expense  NUMERIC(24,4),

    ADD COLUMN cash_q              NUMERIC(24,4),
    ADD COLUMN gross_profit_q      NUMERIC(24,4),
    ADD COLUMN operating_profit_q  NUMERIC(24,4),
    ADD COLUMN ebitda_q            NUMERIC(24,4),
    ADD COLUMN cfo_q               NUMERIC(24,4),
    ADD COLUMN capex_q             NUMERIC(24,4),
    ADD COLUMN fcf_q               NUMERIC(24,4),
    ADD COLUMN total_debt_q        NUMERIC(24,4),
    ADD COLUMN interest_expense_q  NUMERIC(24,4),

    ADD COLUMN cash_ttm              NUMERIC(24,4),
    ADD COLUMN gross_profit_ttm      NUMERIC(24,4),
    ADD COLUMN operating_profit_ttm  NUMERIC(24,4),
    ADD COLUMN ebitda_ttm            NUMERIC(24,4),
    ADD COLUMN cfo_ttm               NUMERIC(24,4),
    ADD COLUMN capex_ttm             NUMERIC(24,4),
    ADD COLUMN fcf_ttm               NUMERIC(24,4),
    ADD COLUMN total_debt_ttm        NUMERIC(24,4),
    ADD COLUMN interest_expense_ttm  NUMERIC(24,4),

    -- provenance / basis
    ADD COLUMN currency          TEXT,
    ADD COLUMN fx_rate           NUMERIC(20,8),
    ADD COLUMN fx_date           DATE,
    ADD COLUMN period_basis      TEXT,
    ADD COLUMN methodology       TEXT,

    -- per-field coverage: {"cash":"observed","gross_profit":"not_applicable",...}
    ADD COLUMN depth_coverage    JSONB,
    -- original reporting-currency values + source codes, kept for audit
    ADD COLUMN depth_raw         JSONB,
    -- Tier-1 vs Tier-2 disagreements (Tier 1 always wins for its own fields)
    ADD COLUMN depth_conflicts   JSONB,

    ADD COLUMN depth_fetched_at  TIMESTAMPTZ,
    ADD COLUMN depth_status      TEXT,
    ADD COLUMN depth_content_hash TEXT,

    ADD CONSTRAINT financial_statements_period_basis_check
        CHECK (period_basis IS NULL OR period_basis IN ('cumulative_ytd', 'annual_12m')),
    ADD CONSTRAINT financial_statements_depth_status_check
        CHECK (depth_status IS NULL OR depth_status IN
               ('observed', 'partial', 'failed', 'not_covered', 'yahoo_lagging')),
    ADD CONSTRAINT financial_statements_fx_rate_check
        CHECK (fx_rate IS NULL OR fx_rate > 0);

CREATE INDEX idx_financial_statements_depth_status
    ON financial_statements (depth_status)
    WHERE depth_status IS NOT NULL;

-- Per-ticker taxonomy + latest depth state. Ticket 06 owns the full universe
-- population; this table is also the sweep's covered/not_covered filter so a
-- ticker with no Yahoo data is not retried every month.
CREATE TABLE ticker_financial_taxonomy (
    ticker            TEXT PRIMARY KEY,
    taxonomy          TEXT NOT NULL CHECK (taxonomy IN
                          ('general_154', 'bank_108', 'insurance_74', 'other')),
    discovered_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source            TEXT NOT NULL,
    depth_status      TEXT NOT NULL DEFAULT 'pending' CHECK (depth_status IN
                          ('pending', 'observed', 'partial', 'failed',
                           'not_covered', 'yahoo_lagging')),
    last_depth_month  DATE,
    last_error        TEXT,
    CHECK (EXTRACT(DAY FROM last_depth_month) = 1 OR last_depth_month IS NULL)
);

CREATE INDEX idx_ticker_financial_taxonomy_depth_status
    ON ticker_financial_taxonomy (depth_status);

-- Monthly depth run ledger: one row per (run month, ticker), written after each
-- ticker so a killed sweep resumes from where it stopped. A repeat run of the
-- same month skips terminal statuses (observed/partial/not_covered) and retries
-- failed/yahoo_lagging tickers.
CREATE TABLE financial_depth_runs (
    run_month     DATE NOT NULL,
    ticker        TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN
                       ('observed', 'partial', 'failed', 'not_covered', 'yahoo_lagging')),
    rows_written  INT NOT NULL DEFAULT 0,
    error         TEXT,
    attempts      INT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_month, ticker),
    CHECK (EXTRACT(DAY FROM run_month) = 1)
);

CREATE INDEX idx_financial_depth_runs_status
    ON financial_depth_runs (run_month, status);

-- Append-only depth trail. The Tier-1 version trail (000023) records the
-- snapshot that carried a statement; this trail records what depth we held for
-- it, one row per (statement, observation month) on first sight and on change,
-- so a restatement of a depth value is auditable without inventing a Tier-1
-- version.
CREATE TABLE financial_statement_depth_versions (
    ticker          TEXT NOT NULL,
    period_end      DATE NOT NULL,
    observed_month  DATE NOT NULL,
    depth_snapshot  JSONB NOT NULL,
    content_hash    TEXT NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, period_end, observed_month),
    CHECK (EXTRACT(DAY FROM (period_end + INTERVAL '1 day')) = 1),
    CHECK (EXTRACT(DAY FROM observed_month) = 1)
);
