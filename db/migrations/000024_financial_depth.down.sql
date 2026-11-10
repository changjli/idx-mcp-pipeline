DROP TABLE IF EXISTS financial_statement_depth_versions;
DROP TABLE IF EXISTS financial_depth_runs;
DROP TABLE IF EXISTS ticker_financial_taxonomy;

DROP INDEX IF EXISTS idx_financial_statements_depth_status;

ALTER TABLE financial_statements
    DROP CONSTRAINT IF EXISTS financial_statements_fx_rate_check,
    DROP CONSTRAINT IF EXISTS financial_statements_depth_status_check,
    DROP CONSTRAINT IF EXISTS financial_statements_period_basis_check,
    DROP COLUMN IF EXISTS depth_content_hash,
    DROP COLUMN IF EXISTS depth_status,
    DROP COLUMN IF EXISTS depth_fetched_at,
    DROP COLUMN IF EXISTS depth_conflicts,
    DROP COLUMN IF EXISTS depth_raw,
    DROP COLUMN IF EXISTS depth_coverage,
    DROP COLUMN IF EXISTS methodology,
    DROP COLUMN IF EXISTS period_basis,
    DROP COLUMN IF EXISTS fx_date,
    DROP COLUMN IF EXISTS fx_rate,
    DROP COLUMN IF EXISTS currency,

    DROP COLUMN IF EXISTS interest_expense_ttm,
    DROP COLUMN IF EXISTS total_debt_ttm,
    DROP COLUMN IF EXISTS fcf_ttm,
    DROP COLUMN IF EXISTS capex_ttm,
    DROP COLUMN IF EXISTS cfo_ttm,
    DROP COLUMN IF EXISTS ebitda_ttm,
    DROP COLUMN IF EXISTS operating_profit_ttm,
    DROP COLUMN IF EXISTS gross_profit_ttm,
    DROP COLUMN IF EXISTS cash_ttm,

    DROP COLUMN IF EXISTS interest_expense_q,
    DROP COLUMN IF EXISTS total_debt_q,
    DROP COLUMN IF EXISTS fcf_q,
    DROP COLUMN IF EXISTS capex_q,
    DROP COLUMN IF EXISTS cfo_q,
    DROP COLUMN IF EXISTS ebitda_q,
    DROP COLUMN IF EXISTS operating_profit_q,
    DROP COLUMN IF EXISTS gross_profit_q,
    DROP COLUMN IF EXISTS cash_q,

    DROP COLUMN IF EXISTS interest_expense,
    DROP COLUMN IF EXISTS total_debt,
    DROP COLUMN IF EXISTS fcf,
    DROP COLUMN IF EXISTS capex,
    DROP COLUMN IF EXISTS cfo,
    DROP COLUMN IF EXISTS ebitda,
    DROP COLUMN IF EXISTS operating_profit,
    DROP COLUMN IF EXISTS gross_profit,
    DROP COLUMN IF EXISTS cash;
