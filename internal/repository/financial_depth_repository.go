package repository

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

// FinancialDepthRepository persists Tier-2 depth into the existing Tier-1
// statement rows, the per-ticker taxonomy, the monthly depth ledger and the
// append-only depth version trail.
type FinancialDepthRepository struct {
	Log *logrus.Logger
}

// NewFinancialDepthRepository builds the repository.
func NewFinancialDepthRepository(log *logrus.Logger) *FinancialDepthRepository {
	return &FinancialDepthRepository{Log: log}
}

// ListDepthSweepTickers returns the tickers the monthly sweep may enrich. The
// universe is the Tier-1 statement store (the ~963 listed set), not the
// 1,184-row ticker table, and a ticker already recorded as not_covered is
// excluded unless includeNotCovered is set (the manual/quarterly retry path).
func (r *FinancialDepthRepository) ListDepthSweepTickers(db *sqlx.DB, includeNotCovered bool) ([]string, error) {
	const query = `
		SELECT DISTINCT fs.ticker
		  FROM financial_statements fs
		  LEFT JOIN ticker_financial_taxonomy t ON t.ticker = fs.ticker
		 WHERE ($1 OR t.depth_status IS DISTINCT FROM 'not_covered')
		 ORDER BY fs.ticker`
	var tickers []string
	if err := db.Select(&tickers, query, includeNotCovered); err != nil {
		return nil, fmt.Errorf("financial depth: list sweep tickers: %w", err)
	}
	return tickers, nil
}

// FindTaxonomy returns one ticker's stored classification, or nil when absent.
func (r *FinancialDepthRepository) FindTaxonomy(db *sqlx.DB, ticker string) (*entity.TickerFinancialTaxonomy, error) {
	var row entity.TickerFinancialTaxonomy
	err := db.Get(&row, `SELECT * FROM ticker_financial_taxonomy WHERE ticker = $1`, ticker)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("financial depth: read taxonomy %s: %w", ticker, err)
	}
	return &row, nil
}

// UpsertTaxonomy records a classification and latest depth state. A taxonomy
// change resets discovered_at; an unchanged classification keeps the original
// discovery time.
func (r *FinancialDepthRepository) UpsertTaxonomy(db *sqlx.DB, row *entity.TickerFinancialTaxonomy) error {
	const query = `
		INSERT INTO ticker_financial_taxonomy (
			ticker, taxonomy, discovered_at, source, depth_status, last_depth_month, last_error
		) VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (ticker) DO UPDATE SET
			taxonomy         = EXCLUDED.taxonomy,
			discovered_at    = CASE WHEN ticker_financial_taxonomy.taxonomy <> EXCLUDED.taxonomy
			                        THEN EXCLUDED.discovered_at
			                        ELSE ticker_financial_taxonomy.discovered_at END,
			source           = EXCLUDED.source,
			depth_status     = EXCLUDED.depth_status,
			last_depth_month = EXCLUDED.last_depth_month,
			last_error       = EXCLUDED.last_error`
	_, err := db.Exec(query,
		row.Ticker, row.Taxonomy, row.DiscoveredAt, row.Source,
		row.DepthStatus, row.LastDepthMonth, row.LastError,
	)
	if err != nil {
		return fmt.Errorf("financial depth: upsert taxonomy %s: %w", row.Ticker, err)
	}
	return nil
}

// FindDepthRun returns one (month, ticker) ledger row, or nil when absent.
func (r *FinancialDepthRepository) FindDepthRun(db *sqlx.DB, month time.Time, ticker string) (*entity.FinancialDepthRun, error) {
	var row entity.FinancialDepthRun
	err := db.Get(&row,
		`SELECT * FROM financial_depth_runs WHERE run_month = $1 AND ticker = $2`,
		financials.FirstOfMonth(month), ticker)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("financial depth: read run %s %s: %w", month.Format("2006-01"), ticker, err)
	}
	return &row, nil
}

// UpsertDepthRun writes one ticker's run outcome. Attempts increments on every
// write, so a retried ticker is visible in the ledger.
func (r *FinancialDepthRepository) UpsertDepthRun(db *sqlx.DB, row *entity.FinancialDepthRun) error {
	const query = `
		INSERT INTO financial_depth_runs (
			run_month, ticker, status, rows_written, error, attempts
		) VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (run_month, ticker) DO UPDATE SET
			status       = EXCLUDED.status,
			rows_written = EXCLUDED.rows_written,
			error        = EXCLUDED.error,
			attempts     = financial_depth_runs.attempts + EXCLUDED.attempts,
			updated_at   = NOW()`
	attempts := row.Attempts
	if attempts < 1 {
		attempts = 1
	}
	_, err := db.Exec(query,
		financials.FirstOfMonth(row.RunMonth), row.Ticker, row.Status,
		row.RowsWritten, row.Error, attempts,
	)
	if err != nil {
		return fmt.Errorf("financial depth: upsert run %s %s: %w",
			row.RunMonth.Format("2006-01"), row.Ticker, err)
	}
	return nil
}

// ApplyDepth writes the prepared depth rows for one ticker in a transaction. A
// row whose content hash is unchanged is skipped entirely, so a repeat run of
// the same month performs no writes (the ledger is the idempotency gate; this
// is the second line of defence). A changed row is updated in place and a
// depth version is appended for the run month.
func (r *FinancialDepthRepository) ApplyDepth(db *sqlx.DB, observedMonth time.Time, rows []entity.FinancialStatementDepth) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	observedMonth = financials.FirstOfMonth(observedMonth)

	tx, err := db.Beginx()
	if err != nil {
		return 0, fmt.Errorf("financial depth: begin apply: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	written := 0
	for _, row := range rows {
		var prior sql.NullString
		err := tx.Get(&prior,
			`SELECT depth_content_hash FROM financial_statements
			  WHERE ticker = $1 AND period_end = $2 FOR UPDATE`,
			row.Ticker, row.PeriodEnd)
		if err == sql.ErrNoRows {
			continue // the Tier-1 row disappeared between read and write
		}
		if err != nil {
			return 0, fmt.Errorf("financial depth: lock %s %s: %w",
				row.Ticker, row.PeriodEnd.Format("2006-01-02"), err)
		}
		if prior.Valid && row.DepthContentHash != nil && prior.String == *row.DepthContentHash {
			continue
		}

		if err := r.updateStatementDepth(tx, row); err != nil {
			return 0, err
		}
		if err := r.upsertDepthVersion(tx, observedMonth, row); err != nil {
			return 0, err
		}
		written++
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("financial depth: commit apply: %w", err)
	}
	return written, nil
}

func (r *FinancialDepthRepository) updateStatementDepth(tx *sqlx.Tx, row entity.FinancialStatementDepth) error {
	const query = `
		UPDATE financial_statements SET
			cash                  = :cash,
			gross_profit          = :gross_profit,
			operating_profit      = :operating_profit,
			ebitda                = :ebitda,
			cfo                   = :cfo,
			capex                 = :capex,
			fcf                   = :fcf,
			total_debt            = :total_debt,
			interest_expense      = :interest_expense,
			cash_q                = :cash_q,
			gross_profit_q        = :gross_profit_q,
			operating_profit_q    = :operating_profit_q,
			ebitda_q              = :ebitda_q,
			cfo_q                 = :cfo_q,
			capex_q               = :capex_q,
			fcf_q                 = :fcf_q,
			total_debt_q          = :total_debt_q,
			interest_expense_q    = :interest_expense_q,
			cash_ttm              = :cash_ttm,
			gross_profit_ttm      = :gross_profit_ttm,
			operating_profit_ttm  = :operating_profit_ttm,
			ebitda_ttm            = :ebitda_ttm,
			cfo_ttm               = :cfo_ttm,
			capex_ttm             = :capex_ttm,
			fcf_ttm               = :fcf_ttm,
			total_debt_ttm        = :total_debt_ttm,
			interest_expense_ttm  = :interest_expense_ttm,
			currency              = :currency,
			fx_rate               = :fx_rate,
			fx_date               = :fx_date,
			period_basis          = :period_basis,
			methodology           = :methodology,
			depth_coverage        = CAST(:depth_coverage AS jsonb),
			depth_raw             = CAST(:depth_raw AS jsonb),
			depth_conflicts       = CAST(:depth_conflicts AS jsonb),
			depth_status          = :depth_status,
			depth_fetched_at      = :depth_fetched_at,
			depth_content_hash    = :depth_content_hash
		WHERE ticker = :ticker AND period_end = :period_end`
	if _, err := tx.NamedExec(query, row); err != nil {
		return fmt.Errorf("financial depth: update %s %s: %w",
			row.Ticker, row.PeriodEnd.Format("2006-01-02"), err)
	}
	return nil
}

func (r *FinancialDepthRepository) upsertDepthVersion(tx *sqlx.Tx, observedMonth time.Time, row entity.FinancialStatementDepth) error {
	snapshot := "{}"
	if row.DepthSnapshot != nil && *row.DepthSnapshot != "" {
		snapshot = *row.DepthSnapshot
	}
	hash := ""
	if row.DepthContentHash != nil {
		hash = *row.DepthContentHash
	}
	const query = `
		INSERT INTO financial_statement_depth_versions (
			ticker, period_end, observed_month, depth_snapshot, content_hash
		) VALUES (
			:ticker, :period_end, :observed_month, CAST(:depth_snapshot AS jsonb), :content_hash
		)
		ON CONFLICT (ticker, period_end, observed_month) DO UPDATE SET
			depth_snapshot = EXCLUDED.depth_snapshot,
			content_hash   = EXCLUDED.content_hash,
			recorded_at    = NOW()`
	_, err := tx.NamedExec(query, map[string]any{
		"ticker":         row.Ticker,
		"period_end":     row.PeriodEnd,
		"observed_month": observedMonth,
		"depth_snapshot": snapshot,
		"content_hash":   hash,
	})
	if err != nil {
		return fmt.Errorf("financial depth: version %s %s: %w",
			row.Ticker, row.PeriodEnd.Format("2006-01-02"), err)
	}
	return nil
}

// FindDepthVersions returns one statement's depth trail, oldest first.
func (r *FinancialDepthRepository) FindDepthVersions(db *sqlx.DB, ticker string, periodEnd time.Time) ([]entity.FinancialStatementDepthVersion, error) {
	var rows []entity.FinancialStatementDepthVersion
	err := db.Select(&rows,
		`SELECT ticker, period_end, observed_month, depth_snapshot::text AS depth_snapshot,
		        content_hash, recorded_at
		   FROM financial_statement_depth_versions
		  WHERE ticker = $1 AND period_end = $2
		  ORDER BY observed_month`, ticker, periodEnd)
	if err != nil {
		return nil, fmt.Errorf("financial depth: read versions %s %s: %w",
			ticker, periodEnd.Format("2006-01-02"), err)
	}
	return rows, nil
}
