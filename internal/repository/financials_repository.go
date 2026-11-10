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

// FinancialsRepository persists the monthly ratio snapshot into the canonical
// statement table, its append-only version trail and the run ledger. Pattern B
// — it does NOT embed the generic Repository: the canonical table has a
// composite natural key and no BIGSERIAL id.
type FinancialsRepository struct {
	Log *logrus.Logger
}

// NewFinancialsRepository builds the repository.
func NewFinancialsRepository(log *logrus.Logger) *FinancialsRepository {
	return &FinancialsRepository{Log: log}
}

// FinancialsIngestStats is what one snapshot write did, per statement.
type FinancialsIngestStats struct {
	Inserted  int
	Updated   int // a known statement whose content hash changed
	Unchanged int // a known statement repeated with an identical hash
	Skipped   int // an older snapshot for a statement already observed later
	Versions  int // version rows written
}

// IngestSnapshot writes one validated snapshot and its run-ledger row in a
// single transaction.
//
// Ordering is the hard part: the upsert only applies when the incoming
// snapshot month is at least the stored last_seen_month, so a backfill running
// backwards can never overwrite a newer observation. A version row is written
// on first sight and when the content hash changes — never for an unchanged
// monthly repeat.
func (r *FinancialsRepository) IngestSnapshot(db *sqlx.DB, snap *financials.Snapshot, run *entity.FinancialSnapshotRun) (*FinancialsIngestStats, error) {
	stats := &FinancialsIngestStats{}
	if snap == nil {
		return stats, fmt.Errorf("financials: nil snapshot")
	}
	month := financials.FirstOfMonth(snap.Month)

	tx, err := db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("financials: begin ingest %s: %w", month.Format("2006-01"), err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	if snap.Status == financials.StatusOK && len(snap.Statements) > 0 {
		existing, err := r.lockExisting(tx, snap.Statements)
		if err != nil {
			return nil, err
		}

		for _, st := range snap.Statements {
			hash := st.ContentHash()
			key := statementKey(st.Ticker, st.PeriodEnd)
			prior, known := existing[key]

			// An older snapshot must not touch a newer observation, and must
			// not append a version row either.
			if known && prior.LastSeenMonth.After(month) {
				stats.Skipped++
				continue
			}

			if err := r.upsertStatement(tx, st, month, hash); err != nil {
				return nil, err
			}

			changed := !known || prior.ContentHash != hash
			switch {
			case !known:
				stats.Inserted++
			case changed:
				stats.Updated++
			default:
				stats.Unchanged++
			}
			if changed {
				if err := r.upsertVersion(tx, st, month, hash); err != nil {
					return nil, err
				}
				stats.Versions++
			}
		}
	}

	if run != nil {
		if err := r.UpsertRunTx(tx, run); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("financials: commit ingest %s: %w", month.Format("2006-01"), err)
	}
	return stats, nil
}

// lockExisting reads the current canonical rows for the snapshot's tickers and
// locks them for the duration of the transaction, so the version decision and
// the ordered upsert see the same state.
func (r *FinancialsRepository) lockExisting(tx *sqlx.Tx, statements []financials.Statement) (map[string]entity.FinancialStatement, error) {
	tickers := make([]string, 0, len(statements))
	seen := make(map[string]struct{}, len(statements))
	for _, st := range statements {
		if _, ok := seen[st.Ticker]; ok {
			continue
		}
		seen[st.Ticker] = struct{}{}
		tickers = append(tickers, st.Ticker)
	}
	if len(tickers) == 0 {
		return map[string]entity.FinancialStatement{}, nil
	}

	query, args, err := sqlx.In(
		`SELECT ticker, period_end, content_hash, last_seen_month
		   FROM financial_statements
		  WHERE ticker IN (?) FOR UPDATE`, tickers)
	if err != nil {
		return nil, fmt.Errorf("financials: build existing query: %w", err)
	}
	query = tx.Rebind(query)

	var rows []struct {
		Ticker        string    `db:"ticker"`
		PeriodEnd     time.Time `db:"period_end"`
		ContentHash   string    `db:"content_hash"`
		LastSeenMonth time.Time `db:"last_seen_month"`
	}
	if err := tx.Select(&rows, query, args...); err != nil {
		return nil, fmt.Errorf("financials: read existing statements: %w", err)
	}
	out := make(map[string]entity.FinancialStatement, len(rows))
	for _, row := range rows {
		out[statementKey(row.Ticker, row.PeriodEnd)] = entity.FinancialStatement{
			Ticker:        row.Ticker,
			PeriodEnd:     row.PeriodEnd,
			ContentHash:   row.ContentHash,
			LastSeenMonth: row.LastSeenMonth,
		}
	}
	return out, nil
}

// upsertStatement applies the ordered upsert guard for one statement.
func (r *FinancialsRepository) upsertStatement(tx *sqlx.Tx, st financials.Statement, month time.Time, hash string) error {
	const query = `
		INSERT INTO financial_statements (
			ticker, period_end, fiscal_year_end, duration_months,
			assets, liabilities, equity, revenue, ebt, net_profit, net_profit_parent,
			eps, book_value, roa, roe, npm, der,
			audit_status, opinion, source, value_basis, source_fs_date,
			first_seen_month, last_seen_month, revision_count, content_hash
		) VALUES (
			$1,$2,$3,$4,
			$5,$6,$7,$8,$9,$10,$11,
			$12,$13,$14,$15,$16,$17,
			$18,$19,$20,$21,$22,
			$23,$24,0,$25
		)
		ON CONFLICT (ticker, period_end) DO UPDATE SET
			fiscal_year_end   = EXCLUDED.fiscal_year_end,
			duration_months   = EXCLUDED.duration_months,
			assets            = EXCLUDED.assets,
			liabilities       = EXCLUDED.liabilities,
			equity            = EXCLUDED.equity,
			revenue           = EXCLUDED.revenue,
			ebt               = EXCLUDED.ebt,
			net_profit        = EXCLUDED.net_profit,
			net_profit_parent = EXCLUDED.net_profit_parent,
			eps               = EXCLUDED.eps,
			book_value        = EXCLUDED.book_value,
			roa               = EXCLUDED.roa,
			roe               = EXCLUDED.roe,
			npm               = EXCLUDED.npm,
			der               = EXCLUDED.der,
			audit_status      = EXCLUDED.audit_status,
			opinion           = EXCLUDED.opinion,
			source            = EXCLUDED.source,
			value_basis       = EXCLUDED.value_basis,
			source_fs_date    = EXCLUDED.source_fs_date,
			last_seen_month   = GREATEST(financial_statements.last_seen_month, EXCLUDED.last_seen_month),
			content_hash      = EXCLUDED.content_hash,
			revision_count    = financial_statements.revision_count
			                    + CASE WHEN financial_statements.content_hash <> EXCLUDED.content_hash
			                           THEN 1 ELSE 0 END,
			updated_at        = CASE WHEN financial_statements.content_hash <> EXCLUDED.content_hash
			                         THEN NOW() ELSE financial_statements.updated_at END
		WHERE EXCLUDED.last_seen_month >= financial_statements.last_seen_month`

	_, err := tx.Exec(query,
		st.Ticker, st.PeriodEnd, st.FiscalYearEnd, st.DurationMonths,
		st.Assets, st.Liabilities, st.Equity, st.Revenue, st.EBT, st.NetProfit, st.NetProfitParent,
		st.EPS, st.BookValue, st.ROA, st.ROE, st.NPM, st.DER,
		st.AuditStatus, st.Opinion, st.Source, st.ValueBasis, st.SourceFSDate,
		month, month, hash,
	)
	if err != nil {
		return fmt.Errorf("financials: upsert %s %s: %w", st.Ticker, st.PeriodEnd.Format("2006-01-02"), err)
	}
	return nil
}

// upsertVersion appends one point-in-time observation. The key includes the
// seen month, so re-running a month overwrites that month's version rather
// than duplicating it.
func (r *FinancialsRepository) upsertVersion(tx *sqlx.Tx, st financials.Statement, month time.Time, hash string) error {
	const query = `
		INSERT INTO financial_statement_versions (
			ticker, period_end, seen_month, fiscal_year_end, duration_months,
			assets, liabilities, equity, revenue, ebt, net_profit, net_profit_parent,
			eps, book_value, roa, roe, npm, der,
			audit_status, opinion, content_hash
		) VALUES (
			$1,$2,$3,$4,$5,
			$6,$7,$8,$9,$10,$11,$12,
			$13,$14,$15,$16,$17,$18,
			$19,$20,$21
		)
		ON CONFLICT (ticker, period_end, seen_month) DO UPDATE SET
			fiscal_year_end   = EXCLUDED.fiscal_year_end,
			duration_months   = EXCLUDED.duration_months,
			assets            = EXCLUDED.assets,
			liabilities       = EXCLUDED.liabilities,
			equity            = EXCLUDED.equity,
			revenue           = EXCLUDED.revenue,
			ebt               = EXCLUDED.ebt,
			net_profit        = EXCLUDED.net_profit,
			net_profit_parent = EXCLUDED.net_profit_parent,
			eps               = EXCLUDED.eps,
			book_value        = EXCLUDED.book_value,
			roa               = EXCLUDED.roa,
			roe               = EXCLUDED.roe,
			npm               = EXCLUDED.npm,
			der               = EXCLUDED.der,
			audit_status      = EXCLUDED.audit_status,
			opinion           = EXCLUDED.opinion,
			content_hash      = EXCLUDED.content_hash,
			recorded_at       = NOW()`

	_, err := tx.Exec(query,
		st.Ticker, st.PeriodEnd, month, st.FiscalYearEnd, st.DurationMonths,
		st.Assets, st.Liabilities, st.Equity, st.Revenue, st.EBT, st.NetProfit, st.NetProfitParent,
		st.EPS, st.BookValue, st.ROA, st.ROE, st.NPM, st.DER,
		st.AuditStatus, st.Opinion, hash,
	)
	if err != nil {
		return fmt.Errorf("financials: version %s %s: %w", st.Ticker, st.PeriodEnd.Format("2006-01-02"), err)
	}
	return nil
}

// FindRun returns one month's ledger row, or nil when the month has not been
// attempted.
func (r *FinancialsRepository) FindRun(db *sqlx.DB, month time.Time) (*entity.FinancialSnapshotRun, error) {
	var run entity.FinancialSnapshotRun
	err := db.Get(&run, `SELECT * FROM financial_snapshot_runs WHERE snapshot_month = $1`,
		financials.FirstOfMonth(month))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("financials: read run %s: %w", month.Format("2006-01"), err)
	}
	return &run, nil
}

// UpsertRun writes one ledger row outside a snapshot transaction (the
// empty/suspect/failed cases, where there are no statements to write).
func (r *FinancialsRepository) UpsertRun(db *sqlx.DB, run *entity.FinancialSnapshotRun) error {
	if run == nil {
		return nil
	}
	return r.UpsertRunTx(db, run)
}

// runExecutor is satisfied by *sqlx.DB and *sqlx.Tx.
type runExecutor interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// UpsertRunTx writes one ledger row on any executor (a DB or an open tx).
func (r *FinancialsRepository) UpsertRunTx(ex runExecutor, run *entity.FinancialSnapshotRun) error {
	const query = `
		INSERT INTO financial_snapshot_runs (
			snapshot_month, source, status, row_count, distinct_periods,
			quarter_end_share, content_hash, error
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (snapshot_month) DO UPDATE SET
			source            = EXCLUDED.source,
			status            = EXCLUDED.status,
			row_count         = EXCLUDED.row_count,
			distinct_periods  = EXCLUDED.distinct_periods,
			quarter_end_share = EXCLUDED.quarter_end_share,
			content_hash      = EXCLUDED.content_hash,
			error             = EXCLUDED.error,
			fetched_at        = NOW()`

	_, err := ex.Exec(query,
		financials.FirstOfMonth(run.SnapshotMonth), run.Source, run.Status,
		run.RowCount, run.DistinctPeriods, run.QuarterEndShare, run.ContentHash, run.Error,
	)
	if err != nil {
		return fmt.Errorf("financials: upsert run %s: %w", run.SnapshotMonth.Format("2006-01"), err)
	}
	return nil
}

// FindByTicker returns a ticker's stored statements, newest period first. It
// is the read primitive ticket 03 builds the get_financials views on. The
// column list is explicit: the table also carries the *_q / *_ttm depth
// families, which the row-shaped entity does not scan.
func (r *FinancialsRepository) FindByTicker(db *sqlx.DB, ticker string) ([]entity.FinancialStatement, error) {
	var rows []entity.FinancialStatement
	err := db.Select(&rows,
		`SELECT ticker, period_end, fiscal_year_end, duration_months,
		        assets, liabilities, equity, revenue, ebt, net_profit, net_profit_parent,
		        eps, book_value, roa, roe, npm, der,
		        audit_status, opinion, source, value_basis, source_fs_date,
		        first_seen_month, last_seen_month, revision_count, content_hash,
		        stored_at, updated_at,
		        cash, gross_profit, operating_profit, ebitda, cfo, capex, fcf,
		        total_debt, interest_expense,
		        currency, fx_rate, fx_date, period_basis, methodology,
		        depth_status, depth_coverage, depth_raw, depth_conflicts, depth_fetched_at
		   FROM financial_statements
		  WHERE ticker = $1
		  ORDER BY period_end DESC`, ticker)
	if err != nil {
		return nil, fmt.Errorf("financials: read %s: %w", ticker, err)
	}
	return rows, nil
}

// FindVersions returns one statement's point-in-time trail, oldest first.
func (r *FinancialsRepository) FindVersions(db *sqlx.DB, ticker string, periodEnd time.Time) ([]entity.FinancialStatementVersion, error) {
	var rows []entity.FinancialStatementVersion
	err := db.Select(&rows,
		`SELECT * FROM financial_statement_versions
		  WHERE ticker = $1 AND period_end = $2 ORDER BY seen_month`, ticker, periodEnd)
	if err != nil {
		return nil, fmt.Errorf("financials: read versions %s %s: %w", ticker, periodEnd.Format("2006-01-02"), err)
	}
	return rows, nil
}

// statementKey keys a canonical statement by ticker and period.
func statementKey(ticker string, periodEnd time.Time) string {
	return ticker + "|" + periodEnd.Format("2006-01-02")
}
