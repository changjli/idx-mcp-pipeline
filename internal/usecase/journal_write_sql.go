package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// JournalSQLWriter appends Verdicts to the measurement_journal table. It is the
// concrete MeasurementJournalWriter; the append and the commit move share one
// transaction, so the partial unique committed index can never be violated by a
// half-applied write.
type JournalSQLWriter struct {
	DB *sqlx.DB
}

func NewJournalSQLWriter(db *sqlx.DB) *JournalSQLWriter { return &JournalSQLWriter{DB: db} }

const journalInsert = `
	INSERT INTO measurement_journal
		(decision_day, ticker, verdict,
		 entry_low, entry_high, cl_low, cl_high, tp_low, tp_high,
		 rationale, session, supersedes, committed)
	VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	RETURNING id, created_at`

// Write appends v. When commit is true it first clears any committed row on the
// key, so exactly one row is committed; both happen in one transaction.
func (w *JournalSQLWriter) Write(ctx context.Context, v JournalVerdict, commit bool) (int64, time.Time, error) {
	if w.DB == nil {
		return 0, time.Time{}, fmt.Errorf("journal writer: no database")
	}
	tx, err := w.DB.BeginTxx(ctx, nil)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("begin journal write: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	if commit {
		if _, err := tx.ExecContext(ctx,
			`UPDATE measurement_journal SET committed = FALSE
			 WHERE decision_day = $1::date AND ticker = $2 AND committed`,
			v.Date, v.Ticker); err != nil {
			return 0, time.Time{}, fmt.Errorf("clear prior committed verdict: %w", err)
		}
	}

	var id int64
	var createdAt time.Time
	if err := tx.QueryRowxContext(ctx, journalInsert,
		v.Date, v.Ticker, v.Verdict,
		v.EntryLow, v.EntryHigh, v.CLLow, v.CLHigh, v.TPLow, v.TPHigh,
		v.Rationale, v.Session, v.Supersedes, commit,
	).Scan(&id, &createdAt); err != nil {
		return 0, time.Time{}, fmt.Errorf("append verdict: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, time.Time{}, fmt.Errorf("commit journal write: %w", err)
	}
	return id, createdAt, nil
}

// ReadKey returns every row on a (day, ticker) key, oldest-first (created_at,
// id), so the caller's same-session last-wins and conflict detection read insert
// order.
func (w *JournalSQLWriter) ReadKey(ctx context.Context, day time.Time, ticker string) ([]JournalVerdict, error) {
	if w.DB == nil {
		return nil, fmt.Errorf("journal writer: no database")
	}
	const q = `
		SELECT id, decision_day, ticker, verdict,
		       entry_low, entry_high, cl_low, cl_high, tp_low, tp_high,
		       rationale, session, supersedes, committed, created_at
		FROM measurement_journal
		WHERE decision_day = $1::date AND ticker = $2
		ORDER BY created_at, id`

	rows, err := w.DB.QueryxContext(ctx, q, day, ticker)
	if err != nil {
		return nil, fmt.Errorf("read measurement_journal key: %w", err)
	}
	defer rows.Close()

	var out []JournalVerdict
	for rows.Next() {
		var row journalRow
		if err := rows.StructScan(&row); err != nil {
			return nil, fmt.Errorf("read measurement_journal key: %w", err)
		}
		out = append(out, row.verdict())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read measurement_journal key: %w", err)
	}
	return out, nil
}

// compile-time check: the SQL writer satisfies the seam.
var _ MeasurementJournalWriter = (*JournalSQLWriter)(nil)
