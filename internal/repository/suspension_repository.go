package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// SuspensionRepository persists BEI UMA/suspension rows from the
// GetSuspension/GetUma sources. Pattern B — it does NOT embed the generic
// Repository: id is a local BIGSERIAL surrogate, and the idempotency key is
// the composite (ticker, event_date, type, reason), with no promoted
// FindByID/DeleteByID/Count caller.
type SuspensionRepository struct {
	Log *logrus.Logger
}

func NewSuspensionRepository(log *logrus.Logger) *SuspensionRepository {
	return &SuspensionRepository{Log: log}
}

// Upsert inserts or refreshes rows keyed by (ticker, event_date, type, reason)
// in one multi-row statement (single atomic round trip). An empty slice is a
// no-op. Idempotent: refetching a window overwrites, never duplicates.
func (r *SuspensionRepository) Upsert(db *sqlx.DB, rows []entity.Suspension) error {
	if len(rows) == 0 {
		return nil
	}
	query, args, err := buildSuspensionUpsert(rows)
	if err != nil {
		return err
	}
	_, err = db.Exec(query, args...)
	return err
}

// buildSuspensionUpsert builds a single multi-row INSERT ... ON CONFLICT for a
// batch of suspension rows (one round trip instead of one per row). reason is
// part of the conflict key, so the DO UPDATE only refreshes stored_at.
func buildSuspensionUpsert(rows []entity.Suspension) (string, []interface{}, error) {
	const cols = 4 // ticker, event_date, type, reason
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4,
		))
		args = append(args, r.Ticker, r.EventDate, r.Type, r.Reason)
	}
	query := fmt.Sprintf(`
		INSERT INTO suspensions (ticker, event_date, type, reason)
		VALUES %s
		ON CONFLICT (ticker, event_date, type, reason) DO UPDATE SET
			stored_at = NOW()
	`, strings.Join(valueStrings, ","))
	return query, args, nil
}

// FindByDateRange returns rows with event_date in [from, to] (inclusive),
// ordered by event_date then ticker. An optional ticker narrows the read (nil
// returns every ticker). An empty range returns an empty slice, no error.
func (r *SuspensionRepository) FindByDateRange(db *sqlx.DB, from, to time.Time, ticker *string) ([]entity.Suspension, error) {
	var rows []entity.Suspension
	if ticker != nil {
		err := db.Select(&rows,
			"SELECT * FROM suspensions WHERE event_date >= $1 AND event_date <= $2 AND ticker = $3 ORDER BY event_date, ticker",
			from, to, *ticker,
		)
		return rows, err
	}
	err := db.Select(&rows,
		"SELECT * FROM suspensions WHERE event_date >= $1 AND event_date <= $2 ORDER BY event_date, ticker",
		from, to,
	)
	return rows, err
}
