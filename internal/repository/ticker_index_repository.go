package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// TickerIndexRepository persists point-in-time index-membership rows (issue
// 15b). Pattern B — it does NOT embed the generic Repository: the PK is the
// composite (ticker_code, index_code, effective_date), not a local BIGSERIAL.
type TickerIndexRepository struct {
	Log *logrus.Logger
}

func NewTickerIndexRepository(log *logrus.Logger) *TickerIndexRepository {
	return &TickerIndexRepository{Log: log}
}

// ReplaceMembership atomically replaces the index-membership snapshot for one
// effective_date: the day's existing rows are deleted, then the new set is
// inserted in one transaction. Point-in-time semantics (issue 15b): each run
// writes a fresh snapshot dated with the run date, and a ticker that dropped
// out of an index is removed from that day's snapshot, not orphaned. A
// same-day re-run is idempotent (delete + re-insert the same set). An empty
// rows slice clears the day's snapshot.
func (r *TickerIndexRepository) ReplaceMembership(db *sqlx.DB, effectiveDate time.Time, rows []entity.TickerIndex) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM ticker_indices WHERE effective_date = $1", effectiveDate); err != nil {
		return err
	}
	if len(rows) > 0 {
		query, args, err := buildTickerIndexInsert(rows)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(query, args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// buildTickerIndexInsert builds a single multi-row INSERT for a membership
// batch (one round trip instead of one per row). ON CONFLICT DO NOTHING is a
// safety net for a concurrent same-day run — the delete above already cleared
// the day's rows.
func buildTickerIndexInsert(rows []entity.TickerIndex) (string, []interface{}, error) {
	const cols = 3 // ticker_code, index_code, effective_date
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d)",
			base+1, base+2, base+3,
		))
		args = append(args, r.TickerCode, r.IndexCode, r.EffectiveDate)
	}
	query := fmt.Sprintf(`
		INSERT INTO ticker_indices (ticker_code, index_code, effective_date)
		VALUES %s
		ON CONFLICT (ticker_code, index_code, effective_date) DO NOTHING
	`, strings.Join(valueStrings, ","))
	return query, args, nil
}
