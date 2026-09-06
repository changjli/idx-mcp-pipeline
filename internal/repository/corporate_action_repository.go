package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// CorporateActionRepository persists corporate-action calendar rows from the
// IDX GetIssuedHistory source. Pattern B — it does NOT embed the generic
// Repository: id is a natural IDX surrogate (not a local BIGSERIAL), and no
// promoted FindByID/DeleteByID/Count caller exists.
type CorporateActionRepository struct {
	Log *logrus.Logger
}

func NewCorporateActionRepository(log *logrus.Logger) *CorporateActionRepository {
	return &CorporateActionRepository{Log: log}
}

// Upsert inserts or refreshes rows keyed by the IDX surrogate id in one
// multi-row statement (single atomic round trip). An empty slice is a no-op.
// Idempotent: refetching a window overwrites, never duplicates.
func (r *CorporateActionRepository) Upsert(db *sqlx.DB, rows []entity.CorporateAction) error {
	if len(rows) == 0 {
		return nil
	}
	query, args, err := buildCorporateActionUpsert(rows)
	if err != nil {
		return err
	}
	_, err = db.Exec(query, args...)
	return err
}

// buildCorporateActionUpsert builds a single multi-row INSERT ... ON CONFLICT
// for a batch of corporate-actions rows (one round trip instead of one per row).
func buildCorporateActionUpsert(rows []entity.CorporateAction) (string, []interface{}, error) {
	const cols = 6 // id, ticker, event_date, type, jumlah_saham, jumlah_saham_setelah_tindakan
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6,
		))
		args = append(args, r.Id, r.Ticker, r.EventDate, r.Type, r.JumlahSaham, r.JumlahSahamSetelahTindakan)
	}
	query := fmt.Sprintf(`
		INSERT INTO corporate_actions (id, ticker, event_date, type, jumlah_saham, jumlah_saham_setelah_tindakan)
		VALUES %s
		ON CONFLICT (id) DO UPDATE SET
			ticker = EXCLUDED.ticker,
			event_date = EXCLUDED.event_date,
			type = EXCLUDED.type,
			jumlah_saham = EXCLUDED.jumlah_saham,
			jumlah_saham_setelah_tindakan = EXCLUDED.jumlah_saham_setelah_tindakan,
			stored_at = NOW()
	`, strings.Join(valueStrings, ","))
	return query, args, nil
}

// FindByDateRange returns rows with event_date in [from, to] (inclusive),
// ordered by event_date then ticker. An optional ticker narrows the read (nil
// returns every ticker). An empty range returns an empty slice, no error.
func (r *CorporateActionRepository) FindByDateRange(db *sqlx.DB, from, to time.Time, ticker *string) ([]entity.CorporateAction, error) {
	var rows []entity.CorporateAction
	if ticker != nil {
		err := db.Select(&rows,
			"SELECT * FROM corporate_actions WHERE event_date >= $1 AND event_date <= $2 AND ticker = $3 ORDER BY event_date, ticker",
			from, to, *ticker,
		)
		return rows, err
	}
	err := db.Select(&rows,
		"SELECT * FROM corporate_actions WHERE event_date >= $1 AND event_date <= $2 ORDER BY event_date, ticker",
		from, to,
	)
	return rows, err
}
