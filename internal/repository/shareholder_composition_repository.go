package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// shareholderCompositionBatchSize caps the multi-row INSERT's parameter
// count: 25 columns × 1000 rows = 25k parameters, comfortably under the
// Postgres 65,535-parameter protocol limit for the whole-market file.
const shareholderCompositionBatchSize = 1000

// ShareholderCompositionRepository persists KSEI balance-position rows (issue
// 08). Pattern B — it does NOT embed the generic Repository: (ticker,
// position_date) is a natural key (not a local BIGSERIAL), and no promoted
// FindByID/DeleteByID/Count caller exists.
type ShareholderCompositionRepository struct {
	Log *logrus.Logger
}

func NewShareholderCompositionRepository(log *logrus.Logger) *ShareholderCompositionRepository {
	return &ShareholderCompositionRepository{Log: log}
}

// Upsert inserts or refreshes rows keyed by (ticker, position_date),
// chunked into multi-row statements. Idempotent: re-ingesting a file
// overwrites, never duplicates. An empty slice is a no-op.
func (r *ShareholderCompositionRepository) Upsert(db *sqlx.DB, rows []entity.ShareholderComposition) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += shareholderCompositionBatchSize {
		end := start + shareholderCompositionBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		query, args := buildShareholderCompositionUpsert(rows[start:end])
		if _, err := db.Exec(query, args...); err != nil {
			return err
		}
	}
	return nil
}

// buildShareholderCompositionUpsert builds one multi-row INSERT ... ON
// CONFLICT for a chunk of rows (one round trip per chunk instead of one per
// row). Total = local_total + foreign_total (the file has no grand-total
// column — its 25th field is the foreign block's Total).
func buildShareholderCompositionUpsert(rows []entity.ShareholderComposition) (string, []interface{}) {
	const cols = 25
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10,
			base+11, base+12, base+13, base+14, base+15, base+16, base+17, base+18, base+19, base+20,
			base+21, base+22, base+23, base+24, base+25,
		))
		args = append(args,
			r.Ticker, r.PositionDate, r.SecNum, r.Price,
			r.LocalIS, r.LocalCP, r.LocalPF, r.LocalIB, r.LocalID, r.LocalMF, r.LocalSC, r.LocalFD, r.LocalOT, r.LocalTotal,
			r.ForeignIS, r.ForeignCP, r.ForeignPF, r.ForeignIB, r.ForeignID, r.ForeignMF, r.ForeignSC, r.ForeignFD, r.ForeignOT, r.ForeignTotal,
			r.Total,
		)
	}
	query := fmt.Sprintf(`
		INSERT INTO shareholder_composition (
			ticker, position_date, sec_num, price,
			local_is, local_cp, local_pf, local_ib, local_id, local_mf, local_sc, local_fd, local_ot, local_total,
			foreign_is, foreign_cp, foreign_pf, foreign_ib, foreign_id, foreign_mf, foreign_sc, foreign_fd, foreign_ot, foreign_total,
			total
		)
		VALUES %s
		ON CONFLICT (ticker, position_date) DO UPDATE SET
			sec_num = EXCLUDED.sec_num,
			price = EXCLUDED.price,
			local_is = EXCLUDED.local_is,
			local_cp = EXCLUDED.local_cp,
			local_pf = EXCLUDED.local_pf,
			local_ib = EXCLUDED.local_ib,
			local_id = EXCLUDED.local_id,
			local_mf = EXCLUDED.local_mf,
			local_sc = EXCLUDED.local_sc,
			local_fd = EXCLUDED.local_fd,
			local_ot = EXCLUDED.local_ot,
			local_total = EXCLUDED.local_total,
			foreign_is = EXCLUDED.foreign_is,
			foreign_cp = EXCLUDED.foreign_cp,
			foreign_pf = EXCLUDED.foreign_pf,
			foreign_ib = EXCLUDED.foreign_ib,
			foreign_id = EXCLUDED.foreign_id,
			foreign_mf = EXCLUDED.foreign_mf,
			foreign_sc = EXCLUDED.foreign_sc,
			foreign_fd = EXCLUDED.foreign_fd,
			foreign_ot = EXCLUDED.foreign_ot,
			foreign_total = EXCLUDED.foreign_total,
			total = EXCLUDED.total,
			stored_at = NOW()
	`, strings.Join(valueStrings, ","))
	return query, args
}

// FindByTickerRange returns one ticker's rows with position_date in [from,
// to] (inclusive), ascending by position date.
func (r *ShareholderCompositionRepository) FindByTickerRange(db *sqlx.DB, ticker string, from, to time.Time) ([]entity.ShareholderComposition, error) {
	var rows []entity.ShareholderComposition
	err := db.Select(&rows,
		"SELECT * FROM shareholder_composition WHERE ticker = $1 AND position_date >= $2 AND position_date <= $3 ORDER BY position_date",
		ticker, from, to,
	)
	return rows, err
}

// LatestPositionDate returns the newest position_date stored, or nil when the
// table is empty. Lets the tool layer report coverage without a COUNT.
func (r *ShareholderCompositionRepository) LatestPositionDate(db *sqlx.DB) (*time.Time, error) {
	var latest *time.Time
	err := db.Get(&latest, "SELECT MAX(position_date) FROM shareholder_composition")
	if err != nil {
		return nil, err
	}
	return latest, nil
}
