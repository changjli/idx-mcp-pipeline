package repository

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// IndexSummaryRepository persists index/sector summary rows from the IDX
// GetIndexSummary source (issue 18). Pattern B — it does NOT embed the generic
// Repository: the key is the natural composite (index_code, date), and no
// promoted FindByID/DeleteByID/Count caller exists.
type IndexSummaryRepository struct {
	Log *logrus.Logger
}

func NewIndexSummaryRepository(log *logrus.Logger) *IndexSummaryRepository {
	return &IndexSummaryRepository{Log: log}
}

// Upsert inserts or refreshes rows keyed by (index_code, date) in one multi-row
// statement (single atomic round trip). An empty slice is a no-op. Idempotent:
// a same-day re-run (or a holiday fetch returning the last trading day's rows)
// overwrites, never duplicates.
func (r *IndexSummaryRepository) Upsert(db *sqlx.DB, rows []entity.IndexSummary) error {
	if len(rows) == 0 {
		return nil
	}
	query, args, err := buildIndexSummaryUpsert(rows)
	if err != nil {
		return err
	}
	_, err = db.Exec(query, args...)
	return err
}

// buildIndexSummaryUpsert builds a single multi-row INSERT ... ON CONFLICT for
// a batch of index-summary rows (one round trip instead of one per row).
func buildIndexSummaryUpsert(rows []entity.IndexSummary) (string, []interface{}, error) {
	const cols = 12 // index_code, date, previous, high, low, close, change, number_of_stock, volume, value, frequency, market_cap
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11, base+12,
		))
		args = append(args, r.IndexCode, r.Date, r.Previous, r.High, r.Low, r.Close, r.Change, r.NumberOfStock, r.Volume, r.Value, r.Frequency, r.MarketCap)
	}
	query := fmt.Sprintf(`
		INSERT INTO index_summaries (index_code, date, previous, high, low, close, change, number_of_stock, volume, value, frequency, market_cap)
		VALUES %s
		ON CONFLICT (index_code, date) DO UPDATE SET
			previous = EXCLUDED.previous,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			change = EXCLUDED.change,
			number_of_stock = EXCLUDED.number_of_stock,
			volume = EXCLUDED.volume,
			value = EXCLUDED.value,
			frequency = EXCLUDED.frequency,
			market_cap = EXCLUDED.market_cap,
			stored_at = NOW()
	`, strings.Join(valueStrings, ","))
	return query, args, nil
}
