package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// BrokerStockSummaryRepository persists per-stock broker summaries. It does
// NOT embed the generic Repository — the table has a composite PK and no id
// column, so the promoted FindByID/DeleteByID/Count would fail at runtime.
type BrokerStockSummaryRepository struct {
	Log *logrus.Logger
}

func NewBrokerStockSummaryRepository(log *logrus.Logger) *BrokerStockSummaryRepository {
	return &BrokerStockSummaryRepository{Log: log}
}

// UpsertDay atomically replaces one ticker+day's broker rows and totals in a
// single transaction. Rows that dropped out of IPOT's top-10 on a refetch are
// deleted (not accumulated), and the totals row is written in the same
// transaction so rows and totals can never diverge. Idempotent: refetching the
// same day yields the same rows, no duplicates.
func (r *BrokerStockSummaryRepository) UpsertDay(db *sqlx.DB, rows []entity.BrokerStockSummary, totals *entity.BrokerStockSummaryTotals) error {
	// The day's key comes from the rows or the totals row.
	var ticker string
	var day time.Time
	switch {
	case len(rows) > 0:
		ticker, day = rows[0].Ticker, rows[0].TradingDay
	case totals != nil:
		ticker, day = totals.Ticker, totals.TradingDay
	default:
		return nil // nothing to write
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Replace the day's rows wholesale so brokers that dropped out of the
	// top-10 are removed, not kept as stale rows. Runs even when rows is empty
	// (a caller clearing a day) so rows and totals can never diverge.
	if _, err := tx.Exec(
		"DELETE FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2",
		ticker, day,
	); err != nil {
		return err
	}

	if len(rows) > 0 {
		query, args, err := buildMultiRowUpsert(rows)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(query, args...); err != nil {
			return err
		}
	}

	if totals != nil {
		if _, err := tx.NamedExec(`
			INSERT INTO broker_stock_summary_totals (ticker, trading_day, t_val, f_nval, t_lot, avg, others_net)
			VALUES (:ticker, :trading_day, :t_val, :f_nval, :t_lot, :avg, :others_net)
			ON CONFLICT (ticker, trading_day) DO UPDATE SET
				t_val = EXCLUDED.t_val,
				f_nval = EXCLUDED.f_nval,
				t_lot = EXCLUDED.t_lot,
				avg = EXCLUDED.avg,
				others_net = EXCLUDED.others_net
		`, totals); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// buildMultiRowUpsert builds a single multi-row INSERT ... ON CONFLICT for a
// batch of broker rows (one round trip instead of one per row).
func buildMultiRowUpsert(rows []entity.BrokerStockSummary) (string, []interface{}, error) {
	const cols = 8 // ticker, broker_code, side, trading_day, lot, value, avg_price, rank
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8,
		))
		args = append(args, r.Ticker, r.BrokerCode, r.Side, r.TradingDay, r.Lot, r.Value, r.AvgPrice, r.Rank)
	}
	query := fmt.Sprintf(`
		INSERT INTO broker_stock_summaries (ticker, broker_code, side, trading_day, lot, value, avg_price, rank)
		VALUES %s
		ON CONFLICT (ticker, trading_day, broker_code, side) DO UPDATE SET
			lot = EXCLUDED.lot,
			value = EXCLUDED.value,
			avg_price = EXCLUDED.avg_price,
			rank = EXCLUDED.rank
	`, strings.Join(valueStrings, ","))
	return query, args, nil
}

// HasStoredDay reports whether any broker_stock_summaries rows exist for a
// ticker+day. Cheap EXISTS check — the full-market sweep's skip-if-stored
// guard. Idempotency note: rows present means the day is already covered, so
// the sweep skips the ticker entirely (no IPOT call); a race between sweep and
// an anomaly-gated refetch is harmless because UpsertDay replaces wholesale.
func (r *BrokerStockSummaryRepository) HasStoredDay(db *sqlx.DB, ticker string, day time.Time) (bool, error) {
	var exists bool
	err := db.Get(&exists,
		"SELECT EXISTS(SELECT 1 FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2)",
		ticker, day,
	)
	return exists, err
}

// FindByTickerAndDay returns the stored broker rows for a ticker+day,
// ordered by side then rank.
func (r *BrokerStockSummaryRepository) FindByTickerAndDay(db *sqlx.DB, ticker string, day time.Time) ([]entity.BrokerStockSummary, error) {
	var rows []entity.BrokerStockSummary
	err := db.Select(&rows,
		"SELECT * FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2 ORDER BY side, rank",
		ticker, day,
	)
	return rows, err
}

// FindTotalsByTickerAndDay returns the stored footer summary for a ticker+day.
func (r *BrokerStockSummaryRepository) FindTotalsByTickerAndDay(db *sqlx.DB, ticker string, day time.Time) (*entity.BrokerStockSummaryTotals, error) {
	var totals entity.BrokerStockSummaryTotals
	err := db.Get(&totals,
		"SELECT * FROM broker_stock_summary_totals WHERE ticker = $1 AND trading_day = $2",
		ticker, day,
	)
	if err != nil {
		return nil, err
	}
	return &totals, nil
}

// FindByTickerAndDateRange returns the stored broker rows for a ticker between
// two trading days (inclusive), ordered by trading day then side/rank. An
// empty range returns an empty slice, no error.
func (r *BrokerStockSummaryRepository) FindByTickerAndDateRange(db *sqlx.DB, ticker string, from, to time.Time) ([]entity.BrokerStockSummary, error) {
	var rows []entity.BrokerStockSummary
	err := db.Select(&rows,
		"SELECT * FROM broker_stock_summaries WHERE ticker = $1 AND trading_day BETWEEN $2 AND $3 ORDER BY trading_day, side, rank",
		ticker, from, to,
	)
	return rows, err
}

// FindByDateRangeAll returns the stored broker rows for every ticker between
// two trading days (inclusive), ordered by trading day then ticker. Feeds
// get_broker_net_flow's market-wide mode.
func (r *BrokerStockSummaryRepository) FindByDateRangeAll(db *sqlx.DB, from, to time.Time) ([]entity.BrokerStockSummary, error) {
	var rows []entity.BrokerStockSummary
	err := db.Select(&rows,
		"SELECT * FROM broker_stock_summaries WHERE trading_day BETWEEN $1 AND $2 ORDER BY trading_day, ticker, side, rank",
		from, to,
	)
	return rows, err
}

// FindTotalsByTickerAndDateRange returns the stored footer summaries for a
// ticker between two trading days (inclusive), ordered by trading day.
func (r *BrokerStockSummaryRepository) FindTotalsByTickerAndDateRange(db *sqlx.DB, ticker string, from, to time.Time) ([]entity.BrokerStockSummaryTotals, error) {
	var totals []entity.BrokerStockSummaryTotals
	err := db.Select(&totals,
		"SELECT * FROM broker_stock_summary_totals WHERE ticker = $1 AND trading_day BETWEEN $2 AND $3 ORDER BY trading_day",
		ticker, from, to,
	)
	return totals, err
}

// DeleteOlderThan deletes broker_stock_summaries rows whose trading_day is
// older than the retention window. Returns rows deleted.
func (r *BrokerStockSummaryRepository) DeleteOlderThan(db *sqlx.DB, days int) (int64, error) {
	res, err := db.Exec(
		"DELETE FROM broker_stock_summaries WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DeleteTotalsOlderThan deletes broker_stock_summary_totals rows whose
// trading_day is older than the retention window. Returns rows deleted.
func (r *BrokerStockSummaryRepository) DeleteTotalsOlderThan(db *sqlx.DB, days int) (int64, error) {
	res, err := db.Exec(
		"DELETE FROM broker_stock_summary_totals WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
