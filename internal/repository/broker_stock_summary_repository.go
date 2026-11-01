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
// single transaction. Rows that disappeared from a refetch are deleted (not
// accumulated), and the totals row is written in the same transaction so rows
// and totals can never diverge. Idempotent: refetching the same day yields the
// same rows, no duplicates.
//
// A caller that leaves Source empty is seeding a legacy-shaped row (the DB
// tests do this); production writers always state live, sweep or backfill.
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

	for i := range rows {
		if rows[i].Source == "" {
			rows[i].Source = entity.BrokerSourceLegacyIPOT
		}
	}
	if totals != nil && totals.Source == "" {
		totals.Source = entity.BrokerSourceLegacyIPOT
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Replace the day's rows wholesale so brokers that disappeared from a
	// refetch are removed, not kept as stale rows. Runs even when rows is empty
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
			INSERT INTO broker_stock_summary_totals (ticker, trading_day, t_val, f_nval, t_lot, avg, source)
			VALUES (:ticker, :trading_day, :t_val, :f_nval, :t_lot, :avg, :source)
			ON CONFLICT (ticker, trading_day) DO UPDATE SET
				t_val = EXCLUDED.t_val,
				f_nval = EXCLUDED.f_nval,
				t_lot = EXCLUDED.t_lot,
				avg = EXCLUDED.avg,
				source = EXCLUDED.source
		`, totals); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// buildMultiRowUpsert builds a single multi-row INSERT ... ON CONFLICT for a
// batch of broker rows (one round trip instead of one per row).
func buildMultiRowUpsert(rows []entity.BrokerStockSummary) (string, []interface{}, error) {
	const cols = 9 // ticker, broker_code, side, trading_day, lot, value, avg_price, rank, source
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9,
		))
		args = append(args, r.Ticker, r.BrokerCode, r.Side, r.TradingDay, r.Lot, r.Value, r.AvgPrice, r.Rank, r.Source)
	}
	query := fmt.Sprintf(`
		INSERT INTO broker_stock_summaries (ticker, broker_code, side, trading_day, lot, value, avg_price, rank, source)
		VALUES %s
		ON CONFLICT (ticker, trading_day, broker_code, side) DO UPDATE SET
			lot = EXCLUDED.lot,
			value = EXCLUDED.value,
			avg_price = EXCLUDED.avg_price,
			rank = EXCLUDED.rank,
			source = EXCLUDED.source
	`, strings.Join(valueStrings, ","))
	return query, args, nil
}

// HasStoredDay reports whether any broker_stock_summaries rows exist for a
// ticker+day. Cheap EXISTS check — the sweep's skip-if-stored guard.
// Idempotency note: rows present means the day is already covered, so the
// sweep skips the ticker entirely; a race between the sweep and an on-demand
// refetch is harmless because UpsertDay replaces wholesale.
func (r *BrokerStockSummaryRepository) HasStoredDay(db *sqlx.DB, ticker string, day time.Time) (bool, error) {
	var exists bool
	err := db.Get(&exists,
		"SELECT EXISTS(SELECT 1 FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2)",
		ticker, day,
	)
	return exists, err
}

// StoredTickersForDay reports which of the given tickers already have stored
// rows for a trading day, in one query. It is the batch form of HasStoredDay:
// the sweep resolves the whole candidate list's coverage before planning, so a
// re-run of the same anchor day (manual, self-heal, or a second worker) never
// pays for a ticker-day twice.
func (r *BrokerStockSummaryRepository) StoredTickersForDay(db *sqlx.DB, day time.Time, tickers []string) (map[string]bool, error) {
	out := make(map[string]bool, len(tickers))
	if len(tickers) == 0 {
		return out, nil
	}
	var stored []string
	err := db.Select(&stored, `
		SELECT DISTINCT ticker
		FROM broker_stock_summaries
		WHERE trading_day = $1::date
		  AND ticker = ANY(string_to_array($2, ','))`,
		day.Format("2006-01-02"), strings.Join(tickers, ","))
	if err != nil {
		return nil, err
	}
	for _, t := range stored {
		out[t] = true
	}
	return out, nil
}

// LastObservedByTicker returns, for each given ticker, the most recent trading
// day with a stored tally at or before upTo. It is the sweep's rotation key:
// tickers observed longest ago sort first, so a budget-limited sweep cycles
// through the universe instead of re-fetching the same liquid names every day.
// A ticker with no stored day is absent from the map (never observed).
func (r *BrokerStockSummaryRepository) LastObservedByTicker(db *sqlx.DB, tickers []string, upTo time.Time) (map[string]time.Time, error) {
	out := make(map[string]time.Time, len(tickers))
	if len(tickers) == 0 {
		return out, nil
	}
	var rows []struct {
		Ticker string    `db:"ticker"`
		Last   time.Time `db:"last_day"`
	}
	err := db.Select(&rows, `
		SELECT ticker, MAX(trading_day) AS last_day
		FROM broker_stock_summary_totals
		WHERE trading_day <= $1::date
		  AND ticker = ANY(string_to_array($2, ','))
		GROUP BY ticker`,
		upTo.Format("2006-01-02"), strings.Join(tickers, ","))
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[r.Ticker] = r.Last
	}
	return out, nil
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

// FindTotalsByDateRangeAll returns the stored footer summaries for every
// ticker between two trading days (inclusive), ordered by trading day then
// ticker. Feeds get_sector_flow's foreign net and per-ticker observed-day
// coverage.
func (r *BrokerStockSummaryRepository) FindTotalsByDateRangeAll(db *sqlx.DB, from, to time.Time) ([]entity.BrokerStockSummaryTotals, error) {
	var totals []entity.BrokerStockSummaryTotals
	err := db.Select(&totals,
		"SELECT * FROM broker_stock_summary_totals WHERE trading_day BETWEEN $1 AND $2 ORDER BY trading_day, ticker",
		from, to,
	)
	return totals, err
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

// TickerForeignNet is one ticker's stored foreign net over a flow window, with
// the day count that makes it readable: Days is how many of the window's
// trading days had a stored foreign net at all. A ticker with no stored broker
// rows is absent from the result rather than present with a zero — "not
// observed" and "net zero" are different answers, and the screener renders them
// differently.
type TickerForeignNet struct {
	Ticker     string `db:"ticker"`
	ForeignNet int64  `db:"foreign_net"`
	Days       int    `db:"days"`
}

// ForeignNetWindow is the window a foreign-net read actually covered: its first
// stored trading day and how many market-wide trading days it spans. The
// denominator is what makes a row's day count interpretable ("7 of 20") instead
// of an unanchored number, so a caller can tell a thin window from a thin
// ticker.
type ForeignNetWindow struct {
	From      time.Time
	TradeDays int
	Rows      []TickerForeignNet
}

// SumForeignNetByTickers sums the stored foreign net (f_nval) of the footer
// totals for each given ticker over the `tradingDays` most recent market-wide
// trading days at or before `to`. It is the screener's broker-flow column and
// Broker Confirmation's read: a pure DB read at the ticker-day grain, so it
// covers foreign flow the per-broker rows cannot attribute.
//
// The window walks the same market-wide stored day axis the screener's other
// windows use, so weekends and holidays never shorten it, and it is bound as a
// calendar date (`::date`) for the reason FindByTickersUpTo documents: a
// timestamptz parameter would let the session timezone pick the day.
//
// Broker coverage is a budget-capped rotation and therefore sparse by design: a
// ticker with no stored rows in the window is simply absent from Rows, and a
// stored footer whose f_nval is null contributes no day — so a ticker whose
// every stored day had a null foreign net is absent too, rather than reported
// as a zero net over no days. Nothing here infers a missing day.
func (r *BrokerStockSummaryRepository) SumForeignNetByTickers(db *sqlx.DB, tickers []string, to time.Time, tradingDays int) (*ForeignNetWindow, error) {
	window := &ForeignNetWindow{Rows: []TickerForeignNet{}}

	// The floor and the span come from the market-wide daily_prices calendar, so
	// the denominator counts trading days the market actually had, not days the
	// sparsely covered broker table happens to have.
	var bounds struct {
		From      *time.Time `db:"from_day"`
		TradeDays int        `db:"trade_days"`
	}
	err := db.Get(&bounds, `
		WITH window_days AS (
			SELECT MIN(trading_day) AS day FROM (
				SELECT DISTINCT trading_day FROM daily_prices
				WHERE trading_day <= $1::date
				ORDER BY trading_day DESC
				LIMIT $2
			) recent
		)
		SELECT w.day AS from_day,
			(SELECT COUNT(DISTINCT trading_day) FROM daily_prices
			 WHERE trading_day BETWEEN w.day AND $1::date) AS trade_days
		FROM window_days w`,
		to.Format("2006-01-02"), tradingDays)
	if err != nil {
		return nil, err
	}
	if bounds.From == nil {
		// No stored trading day at or before `to` — nothing to sum over, and the
		// caller's rows are all unobserved. Not an error: an empty market is a
		// legitimate (if empty) answer.
		return window, nil
	}
	window.From = *bounds.From
	window.TradeDays = bounds.TradeDays

	if len(tickers) == 0 {
		return window, nil
	}

	// The ticker list rides as a comma-joined string split in SQL: ticker codes
	// are 2-6 uppercase letters, so the delimiter can never appear in one, and an
	// array parameter would depend on how the active driver binds []string (the
	// pgx stdlib driver the server connects with differs from lib/pq) — the same
	// reason FindByTickersUpTo does it this way.
	err = db.Select(&window.Rows, `
		SELECT t.ticker,
			COALESCE(SUM(t.f_nval), 0)::bigint AS foreign_net,
			COUNT(t.f_nval)::int AS days
		FROM broker_stock_summary_totals t
		WHERE t.ticker = ANY(string_to_array($1, ','))
			AND t.trading_day BETWEEN $2::date AND $3::date
		GROUP BY t.ticker
		HAVING COUNT(t.f_nval) > 0
		ORDER BY t.ticker`,
		strings.Join(tickers, ","), window.From.Format("2006-01-02"), to.Format("2006-01-02"))
	if err != nil {
		return nil, err
	}
	return window, nil
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
