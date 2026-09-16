package repository

import (
	"database/sql"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type DailyPriceRepository struct {
	*Repository[entity.DailyPrice]
	Log *logrus.Logger
}

func NewDailyPriceRepository(log *logrus.Logger) *DailyPriceRepository {
	return &DailyPriceRepository{
		Repository: &Repository[entity.DailyPrice]{},
		Log:        log,
	}
}

func (r *DailyPriceRepository) Upsert(db *sqlx.DB, price *entity.DailyPrice) error {
	query := `
		INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source, fetched_at)
		VALUES (:ticker, :trading_day, :open, :high, :low, :close, :volume, :value, :frequency, :source, NOW())
		ON CONFLICT (ticker, trading_day) DO UPDATE SET
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			value = EXCLUDED.value,
			frequency = EXCLUDED.frequency,
			source = EXCLUDED.source,
			fetched_at = NOW()
	`
	_, err := db.NamedExec(query, price)
	return err
}

func (r *DailyPriceRepository) FindByTicker(db *sqlx.DB, ticker string, limit int) ([]entity.DailyPrice, error) {
	var prices []entity.DailyPrice
	err := db.Select(&prices,
		"SELECT * FROM daily_prices WHERE ticker = $1 ORDER BY trading_day DESC LIMIT $2",
		ticker, limit,
	)
	return prices, err
}

func (r *DailyPriceRepository) FindByTickerAndDay(db *sqlx.DB, ticker string, tradingDay string) (*entity.DailyPrice, error) {
	var price entity.DailyPrice
	err := db.Get(&price,
		"SELECT * FROM daily_prices WHERE ticker = $1 AND trading_day = $2",
		ticker, tradingDay,
	)
	if err != nil {
		return nil, err
	}
	return &price, nil
}

// LatestTradingDay returns the most recent trading day with a stored EOD row
// for a ticker. Data presence is the trading-day signal (no separate calendar).
// Returns sql.ErrNoRows when the ticker has no price history.
func (r *DailyPriceRepository) LatestTradingDay(db *sqlx.DB, ticker string) (*time.Time, error) {
	var day sql.NullTime
	err := db.Get(&day,
		"SELECT MAX(trading_day) FROM daily_prices WHERE ticker = $1",
		ticker,
	)
	if err != nil {
		return nil, err
	}
	if !day.Valid {
		return nil, sql.ErrNoRows
	}
	return &day.Time, nil
}

// LatestTradingDayAll returns the most recent trading day with any stored EOD
// row across all tickers. Used by MCP tools that default their date argument to
// the most recent trading day. Returns sql.ErrNoRows when daily_prices is empty.
func (r *DailyPriceRepository) LatestTradingDayAll(db *sqlx.DB) (*time.Time, error) {
	var day sql.NullTime
	err := db.Get(&day, "SELECT MAX(trading_day) FROM daily_prices")
	if err != nil {
		return nil, err
	}
	if !day.Valid {
		return nil, sql.ErrNoRows
	}
	return &day.Time, nil
}

// TradingDaysInRange returns the distinct trading days with a stored EOD row
// for a ticker between two dates (inclusive), ascending. Data presence is the
// trading-day signal — weekends and IDX holidays have no row, so they are
// naturally excluded without a calendar.
func (r *DailyPriceRepository) TradingDaysInRange(db *sqlx.DB, ticker string, from, to time.Time) ([]time.Time, error) {
	var days []time.Time
	err := db.Select(&days,
		"SELECT DISTINCT trading_day FROM daily_prices WHERE ticker = $1 AND trading_day BETWEEN $2 AND $3 ORDER BY trading_day",
		ticker, from, to,
	)
	return days, err
}

// TradingDaysInRangeAll returns the distinct trading days with any stored EOD
// row between two dates (inclusive), ascending — the market-wide trading-day
// calendar, used for coverage math in get_broker_net_flow.
func (r *DailyPriceRepository) TradingDaysInRangeAll(db *sqlx.DB, from, to time.Time) ([]time.Time, error) {
	var days []time.Time
	err := db.Select(&days,
		"SELECT DISTINCT trading_day FROM daily_prices WHERE trading_day BETWEEN $1 AND $2 ORDER BY trading_day",
		from, to,
	)
	return days, err
}

// FindByTickerAndDateRange returns the OHLCV rows for a ticker between two
// dates (inclusive), ascending by trading day.
func (r *DailyPriceRepository) FindByTickerAndDateRange(db *sqlx.DB, ticker string, from, to time.Time) ([]entity.DailyPrice, error) {
	var prices []entity.DailyPrice
	err := db.Select(&prices,
		"SELECT * FROM daily_prices WHERE ticker = $1 AND trading_day BETWEEN $2 AND $3 ORDER BY trading_day",
		ticker, from, to,
	)
	return prices, err
}

// FindByTickerUpTo returns the most recent `limit` OHLCV rows for a ticker at
// or before `to`, ascending by trading day — the window the indicator registry
// reads. Anchoring on stored rows (not a calendar span) means an indicator's
// warm-up is counted in trading days, so a short window is never padded by
// weekends or IDX holidays; the inner ORDER BY DESC uses the
// (ticker, trading_day DESC) index.
func (r *DailyPriceRepository) FindByTickerUpTo(db *sqlx.DB, ticker string, to time.Time, limit int) ([]entity.DailyPrice, error) {
	var prices []entity.DailyPrice
	err := db.Select(&prices, `
		SELECT * FROM (
			SELECT * FROM daily_prices
			WHERE ticker = $1 AND trading_day <= $2
			ORDER BY trading_day DESC
			LIMIT $3
		) recent
		ORDER BY trading_day`,
		ticker, to, limit,
	)
	return prices, err
}

// ScreenCandidate is one ticker that cleared the screener's SQL hard filters:
// its anchor-day transaction value and close, which the funnel reports and the
// ranking sorts on.
type ScreenCandidate struct {
	Ticker string   `db:"ticker"`
	Value  *int64   `db:"value"`
	Close  *float64 `db:"close"`
}

// ScreenUniverse counts the tickers with any stored row inside the `window`
// most recent trading days at or before anchor — the screener funnel's first
// number, the population the hard filters cut down.
//
// The window is counted in market-wide stored trading days (the DISTINCT
// trading_day axis daily_prices itself defines), never in calendar days, so
// weekends and IDX holidays never pad it — the same rule the indicator window
// uses. Anchoring on days that carry data also means "the universe" is exactly
// the population a screen can read, not every code that ever traded.
//
// The anchor is bound as a calendar day (YYYY-MM-DD, cast with ::date in SQL)
// rather than as an instant: trading_day is a DATE, and comparing it to a
// timestamptz would let the session timezone decide which day the screen reads
// — the equality against the anchor would silently match nothing in a timezone
// west of UTC, emptying the funnel. FindByTickerAndDay takes a day string for
// the same reason.
func (r *DailyPriceRepository) ScreenUniverse(db *sqlx.DB, anchor time.Time, window int) (int, error) {
	var count int
	err := db.Get(&count, `
		WITH recent_days AS (
			SELECT DISTINCT trading_day FROM daily_prices
			WHERE trading_day <= $1::date
			ORDER BY trading_day DESC
			LIMIT $2
		)
		SELECT COUNT(DISTINCT p.ticker)
		FROM daily_prices p
		JOIN recent_days d ON d.trading_day = p.trading_day`,
		anchor.Format("2006-01-02"), window)
	return count, err
}

// ScreenCandidates returns the tickers that clear every SQL hard filter for an
// anchor day: a stored row ON the anchor day (the ticker is still trading — a
// halted or delisted name's latest row is older, so it drops out), a stored
// transaction value at or above minValue, and no suspension-related event in
// the last suspensionDays trading days.
//
// The suspension window is counted in trading days by walking the same stored
// day axis back from the anchor, and matched against suspensions.event_date,
// which is the listing/action date. UMA and "trading resumed" (UPT) events
// count as well as suspensions (SPT): a resume inside the window means the
// ticker was suspended inside the window, which is exactly what the filter
// guards against — so the check is a plain event-date match, with no per-type
// rule to explain in the response.
//
// A row with no stored value never clears a liquidity floor (unknown is not
// liquid), so it is excluded even at minValue 0. Candidates come back ordered
// by ticker, so identical inputs produce identical funnel output.
func (r *DailyPriceRepository) ScreenCandidates(db *sqlx.DB, anchor time.Time, minValue int64, suspensionDays int) ([]ScreenCandidate, error) {
	var candidates []ScreenCandidate
	err := db.Select(&candidates, `
		WITH suspension_from AS (
			SELECT MIN(trading_day) AS day FROM (
				SELECT DISTINCT trading_day FROM daily_prices
				WHERE trading_day <= $1::date
				ORDER BY trading_day DESC
				LIMIT $3
			) window_days
		)
		SELECT p.ticker, p.value, p.close
		FROM daily_prices p
		WHERE p.trading_day = $1::date
			AND p.value IS NOT NULL
			AND p.value >= $2
			AND NOT EXISTS (
				SELECT 1 FROM suspensions s, suspension_from f
				WHERE s.ticker = p.ticker
					AND s.event_date >= f.day
					AND s.event_date <= $1::date
			)
		ORDER BY p.ticker`,
		anchor.Format("2006-01-02"), minValue, suspensionDays)
	return candidates, err
}

// FindByTickersUpTo returns the most recent `limit` OHLCV rows for each of the
// given tickers at or before `to`, ascending by ticker then trading day. It is
// the screener's survivor read: one round trip covers every ticker that cleared
// the SQL hard filters, and a ticker the filters dropped is never read at all.
//
// The LATERAL join applies the per-ticker LIMIT through the
// (ticker, trading_day DESC) index, so the work is proportional to the
// survivors rather than to the stored history behind them. The ticker list
// rides as a comma-joined string split in SQL (ticker codes are 2-6 uppercase
// letters, so the delimiter can never appear in one) — an array parameter would
// depend on how the active driver binds []string, which differs between the
// pgx stdlib driver the server connects with and lib/pq.
func (r *DailyPriceRepository) FindByTickersUpTo(db *sqlx.DB, tickers []string, to time.Time, limit int) ([]entity.DailyPrice, error) {
	if len(tickers) == 0 {
		return nil, nil
	}
	var prices []entity.DailyPrice
	err := db.Select(&prices, `
		SELECT p.*
		FROM unnest(string_to_array($1, ',')) AS t(ticker)
		CROSS JOIN LATERAL (
			SELECT * FROM daily_prices
			WHERE ticker = t.ticker AND trading_day <= $2::date
			ORDER BY trading_day DESC
			LIMIT $3
		) p
		ORDER BY p.ticker, p.trading_day`,
		strings.Join(tickers, ","), to.Format("2006-01-02"), limit)
	return prices, err
}

// DeleteOlderThan deletes daily_prices rows whose trading_day is older than
// the retention window. Returns the number of rows deleted (0 on a re-run —
// the delete is idempotent).
func (r *DailyPriceRepository) DeleteOlderThan(db *sqlx.DB, days int) (int64, error) {
	res, err := db.Exec(
		"DELETE FROM daily_prices WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// AnomalyCandidate is one row of the anomaly detection query: a ticker that
// traded on the target day, plus its volume baseline and prior close.
type AnomalyCandidate struct {
	Ticker         string   `db:"ticker"`
	TodayVolume    *int64   `db:"today_volume"`
	TodayValue     *int64   `db:"today_value"`
	TodayClose     *float64 `db:"today_close"`
	BaselineVolume *float64 `db:"baseline_volume"`
	BaselineDays   int      `db:"baseline_days"`
	PrevClose      *float64 `db:"prev_close"`
}

// ExistsForDate reports whether any daily_prices rows exist for a trading day.
func (r *DailyPriceRepository) ExistsForDate(db *sqlx.DB, tradingDay string) (bool, error) {
	var count int
	err := db.Get(&count,
		"SELECT COUNT(*) FROM daily_prices WHERE trading_day = $1",
		tradingDay,
	)
	return count > 0, err
}

// ADTVEligibleTickers returns the tickers that clear the liquidity floor over
// a trailing window: at least minDays trading days and average daily trade
// value >= minADTV (Rp). Computed fresh from daily_prices each call — the
// weekly sweep's universe filter (issue 14b). Membership drifts with the
// market: a new IPO enters once it accumulates enough trading days, a
// suspended/dead name drops out when its window ADTV falls below the floor.
// The minADTV bar is the anomaly detector's own DefaultADTVMinValue, so one
// liquidity definition spans the pipeline.
func (r *DailyPriceRepository) ADTVEligibleTickers(db *sqlx.DB, from, to time.Time, minDays int, minADTV int64) ([]string, error) {
	var tickers []string
	err := db.Select(&tickers, `
		SELECT ticker FROM daily_prices
		WHERE trading_day BETWEEN $1 AND $2
		GROUP BY ticker
		HAVING COUNT(*) >= $3 AND AVG(value) >= $4
		ORDER BY ticker`,
		from, to, minDays, minADTV,
	)
	return tickers, err
}

// AnomalyCandidates returns, per ticker that traded on the given day, the
// today volume/close, the 20-day volume baseline (recomputed on read from
// daily_prices via the (ticker, trading_day DESC) index), and the prior
// trading day's close. No stored baseline column.
func (r *DailyPriceRepository) AnomalyCandidates(db *sqlx.DB, tradingDay time.Time) ([]AnomalyCandidate, error) {
	query := `
		WITH today AS (
			SELECT ticker, volume, value, close
			FROM daily_prices
			WHERE trading_day = $1
		),
		hist AS (
			SELECT ticker, volume, close,
			       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trading_day DESC) AS rn
			FROM daily_prices
			WHERE trading_day < $1
		),
		baseline AS (
			SELECT ticker,
			       AVG(volume) AS baseline_volume,
			       COUNT(*)    AS baseline_days
			FROM hist
			WHERE rn <= 20
			GROUP BY ticker
		),
		prev AS (
			SELECT ticker, close AS prev_close
			FROM hist
			WHERE rn = 1
		)
		SELECT t.ticker,
		       t.volume AS today_volume,
		       t.value  AS today_value,
		       t.close  AS today_close,
		       b.baseline_volume,
		       COALESCE(b.baseline_days, 0) AS baseline_days,
		       p.prev_close
		FROM today t
		LEFT JOIN baseline b ON b.ticker = t.ticker
		LEFT JOIN prev p ON p.ticker = t.ticker
		ORDER BY t.ticker
	`
	var candidates []AnomalyCandidate
	err := db.Select(&candidates, query, tradingDay)
	return candidates, err
}
