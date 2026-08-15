package repository

import (
	"database/sql"
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

func (r *DailyPriceRepository) DeleteOlderThan(db *sqlx.DB, days int) error {
	_, err := db.Exec(
		"DELETE FROM daily_prices WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	return err
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
