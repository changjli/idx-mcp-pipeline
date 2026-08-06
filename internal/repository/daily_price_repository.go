package repository

import (
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

func (r *DailyPriceRepository) DeleteOlderThan(db *sqlx.DB, days int) error {
	_, err := db.Exec(
		"DELETE FROM daily_prices WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	return err
}
