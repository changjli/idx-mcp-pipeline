package repository

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type BrokerRepository struct {
	*Repository[entity.BrokerSummary]
	Log *logrus.Logger
}

func NewBrokerRepository(log *logrus.Logger) *BrokerRepository {
	return &BrokerRepository{
		Repository: &Repository[entity.BrokerSummary]{},
		Log:        log,
	}
}

func (r *BrokerRepository) Upsert(db *sqlx.DB, summary *entity.BrokerSummary) error {
	query := `
		INSERT INTO broker_summaries (broker_code, trading_day, firm_name, volume, value, frequency)
		VALUES (:broker_code, :trading_day, :firm_name, :volume, :value, :frequency)
		ON CONFLICT (broker_code, trading_day) DO UPDATE SET
			firm_name = EXCLUDED.firm_name,
			volume = EXCLUDED.volume,
			value = EXCLUDED.value,
			frequency = EXCLUDED.frequency
	`
	_, err := db.NamedExec(query, summary)
	return err
}

func (r *BrokerRepository) FindByDate(db *sqlx.DB, tradingDay string) ([]entity.BrokerSummary, error) {
	var summaries []entity.BrokerSummary
	err := db.Select(&summaries,
		"SELECT * FROM broker_summaries WHERE trading_day = $1 ORDER BY broker_code",
		tradingDay,
	)
	return summaries, err
}

func (r *BrokerRepository) DeleteOlderThan(db *sqlx.DB, days int) error {
	_, err := db.Exec(
		"DELETE FROM broker_summaries WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	return err
}
