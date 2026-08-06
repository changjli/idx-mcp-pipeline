package repository

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type AnomalyRepository struct {
	*Repository[entity.Anomaly]
	Log *logrus.Logger
}

func NewAnomalyRepository(log *logrus.Logger) *AnomalyRepository {
	return &AnomalyRepository{
		Repository: &Repository[entity.Anomaly]{},
		Log:        log,
	}
}

func (r *AnomalyRepository) Insert(db *sqlx.DB, anomaly *entity.Anomaly) error {
	query := `
		INSERT INTO anomalies (ticker, trading_day, type, direction, magnitude_pct, baseline_ref, observed_value, prior_value)
		VALUES (:ticker, :trading_day, :type, :direction, :magnitude_pct, :baseline_ref, :observed_value, :prior_value)
		ON CONFLICT (ticker, trading_day, type) DO UPDATE SET
			direction = EXCLUDED.direction,
			magnitude_pct = EXCLUDED.magnitude_pct,
			baseline_ref = EXCLUDED.baseline_ref,
			observed_value = EXCLUDED.observed_value,
			prior_value = EXCLUDED.prior_value
	`
	_, err := db.NamedExec(query, anomaly)
	return err
}

func (r *AnomalyRepository) FindByDate(db *sqlx.DB, tradingDay string) ([]entity.Anomaly, error) {
	var anomalies []entity.Anomaly
	err := db.Select(&anomalies,
		"SELECT * FROM anomalies WHERE trading_day = $1 ORDER BY ticker",
		tradingDay,
	)
	return anomalies, err
}

func (r *AnomalyRepository) FindByTickerAndDate(db *sqlx.DB, ticker string, tradingDay string) ([]entity.Anomaly, error) {
	var anomalies []entity.Anomaly
	err := db.Select(&anomalies,
		"SELECT * FROM anomalies WHERE ticker = $1 AND trading_day = $2 ORDER BY type",
		ticker, tradingDay,
	)
	return anomalies, err
}

func (r *AnomalyRepository) ExistsForDate(db *sqlx.DB, tradingDay string) (bool, error) {
	var count int
	err := db.Get(&count,
		"SELECT COUNT(*) FROM anomalies WHERE trading_day = $1",
		tradingDay,
	)
	return count > 0, err
}
