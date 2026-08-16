package repository

import (
	"time"

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

// ExistsForTickerInWindow reports whether an anomaly exists for the ticker
// with trading_day within [announcementDate, announcementDate + lookbackDays].
// The anomaly-gate (ticket 11): a disclosure's market impact can lag its
// announcement by days, so the match window extends forward from the
// announcement date, not just the same day.
func (r *AnomalyRepository) ExistsForTickerInWindow(db *sqlx.DB, ticker string, announcementDate time.Time, lookbackDays int) (bool, error) {
	end := announcementDate.AddDate(0, 0, lookbackDays)
	var exists bool
	err := db.Get(&exists, `
		SELECT EXISTS(
			SELECT 1 FROM anomalies
			WHERE ticker = $1 AND trading_day BETWEEN $2 AND $3
		)
	`, ticker, announcementDate, end)
	return exists, err
}
