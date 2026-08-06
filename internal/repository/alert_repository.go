package repository

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type AlertRepository struct {
	*Repository[entity.Alert]
	Log *logrus.Logger
}

func NewAlertRepository(log *logrus.Logger) *AlertRepository {
	return &AlertRepository{
		Repository: &Repository[entity.Alert]{},
		Log:        log,
	}
}

func (r *AlertRepository) Insert(db *sqlx.DB, alert *entity.Alert) error {
	query := `
		INSERT INTO alerts (source, alert_type, message, raised_at)
		VALUES (:source, :alert_type, :message, NOW())
	`
	_, err := db.NamedExec(query, alert)
	return err
}

func (r *AlertRepository) FindRecent(db *sqlx.DB, limit int) ([]entity.Alert, error) {
	var alerts []entity.Alert
	err := db.Select(&alerts,
		"SELECT * FROM alerts ORDER BY raised_at DESC LIMIT $1",
		limit,
	)
	return alerts, err
}

func (r *AlertRepository) DeleteOlderThan(db *sqlx.DB, days int) error {
	_, err := db.Exec(
		"DELETE FROM alerts WHERE raised_at < NOW() - make_interval(days => $1)",
		days,
	)
	return err
}
