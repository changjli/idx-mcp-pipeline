package repository

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type SourceStatusRepository struct {
	*Repository[entity.SourceStatus]
	Log *logrus.Logger
}

func NewSourceStatusRepository(log *logrus.Logger) *SourceStatusRepository {
	return &SourceStatusRepository{
		Repository: &Repository[entity.SourceStatus]{},
		Log:        log,
	}
}

func (r *SourceStatusRepository) Upsert(db *sqlx.DB, status *entity.SourceStatus) error {
	query := `
		INSERT INTO source_status (source, last_success_at, last_attempt_at, last_error, consecutive_failures, stale, max_age_seconds, high_water_mark)
		VALUES (:source, :last_success_at, :last_attempt_at, :last_error, :consecutive_failures, :stale, :max_age_seconds, :high_water_mark)
		ON CONFLICT (source) DO UPDATE SET
			last_success_at = EXCLUDED.last_success_at,
			last_attempt_at = EXCLUDED.last_attempt_at,
			last_error = EXCLUDED.last_error,
			consecutive_failures = EXCLUDED.consecutive_failures,
			stale = EXCLUDED.stale,
			high_water_mark = EXCLUDED.high_water_mark
	`
	_, err := db.NamedExec(query, status)
	return err
}

func (r *SourceStatusRepository) FindAll(db *sqlx.DB) ([]entity.SourceStatus, error) {
	var statuses []entity.SourceStatus
	err := db.Select(&statuses, "SELECT * FROM source_status ORDER BY source")
	return statuses, err
}

func (r *SourceStatusRepository) FindBySource(db *sqlx.DB, source string) (*entity.SourceStatus, error) {
	var status entity.SourceStatus
	err := db.Get(&status, "SELECT * FROM source_status WHERE source = $1", source)
	if err != nil {
		return nil, err
	}
	return &status, nil
}
