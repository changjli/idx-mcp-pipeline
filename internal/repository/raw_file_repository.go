package repository

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type RawFileRepository struct {
	*Repository[entity.RawFile]
	Log *logrus.Logger
}

func NewRawFileRepository(log *logrus.Logger) *RawFileRepository {
	return &RawFileRepository{
		Repository: &Repository[entity.RawFile]{},
		Log:        log,
	}
}

func (r *RawFileRepository) Insert(db *sqlx.DB, rf *entity.RawFile) error {
	query := `
		INSERT INTO raw_files (storage_key, kind, source_ref, size_bytes, stored_at, retention_days)
		VALUES (:storage_key, :kind, :source_ref, :size_bytes, NOW(), :retention_days)
		ON CONFLICT (storage_key) DO UPDATE SET
			size_bytes = EXCLUDED.size_bytes,
			stored_at = NOW()
	`
	_, err := db.NamedExec(query, rf)
	return err
}

// FindByStorageKey returns the claim-check row for a stored object, or
// sql.ErrNoRows. read_idx_disclosure uses it to detect eviction (deleted_at
// set) before attempting an R2 fetch.
func (r *RawFileRepository) FindByStorageKey(db *sqlx.DB, storageKey string) (*entity.RawFile, error) {
	var rf entity.RawFile
	if err := db.Get(&rf, "SELECT * FROM raw_files WHERE storage_key = $1", storageKey); err != nil {
		return nil, err
	}
	return &rf, nil
}

func (r *RawFileRepository) MarkDeleted(db *sqlx.DB, storageKey string) error {
	_, err := db.Exec(
		"UPDATE raw_files SET deleted_at = NOW() WHERE storage_key = $1",
		storageKey,
	)
	return err
}

func (r *RawFileRepository) FindExpired(db *sqlx.DB) ([]entity.RawFile, error) {
	var files []entity.RawFile
	err := db.Select(&files, `
		SELECT * FROM raw_files
		WHERE deleted_at IS NULL
		AND stored_at < NOW() - (retention_days || ' days')::INTERVAL
		ORDER BY stored_at
	`)
	return files, err
}
