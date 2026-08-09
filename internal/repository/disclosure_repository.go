package repository

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type DisclosureRepository struct {
	*Repository[entity.Disclosure]
	Log *logrus.Logger
}

func NewDisclosureRepository(log *logrus.Logger) *DisclosureRepository {
	return &DisclosureRepository{
		Repository: &Repository[entity.Disclosure]{},
		Log:        log,
	}
}

// Upsert inserts or updates a disclosure keyed by the unique pdf_url.
// On conflict it refreshes only the metadata, preserving extraction results.
// passed_filter and extraction_status are NOT overwritten on re-fetch — they
// are owned by the downstream filter/extraction pipeline (tickets 10-11).
// extraction_status is omitted from the INSERT so the DB default ('pending')
// applies; an empty entity value would violate the column's CHECK constraint.
func (r *DisclosureRepository) Upsert(db *sqlx.DB, disclosure *entity.Disclosure) error {
	query := `
		INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, attachment_idx, categories, passed_filter, fetched_at)
		VALUES (:ticker, :announcement_date, :title, :pdf_url, :attachment_idx, :categories, :passed_filter, NOW())
		ON CONFLICT (pdf_url) DO UPDATE SET
			ticker = EXCLUDED.ticker,
			announcement_date = EXCLUDED.announcement_date,
			title = EXCLUDED.title,
			attachment_idx = EXCLUDED.attachment_idx,
			categories = EXCLUDED.categories,
			fetched_at = NOW()
	`
	_, err := db.NamedExec(query, disclosure)
	return err
}

func (r *DisclosureRepository) FindByID(db *sqlx.DB, id int64) (*entity.Disclosure, error) {
	return r.Repository.FindByID(db, id)
}

func (r *DisclosureRepository) FindByTicker(db *sqlx.DB, ticker string, limit int) ([]entity.Disclosure, error) {
	var disclosures []entity.Disclosure
	err := db.Select(&disclosures,
		"SELECT * FROM disclosures WHERE ticker = $1 ORDER BY announcement_date DESC LIMIT $2",
		ticker, limit,
	)
	return disclosures, err
}

func (r *DisclosureRepository) FindByTickerAndDate(db *sqlx.DB, ticker string, date string) ([]entity.Disclosure, error) {
	var disclosures []entity.Disclosure
	err := db.Select(&disclosures,
		"SELECT * FROM disclosures WHERE ticker = $1 AND announcement_date = $2 AND passed_filter = true ORDER BY id",
		ticker, date,
	)
	return disclosures, err
}

func (r *DisclosureRepository) UpdateExtractionStatus(db *sqlx.DB, id int64, status string, r2Key *string, errMsg *string) error {
	query := `
		UPDATE disclosures SET
			extraction_status = $1,
			text_r2_key = $2,
			extraction_error = $3,
			extracted_at = CASE WHEN $1 = 'ok' THEN NOW() ELSE extracted_at END
		WHERE id = $4
	`
	_, err := db.Exec(query, status, r2Key, errMsg, id)
	return err
}
