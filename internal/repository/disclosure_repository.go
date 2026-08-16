package repository

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// DisclosureFilterLookbackDays is the anomaly-gate lookback window: a
// disclosure matches an anomaly when its announcement_date falls within this
// many days before the anomaly's trading_day. Shared by the filter task and
// the read-time join (ticket 10).
const DisclosureFilterLookbackDays = 7

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

// FindByTickerWithDate returns a ticker's disclosures (metadata only — the
// caller selects fields), optionally filtered to one announcement date, newest
// first. date is "YYYY-MM-DD"; nil means all dates.
func (r *DisclosureRepository) FindByTickerWithDate(db *sqlx.DB, ticker string, date *string, limit int) ([]entity.Disclosure, error) {
	var rows []entity.Disclosure
	err := db.Select(&rows, `
		SELECT * FROM disclosures
		WHERE ticker = $1
		  AND ($2::date IS NULL OR announcement_date = $2::date)
		ORDER BY announcement_date DESC, id DESC
		LIMIT $3
	`, ticker, date, limit)
	return rows, err
}

// FindPendingForFilter returns the disclosures the filter task (ticket 11)
// should process on the given run date:
//   - never-filtered rows (passed_filter IS NULL, any date — catch-up for
//     days the filter didn't run);
//   - rejected rows announced within the 7-day lookback (re-checked for
//     delayed anomalies — a disclosure's market impact can lag its
//     announcement by days);
//   - passing rows still awaiting extraction (re-enqueue extract so a missed
//     or R2-less run self-heals).
//
// Passing rows are sticky: once true, the gate is never re-evaluated.
func (r *DisclosureRepository) FindPendingForFilter(db *sqlx.DB, today time.Time) ([]entity.Disclosure, error) {
	lookback := today.AddDate(0, 0, -DisclosureFilterLookbackDays)
	var rows []entity.Disclosure
	err := db.Select(&rows, `
		SELECT * FROM disclosures
		WHERE passed_filter IS NULL
		   OR (passed_filter = false AND announcement_date >= $1)
		   OR (passed_filter = true AND extraction_status = 'pending')
		ORDER BY announcement_date, id
	`, lookback)
	return rows, err
}

// ExistsFetchedOnDate reports whether any disclosure was upserted (fetched_at)
// on the given calendar day. It is the "idx:announcements ran today" marker
// used by filter:disclosures self-sync — announcements stamps fetched_at=NOW()
// on every upserted row, so a row fetched on day D means announcements
// completed for D. date is "YYYY-MM-DD". The day-range predicate keeps the
// query sargable if an index on fetched_at is added later.
func (r *DisclosureRepository) ExistsFetchedOnDate(db *sqlx.DB, date string) (bool, error) {
	var ok bool
	err := db.Get(&ok,
		`SELECT EXISTS(SELECT 1 FROM disclosures
		   WHERE fetched_at >= $1::date AND fetched_at < ($1::date + interval '1 day'))`,
		date,
	)
	return ok, err
}

// MarkFiltered records the filter verdict. categories is stored only for
// passing rows; rejected rows get NULL.
func (r *DisclosureRepository) MarkFiltered(db *sqlx.DB, id int64, passed bool, categories []string) error {
	var cats any
	if passed {
		cats = pq.StringArray(categories)
	}
	_, err := db.Exec(
		"UPDATE disclosures SET passed_filter = $2, categories = $3 WHERE id = $1",
		id, passed, cats,
	)
	return err
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
