package repository

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type TickerRepository struct {
	*Repository[entity.Ticker]
	Log *logrus.Logger
}

func NewTickerRepository(log *logrus.Logger) *TickerRepository {
	return &TickerRepository{
		Repository: &Repository[entity.Ticker]{},
		Log:        log,
	}
}

func (r *TickerRepository) FindAll(db *sqlx.DB) ([]entity.Ticker, error) {
	var tickers []entity.Ticker
	err := db.Select(&tickers, "SELECT * FROM tickers WHERE active = true ORDER BY code")
	return tickers, err
}

func (r *TickerRepository) FindByCode(db *sqlx.DB, code string) (*entity.Ticker, error) {
	var ticker entity.Ticker
	err := db.Get(&ticker, "SELECT * FROM tickers WHERE code = $1", code)
	if err != nil {
		return nil, err
	}
	return &ticker, nil
}

// InsertIfAbsent inserts a minimal ticker row (code, name) only when the code
// is not already present. Unlike Upsert it never updates an existing row, so a
// light-touch caller (e.g. the news matcher seeding an FK) can't wipe the
// metadata — shares, listing info — that stock_summary populated.
func (r *TickerRepository) InsertIfAbsent(db *sqlx.DB, code, name string) error {
	if name == "" {
		name = code
	}
	_, err := db.Exec(`
		INSERT INTO tickers (code, name, active)
		VALUES ($1, $2, true)
		ON CONFLICT (code) DO NOTHING
	`, code, name)
	return err
}

func (r *TickerRepository) Upsert(db *sqlx.DB, ticker *entity.Ticker) error {
	query := `
		INSERT INTO tickers (code, name, listing_date, shares, listing_board, sektor, industri, active, first_seen_at, updated_at)
		VALUES (:code, :name, :listing_date, :shares, :listing_board, :sektor, :industri, :active, :first_seen_at, NOW())
		ON CONFLICT (code) DO UPDATE SET
			name = EXCLUDED.name,
			listing_date = EXCLUDED.listing_date,
			shares = EXCLUDED.shares,
			listing_board = EXCLUDED.listing_board,
			sektor = EXCLUDED.sektor,
			industri = EXCLUDED.industri,
			active = EXCLUDED.active,
			updated_at = NOW()
	`
	_, err := db.NamedExec(query, ticker)
	return err
}

// UpsertProfiles inserts or refreshes company-profile rows (name, listing
// board, listing date, active) keyed by code in one multi-row statement (issue
// 11b). It deliberately touches only the profile columns — shares, sektor,
// industri (populated by other sources) are left untouched, so a re-run can't
// wipe them. An empty slice is a no-op. Idempotent: re-running picks up new
// IPOs and refreshes changed boards without duplicating.
func (r *TickerRepository) UpsertProfiles(db *sqlx.DB, rows []entity.Ticker) error {
	if len(rows) == 0 {
		return nil
	}
	query, args, err := buildTickerProfileUpsert(rows)
	if err != nil {
		return err
	}
	_, err = db.Exec(query, args...)
	return err
}

// buildTickerProfileUpsert builds a single multi-row INSERT ... ON CONFLICT
// for a batch of profile rows (one round trip instead of one per row). Only
// the profile columns are written; the DO UPDATE leaves shares/sektor/industri
// alone. listing_date is COALESCE'd so a re-run with an unparseable wire date
// (nil) can't clobber a previously-good value.
func buildTickerProfileUpsert(rows []entity.Ticker) (string, []interface{}, error) {
	const cols = 5 // code, name, listing_date, listing_board, active
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5,
		))
		args = append(args, r.Code, r.Name, r.ListingDate, r.ListingBoard, r.Active)
	}
	query := fmt.Sprintf(`
		INSERT INTO tickers (code, name, listing_date, listing_board, active)
		VALUES %s
		ON CONFLICT (code) DO UPDATE SET
			name = EXCLUDED.name,
			listing_date = COALESCE(EXCLUDED.listing_date, tickers.listing_date),
			listing_board = EXCLUDED.listing_board,
			active = EXCLUDED.active,
			updated_at = NOW()
	`, strings.Join(valueStrings, ","))
	return query, args, nil
}
