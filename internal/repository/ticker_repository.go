package repository

import (
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
