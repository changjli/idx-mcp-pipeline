package repository

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type NewsTickerRepository struct {
	*Repository[entity.NewsTicker]
	Log *logrus.Logger
}

func NewNewsTickerRepository(log *logrus.Logger) *NewsTickerRepository {
	return &NewsTickerRepository{
		Repository: &Repository[entity.NewsTicker]{},
		Log:        log,
	}
}

func (r *NewsTickerRepository) Insert(db *sqlx.DB, nt *entity.NewsTicker) error {
	query := `
		INSERT INTO news_tickers (news_id, ticker, match_method)
		VALUES (:news_id, :ticker, :match_method)
		ON CONFLICT (news_id, ticker) DO NOTHING
	`
	_, err := db.NamedExec(query, nt)
	return err
}
