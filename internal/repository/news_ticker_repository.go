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

// DeleteOlderThan deletes news_tickers join rows whose parent news_item is
// older than the retention window. Must run before NewsRepository.DeleteOlderThan
// (news_tickers.news_id FK references news_items.id). Returns rows deleted.
func (r *NewsTickerRepository) DeleteOlderThan(db *sqlx.DB, days int) (int64, error) {
	res, err := db.Exec(`
		DELETE FROM news_tickers nt
		USING news_items ni
		WHERE nt.news_id = ni.id
		  AND ni.published_at < NOW() - make_interval(days => $1)`,
		days,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
