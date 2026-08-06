package repository

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type NewsRepository struct {
	*Repository[entity.NewsItem]
	Log *logrus.Logger
}

func NewNewsRepository(log *logrus.Logger) *NewsRepository {
	return &NewsRepository{
		Repository: &Repository[entity.NewsItem]{},
		Log:        log,
	}
}

func (r *NewsRepository) Upsert(db *sqlx.DB, item *entity.NewsItem) error {
	query := `
		INSERT INTO news_items (title, url, source, published_at, snippet, fetched_at)
		VALUES (:title, :url, :source, :published_at, :snippet, NOW())
		ON CONFLICT (url) DO UPDATE SET
			title = EXCLUDED.title,
			snippet = EXCLUDED.snippet,
			fetched_at = NOW()
	`
	_, err := db.NamedExec(query, item)
	return err
}

func (r *NewsRepository) FindByTicker(db *sqlx.DB, ticker string, limit int) ([]entity.NewsItem, error) {
	var items []entity.NewsItem
	err := db.Select(&items, `
		SELECT ni.* FROM news_items ni
		JOIN news_tickers nt ON nt.news_id = ni.id
		WHERE nt.ticker = $1
		ORDER BY ni.published_at DESC
		LIMIT $2
	`, ticker, limit)
	return items, err
}

func (r *NewsRepository) DeleteOlderThan(db *sqlx.DB, days int) error {
	_, err := db.Exec(
		"DELETE FROM news_items WHERE published_at < NOW() - make_interval(days => $1)",
		days,
	)
	return err
}
