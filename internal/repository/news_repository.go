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

// Upsert inserts a news item idempotently (url UNIQUE) and returns the row id.
// The id feeds the news_tickers join — callers insert one join row per matched
// ticker after this returns. On conflict only the mutable fields refresh:
// source and published_at keep first-seen values, so a syndicated re-emission
// by a second feed keeps the original publisher's attribution.
func (r *NewsRepository) Upsert(db *sqlx.DB, item *entity.NewsItem) (int64, error) {
	query := `
		INSERT INTO news_items (title, url, source, published_at, snippet, fetched_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (url) DO UPDATE SET
			title = EXCLUDED.title,
			snippet = EXCLUDED.snippet,
			fetched_at = NOW()
		RETURNING id
	`
	var id int64
	err := db.QueryRowx(query, item.Title, item.URL, item.Source, item.PublishedAt, item.Snippet).Scan(&id)
	return id, err
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
