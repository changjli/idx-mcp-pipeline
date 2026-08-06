package entity

import "time"

type NewsItem struct {
	ID          int64     `db:"id"`
	Title       string    `db:"title"`
	URL         string    `db:"url"`
	Source      string    `db:"source"`
	PublishedAt time.Time `db:"published_at"`
	Snippet     *string   `db:"snippet"`
	FetchedAt   time.Time `db:"fetched_at"`
}

func (NewsItem) TableName() string { return "news_items" }
