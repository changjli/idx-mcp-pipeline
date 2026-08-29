package pipeline

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// NewsArticle is one parsed feed item handed to the news ingest usecase.
type NewsArticle struct {
	Title       string
	URL         string
	PublishedAt time.Time
	Snippet     string
}

// MatchedTicker is one article→ticker match result. name is the full company
// name, used to seed the tickers row without clobbering the real name with a
// bare code.
type MatchedTicker struct {
	Code   string
	Name   string
	Method string // "code" or "name"
}

// TickerMatcher matches an article's title+snippet against the ticker
// universe. Satisfied by the task layer's tickerMatcher (code/name matching
// stays with the RSS glue it feeds on).
type TickerMatcher interface {
	Match(title, snippet string) []MatchedTicker
}

// NewsStore persists news items (url-UNIQUE idempotent upsert). Consumer-side
// interface (ADR-0006): satisfied by the sqlx-backed NewsRepository via
// NewSQLNewsStore; tests provide the second adapter.
type NewsStore interface {
	Upsert(news *entity.NewsItem) (int64, error)
}

// TickerSeeder seeds/updates a ticker row (FK dependency).
type TickerSeeder interface {
	InsertIfAbsent(code, name string) error
}

// NewsTickerStore links a news item to a matched ticker.
type NewsTickerStore interface {
	Insert(link *entity.NewsTicker) error
}

// SQLNewsStore adapts NewsRepository to NewsStore.
type SQLNewsStore struct {
	repo *repository.NewsRepository
	db   *sqlx.DB
}

// NewSQLNewsStore binds a news repository to its database.
func NewSQLNewsStore(repo *repository.NewsRepository, db *sqlx.DB) *SQLNewsStore {
	return &SQLNewsStore{repo: repo, db: db}
}

// Upsert writes one news item, returning its id.
func (s *SQLNewsStore) Upsert(news *entity.NewsItem) (int64, error) {
	return s.repo.Upsert(s.db, news)
}

// SQLTickerSeeder adapts TickerRepository to TickerSeeder.
type SQLTickerSeeder struct {
	repo *repository.TickerRepository
	db   *sqlx.DB
}

// NewSQLTickerSeeder binds a ticker repository to its database.
func NewSQLTickerSeeder(repo *repository.TickerRepository, db *sqlx.DB) *SQLTickerSeeder {
	return &SQLTickerSeeder{repo: repo, db: db}
}

// InsertIfAbsent seeds one ticker row.
func (s *SQLTickerSeeder) InsertIfAbsent(code, name string) error {
	return s.repo.InsertIfAbsent(s.db, code, name)
}

// SQLNewsTickerStore adapts NewsTickerRepository to NewsTickerStore.
type SQLNewsTickerStore struct {
	repo *repository.NewsTickerRepository
	db   *sqlx.DB
}

// NewSQLNewsTickerStore binds a news-ticker repository to its database.
func NewSQLNewsTickerStore(repo *repository.NewsTickerRepository, db *sqlx.DB) *SQLNewsTickerStore {
	return &SQLNewsTickerStore{repo: repo, db: db}
}

// Insert writes one news↔ticker link.
func (s *SQLNewsTickerStore) Insert(link *entity.NewsTicker) error {
	return s.repo.Insert(s.db, link)
}

// NewsIngest persists one RSS feed's articles: unmatched items are discarded,
// matched ones land in news_items and are linked to their tickers
// (news_tickers) with the ticker auto-seeded on first sight. Batch error
// policy: fail-fast — the first write failure aborts the run and returns the
// error so asynq retries and source_status records the gap. Declared policy;
// do not soften in place (follow-up 07 tracks the whole policy).
type NewsIngest struct {
	news    NewsStore
	tickers TickerSeeder
	links   NewsTickerStore
	log     *logrus.Logger
}

// NewNewsIngest wires the news ingest usecase over its stores. The matcher is
// per-call (Store takes it) because the matching universe is reloaded from the
// DB tickers table each run — new listings must match the same day.
func NewNewsIngest(news NewsStore, tickers TickerSeeder, links NewsTickerStore, log *logrus.Logger) *NewsIngest {
	return &NewsIngest{news: news, tickers: tickers, links: links, log: log}
}

// Store persists one feed's articles. Returns the number of articles written
// (those with at least one ticker match).
func (n *NewsIngest) Store(matcher TickerMatcher, feedName string, articles []NewsArticle) (int, error) {
	stored := 0
	for _, a := range articles {
		matches := matcher.Match(a.Title, a.Snippet)
		if len(matches) == 0 {
			n.log.Debugf("rss: discarding unmatched article %q", Truncate(a.Title, 80))
			continue
		}

		var snippet *string
		if a.Snippet != "" {
			snippet = &a.Snippet
		}
		id, err := n.news.Upsert(&entity.NewsItem{
			Title:       a.Title,
			URL:         a.URL,
			Source:      feedName,
			PublishedAt: a.PublishedAt,
			Snippet:     snippet,
		})
		if err != nil {
			return stored, fmt.Errorf("news upsert %q: %w", a.URL, err)
		}

		for _, mt := range matches {
			if err := n.tickers.InsertIfAbsent(mt.Code, mt.Name); err != nil {
				return stored, fmt.Errorf("ticker seed %s: %w", mt.Code, err)
			}
			if err := n.links.Insert(&entity.NewsTicker{
				NewsID:      id,
				Ticker:      mt.Code,
				MatchMethod: mt.Method,
			}); err != nil {
				return stored, fmt.Errorf("news_ticker insert %s: %w", mt.Code, err)
			}
		}
		stored++
	}
	return stored, nil
}
