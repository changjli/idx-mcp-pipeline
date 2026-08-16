package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

type NewsUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	Validate       *validator.Validate
	NewsRepo       *repository.NewsRepository
	NewsTickerRepo *repository.NewsTickerRepository
}

func NewNewsUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	newsRepo *repository.NewsRepository,
	newsTickerRepo *repository.NewsTickerRepository,
) *NewsUseCase {
	return &NewsUseCase{
		DB:             db,
		Log:            log,
		Validate:       validate,
		NewsRepo:       newsRepo,
		NewsTickerRepo: newsTickerRepo,
	}
}

// TickerNewsItem is one news item in a get_ticker_news response, with the
// match_method recorded on its news_tickers join row.
type TickerNewsItem struct {
	ID          int64   `json:"id"`
	Title       string  `json:"title"`
	URL         string  `json:"url"`
	Source      string  `json:"source"`
	PublishedAt string  `json:"published_at"`
	Snippet     *string `json:"snippet"`
	MatchMethod string  `json:"match_method"`
}

// TickerNewsData is the data payload of a get_ticker_news response.
type TickerNewsData struct {
	Ticker string           `json:"ticker"`
	Items  []TickerNewsItem `json:"items"`
}

// GetTickerNews returns a ticker's news items, newest first, optionally
// filtered to published_at >= since ("YYYY-MM-DD"). limit caps the result.
func (uc *NewsUseCase) GetTickerNews(ctx context.Context, ticker string, since *string, limit int) (*TickerNewsData, error) {
	var sinceT *time.Time
	if since != nil && *since != "" {
		t, err := time.Parse("2006-01-02", *since)
		if err != nil {
			return nil, ErrInvalidArgument
		}
		sinceT = &t
	}

	rows, err := uc.NewsRepo.FindByTickerWithMatch(uc.DB, ticker, sinceT, limit)
	if err != nil {
		return nil, fmt.Errorf("query news: %w", err)
	}

	resp := &TickerNewsData{Ticker: ticker, Items: []TickerNewsItem{}}
	for _, r := range rows {
		resp.Items = append(resp.Items, TickerNewsItem{
			ID:          r.ID,
			Title:       r.Title,
			URL:         r.URL,
			Source:      r.Source,
			PublishedAt: r.PublishedAt.Format(time.RFC3339),
			Snippet:     r.Snippet,
			MatchMethod: r.MatchMethod,
		})
	}
	return resp, nil
}
