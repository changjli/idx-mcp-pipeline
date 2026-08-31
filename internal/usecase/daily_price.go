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

// DailyPriceUseCase reads the stored Daily Price (OHLCV) series for a ticker
// over a date range. Pure DB read — no upstream call.
type DailyPriceUseCase struct {
	DB       *sqlx.DB
	Log      *logrus.Logger
	Validate *validator.Validate
	Repo     *repository.DailyPriceRepository
}

func NewDailyPriceUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	repo *repository.DailyPriceRepository,
) *DailyPriceUseCase {
	return &DailyPriceUseCase{
		DB:       db,
		Log:      log,
		Validate: validate,
		Repo:     repo,
	}
}

// DailyPriceRow is one trading day's OHLCV in a get_daily_prices response.
type DailyPriceRow struct {
	TradingDay string   `json:"trading_day"`
	Open       *float64 `json:"open"`
	High       *float64 `json:"high"`
	Low        *float64 `json:"low"`
	Close      *float64 `json:"close"`
	Volume     *int64   `json:"volume"`
	Value      *int64   `json:"value"`
	Frequency  *int32   `json:"frequency"`
}

// DailyPricesData is the data payload of a get_daily_prices response.
type DailyPricesData struct {
	Ticker string          `json:"ticker"`
	From   string          `json:"from"`
	To     string          `json:"to"`
	Prices []DailyPriceRow `json:"prices"`
}

// GetDailyPrices returns the OHLCV rows for a ticker between two dates
// (inclusive), ascending by trading day. An empty range returns an empty list.
func (uc *DailyPriceUseCase) GetDailyPrices(ctx context.Context, ticker string, from, to time.Time) (*DailyPricesData, error) {
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}
	if from.After(to) {
		return nil, ErrInvalidRange
	}

	rows, err := uc.Repo.FindByTickerAndDateRange(uc.DB, ticker, from, to)
	if err != nil {
		return nil, fmt.Errorf("query daily prices: %w", err)
	}

	resp := &DailyPricesData{
		Ticker: ticker,
		From:   from.Format("2006-01-02"),
		To:     to.Format("2006-01-02"),
		Prices: []DailyPriceRow{},
	}
	for _, r := range rows {
		resp.Prices = append(resp.Prices, DailyPriceRow{
			TradingDay: r.TradingDay.Format("2006-01-02"),
			Open:       r.Open,
			High:       r.High,
			Low:        r.Low,
			Close:      r.Close,
			Volume:     r.Volume,
			Value:      r.Value,
			Frequency:  r.Frequency,
		})
	}
	return resp, nil
}
