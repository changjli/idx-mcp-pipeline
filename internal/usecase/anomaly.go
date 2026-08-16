package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

type AnomalyUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	Validate       *validator.Validate
	DailyPriceRepo *repository.DailyPriceRepository
	AnomalyRepo    *repository.AnomalyRepository
}

func NewAnomalyUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	dailyPriceRepo *repository.DailyPriceRepository,
	anomalyRepo *repository.AnomalyRepository,
) *AnomalyUseCase {
	return &AnomalyUseCase{
		DB:             db,
		Log:            log,
		Validate:       validate,
		DailyPriceRepo: dailyPriceRepo,
		AnomalyRepo:    anomalyRepo,
	}
}

// MarketAnomaly is one anomaly row in a get_market_anomalies response, with the
// disclosure IDs derived at read time via the disclosures JOIN.
type MarketAnomaly struct {
	Ticker        string   `json:"ticker"`
	Type          string   `json:"type"`
	Direction     string   `json:"direction"`
	MagnitudePct  *float64 `json:"magnitude_pct"`
	BaselineRef   *float64 `json:"baseline_ref"`
	ObservedValue *float64 `json:"observed_value"`
	PriorValue    *float64 `json:"prior_value"`
	DisclosureIDs []int64  `json:"disclosure_ids"`
}

// MarketAnomaliesData is the data payload of a get_market_anomalies response.
type MarketAnomaliesData struct {
	Date      string          `json:"date"`
	Anomalies []MarketAnomaly `json:"anomalies"`
}

// GetMarketAnomalies returns the anomalies for a trading day (defaulting to
// the most recent trading day with any stored EOD data), optionally filtered
// to one ticker. Each anomaly carries the disclosure_ids derived at read time
// from the disclosures JOIN (same ticker, announced within the filter's
// lookback window before the anomaly's trading day, passed_filter=true).
func (uc *AnomalyUseCase) GetMarketAnomalies(ctx context.Context, date *string, ticker *string) (*MarketAnomaliesData, error) {
	day, err := resolveTradingDay(uc.DB, uc.DailyPriceRepo, date)
	if err != nil {
		return nil, err
	}

	rows, err := uc.AnomalyRepo.FindByDateWithDisclosures(uc.DB, day, ticker)
	if err != nil {
		return nil, fmt.Errorf("query anomalies: %w", err)
	}

	resp := &MarketAnomaliesData{Date: day, Anomalies: []MarketAnomaly{}}
	for _, r := range rows {
		resp.Anomalies = append(resp.Anomalies, MarketAnomaly{
			Ticker:        r.Ticker,
			Type:          r.Type,
			Direction:     r.Direction,
			MagnitudePct:  r.MagnitudePct,
			BaselineRef:   r.BaselineRef,
			ObservedValue: r.ObservedValue,
			PriorValue:    r.PriorValue,
			DisclosureIDs: []int64(r.DisclosureIDs),
		})
	}
	return resp, nil
}

// resolveTradingDay returns the requested date ("YYYY-MM-DD") or, when date is
// nil/empty, the most recent trading day with any stored EOD data. Returns
// ErrNoTradingDay when daily_prices is empty and no date was given.
func resolveTradingDay(db *sqlx.DB, dailyPriceRepo *repository.DailyPriceRepository, date *string) (string, error) {
	if date != nil && *date != "" {
		if _, err := time.Parse("2006-01-02", *date); err != nil {
			return "", ErrInvalidArgument
		}
		return *date, nil
	}
	latest, err := dailyPriceRepo.LatestTradingDayAll(db)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNoTradingDay
		}
		return "", fmt.Errorf("resolve latest trading day: %w", err)
	}
	return latest.Format("2006-01-02"), nil
}
