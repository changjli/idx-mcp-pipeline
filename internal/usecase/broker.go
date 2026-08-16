package usecase

import (
	"context"
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

type BrokerUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	Validate       *validator.Validate
	BrokerRepo     *repository.BrokerRepository
	DailyPriceRepo *repository.DailyPriceRepository
}

func NewBrokerUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	brokerRepo *repository.BrokerRepository,
	dailyPriceRepo *repository.DailyPriceRepository,
) *BrokerUseCase {
	return &BrokerUseCase{
		DB:             db,
		Log:            log,
		Validate:       validate,
		BrokerRepo:     brokerRepo,
		DailyPriceRepo: dailyPriceRepo,
	}
}

// BrokerActivityRow is one broker's aggregate activity in a get_broker_summary
// response.
type BrokerActivityRow struct {
	BrokerCode string  `json:"broker_code"`
	FirmName   *string `json:"firm_name"`
	Volume     *int64  `json:"volume"`
	Value      *int64  `json:"value"`
	Frequency  *int32  `json:"frequency"`
}

// BrokerSummaryData is the data payload of a get_broker_summary response.
type BrokerSummaryData struct {
	Date    string              `json:"date"`
	Brokers []BrokerActivityRow `json:"brokers"`
}

// GetBrokerSummary returns the aggregate per-broker activity for a trading day
// (defaulting to the most recent trading day with any stored EOD data).
func (uc *BrokerUseCase) GetBrokerSummary(ctx context.Context, date *string) (*BrokerSummaryData, error) {
	day, err := resolveTradingDay(uc.DB, uc.DailyPriceRepo, date)
	if err != nil {
		return nil, err
	}

	rows, err := uc.BrokerRepo.FindByDate(uc.DB, day)
	if err != nil {
		return nil, fmt.Errorf("query broker summaries: %w", err)
	}

	resp := &BrokerSummaryData{Date: day, Brokers: []BrokerActivityRow{}}
	for _, r := range rows {
		resp.Brokers = append(resp.Brokers, BrokerActivityRow{
			BrokerCode: r.BrokerCode,
			FirmName:   r.FirmName,
			Volume:     r.Volume,
			Value:      r.Value,
			Frequency:  r.Frequency,
		})
	}
	return resp, nil
}
