package usecase

import (
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
