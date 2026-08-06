package usecase

import (
	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

type PipelineUseCase struct {
	DB               *sqlx.DB
	Log              *logrus.Logger
	Validate         *validator.Validate
	SourceStatusRepo *repository.SourceStatusRepository
	AlertRepo        *repository.AlertRepository
}

func NewPipelineUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	sourceStatusRepo *repository.SourceStatusRepository,
	alertRepo *repository.AlertRepository,
) *PipelineUseCase {
	return &PipelineUseCase{
		DB:               db,
		Log:              log,
		Validate:         validate,
		SourceStatusRepo: sourceStatusRepo,
		AlertRepo:        alertRepo,
	}
}
