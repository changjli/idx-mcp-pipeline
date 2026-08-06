package usecase

import (
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
