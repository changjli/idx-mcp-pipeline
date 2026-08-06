package usecase

import (
	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

type BrokerUseCase struct {
	DB         *sqlx.DB
	Log        *logrus.Logger
	Validate   *validator.Validate
	BrokerRepo *repository.BrokerRepository
}

func NewBrokerUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	brokerRepo *repository.BrokerRepository,
) *BrokerUseCase {
	return &BrokerUseCase{
		DB:         db,
		Log:        log,
		Validate:   validate,
		BrokerRepo: brokerRepo,
	}
}
