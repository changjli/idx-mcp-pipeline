package usecase

import (
	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

type DisclosureUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	Validate       *validator.Validate
	DisclosureRepo *repository.DisclosureRepository
}

func NewDisclosureUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	disclosureRepo *repository.DisclosureRepository,
) *DisclosureUseCase {
	return &DisclosureUseCase{
		DB:             db,
		Log:            log,
		Validate:       validate,
		DisclosureRepo: disclosureRepo,
	}
}
