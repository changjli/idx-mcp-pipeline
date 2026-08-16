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

// DisclosureListItem is one disclosure row in a list_idx_disclosures response.
// Metadata only — no extracted text.
type DisclosureListItem struct {
	DisclosureID     int64    `json:"disclosure_id"`
	Title            string   `json:"title"`
	Date             string   `json:"date"`
	Categories       []string `json:"categories"`
	PassedFilter     *bool    `json:"passed_filter"`
	ExtractionStatus string   `json:"extraction_status"`
}

// DisclosureListData is the data payload of a list_idx_disclosures response.
type DisclosureListData struct {
	Ticker      string               `json:"ticker"`
	Disclosures []DisclosureListItem `json:"disclosures"`
}

// ListIdxDisclosures returns a ticker's disclosure metadata, newest first,
// optionally filtered to one announcement date ("YYYY-MM-DD"). limit caps the
// result. No extracted text is returned — metadata only.
func (uc *DisclosureUseCase) ListIdxDisclosures(ctx context.Context, ticker string, date *string, limit int) (*DisclosureListData, error) {
	if date != nil && *date != "" {
		if _, err := time.Parse("2006-01-02", *date); err != nil {
			return nil, ErrInvalidArgument
		}
	}
	rows, err := uc.DisclosureRepo.FindByTickerWithDate(uc.DB, ticker, date, limit)
	if err != nil {
		return nil, fmt.Errorf("query disclosures: %w", err)
	}

	resp := &DisclosureListData{Ticker: ticker, Disclosures: []DisclosureListItem{}}
	for _, r := range rows {
		resp.Disclosures = append(resp.Disclosures, DisclosureListItem{
			DisclosureID:     r.ID,
			Title:            r.Title,
			Date:             r.AnnouncementDate.Format("2006-01-02"),
			Categories:       []string(r.Categories),
			PassedFilter:     r.PassedFilter,
			ExtractionStatus: r.ExtractionStatus,
		})
	}
	return resp, nil
}
