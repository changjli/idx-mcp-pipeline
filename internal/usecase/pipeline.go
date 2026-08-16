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

// SourceHealth is one source's health row in a get_pipeline_status response.
type SourceHealth struct {
	Source        string  `json:"source"`
	LastSuccessAt *string `json:"last_success_at"`
	Stale         bool    `json:"stale"`
	LastError     *string `json:"last_error"`
}

// AlertRow is one recent alert in a get_pipeline_status response.
type AlertRow struct {
	ID        int64  `json:"id"`
	Source    string `json:"source"`
	AlertType string `json:"alert_type"`
	Message   string `json:"message"`
	RaisedAt  string `json:"raised_at"`
}

// PipelineStatusData is the data payload of a get_pipeline_status response.
type PipelineStatusData struct {
	Sources      []SourceHealth `json:"sources"`
	RecentAlerts []AlertRow     `json:"recent_alerts"`
}

// GetPipelineStatus returns per-source health from source_status plus the most
// recent alerts. The stale flag is the stored per-source value maintained by
// the ingestion tasks.
func (uc *PipelineUseCase) GetPipelineStatus(ctx context.Context) (*PipelineStatusData, error) {
	statuses, err := uc.SourceStatusRepo.FindAll(uc.DB)
	if err != nil {
		return nil, fmt.Errorf("query source status: %w", err)
	}
	alerts, err := uc.AlertRepo.FindRecent(uc.DB, 10)
	if err != nil {
		return nil, fmt.Errorf("query alerts: %w", err)
	}

	resp := &PipelineStatusData{Sources: []SourceHealth{}, RecentAlerts: []AlertRow{}}
	for _, s := range statuses {
		var last *string
		if s.LastSuccessAt != nil {
			v := s.LastSuccessAt.Format(time.RFC3339)
			last = &v
		}
		resp.Sources = append(resp.Sources, SourceHealth{
			Source:        s.Source,
			LastSuccessAt: last,
			Stale:         s.Stale,
			LastError:     s.LastError,
		})
	}
	for _, a := range alerts {
		resp.RecentAlerts = append(resp.RecentAlerts, AlertRow{
			ID:        a.ID,
			Source:    a.Source,
			AlertType: a.AlertType,
			Message:   a.Message,
			RaisedAt:  a.RaisedAt.Format(time.RFC3339),
		})
	}
	return resp, nil
}
