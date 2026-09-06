package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// CorporateActionsReader is the read surface the MCP server depends on, so its
// handler tests can wire a fake and run without a DB (DisclosureReader
// precedent).
type CorporateActionsReader interface {
	GetCorporateActions(ctx context.Context, from, to time.Time, ticker *string) (*CorporateActionsResponse, error)
}

// CorporateActionsUseCase reads the corporate-actions calendar from the
// corporate_actions table. The table is populated by the daily
// idx:corporate_actions task (issue 09) — this usecase is a pure DB read, so
// the MCP request path never touches the nodriver sidecar (Heroku H12
// constraint, ADR-0009).
type CorporateActionsUseCase struct {
	DB   *sqlx.DB
	Log  *logrus.Logger
	Repo *repository.CorporateActionRepository
}

func NewCorporateActionsUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	repo *repository.CorporateActionRepository,
) *CorporateActionsUseCase {
	return &CorporateActionsUseCase{DB: db, Log: log, Repo: repo}
}

// CorporateActionEvent is one corporate-action row in the response. Detail
// carries jumlah_saham / jumlah_saham_setelah_tindakan — either may be 0.
type CorporateActionEvent struct {
	EventDate string                       `json:"event_date"` // YYYY-MM-DD
	Ticker    string                       `json:"ticker"`
	Type      string                       `json:"type"`
	Detail    client.CorporateActionDetail `json:"detail"`
}

// CorporateActionsResponse is the structured MCP tool result. Events are
// ascending by event date then ticker (the repository's ordering); an empty
// range yields an empty list.
type CorporateActionsResponse struct {
	From   string                 `json:"from"`
	To     string                 `json:"to"`
	Count  int                    `json:"count"`
	Events []CorporateActionEvent `json:"events"`
}

// GetCorporateActions returns the stored corporate actions with an event date
// in [from, to] (inclusive), optionally narrowed to one ticker. Pure DB read —
// no upstream call. Coverage is bounded by the daily task's rolling fetch
// window; events outside it are simply not stored.
func (uc *CorporateActionsUseCase) GetCorporateActions(ctx context.Context, from, to time.Time, ticker *string) (*CorporateActionsResponse, error) {
	if from.After(to) {
		return nil, ErrInvalidRange
	}
	var norm string
	if ticker != nil {
		norm = strings.ToUpper(strings.TrimSpace(*ticker))
		if !tickerPattern.MatchString(norm) {
			return nil, ErrInvalidTicker
		}
	}
	var tickerPtr *string
	if norm != "" {
		tickerPtr = &norm
	}

	rows, err := uc.Repo.FindByDateRange(uc.DB, from, to, tickerPtr)
	if err != nil {
		return nil, fmt.Errorf("read corporate actions: %w", err)
	}

	events := make([]CorporateActionEvent, 0, len(rows))
	for _, r := range rows {
		events = append(events, CorporateActionEvent{
			EventDate: r.EventDate.Format("2006-01-02"),
			Ticker:    r.Ticker,
			Type:      r.Type,
			Detail: client.CorporateActionDetail{
				JumlahSaham:                float64(r.JumlahSaham),
				JumlahSahamSetelahTindakan: float64(r.JumlahSahamSetelahTindakan),
			},
		})
	}
	return &CorporateActionsResponse{
		From:   from.Format("2006-01-02"),
		To:     to.Format("2006-01-02"),
		Count:  len(events),
		Events: events,
	}, nil
}
