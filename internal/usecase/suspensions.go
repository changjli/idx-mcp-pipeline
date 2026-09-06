package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// SuspensionsReader is the read surface the MCP server depends on, so its
// handler tests can wire a fake and run without a DB (CorporateActionsReader
// precedent).
type SuspensionsReader interface {
	GetSuspensions(ctx context.Context, from, to time.Time, ticker *string) (*SuspensionsResponse, error)
}

// SuspensionsUseCase reads the BEI UMA/suspension list from the suspensions
// table. The table is populated by the daily idx:suspensions task (issue 10) —
// this usecase is a pure DB read, so the MCP request path never touches the
// nodriver sidecar (Heroku H12 constraint, ADR-0009).
type SuspensionsUseCase struct {
	DB   *sqlx.DB
	Log  *logrus.Logger
	Repo *repository.SuspensionRepository
}

func NewSuspensionsUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	repo *repository.SuspensionRepository,
) *SuspensionsUseCase {
	return &SuspensionsUseCase{DB: db, Log: log, Repo: repo}
}

// SuspensionEvent is one UMA/suspension row in the response. Type is the raw
// IDX discriminator: SPT (suspend), UPT (unsuspend), or UMA.
type SuspensionEvent struct {
	EventDate string `json:"event_date"` // YYYY-MM-DD
	Ticker    string `json:"ticker"`
	Type      string `json:"type"`
	Reason    string `json:"reason"`
}

// SuspensionsResponse is the structured MCP tool result. Events are ascending
// by event date then ticker (the repository's ordering); an empty range yields
// an empty list.
type SuspensionsResponse struct {
	From   string            `json:"from"`
	To     string            `json:"to"`
	Count  int               `json:"count"`
	Events []SuspensionEvent `json:"events"`
}

// GetSuspensions returns the stored UMA/suspension events with an event date in
// [from, to] (inclusive), optionally narrowed to one ticker. Pure DB read — no
// upstream call. Coverage is bounded by the daily task's rolling fetch window;
// events outside it are simply not stored.
func (uc *SuspensionsUseCase) GetSuspensions(ctx context.Context, from, to time.Time, ticker *string) (*SuspensionsResponse, error) {
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
		return nil, fmt.Errorf("read suspensions: %w", err)
	}

	events := make([]SuspensionEvent, 0, len(rows))
	for _, r := range rows {
		events = append(events, SuspensionEvent{
			EventDate: r.EventDate.Format("2006-01-02"),
			Ticker:    r.Ticker,
			Type:      r.Type,
			Reason:    r.Reason,
		})
	}
	return &SuspensionsResponse{
		From:   from.Format("2006-01-02"),
		To:     to.Format("2006-01-02"),
		Count:  len(events),
		Events: events,
	}, nil
}
