package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// suspensionsMaxAgeSeconds is the source_status max age for idx:suspensions
	// (24 hours). The UMA/suspension list changes slowly and the daily fetch is
	// enough to keep the envelope fresh.
	suspensionsMaxAgeSeconds int32 = 86400
	// DefaultSuspensionsLookbackDays is how far back the daily fetch window
	// starts. Suspensions are transient (typically days), so a short lookback
	// still covers them fully.
	DefaultSuspensionsLookbackDays = 7
	// DefaultSuspensionsLookaheadDays is how far forward the daily fetch window
	// reaches. BEI publishes suspensions in advance (cooling-down), so a short
	// lookahead catches upcoming entries.
	DefaultSuspensionsLookaheadDays = 7
)

// SuspensionsPayload is the payload for an idx:suspensions task.
type SuspensionsPayload struct {
	Date string `json:"date"` // YYYY-MM-DD run date
}

// SuspensionsFetcher fetches the BEI UMA/suspension lists for a date range.
// *client.Client satisfies this; tests use a fake.
type SuspensionsFetcher interface {
	FetchSuspensions(ctx context.Context, from, to time.Time) ([]client.Suspension, error)
	FetchUma(ctx context.Context, from, to time.Time) ([]client.Uma, error)
}

// NewSuspensionsHandler returns an asynq handler for the idx:suspensions task
// type. It fetches both the GetSuspension and GetUma endpoints over a rolling
// window [runDate - lookback, runDate + lookahead], upserts every row into
// suspensions, and records source_status. The MCP tool reads the stored rows —
// the request path never touches the nodriver sidecar (Heroku H12 constraint,
// ADR-0009).
func NewSuspensionsHandler(
	log *logrus.Logger,
	fetcher SuspensionsFetcher,
	db *sqlx.DB,
	repo *repository.SuspensionRepository,
	recorder *pipeline.SourceStatusRecorder,
	lookbackDays, lookaheadDays int,
) asynq.HandlerFunc {
	if lookbackDays <= 0 {
		lookbackDays = DefaultSuspensionsLookbackDays
	}
	if lookaheadDays <= 0 {
		lookaheadDays = DefaultSuspensionsLookaheadDays
	}
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[SuspensionsPayload](t)
		if err != nil {
			return err
		}
		runDate, err := pipeline.ParseTaskDay(p.Date)
		if err != nil {
			return err
		}

		from := runDate.AddDate(0, 0, -lookbackDays)
		to := runDate.AddDate(0, 0, lookaheadDays)

		log.Infof("suspensions: fetching UMA/suspension lists, run date=%s window=%s..%s",
			p.Date, from.Format("2006-01-02"), to.Format("2006-01-02"))

		suspensions, suspErr := fetcher.FetchSuspensions(ctx, from, to)
		if suspErr != nil {
			recorder.Failure(TypeSuspensions, suspensionsMaxAgeSeconds, p.Date, suspErr)
			return suspErr
		}
		umas, umaErr := fetcher.FetchUma(ctx, from, to)
		if umaErr != nil {
			recorder.Failure(TypeSuspensions, suspensionsMaxAgeSeconds, p.Date, umaErr)
			return umaErr
		}

		rows := suspensionRowsToEntities(suspensions, umas)
		if err := repo.Upsert(db, rows); err != nil {
			recorder.Failure(TypeSuspensions, suspensionsMaxAgeSeconds, p.Date, err)
			return err
		}

		recorder.Success(TypeSuspensions, suspensionsMaxAgeSeconds, suspensionsMaxEventDate(suspensions, umas))
		log.Infof("suspensions: upserted %d row(s) (%d suspension, %d UMA)", len(rows), len(suspensions), len(umas))
		return nil
	}
}

// suspensionRowsToEntities converts the fetched suspension and UMA events into
// a single entity slice for the upsert. Suspension rows carry type SPT/UPT;
// UMA rows carry type UMA.
func suspensionRowsToEntities(suspensions []client.Suspension, umas []client.Uma) []entity.Suspension {
	rows := make([]entity.Suspension, 0, len(suspensions)+len(umas))
	for _, s := range suspensions {
		rows = append(rows, entity.Suspension{
			Ticker:    s.Ticker,
			EventDate: s.EventDate,
			Type:      s.Type,
			Reason:    s.Reason,
		})
	}
	for _, u := range umas {
		rows = append(rows, entity.Suspension{
			Ticker:    u.Ticker,
			EventDate: u.EventDate,
			Type:      "UMA",
			Reason:    u.Reason,
		})
	}
	return rows
}

// suspensionsMaxEventDate returns the newest event date in the fetched sets,
// or nil when both are empty (the recorder then carries the watermark forward).
func suspensionsMaxEventDate(suspensions []client.Suspension, umas []client.Uma) *time.Time {
	var max time.Time
	for _, s := range suspensions {
		if s.EventDate.After(max) {
			max = s.EventDate
		}
	}
	for _, u := range umas {
		if u.EventDate.After(max) {
			max = u.EventDate
		}
	}
	if max.IsZero() {
		return nil
	}
	return &max
}
