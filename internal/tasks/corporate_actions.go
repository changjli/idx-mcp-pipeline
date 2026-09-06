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
	// corporateActionsMaxAgeSeconds is the source_status max age for
	// idx:corporate_actions (24 hours). A corporate-actions calendar changes
	// slowly; 24h keeps the envelope fresh while tolerating a daily cadence.
	corporateActionsMaxAgeSeconds int32 = 86400
	// DefaultCorporateActionsLookbackDays is how far back the daily fetch
	// window starts (recent context alongside the upcoming events).
	DefaultCorporateActionsLookbackDays = 7
	// DefaultCorporateActionsLookaheadDays is how far forward the daily fetch
	// window reaches — the "upcoming" horizon the analyst asks for.
	DefaultCorporateActionsLookaheadDays = 90
)

// CorporateActionsPayload is the payload for an idx:corporate_actions task.
type CorporateActionsPayload struct {
	Date string `json:"date"` // YYYY-MM-DD run date
}

// CorporateActionsFetcher fetches the corporate-actions calendar for a date
// range. *client.Client satisfies this; tests use a fake.
type CorporateActionsFetcher interface {
	FetchCorporateActions(ctx context.Context, from, to time.Time) ([]client.CorporateAction, error)
}

// NewCorporateActionsHandler returns an asynq handler for the
// idx:corporate_actions task type. It fetches the IDX GetIssuedHistory calendar
// over a rolling window [runDate - lookback, runDate + lookahead] in one
// request, upserts every returned row, and records source_status. The MCP tool
// reads the stored rows — the request path never touches the nodriver sidecar
// (Heroku H12 constraint, ADR-0009).
func NewCorporateActionsHandler(
	log *logrus.Logger,
	fetcher CorporateActionsFetcher,
	db *sqlx.DB,
	repo *repository.CorporateActionRepository,
	recorder *pipeline.SourceStatusRecorder,
	lookbackDays, lookaheadDays int,
) asynq.HandlerFunc {
	if lookbackDays <= 0 {
		lookbackDays = DefaultCorporateActionsLookbackDays
	}
	if lookaheadDays <= 0 {
		lookaheadDays = DefaultCorporateActionsLookaheadDays
	}
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[CorporateActionsPayload](t)
		if err != nil {
			return err
		}
		runDate, err := pipeline.ParseTaskDay(p.Date)
		if err != nil {
			return err
		}

		from := runDate.AddDate(0, 0, -lookbackDays)
		to := runDate.AddDate(0, 0, lookaheadDays)

		log.Infof("corporate actions: fetching calendar, run date=%s window=%s..%s",
			p.Date, from.Format("2006-01-02"), to.Format("2006-01-02"))

		actions, fetchErr := fetcher.FetchCorporateActions(ctx, from, to)
		if fetchErr != nil {
			recorder.Failure(TypeCorporateActions, corporateActionsMaxAgeSeconds, p.Date, fetchErr)
			return fetchErr
		}

		rows, err := corporateActionsToEntities(actions)
		if err != nil {
			recorder.Failure(TypeCorporateActions, corporateActionsMaxAgeSeconds, p.Date, err)
			return err
		}
		if err := repo.Upsert(db, rows); err != nil {
			recorder.Failure(TypeCorporateActions, corporateActionsMaxAgeSeconds, p.Date, err)
			return err
		}

		recorder.Success(TypeCorporateActions, corporateActionsMaxAgeSeconds, corporateActionsMaxEventDate(actions))
		log.Infof("corporate actions: upserted %d row(s)", len(rows))
		return nil
	}
}

// corporateActionsToEntities converts fetched actions into entity rows for the
// upsert. The wire amounts are integral floats (share counts); int64() truncates
// safely.
func corporateActionsToEntities(actions []client.CorporateAction) ([]entity.CorporateAction, error) {
	rows := make([]entity.CorporateAction, 0, len(actions))
	for _, a := range actions {
		rows = append(rows, entity.CorporateAction{
			Id:                         a.ID,
			Ticker:                     a.Ticker,
			EventDate:                  a.EventDate,
			Type:                       a.Type,
			JumlahSaham:                int64(a.Detail.JumlahSaham),
			JumlahSahamSetelahTindakan: int64(a.Detail.JumlahSahamSetelahTindakan),
		})
	}
	return rows, nil
}

// corporateActionsMaxEventDate returns the newest event date in the fetched
// set, or nil when empty (the recorder then carries the watermark forward).
func corporateActionsMaxEventDate(actions []client.CorporateAction) *time.Time {
	var max time.Time
	for _, a := range actions {
		if a.EventDate.After(max) {
			max = a.EventDate
		}
	}
	if max.IsZero() {
		return nil
	}
	return &max
}
