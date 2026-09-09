package usecase

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// tickerProfileSource is the source_status label for the profile seeder. It
	// names the endpoint/source (idx:company_profiles), not a task — there is no
	// task.
	tickerProfileSource = "idx:company_profiles"
	// tickerProfileMaxAgeSeconds is the source_status freshness window (30 days).
	// The source is a manual backfill with no scheduler; board/listing date
	// change rarely, and the intended refresh cadence is monthly
	// (findings-sector-index-membership.md), so 30 days reflects that without
	// going stale overnight.
	tickerProfileMaxAgeSeconds int32 = 30 * 86400
)

// TickerProfileFetcher fetches the IDX listed-companies profile list.
// *client.Client satisfies this; tests use a fake.
type TickerProfileFetcher interface {
	FetchCompanyProfiles(ctx context.Context) ([]client.CompanyProfile, error)
}

// TickerProfileUseCase orchestrates the one-time ticker-profile seeder (issue
// 11b): fetch the full GetCompanyProfiles list, upsert name/board/listing-date/
// status into tickers, and record source_status. The CLI (enqueue-daily --task
// ticker-profile) wires it; no asynq, no scheduler — re-run manually to pick up
// new IPOs.
type TickerProfileUseCase struct {
	DB       *sqlx.DB
	Log      *logrus.Logger
	Fetcher  TickerProfileFetcher
	Repo     *repository.TickerRepository
	Recorder *pipeline.SourceStatusRecorder
}

func NewTickerProfileUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	fetcher TickerProfileFetcher,
	repo *repository.TickerRepository,
	recorder *pipeline.SourceStatusRecorder,
) *TickerProfileUseCase {
	return &TickerProfileUseCase{DB: db, Log: log, Fetcher: fetcher, Repo: repo, Recorder: recorder}
}

// SeedTickerProfiles fetches every listed company's profile and upserts it
// into tickers, recording source_status on success/failure. Returns the number
// of profiles upserted.
func (uc *TickerProfileUseCase) SeedTickerProfiles(ctx context.Context) (int, error) {
	runDate := time.Now().Format("2006-01-02")

	uc.Log.Info("ticker profile seeder: fetching company profiles")
	profiles, err := uc.Fetcher.FetchCompanyProfiles(ctx)
	if err != nil {
		uc.Recorder.Failure(tickerProfileSource, tickerProfileMaxAgeSeconds, runDate, err)
		return 0, err
	}

	rows := companyProfilesToEntities(profiles)
	if err := uc.Repo.UpsertProfiles(uc.DB, rows); err != nil {
		uc.Recorder.Failure(tickerProfileSource, tickerProfileMaxAgeSeconds, runDate, err)
		return 0, err
	}

	now := time.Now()
	uc.Recorder.Success(tickerProfileSource, tickerProfileMaxAgeSeconds, &now)
	return len(rows), nil
}

// companyProfilesToEntities converts fetched profiles into entity rows for the
// upsert. Only the profile columns are set — shares/sektor/industri stay nil so
// the upsert leaves them untouched.
func companyProfilesToEntities(profiles []client.CompanyProfile) []entity.Ticker {
	rows := make([]entity.Ticker, 0, len(profiles))
	for _, p := range profiles {
		board := p.ListingBoard
		rows = append(rows, entity.Ticker{
			Code:         p.Ticker,
			Name:         p.Name,
			ListingDate:  p.ListingDate,
			ListingBoard: &board,
			Active:       p.Active,
		})
	}
	return rows
}
