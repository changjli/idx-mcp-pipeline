package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// ucTickerProfileFetcher returns canned profiles without touching the network.
type ucTickerProfileFetcher struct {
	profiles []client.CompanyProfile
	err      error
}

func (f *ucTickerProfileFetcher) FetchCompanyProfiles(ctx context.Context) ([]client.CompanyProfile, error) {
	return f.profiles, f.err
}

// TestTickerProfileUseCase_SeedTickerProfiles_EndToEnd runs the seeder against
// a real Postgres: profiles are fetched, upserted into tickers, and
// source_status is updated on success. Skipped unless IDX_MCP_DB_DSN is set.
// Cleanup scoped to TESTPA/TESTPB + the idx:company_profiles row.
func TestTickerProfileUseCase_SeedTickerProfiles_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM tickers WHERE code IN ('TESTPA', 'TESTPB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", tickerProfileSource)
	}
	cleanup()
	t.Cleanup(cleanup)

	date := time.Date(2024, 12, 5, 0, 0, 0, 0, time.UTC)
	fetcher := &ucTickerProfileFetcher{profiles: []client.CompanyProfile{
		{Ticker: "TESTPA", Name: "PT Test Alpha Tbk", ListingBoard: "Utama", ListingDate: &date, Active: true},
		{Ticker: "TESTPB", Name: "PT Test Beta Tbk", ListingBoard: "Pengembangan", ListingDate: nil, Active: false},
	}}

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	uc := NewTickerProfileUseCase(
		db, log, fetcher, repository.NewTickerRepository(log), recorder,
	)

	n, err := uc.SeedTickerProfiles(context.Background())
	if err != nil {
		t.Fatalf("SeedTickerProfiles: %v", err)
	}
	if n != 2 {
		t.Errorf("upserted = %d, want 2", n)
	}

	// Rows persisted with profile fields.
	repo := repository.NewTickerRepository(log)
	got, err := repo.FindByCode(db, "TESTPA")
	if err != nil {
		t.Fatalf("FindByCode: %v", err)
	}
	if got.Name != "PT Test Alpha Tbk" || got.ListingBoard == nil || *got.ListingBoard != "Utama" {
		t.Errorf("TESTPA profile = %+v, want name/board populated", got)
	}
	if got.ListingDate == nil || got.ListingDate.Format("2006-01-02") != "2024-12-05" {
		t.Errorf("TESTPA listing_date = %v, want 2024-12-05", got.ListingDate)
	}
	gotB, err := repo.FindByCode(db, "TESTPB")
	if err != nil {
		t.Fatalf("FindByCode TESTPB: %v", err)
	}
	if gotB.Active {
		t.Errorf("TESTPB active = true, want false")
	}

	// source_status success recorded.
	status, err := recorder.CurrentWatermark(tickerProfileSource)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status == nil {
		t.Errorf("watermark = nil, want set on success")
	}
}

// TestTickerProfileUseCase_SeedTickerProfiles_Failure verifies a fetch error
// surfaces and is recorded in source_status.
func TestTickerProfileUseCase_SeedTickerProfiles_Failure(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM tickers WHERE code IN ('TESTPA', 'TESTPB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", tickerProfileSource)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetcher := &ucTickerProfileFetcher{err: errors.New("idx api error: status=403")}

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	uc := NewTickerProfileUseCase(
		db, log, fetcher, repository.NewTickerRepository(log), recorder,
	)

	if _, err := uc.SeedTickerProfiles(context.Background()); err == nil {
		t.Fatal("expected error from fetch failure")
	}

	status, err := recorder.CurrentWatermark(tickerProfileSource)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status != nil {
		t.Errorf("watermark = %v, want nil on failure", status)
	}
}
