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
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// ucSectorIndexFetcher returns canned screener rows without touching the
// network.
type ucSectorIndexFetcher struct {
	rows []client.ScreenerRow
	err  error
}

func (f *ucSectorIndexFetcher) FetchScreener(ctx context.Context) ([]client.ScreenerRow, error) {
	return f.rows, f.err
}

// TestSectorIndexUseCase_SeedSectorIndex_EndToEnd runs the seeder against a
// real Postgres: screener rows are fetched, sector columns upserted into
// tickers, membership replaced for the run date, and source_status updated on
// success. Skipped unless IDX_MCP_DB_DSN is set. Cleanup scoped to
// TESTSA/TESTSB + the idx:sector_index row.
func TestSectorIndexUseCase_SeedSectorIndex_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM tickers WHERE code IN ('TESTSA', 'TESTSB')")
		db.MustExec("DELETE FROM ticker_indices WHERE ticker_code IN ('TESTSA', 'TESTSB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", sectorIndexSource)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Pre-seed TESTSA with a name so the sector upsert's non-touch guarantee is
	// observable.
	repo := repository.NewTickerRepository(log)
	if err := repo.Upsert(db, &entity.Ticker{Code: "TESTSA", Name: "PT Test Sector Alpha Tbk", Active: true}); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}

	fetcher := &ucSectorIndexFetcher{rows: []client.ScreenerRow{
		{StockCode: "TESTSA", Sector: "Energy", SubSector: "Oil, Gas & Coal", Industry: strPtr("Coal"), SubIndustry: strPtr("Coal Production"), SubIndustryCode: strPtr("A121"), IndexCode: strPtr("COMPOSITE, LQ45")},
		{StockCode: "TESTSB", Sector: "Financials", SubSector: "Banks", Industry: strPtr("Banks"), SubIndustry: strPtr("Banks"), SubIndustryCode: strPtr("G111"), IndexCode: nil},
	}}

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	uc := NewSectorIndexUseCase(
		db, log, fetcher, repo, repository.NewTickerIndexRepository(log), recorder,
	)

	runDate := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	n, m, err := uc.SeedSectorIndex(context.Background(), runDate)
	if err != nil {
		t.Fatalf("SeedSectorIndex: %v", err)
	}
	if n != 2 {
		t.Errorf("tickers upserted = %d, want 2", n)
	}
	if m != 2 {
		t.Errorf("membership rows = %d, want 2 (TESTSA only; TESTSB has null indexCode)", m)
	}

	// TESTSA sector columns landed; name untouched.
	got, err := repo.FindByCode(db, "TESTSA")
	if err != nil {
		t.Fatalf("FindByCode: %v", err)
	}
	if got.Name != "PT Test Sector Alpha Tbk" {
		t.Errorf("name = %q, want untouched", got.Name)
	}
	if got.Sektor == nil || *got.Sektor != "Energy" {
		t.Errorf("sektor = %v, want Energy", got.Sektor)
	}
	if got.SubIndustryCode == nil || *got.SubIndustryCode != "A121" {
		t.Errorf("sub_industry_code = %v, want A121", got.SubIndustryCode)
	}

	// Membership rows landed with the effective_date.
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM ticker_indices WHERE effective_date = $1", runDate); err != nil {
		t.Fatalf("membership count: %v", err)
	}
	if count != 2 {
		t.Errorf("membership rows = %d, want 2", count)
	}

	// source_status success recorded with the run-date watermark.
	status, err := recorder.CurrentWatermark(sectorIndexSource)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status == nil || status.Format("2006-01-02") != "2026-09-09" {
		t.Errorf("watermark = %v, want 2026-09-09", status)
	}
}

// TestSectorIndexUseCase_SeedSectorIndex_Failure verifies a fetch error
// surfaces and is recorded in source_status.
func TestSectorIndexUseCase_SeedSectorIndex_Failure(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM tickers WHERE code IN ('TESTSA', 'TESTSB')")
		db.MustExec("DELETE FROM ticker_indices WHERE ticker_code IN ('TESTSA', 'TESTSB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", sectorIndexSource)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetcher := &ucSectorIndexFetcher{err: errors.New("idx api error: status=403")}

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	uc := NewSectorIndexUseCase(
		db, log, fetcher, repository.NewTickerRepository(log), repository.NewTickerIndexRepository(log), recorder,
	)

	if _, _, err := uc.SeedSectorIndex(context.Background(), time.Now()); err == nil {
		t.Fatal("expected error from fetch failure")
	}

	status, err := recorder.CurrentWatermark(sectorIndexSource)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status != nil {
		t.Errorf("watermark = %v, want nil on failure", status)
	}
}
