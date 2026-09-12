package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// taskSectorIndexFetcher returns canned screener rows without touching the
// network.
type taskSectorIndexFetcher struct {
	rows []client.ScreenerRow
	err  error
}

func (f *taskSectorIndexFetcher) FetchScreener(ctx context.Context) ([]client.ScreenerRow, error) {
	return f.rows, f.err
}

func strPtr(s string) *string { return &s }

// TestSectorIndexHandler_EndToEnd runs the idx:sector_index handler against a
// real Postgres: screener rows are fetched, sector columns upserted, membership
// replaced for the run date, and source_status updated on success. Skipped
// unless IDX_MCP_DB_DSN is set. Cleanup scoped to TESTSA/TESTSB + the
// idx:sector_index row.
func TestSectorIndexHandler_EndToEnd(t *testing.T) {
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
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeSectorIndex)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetcher := &taskSectorIndexFetcher{rows: []client.ScreenerRow{
		{StockCode: "TESTSA", Sector: "Energy", SubSector: "Oil, Gas & Coal", Industry: strPtr("Coal"), SubIndustry: strPtr("Coal Production"), SubIndustryCode: strPtr("A121"), IndexCode: strPtr("COMPOSITE, LQ45")},
		{StockCode: "TESTSB", Sector: "Financials", SubSector: "Banks", Industry: strPtr("Banks"), SubIndustry: strPtr("Banks"), SubIndustryCode: strPtr("G111"), IndexCode: nil},
	}}

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	uc := usecase.NewSectorIndexUseCase(
		db, log, fetcher, repository.NewTickerRepository(log), repository.NewTickerIndexRepository(log), recorder,
	)
	handler := NewSectorIndexHandler(log, uc)

	runDate := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	payload := SectorIndexPayload{Date: runDate.Format("2006-01-02")}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeSectorIndex, raw)

	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// Membership rows landed with the effective_date.
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM ticker_indices WHERE effective_date = $1", runDate); err != nil {
		t.Fatalf("membership count: %v", err)
	}
	if count != 2 {
		t.Errorf("membership rows = %d, want 2 (TESTSA only; TESTSB has null indexCode)", count)
	}

	// source_status success recorded with the run-date watermark.
	status, err := recorder.CurrentWatermark(TypeSectorIndex)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status == nil || status.Format("2006-01-02") != "2026-09-09" {
		t.Errorf("watermark = %v, want 2026-09-09", status)
	}
}

// TestSectorIndexHandler_Failure verifies a fetch error surfaces and is
// recorded in source_status.
func TestSectorIndexHandler_Failure(t *testing.T) {
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
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeSectorIndex)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetcher := &taskSectorIndexFetcher{err: errors.New("idx api error: status=403")}

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	uc := usecase.NewSectorIndexUseCase(
		db, log, fetcher, repository.NewTickerRepository(log), repository.NewTickerIndexRepository(log), recorder,
	)
	handler := NewSectorIndexHandler(log, uc)

	payload := SectorIndexPayload{Date: "2026-09-09"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeSectorIndex, raw)

	if err := handler(context.Background(), task); err == nil {
		t.Fatal("expected error from fetch failure")
	}

	status, err := recorder.CurrentWatermark(TypeSectorIndex)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status != nil {
		t.Errorf("watermark = %v, want nil on failure", status)
	}
}
