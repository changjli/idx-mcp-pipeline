package tasks

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// taskFinancialsFetcher returns a canned ok snapshot without touching the
// network and counts calls.
type taskFinancialsFetcher struct {
	calls int
	asset int64
}

func (f *taskFinancialsFetcher) FetchFinancialSnapshot(ctx context.Context, year, month int) (*financials.Snapshot, error) {
	f.calls++
	period := time.Date(2092, time.December, 31, 0, 0, 0, 0, time.UTC)
	share := 1.0
	st := financials.Statement{
		Ticker:         "TESTHD",
		PeriodEnd:      period,
		FiscalYearEnd:  time.Date(2092, time.December, 31, 0, 0, 0, 0, time.UTC),
		DurationMonths: 12,
		Assets:         &f.asset,
		Source:         financials.SourceRatioSnapshot,
		ValueBasis:     financials.ValueBasisIDX,
		SourceFSDate:   period,
	}
	return &financials.Snapshot{
		Month:           time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC),
		Status:          financials.StatusOK,
		RowCount:        1,
		DistinctPeriods: 1,
		QuarterEndShare: &share,
		ContentHash:     "payload",
		Statements:      []financials.Statement{st},
	}, nil
}

// TestFinancialsHandler_EndToEnd runs the idx:financials handler against a real
// Postgres: the previous month is ingested, source_status advances, and a
// second fire is a no-op. Skipped unless IDX_MCP_DB_DSN is set.
func TestFinancialsHandler_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec(`DELETE FROM financial_statement_versions WHERE ticker = 'TESTHD'`)
		db.MustExec(`DELETE FROM financial_statements WHERE ticker = 'TESTHD'`)
		db.MustExec(`DELETE FROM financial_snapshot_runs WHERE snapshot_month >= '2093-01-01' AND snapshot_month < '2094-01-01'`)
		db.MustExec(`DELETE FROM source_status WHERE source = $1`, usecase.FinancialsSource)
	}
	cleanup()
	t.Cleanup(cleanup)

	repo := repository.NewFinancialsRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	fetcher := &taskFinancialsFetcher{asset: 42}
	uc := usecase.NewFinancialsIngestUseCase(db, log, fetcher, repo)
	handler := NewFinancialsHandler(log, uc, recorder)

	runDate := time.Date(2093, 3, 2, 0, 0, 0, 0, time.UTC)
	raw, _ := json.Marshal(FinancialsPayload{Date: runDate.Format("2006-01-02")})
	if err := handler(context.Background(), asynq.NewTask(TypeFinancials, raw)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if fetcher.calls != 1 {
		t.Fatalf("fetcher calls = %d, want 1", fetcher.calls)
	}

	rows, err := repo.FindByTicker(db, "TESTHD")
	if err != nil {
		t.Fatalf("FindByTicker: %v", err)
	}
	if len(rows) != 1 || rows[0].Assets == nil || *rows[0].Assets != 42 {
		t.Fatalf("rows = %+v, want one statement with assets 42", rows)
	}

	status, err := repository.NewSourceStatusRepository(log).FindBySource(db, usecase.FinancialsSource)
	if err != nil {
		t.Fatalf("FindBySource: %v", err)
	}
	if status == nil || status.HighWaterMark == nil {
		t.Fatalf("source_status = %+v, want a high-water mark", status)
	}
	if got := status.HighWaterMark.Format("2006-01"); got != "2093-02" {
		t.Errorf("high-water mark = %s, want 2093-02", got)
	}
	if status.MaxAgeSeconds != FinancialsMaxAgeSeconds {
		t.Errorf("max_age_seconds = %d, want %d", status.MaxAgeSeconds, FinancialsMaxAgeSeconds)
	}

	// A second fire skips the fetch.
	if err := handler(context.Background(), asynq.NewTask(TypeFinancials, raw)); err != nil {
		t.Fatalf("second handler: %v", err)
	}
	if fetcher.calls != 1 {
		t.Errorf("fetcher calls after second fire = %d, want 1", fetcher.calls)
	}
}
