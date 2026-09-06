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
)

// taskSuspensionsFetcher returns canned lists without touching the network; it
// records the requested window and can fail either call independently.
type taskSuspensionsFetcher struct {
	suspensions []client.Suspension
	umas        []client.Uma
	suspErr     error
	umaErr      error
	gotFrom     time.Time
	gotTo       time.Time
}

func (f *taskSuspensionsFetcher) FetchSuspensions(ctx context.Context, from, to time.Time) ([]client.Suspension, error) {
	f.gotFrom = from
	f.gotTo = to
	return f.suspensions, f.suspErr
}

func (f *taskSuspensionsFetcher) FetchUma(ctx context.Context, from, to time.Time) ([]client.Uma, error) {
	return f.umas, f.umaErr
}

// TestSuspensionsHandler_EndToEnd runs the idx:suspensions handler against a
// real Postgres: both lists are fetched, rows persist with the right types,
// and source_status is updated on success. Skipped unless IDX_MCP_DB_DSN is
// set. Cleanup scoped to TESTSA/TESTSB + the idx:suspensions row.
func TestSuspensionsHandler_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM suspensions WHERE ticker IN ('TESTSA', 'TESTSB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeSuspensions)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetcher := &taskSuspensionsFetcher{
		suspensions: []client.Suspension{
			{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), Type: "SPT", Reason: "Penghentian Sementara TESTSA"},
			{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), Type: "UPT", Reason: "Pembukaan Kembali TESTSA"},
		},
		umas: []client.Uma{
			{Ticker: "TESTSB", EventDate: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), AnnouncementNo: "Peng-UMA-1", Reason: "UMA atas Saham TESTSB"},
		},
	}

	repo := repository.NewSuspensionRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewSuspensionsHandler(log, fetcher, db, repo, recorder, 7, 7)

	runDate := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	payload := SuspensionsPayload{Date: runDate.Format("2006-01-02")}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeSuspensions, raw)

	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// Window: runDate - 7d .. runDate + 7d.
	if fetcher.gotFrom.Format("2006-01-02") != "2026-08-30" || fetcher.gotTo.Format("2006-01-02") != "2026-09-13" {
		t.Errorf("window = %s..%s, want 2026-08-30..2026-09-13", fetcher.gotFrom.Format("2006-01-02"), fetcher.gotTo.Format("2006-01-02"))
	}

	// Rows persisted with the right types (UMA row carries type UMA).
	stored, err := repo.FindByDateRange(db, fetcher.gotFrom, fetcher.gotTo, nil)
	if err != nil {
		t.Fatalf("FindByDateRange: %v", err)
	}
	if len(stored) != 3 {
		t.Fatalf("expected 3 stored rows, got %d", len(stored))
	}
	types := map[string]bool{}
	for _, s := range stored {
		types[s.Type] = true
	}
	if !types["SPT"] || !types["UPT"] || !types["UMA"] {
		t.Errorf("stored types = %v, want SPT/UPT/UMA", types)
	}

	// source_status success recorded (max event date = 2026-09-03).
	status, err := recorder.CurrentWatermark(TypeSuspensions)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status == nil || status.Format("2006-01-02") != "2026-09-03" {
		t.Errorf("watermark = %v, want 2026-09-03 (max event date)", status)
	}
}

// TestSuspensionsHandler_Failure verifies a fetch error surfaces and is
// recorded in source_status (per-call failure, not a combined error).
func TestSuspensionsHandler_Failure(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM suspensions WHERE ticker IN ('TESTSA', 'TESTSB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeSuspensions)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetchErr := errors.New("idx api error: status=403")
	fetcher := &taskSuspensionsFetcher{umaErr: fetchErr}

	repo := repository.NewSuspensionRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewSuspensionsHandler(log, fetcher, db, repo, recorder, 7, 7)

	payload := SuspensionsPayload{Date: "2026-09-06"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeSuspensions, raw)

	if err := handler(context.Background(), task); err == nil {
		t.Fatal("expected error from fetch failure")
	}

	status, err := recorder.CurrentWatermark(TypeSuspensions)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status != nil {
		t.Errorf("watermark = %v, want nil on failure", status)
	}
}
