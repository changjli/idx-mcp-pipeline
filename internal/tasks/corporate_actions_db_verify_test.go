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

// taskCorporateActionsFetcher returns canned actions without touching the
// network; it records the requested window.
type taskCorporateActionsFetcher struct {
	actions []client.CorporateAction
	err     error
	gotFrom time.Time
	gotTo   time.Time
}

func (f *taskCorporateActionsFetcher) FetchCorporateActions(ctx context.Context, from, to time.Time) ([]client.CorporateAction, error) {
	f.gotFrom = from
	f.gotTo = to
	return f.actions, f.err
}

// TestCorporateActionsHandler_EndToEnd runs the idx:corporate_actions handler
// against a real Postgres: the rolling window is fetched, rows are persisted,
// and source_status is updated on success. Skipped unless IDX_MCP_DB_DSN is
// set. Cleanup scoped to TESTCA/TESTCB + the idx:corporate_actions row.
func TestCorporateActionsHandler_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM corporate_actions WHERE ticker IN ('TESTCA', 'TESTCB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeCorporateActions)
	}
	cleanup()
	t.Cleanup(cleanup)

	actions := []client.CorporateAction{
		{ID: 9_300_001, Ticker: "TESTCA", EventDate: time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC), Type: "Waran", Detail: client.CorporateActionDetail{JumlahSaham: 1000, JumlahSahamSetelahTindakan: 2000}},
		{ID: 9_300_002, Ticker: "TESTCB", EventDate: time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC), Type: "Stock Split", Detail: client.CorporateActionDetail{JumlahSaham: 0, JumlahSahamSetelahTindakan: 0}},
	}
	fetcher := &taskCorporateActionsFetcher{actions: actions}

	repo := repository.NewCorporateActionRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewCorporateActionsHandler(log, fetcher, db, repo, recorder, 7, 90)

	runDate := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	payload := CorporateActionsPayload{Date: runDate.Format("2006-01-02")}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeCorporateActions, raw)

	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// Window: runDate - 7d .. runDate + 90d.
	if fetcher.gotFrom.Format("2006-01-02") != "2026-08-30" || fetcher.gotTo.Format("2006-01-02") != "2026-12-05" {
		t.Errorf("window = %s..%s, want 2026-08-30..2026-12-05", fetcher.gotFrom.Format("2006-01-02"), fetcher.gotTo.Format("2006-01-02"))
	}

	// Rows persisted.
	stored, err := repo.FindByDateRange(db, fetcher.gotFrom, fetcher.gotTo, nil)
	if err != nil {
		t.Fatalf("FindByDateRange: %v", err)
	}
	if len(stored) != 2 {
		t.Fatalf("expected 2 stored rows, got %d", len(stored))
	}

	// source_status success recorded.
	status, err := recorder.CurrentWatermark(TypeCorporateActions)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status == nil || status.Format("2006-01-02") != "2026-09-06" {
		t.Errorf("watermark = %v, want 2026-09-06 (max event date)", status)
	}
}

// TestCorporateActionsHandler_Failure verifies a fetch error surfaces and is
// recorded in source_status.
func TestCorporateActionsHandler_Failure(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM corporate_actions WHERE ticker IN ('TESTCA', 'TESTCB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeCorporateActions)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetchErr := errors.New("idx api error: status=403")
	fetcher := &taskCorporateActionsFetcher{err: fetchErr}

	repo := repository.NewCorporateActionRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewCorporateActionsHandler(log, fetcher, db, repo, recorder, 7, 90)

	payload := CorporateActionsPayload{Date: "2026-09-06"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeCorporateActions, raw)

	if err := handler(context.Background(), task); err == nil {
		t.Fatal("expected error from fetch failure")
	}

	status, err := recorder.CurrentWatermark(TypeCorporateActions)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status != nil {
		t.Errorf("watermark = %v, want nil on failure", status)
	}
}
