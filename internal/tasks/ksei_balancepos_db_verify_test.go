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

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ksei"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// taskKSEIFetcher returns canned holdings without touching the network; it
// records the requested file date and the number of fetch calls.
type taskKSEIFetcher struct {
	holdings  []ksei.Holding
	err       error
	gotFileAt time.Time
	calls     int
}

func (f *taskKSEIFetcher) FetchBalancepos(ctx context.Context, fileDate time.Time) ([]ksei.Holding, error) {
	f.calls++
	f.gotFileAt = fileDate
	return f.holdings, f.err
}

// kseiTestHoldings returns two TEST tickers mirroring real row shapes.
func kseiTestHoldings() []ksei.Holding {
	return []ksei.Holding{
		{Date: time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC), Code: "TESTKA", Type: "EQUITY",
			SecNum: 1_000_000, Price: 100,
			LocalIS: 10, LocalCP: 20, LocalTotal: 600_000,
			ForeignIS: 1, ForeignCP: 2, ForeignTotal: 400_000},
		{Date: time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC), Code: "TESTKB", Type: "EQUITY",
			SecNum: 2_000_000, Price: 200,
			LocalTotal: 1_500_000, ForeignTotal: 500_000},
	}
}

// TestKSEIBalanceposHandler_EndToEnd runs the ksei:balancepos handler against
// a real Postgres: the preceding month-end file is fetched, rows are
// persisted with Total = LocalTotal + ForeignTotal, and source_status is
// updated. Skipped unless IDX_MCP_DB_DSN is set. Cleanup scoped to
// TESTKA/TESTKB + the ksei:balancepos row.
func TestKSEIBalanceposHandler_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM shareholder_composition WHERE ticker IN ('TESTKA', 'TESTKB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeKSEIBalancepos)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetcher := &taskKSEIFetcher{holdings: kseiTestHoldings()}
	repo := repository.NewShareholderCompositionRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewKSEIBalanceposHandler(log, fetcher, db, repo, recorder)

	runDate := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	payload := KSEIBalanceposPayload{Date: runDate.Format("2006-01-02")}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeKSEIBalancepos, raw)

	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// The targeted file is the preceding month-end.
	if fetcher.gotFileAt.Format("2006-01-02") != "2026-08-31" {
		t.Errorf("file date = %s, want 2026-08-31", fetcher.gotFileAt.Format("2006-01-02"))
	}

	// Rows persisted with computed totals.
	stored, err := repo.FindByTickerRange(db, "TESTKA",
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FindByTickerRange: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("expected 1 stored TESTKA row, got %d", len(stored))
	}
	if stored[0].Total != 1_000_000 {
		t.Errorf("total = %d, want 1000000 (local 600000 + foreign 400000)", stored[0].Total)
	}
	if stored[0].LocalTotal != 600_000 || stored[0].ForeignTotal != 400_000 {
		t.Errorf("totals = %d/%d, want 600000/400000", stored[0].LocalTotal, stored[0].ForeignTotal)
	}

	// source_status watermark recorded at the file date.
	status, err := recorder.CurrentWatermark(TypeKSEIBalancepos)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status == nil || status.Format("2006-01-02") != "2026-08-31" {
		t.Errorf("watermark = %v, want 2026-08-31 (file date)", status)
	}

	// Second run for the same file date: the watermark gate no-ops (no
	// fetch), and the stored rows stay untouched.
	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("second handler run: %v", err)
	}
	if fetcher.calls != 1 {
		t.Errorf("second run fetched again — watermark gate did not no-op (calls=%d)", fetcher.calls)
	}
	stored2, err := repo.FindByTickerRange(db, "TESTKA",
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FindByTickerRange (2nd): %v", err)
	}
	if len(stored2) != 1 || stored2[0].Total != 1_000_000 {
		t.Errorf("rows changed after no-op run: %+v", stored2)
	}
}

// TestKSEIBalanceposHandler_Failure verifies a fetch error surfaces and is
// recorded in source_status.
func TestKSEIBalanceposHandler_Failure(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM shareholder_composition WHERE ticker IN ('TESTKA', 'TESTKB')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeKSEIBalancepos)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetchErr := errors.New("ksei api error: status=404")
	fetcher := &taskKSEIFetcher{err: fetchErr}
	repo := repository.NewShareholderCompositionRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewKSEIBalanceposHandler(log, fetcher, db, repo, recorder)

	payload := KSEIBalanceposPayload{Date: "2026-09-06"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeKSEIBalancepos, raw)

	if err := handler(context.Background(), task); err == nil {
		t.Fatal("expected error from fetch failure")
	}

	status, err := recorder.CurrentWatermark(TypeKSEIBalancepos)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status != nil {
		t.Errorf("watermark = %v, want nil on failure", status)
	}
}
