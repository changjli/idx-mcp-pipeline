package tasks

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// TestBrokerSummarySweepHandler_EndToEnd runs the idx:broker_stock_summary_sweep
// handler against a real Postgres: active tickers that traded on the sweep day
// get rows persisted, and source_status is updated. The sweep day is a
// far-future date seeded with only the TEST tickers' daily_prices rows, so the
// traded-ticker filter isolates the sweep from a shared DB's real market data
// (every other active ticker is not-traded → zero upstream writes). A second
// run is a no-op (skip-if-stored). Skipped unless IDX_MCP_DB_DSN is set.
func TestBrokerSummarySweepHandler_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	uc := usecase.NewBrokerStockSummaryUseCase(
		db, log, validator.New(), taskFakeFetcher{},
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)
	handler := NewBrokerSummarySweepHandler(
		log, db, uc, repository.NewTickerRepository(log),
		pipeline.NewSourceStatusRecorder(
			pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
			pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
			log,
		),
	)

	tickers := []string{"TESTU", "TESTV"}
	day := time.Date(2099, 12, 31, 0, 0, 0, 0, time.UTC) // no real market data this date
	for _, tk := range tickers {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", tk)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", tk)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
		db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
	}
	db.MustExec("DELETE FROM source_status WHERE source = $1", TypeBrokerStockSummary)
	t.Cleanup(func() {
		for _, tk := range tickers {
			db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
		}
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeBrokerStockSummary)
	})

	for _, tk := range tickers {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", tk, tk)
		db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)
			VALUES ($1, $2, 100, 101, 99, 100, 1000, 100000, 10, 'idx')`, tk, day)
	}

	payload := BrokerSummarySweepPayload{Date: day.Format("2006-01-02")}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeBrokerStockSummarySweep, raw)

	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// Both traded test tickers persisted (2 rows each).
	var count int
	for _, tk := range tickers {
		var c int
		err := db.Get(&c, "SELECT COUNT(*) FROM broker_stock_summaries WHERE ticker = $1", tk)
		if err != nil {
			t.Fatalf("count rows %s: %v", tk, err)
		}
		count += c
	}
	if count != 4 {
		t.Errorf("expected 4 persisted rows (2 tickers × 2), got %d", count)
	}

	// Second run: already stored → skipped, no new rows, no error.
	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("second handler run: %v", err)
	}
	var after int
	for _, tk := range tickers {
		var c int
		if err := db.Get(&c, "SELECT COUNT(*) FROM broker_stock_summaries WHERE ticker = $1", tk); err != nil {
			t.Fatalf("count rows second run %s: %v", tk, err)
		}
		after += c
	}
	if after != 4 {
		t.Errorf("second run changed row count: got %d, want 4 (skip-if-stored)", after)
	}

	// source_status recorded with no error.
	var status struct {
		LastError           *string
		ConsecutiveFailures int
	}
	err := db.Get(&status, "SELECT last_error, consecutive_failures FROM source_status WHERE source = $1", TypeBrokerStockSummary)
	if err != nil {
		t.Fatalf("query source_status: %v", err)
	}
	if status.LastError != nil {
		t.Errorf("expected no last_error on success, got %q", *status.LastError)
	}
	if status.ConsecutiveFailures != 0 {
		t.Errorf("expected 0 consecutive failures, got %d", status.ConsecutiveFailures)
	}
}
