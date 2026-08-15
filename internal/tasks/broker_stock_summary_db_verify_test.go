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

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// taskFakeFetcher returns a canned IPOT result without touching the network.
type taskFakeFetcher struct{}

func (taskFakeFetcher) Fetch(_ context.Context, _ string, _ time.Time) (*ipot.Result, error) {
	return &ipot.Result{
		Buyers:  []ipot.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
		Sellers: []ipot.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
		Totals:  ipot.Totals{TVal: 1_500_000_000, FNVal: 100_000_000, TLot: 150, Avg: 100},
	}, nil
}

// TestBrokerStockSummaryHandler_EndToEnd runs the idx:broker_stock_summary
// handler against a real Postgres: the shared fetch+persist core stores rows,
// and source_status is updated on success. Skipped unless IDX_MCP_DB_DSN is
// set. Cleanup scoped to TESTQ + the broker_stock_summary source_status row.
func TestBrokerStockSummaryHandler_EndToEnd(t *testing.T) {
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
	handler := NewBrokerStockSummaryHandler(
		log, db, uc,
		repository.NewSourceStatusRepository(log),
		repository.NewAlertRepository(log),
	)

	ticker := "TESTQ"
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM source_status WHERE source = $1", TypeBrokerStockSummary)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeBrokerStockSummary)
	})

	payload := BrokerStockSummaryPayload{Ticker: ticker, Date: day.Format("2006-01-02")}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeBrokerStockSummary, raw)

	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// Rows persisted.
	var count int
	db.Get(&count, "SELECT COUNT(*) FROM broker_stock_summaries WHERE ticker = $1", ticker)
	if count != 2 {
		t.Errorf("expected 2 persisted rows, got %d", count)
	}

	// source_status updated on success.
	var status entity.SourceStatus
	err := db.Get(&status, "SELECT * FROM source_status WHERE source = $1", TypeBrokerStockSummary)
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
