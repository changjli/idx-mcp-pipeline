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

// taskIndexSummaryFetcher returns canned summaries without touching the
// network.
type taskIndexSummaryFetcher struct {
	rows []client.IndexSummary
	err  error
}

func (f *taskIndexSummaryFetcher) FetchIndexSummary(ctx context.Context) ([]client.IndexSummary, error) {
	return f.rows, f.err
}

// TestIndexSummaryHandler_EndToEnd runs the idx:index_summary handler against a
// real Postgres: the 45-index snapshot is fetched, rows are persisted keyed by
// the wire trading date, a same-day re-run is idempotent, and source_status is
// updated on success. Skipped unless IDX_MCP_DB_DSN is set. Cleanup scoped to
// the TESTI rows + the idx:index_summary source_status row.
func TestIndexSummaryHandler_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM index_summaries WHERE index_code IN ('TESTIC', 'TESTIE')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeIndexSummary)
	}
	cleanup()
	t.Cleanup(cleanup)

	day := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	rows := []client.IndexSummary{
		{Date: day, IndexCode: "TESTIC", Previous: 6686.442, Highest: 6707.305, Lowest: 6642.153, Close: 6678.201, NumberOfStock: 918, Change: -8.241, Volume: 32530207402, Value: 16134522240878, Frequency: 2147005, MarketCap: 1.16749775143559e+16},
		{Date: day, IndexCode: "TESTIE", Previous: 2501.1, Highest: 2530.0, Lowest: 2488.8, Close: 2512.3, NumberOfStock: 90, Change: 11.2, Volume: 123456, Value: 987654321, Frequency: 12345, MarketCap: 12345678901234},
	}
	fetcher := &taskIndexSummaryFetcher{rows: rows}

	repo := repository.NewIndexSummaryRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewIndexSummaryHandler(log, fetcher, db, repo, recorder)

	runDate := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	payload := IndexSummaryPayload{Date: runDate.Format("2006-01-02")}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeIndexSummary, raw)

	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// Rows persisted, keyed by the wire date.
	stored := indexRows(db, "TESTIC", "TESTIE")
	if len(stored) != 2 {
		t.Fatalf("expected 2 stored rows, got %d", len(stored))
	}
	if stored[0].Close == nil || *stored[0].Close != 6678.201 {
		t.Fatalf("expected close 6678.201, got %v", stored[0].Close)
	}
	if stored[1].NumberOfStock == nil || *stored[1].NumberOfStock != 90 {
		t.Fatalf("expected number_of_stock 90, got %v", stored[1].NumberOfStock)
	}
	if stored[0].MarketCap == nil || *stored[0].MarketCap != 11674977514355900 {
		t.Fatalf("expected market_cap 11674977514355900, got %v", stored[0].MarketCap)
	}
	if stored[0].Date.Format("2006-01-02") != "2026-09-09" {
		t.Errorf("stored date = %s, want 2026-09-09 (wire date)", stored[0].Date.Format("2006-01-02"))
	}

	// Same-day re-run is idempotent: same rows, updated close.
	rows[0].Close = 6690.0
	fetcher.rows = rows
	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler re-run: %v", err)
	}
	stored = indexRows(db, "TESTIC", "TESTIE")
	if len(stored) != 2 {
		t.Fatalf("expected 2 stored rows after re-run, got %d", len(stored))
	}
	if *stored[0].Close != 6690.0 {
		t.Errorf("expected close refreshed to 6690.0, got %v", *stored[0].Close)
	}

	// source_status success recorded with the max wire date.
	status, err := recorder.CurrentWatermark(TypeIndexSummary)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status == nil || status.Format("2006-01-02") != "2026-09-09" {
		t.Errorf("watermark = %v, want 2026-09-09 (max wire date)", status)
	}
}

// TestIndexSummaryHandler_Failure verifies a fetch error surfaces and is
// recorded in source_status.
func TestIndexSummaryHandler_Failure(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM index_summaries WHERE index_code IN ('TESTIC', 'TESTIE')")
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeIndexSummary)
	}
	cleanup()
	t.Cleanup(cleanup)

	fetchErr := errors.New("idx api error: status=403")
	fetcher := &taskIndexSummaryFetcher{err: fetchErr}

	repo := repository.NewIndexSummaryRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewIndexSummaryHandler(log, fetcher, db, repo, recorder)

	payload := IndexSummaryPayload{Date: "2026-09-09"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeIndexSummary, raw)

	if err := handler(context.Background(), task); err == nil {
		t.Fatal("expected error from fetch failure")
	}

	status, err := recorder.CurrentWatermark(TypeIndexSummary)
	if err != nil {
		t.Fatalf("CurrentWatermark: %v", err)
	}
	if status != nil {
		t.Errorf("watermark = %v, want nil on failure", status)
	}
}

// indexRows reads stored index_summaries rows for the given codes, newest date
// first (a tiny read helper shared by the handler tests).
func indexRows(db *sqlx.DB, codes ...string) []struct {
	IndexCode     string    `db:"index_code"`
	Date          time.Time `db:"date"`
	Close         *float64  `db:"close"`
	NumberOfStock *int32    `db:"number_of_stock"`
	MarketCap     *int64    `db:"market_cap"`
} {
	query, args, err := sqlx.In(
		"SELECT index_code, date, close, number_of_stock, market_cap FROM index_summaries WHERE index_code IN (?) ORDER BY date DESC, index_code",
		codes,
	)
	if err != nil {
		panic(err)
	}
	query = db.Rebind(query)
	var rows []struct {
		IndexCode     string    `db:"index_code"`
		Date          time.Time `db:"date"`
		Close         *float64  `db:"close"`
		NumberOfStock *int32    `db:"number_of_stock"`
		MarketCap     *int64    `db:"market_cap"`
	}
	if err := db.Select(&rows, query, args...); err != nil {
		panic(err)
	}
	return rows
}
