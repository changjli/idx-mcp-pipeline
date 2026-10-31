package tasks

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// ─── fakes ─────────────────────────────────────────────────────────────────

// taskSweepBudget is a fixed quota view for the handler tests.
type taskSweepBudget struct {
	available int
	perKey    int
}

func (b *taskSweepBudget) Available() int     { return b.available }
func (b *taskSweepBudget) BatchCapacity() int { return b.perKey }
func (b *taskSweepBudget) NextReset() *time.Time {
	t := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	return &t
}
func (b *taskSweepBudget) EnsureSeeded(context.Context) error {
	return nil
}

// taskBatchFetcher returns a fixed valid result per ticker. failAfter makes the
// call after N successes fail, so a quota stop mid-sweep is testable.
type taskBatchFetcher struct {
	failAfter int
	failErr   error
	calls     int
}

func (f *taskBatchFetcher) Fetch(_ context.Context, _ string, _ time.Time) (*broker.Result, error) {
	return taskBatchResult(), nil
}

func (f *taskBatchFetcher) FetchBatch(_ context.Context, tickers []string, _ time.Time) (*broker.Batch, error) {
	f.calls++
	if f.failErr != nil && f.failAfter >= 0 && f.calls > f.failAfter {
		return nil, f.failErr
	}
	b := &broker.Batch{Results: make(map[string]*broker.Result, len(tickers)), Invalid: map[string]error{}}
	for _, t := range tickers {
		b.Results[t] = taskBatchResult()
	}
	return b, nil
}

func taskBatchResult() *broker.Result {
	return &broker.Result{
		Buyers:  []broker.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
		Sellers: []broker.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
		Totals:  broker.Totals{TVal: 1_500_000_000, TLot: 150, Avg: 100},
	}
}

// sweepHandlerFixture seeds the DB and wires the handler. The anchor is a
// far-future date, so the eligibility query only sees the seeded tickers.
func sweepHandlerFixture(t *testing.T, tickers []string, anchor time.Time, f *taskBatchFetcher, b *taskSweepBudget) (asynq.HandlerFunc, *sqlx.DB) {
	t.Helper()
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	windowDays := []time.Time{
		anchor.AddDate(0, 0, -21),
		anchor.AddDate(0, 0, -20),
		anchor.AddDate(0, 0, -17),
		anchor.AddDate(0, 0, -16),
		anchor.AddDate(0, 0, -15),
		anchor.AddDate(0, 0, -14),
		anchor.AddDate(0, 0, -13),
		anchor.AddDate(0, 0, -10),
	}
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
		for _, d := range windowDays {
			db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)
				VALUES ($1, $2, 100, 101, 99, 100, 1000, 10000000000, 10, 'idx')`, tk, d)
		}
	}

	uc := usecase.NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
		usecase.WithBatchSource(f, b),
	)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	return NewBrokerSummarySweepHandler(log, uc, recorder, 5_000_000_000), db
}

func runSweepTask(t *testing.T, h asynq.HandlerFunc, anchor time.Time) {
	t.Helper()
	raw, _ := json.Marshal(BrokerSummarySweepPayload{Date: anchor.Format("2006-01-02")})
	if err := h(context.Background(), asynq.NewTask(TypeBrokerStockSummarySweep, raw)); err != nil {
		t.Fatalf("handler: %v", err)
	}
}

// ─── tests ─────────────────────────────────────────────────────────────────

// TestBrokerSummarySweepHandler_EndToEnd sweeps the anchor day's eligible
// universe, persists each ticker-day, records source_status + watermark, and
// makes the second run a no-op (skip-if-stored).
func TestBrokerSummarySweepHandler_EndToEnd(t *testing.T) {
	tickers := []string{"TESTU", "TESTV"}
	anchor := time.Date(2099, 12, 31, 0, 0, 0, 0, time.UTC)
	f := &taskBatchFetcher{}
	h, db := sweepHandlerFixture(t, tickers, anchor, f, &taskSweepBudget{available: 10, perKey: 50})

	runSweepTask(t, h, anchor)

	// Only the anchor day is swept; each ticker gets both sides.
	var rows int
	if err := db.Get(&rows, "SELECT COUNT(*) FROM broker_stock_summaries WHERE trading_day = $1", anchor); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 4 {
		t.Errorf("persisted rows = %d, want 4 (2 tickers × 2 sides)", rows)
	}
	if f.calls != 1 {
		t.Errorf("batch calls = %d, want 1", f.calls)
	}

	var status struct {
		LastError *string    `db:"last_error"`
		Failures  int        `db:"consecutive_failures"`
		Watermark *time.Time `db:"high_water_mark"`
	}
	if err := db.Get(&status, "SELECT last_error, consecutive_failures, high_water_mark FROM source_status WHERE source = $1", TypeBrokerStockSummary); err != nil {
		t.Fatalf("query source_status: %v", err)
	}
	if status.LastError != nil {
		t.Errorf("last_error = %q, want nil", *status.LastError)
	}
	if status.Failures != 0 {
		t.Errorf("consecutive_failures = %d, want 0", status.Failures)
	}
	if status.Watermark == nil || status.Watermark.Format("2006-01-02") != anchor.Format("2006-01-02") {
		t.Errorf("high_water_mark = %v, want %s", status.Watermark, anchor.Format("2006-01-02"))
	}

	// Second run: every ticker-day is stored → no upstream call, no new rows.
	runSweepTask(t, h, anchor)
	var after int
	if err := db.Get(&after, "SELECT COUNT(*) FROM broker_stock_summaries WHERE trading_day = $1", anchor); err != nil {
		t.Fatalf("count rows after second run: %v", err)
	}
	if after != 4 {
		t.Errorf("second run changed rows: %d, want 4", after)
	}
	if f.calls != 1 {
		t.Errorf("second run made %d extra batch calls", f.calls-1)
	}
}

// TestBrokerSummarySweepHandler_QuotaStopRecordsShortfall simulates the pool
// running dry mid-sweep: the first batch stays stored, the handler returns nil
// (no retry against an exhausted key) and Source Status carries the shortfall
// as a coverage condition, not a parse/network failure.
func TestBrokerSummarySweepHandler_QuotaStopRecordsShortfall(t *testing.T) {
	tickers := []string{"TESTQ", "TESTR", "TESTS"}
	anchor := time.Date(2099, 12, 30, 0, 0, 0, 0, time.UTC)
	// perKey=1 → batches of one ticker; the second call hits the quota wall.
	f := &taskBatchFetcher{failAfter: 1, failErr: broker.ErrQuotaExhausted}
	h, db := sweepHandlerFixture(t, tickers, anchor, f, &taskSweepBudget{available: 10, perKey: 1})

	runSweepTask(t, h, anchor)

	var rows int
	if err := db.Get(&rows, "SELECT COUNT(*) FROM broker_stock_summaries WHERE trading_day = $1", anchor); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 2 {
		t.Errorf("persisted rows = %d, want 2 (first batch intact)", rows)
	}

	var status struct {
		LastError *string `db:"last_error"`
	}
	if err := db.Get(&status, "SELECT last_error FROM source_status WHERE source = $1", TypeBrokerStockSummary); err != nil {
		t.Fatalf("query source_status: %v", err)
	}
	if status.LastError == nil {
		t.Fatal("last_error = nil, want the quota shortfall recorded")
	}
	for _, want := range []string{"quota exhausted", "swept 1 of 3"} {
		if !strings.Contains(*status.LastError, want) {
			t.Errorf("last_error = %q, want it to contain %q", *status.LastError, want)
		}
	}
}
