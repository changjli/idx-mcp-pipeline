package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// sweepFetcher returns a per-ticker canned result: normal data by default, an
// empty Result for tickers in empty, an error for tickers in fail. Records
// every fetch so tests can assert upstream call count (pacing/quota honesty).
type sweepFetcher struct {
	empty map[string]bool
	fail  map[string]bool
	calls []string
}

func (f *sweepFetcher) Fetch(_ context.Context, ticker string, _ time.Time) (*ipot.Result, error) {
	f.calls = append(f.calls, ticker)
	if f.fail[ticker] {
		return nil, errors.New("ipot: upstream error: status=500")
	}
	if f.empty[ticker] {
		return &ipot.Result{}, nil
	}
	return &ipot.Result{
		Buyers:  []ipot.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
		Sellers: []ipot.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
		Totals:  ipot.Totals{TVal: 1_500_000_000, FNVal: 100_000_000, TLot: 150, Avg: 100},
	}, nil
}

func newSweepTestUC(t *testing.T, db *sqlx.DB, f *sweepFetcher) *BrokerStockSummaryUseCase {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)
}

// sweepCleanup scopes cleanup to a set of test tickers + the sweep window's
// daily_prices rows (shared across all sweep tests).
func sweepCleanup(t *testing.T, db *sqlx.DB, tickers []string) {
	t.Helper()
	t.Cleanup(func() {
		for _, tk := range tickers {
			db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
		}
	})
}

// seedTradedRange seeds active tickers with a daily_prices row on each of the
// given days (the sweep window's trading-day calendar).
func seedTradedRange(t *testing.T, db *sqlx.DB, tickers []string, days []time.Time) {
	t.Helper()
	for _, tk := range tickers {
		for _, day := range days {
			seedTickerAndPrice(t, db, tk, day)
		}
	}
}

// sweepWindow is a far-future Mon–Fri window: no real market data exists on
// these dates, so the sweep is isolated from a shared DB's real rows.
var sweepWindow = struct {
	from time.Time
	to   time.Time
	days []time.Time
}{
	from: time.Date(2099, 1, 5, 0, 0, 0, 0, time.UTC),
	to:   time.Date(2099, 1, 9, 0, 0, 0, 0, time.UTC),
	days: []time.Time{
		time.Date(2099, 1, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2099, 1, 6, 0, 0, 0, 0, time.UTC),
		time.Date(2099, 1, 7, 0, 0, 0, 0, time.UTC),
		time.Date(2099, 1, 8, 0, 0, 0, 0, time.UTC),
		time.Date(2099, 1, 9, 0, 0, 0, 0, time.UTC),
	},
}

func TestSweepStockBrokerSummaries_FetchesAndPersistsTraders(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{}
	uc := newSweepTestUC(t, db, f)

	tickers := []string{"TESTA", "TESTB", "TESTC"}
	seedTradedRange(t, db, tickers, sweepWindow.days)
	sweepCleanup(t, db, tickers)

	res, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, sweepWindow.from, sweepWindow.to)
	if err != nil {
		t.Fatalf("SweepStockBrokerSummaries: %v", err)
	}
	if res.Tickers != 3 || res.Days != 15 || res.Fetched != 15 {
		t.Errorf("res = %+v, want tickers=3 days=15 fetched=15", res)
	}
	if res.Skipped != 0 || res.Empty != 0 || res.Failed != 0 {
		t.Errorf("unexpected counters: %+v", res)
	}
	if len(f.calls) != 15 {
		t.Fatalf("expected 15 upstream fetches (3 tickers × 5 days), got %d", len(f.calls))
	}

	// Persisted: 15 days × 2 rows.
	stored, err := uc.Repo.FindByDateRangeAll(db, sweepWindow.from, sweepWindow.to)
	if err != nil {
		t.Fatalf("FindByDateRangeAll: %v", err)
	}
	if len(stored) != 30 {
		t.Errorf("expected 30 persisted rows, got %d", len(stored))
	}
}

// Second sweep of the same window must skip already-stored days without any
// upstream call — the sweep's quota-honesty property.
func TestSweepStockBrokerSummaries_SkipsStoredTickers(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{}
	uc := newSweepTestUC(t, db, f)

	tickers := []string{"TESTA", "TESTB"}
	seedTradedRange(t, db, tickers, sweepWindow.days)
	sweepCleanup(t, db, tickers)

	if _, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, sweepWindow.from, sweepWindow.to); err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	callsAfterFirst := len(f.calls)

	res, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, sweepWindow.from, sweepWindow.to)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if res.Skipped != 10 || res.Fetched != 0 {
		t.Errorf("second sweep = %+v, want skipped=10 fetched=0", res)
	}
	if len(f.calls) != callsAfterFirst {
		t.Errorf("second sweep made upstream calls: %d (was %d)", len(f.calls), callsAfterFirst)
	}
}

// A ticker with no daily_prices rows in the window contributes no days and no
// fetches — the ADTV universe filter upstream already excluded it, and the
// sweep's per-ticker trading-day query is the second gate.
func TestSweepStockBrokerSummaries_SkipsUntradedTickers(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{}
	uc := newSweepTestUC(t, db, f)

	// TESTB has no daily_prices rows in the window → not traded → no days.
	seedTradedRange(t, db, []string{"TESTA"}, sweepWindow.days)
	for _, tk := range []string{"TESTB"} {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true) ON CONFLICT (code) DO NOTHING", tk, tk)
	}
	sweepCleanup(t, db, []string{"TESTA", "TESTB"})

	res, err := uc.SweepStockBrokerSummaries(context.Background(), []string{"TESTA", "TESTB"}, sweepWindow.from, sweepWindow.to)
	if err != nil {
		t.Fatalf("SweepStockBrokerSummaries: %v", err)
	}
	if res.Tickers != 2 || res.Days != 5 || res.Fetched != 5 {
		t.Errorf("res = %+v, want tickers=2 days=5 fetched=5", res)
	}
	if len(f.calls) != 5 {
		t.Errorf("expected 5 upstream fetches (TESTA only), got %d", len(f.calls))
	}
}

// A window with no daily_prices rows at all is a zero-fetch sweep — the
// per-ticker trading-day query IS the calendar.
func TestSweepStockBrokerSummaries_NonTradingDayZeroFetch(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{}
	uc := newSweepTestUC(t, db, f)

	// No daily_prices rows at all in the window.
	tickers := []string{"TESTA", "TESTB"}
	for _, tk := range tickers {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true) ON CONFLICT (code) DO NOTHING", tk, tk)
	}
	sweepCleanup(t, db, tickers)

	res, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, sweepWindow.from, sweepWindow.to)
	if err != nil {
		t.Fatalf("SweepStockBrokerSummaries: %v", err)
	}
	if res.Tickers != 2 || res.Days != 0 || res.Fetched != 0 {
		t.Errorf("res = %+v, want tickers=2 days=0 fetched=0", res)
	}
	if len(f.calls) != 0 {
		t.Errorf("empty-window sweep made upstream calls: %d", len(f.calls))
	}
}

// Empty (IPOT not yet published) and failed (upstream 5xx) tickers are counted
// separately and never abort the sweep; the good ticker still persists.
func TestSweepStockBrokerSummaries_IsolatesEmptyAndFailed(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{empty: map[string]bool{"TESTB": true}, fail: map[string]bool{"TESTC": true}}
	uc := newSweepTestUC(t, db, f)

	tickers := []string{"TESTA", "TESTB", "TESTC"}
	seedTradedRange(t, db, tickers, sweepWindow.days)
	sweepCleanup(t, db, tickers)

	res, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, sweepWindow.from, sweepWindow.to)
	if err != nil {
		t.Fatalf("SweepStockBrokerSummaries: %v", err)
	}
	if res.Fetched != 5 || res.Empty != 5 || res.Failed != 5 {
		t.Errorf("res = %+v, want fetched=5 empty=5 failed=5", res)
	}
	if res.Skipped != 0 {
		t.Errorf("unexpected skipped: %+v", res)
	}

	// Only the good ticker persisted (5 days × 2 rows).
	stored, err := uc.Repo.FindByDateRangeAll(db, sweepWindow.from, sweepWindow.to)
	if err != nil {
		t.Fatalf("FindByDateRangeAll: %v", err)
	}
	if len(stored) != 10 {
		t.Errorf("expected 10 persisted rows (TESTA only), got %d", len(stored))
	}
}
