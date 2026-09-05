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

// sweepCleanup scopes cleanup to a set of test tickers + the sweep date's
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

// seedTradedOn seeds active tickers with a daily_prices row on the given day.
func seedTradedOn(t *testing.T, db *sqlx.DB, tickers []string, day time.Time) {
	t.Helper()
	for _, tk := range tickers {
		seedTickerAndPrice(t, db, tk, day)
	}
}

func TestSweepStockBrokerSummaries_FetchesAndPersistsTraders(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{}
	uc := newSweepTestUC(t, db, f)

	day := time.Date(2099, 1, 5, 0, 0, 0, 0, time.UTC)
	tickers := []string{"TESTA", "TESTB", "TESTC"}
	seedTradedOn(t, db, tickers, day)
	sweepCleanup(t, db, tickers)

	res, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, day)
	if err != nil {
		t.Fatalf("SweepStockBrokerSummaries: %v", err)
	}
	if res.Total != 3 || res.Fetched != 3 {
		t.Errorf("res = %+v, want total=3 fetched=3", res)
	}
	if res.Skipped != 0 || res.NotTraded != 0 || res.Empty != 0 || res.Failed != 0 {
		t.Errorf("unexpected counters: %+v", res)
	}
	if len(f.calls) != 3 {
		t.Fatalf("expected 3 upstream fetches, got %v", f.calls)
	}

	// Persisted: 3 tickers × 2 rows.
	stored, err := uc.Repo.FindByDateRangeAll(db, day, day)
	if err != nil {
		t.Fatalf("FindByDateRangeAll: %v", err)
	}
	if len(stored) != 6 {
		t.Errorf("expected 6 persisted rows, got %d", len(stored))
	}
}

// Second sweep of the same day must skip already-stored tickers without any
// upstream call — the sweep's quota-honesty property.
func TestSweepStockBrokerSummaries_SkipsStoredTickers(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{}
	uc := newSweepTestUC(t, db, f)

	day := time.Date(2099, 1, 5, 0, 0, 0, 0, time.UTC)
	tickers := []string{"TESTA", "TESTB"}
	seedTradedOn(t, db, tickers, day)
	sweepCleanup(t, db, tickers)

	if _, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, day); err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	callsAfterFirst := len(f.calls)

	res, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, day)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if res.Skipped != 2 || res.Fetched != 0 {
		t.Errorf("second sweep = %+v, want skipped=2 fetched=0", res)
	}
	if len(f.calls) != callsAfterFirst {
		t.Errorf("second sweep made upstream calls: %v (was %d)", f.calls, callsAfterFirst)
	}
}

// Tickers without a daily_prices row for the day are not traded → skipped
// without an upstream call (data presence is the trading-day signal).
func TestSweepStockBrokerSummaries_SkipsUntradedTickers(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{}
	uc := newSweepTestUC(t, db, f)

	day := time.Date(2099, 1, 5, 0, 0, 0, 0, time.UTC)
	// TESTB has no daily_prices row → not traded that day.
	seedTradedOn(t, db, []string{"TESTA"}, day)
	for _, tk := range []string{"TESTB"} {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true) ON CONFLICT (code) DO NOTHING", tk, tk)
	}
	sweepCleanup(t, db, []string{"TESTA", "TESTB"})

	res, err := uc.SweepStockBrokerSummaries(context.Background(), []string{"TESTA", "TESTB"}, day)
	if err != nil {
		t.Fatalf("SweepStockBrokerSummaries: %v", err)
	}
	if res.Total != 2 || res.NotTraded != 1 || res.Fetched != 1 {
		t.Errorf("res = %+v, want total=2 not_traded=1 fetched=1", res)
	}
	if len(f.calls) != 1 {
		t.Errorf("expected 1 upstream fetch (TESTA only), got %v", f.calls)
	}
}

// A non-trading day (no daily_prices rows at all) is a zero-fetch sweep — the
// traded-ticker query IS the calendar.
func TestSweepStockBrokerSummaries_NonTradingDayZeroFetch(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &sweepFetcher{}
	uc := newSweepTestUC(t, db, f)

	weekend := time.Date(2099, 1, 6, 0, 0, 0, 0, time.UTC) // no daily_prices rows at all
	tickers := []string{"TESTA", "TESTB"}
	for _, tk := range tickers {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true) ON CONFLICT (code) DO NOTHING", tk, tk)
	}
	sweepCleanup(t, db, tickers)

	res, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, weekend)
	if err != nil {
		t.Fatalf("SweepStockBrokerSummaries: %v", err)
	}
	if res.Total != 2 || res.NotTraded != 2 || res.Fetched != 0 {
		t.Errorf("res = %+v, want total=2 not_traded=2 fetched=0", res)
	}
	if len(f.calls) != 0 {
		t.Errorf("weekend sweep made upstream calls: %v", f.calls)
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

	day := time.Date(2099, 1, 5, 0, 0, 0, 0, time.UTC)
	tickers := []string{"TESTA", "TESTB", "TESTC"}
	seedTradedOn(t, db, tickers, day)
	sweepCleanup(t, db, tickers)

	res, err := uc.SweepStockBrokerSummaries(context.Background(), tickers, day)
	if err != nil {
		t.Fatalf("SweepStockBrokerSummaries: %v", err)
	}
	if res.Fetched != 1 || res.Empty != 1 || res.Failed != 1 {
		t.Errorf("res = %+v, want fetched=1 empty=1 failed=1", res)
	}
	if res.Skipped != 0 || res.NotTraded != 0 {
		t.Errorf("unexpected counters: %+v", res)
	}

	// Only the good ticker persisted.
	stored, err := uc.Repo.FindByDateRangeAll(db, day, day)
	if err != nil {
		t.Fatalf("FindByDateRangeAll: %v", err)
	}
	if len(stored) != 2 {
		t.Errorf("expected 2 persisted rows (TESTA only), got %d", len(stored))
	}
}
