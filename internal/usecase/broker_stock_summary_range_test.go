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

// rangeFetcher returns a canned result per day, or an error for a configured
// failing day — simulating a partial upstream failure mid-range.
type rangeFetcher struct {
	failDay time.Time
	calls   []time.Time
}

func (f *rangeFetcher) Fetch(_ context.Context, _ string, date time.Time) (*ipot.Result, error) {
	f.calls = append(f.calls, date)
	if !f.failDay.IsZero() && date.Equal(f.failDay) {
		return nil, errors.New("ipot: upstream error: status=500")
	}
	return &ipot.Result{
		Buyers:  []ipot.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
		Sellers: []ipot.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
		Totals:  ipot.Totals{TVal: 1_500_000_000, FNVal: 100_000_000, TLot: 150, Avg: 100},
	}, nil
}

func newRangeTestUC(t *testing.T, db *sqlx.DB, f BrokerSummaryFetcher) *BrokerStockSummaryUseCase {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)
}

func TestGetStockBrokerSummaryRange_IteratesTradingDays(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &rangeFetcher{}
	uc := newRangeTestUC(t, db, f)

	ticker := "TESTG"
	// Mon 8/10, Tue 8/11, Wed 8/12 — weekend 8/8-8/9 has no daily_price row.
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC))
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC))
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})

	start := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC) // Saturday
	end := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)  // Wednesday

	res, err := uc.GetStockBrokerSummaryRange(context.Background(), ticker, start, end)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryRange: %v", err)
	}

	if res.Fetched != 3 {
		t.Errorf("expected 3 days fetched, got %d", res.Fetched)
	}
	if res.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", res.Failed)
	}
	if len(res.Days) != 3 {
		t.Fatalf("expected 3 days in response, got %d", len(res.Days))
	}
	// Weekend (8/8, 8/9) skipped — only trading days iterated.
	if len(f.calls) != 3 {
		t.Errorf("expected 3 fetcher calls (weekend skipped), got %d", len(f.calls))
	}
	if res.Days[0].TradingDay != "2026-08-10" {
		t.Errorf("days[0].TradingDay = %q, want 2026-08-10", res.Days[0].TradingDay)
	}

	// Persisted: 3 days × 2 rows.
	stored, err := uc.Repo.FindByTickerAndDateRange(db, ticker, start, end)
	if err != nil {
		t.Fatalf("FindByTickerAndDateRange: %v", err)
	}
	if len(stored) != 6 {
		t.Errorf("expected 6 persisted rows (3 days × 2), got %d", len(stored))
	}
}

func TestGetStockBrokerSummaryRange_PartialFailureKeepsSuccesses(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	failDay := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	f := &rangeFetcher{failDay: failDay}
	uc := newRangeTestUC(t, db, f)

	ticker := "TESTK"
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC))
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC))
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})

	start := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	res, err := uc.GetStockBrokerSummaryRange(context.Background(), ticker, start, end)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryRange with partial failure should not error, got: %v", err)
	}
	if res.Fetched != 2 {
		t.Errorf("expected 2 days fetched, got %d", res.Fetched)
	}
	if res.Failed != 1 {
		t.Errorf("expected 1 failed day, got %d", res.Failed)
	}
	if len(res.Days) != 2 {
		t.Errorf("expected 2 days in response (failed day excluded), got %d", len(res.Days))
	}
	// Failed day not persisted.
	stored, err := uc.Repo.FindByTickerAndDateRange(db, ticker, start, end)
	if err != nil {
		t.Fatalf("FindByTickerAndDateRange: %v", err)
	}
	if len(stored) != 4 {
		t.Errorf("expected 4 persisted rows (2 successful days), got %d", len(stored))
	}
}

// emptyDayFetcher returns a canned result except for one configured day, which
// returns an empty result — simulating a day IPOT hasn't published yet.
type emptyDayFetcher struct {
	emptyDay time.Time
}

func (f *emptyDayFetcher) Fetch(_ context.Context, _ string, date time.Time) (*ipot.Result, error) {
	if !f.emptyDay.IsZero() && date.Equal(f.emptyDay) {
		return &ipot.Result{}, nil
	}
	return &ipot.Result{
		Buyers:  []ipot.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
		Sellers: []ipot.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
		Totals:  ipot.Totals{TVal: 1_500_000_000, FNVal: 100_000_000, TLot: 150, Avg: 100},
	}, nil
}

func TestGetStockBrokerSummaryRange_EmptyDayCountedSeparately(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	emptyDay := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	f := &emptyDayFetcher{emptyDay: emptyDay}
	uc := newRangeTestUC(t, db, f)

	ticker := "TESTM"
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC))
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC))
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})

	start := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	res, err := uc.GetStockBrokerSummaryRange(context.Background(), ticker, start, end)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryRange: %v", err)
	}
	if res.Fetched != 2 {
		t.Errorf("expected 2 fetched, got %d", res.Fetched)
	}
	if res.Empty != 1 {
		t.Errorf("expected 1 empty day, got %d", res.Empty)
	}
	if len(res.Days) != 2 {
		t.Errorf("expected 2 days in response (empty day excluded), got %d", len(res.Days))
	}
}

func TestGetStockBrokerSummaryRange_InvalidRange(t *testing.T) {
	uc := &BrokerStockSummaryUseCase{}
	_, err := uc.GetStockBrokerSummaryRange(
		context.Background(), "BBCA",
		time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange, got %v", err)
	}
}
