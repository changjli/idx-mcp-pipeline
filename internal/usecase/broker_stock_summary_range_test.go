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

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// rangeFetcher records the windows it was asked for and returns one f-lane
// window result.
type rangeFetcher struct {
	err   error
	calls []brokerWindowCall
}

type brokerWindowCall struct {
	from, to time.Time
}

func (f *rangeFetcher) Fetch(ctx context.Context, ticker string, date time.Time) (*broker.Result, error) {
	return f.FetchRange(ctx, ticker, date, date, broker.InvestorForeign)
}

func (f *rangeFetcher) FetchRange(_ context.Context, _ string, from, to time.Time, _ string) (*broker.Result, error) {
	f.calls = append(f.calls, brokerWindowCall{from: from, to: to})
	if f.err != nil {
		return nil, f.err
	}
	return &broker.Result{
		Investor: broker.InvestorForeign,
		Buyers:   []broker.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
		Sellers:  []broker.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
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

// TestGetStockBrokerSummaryRange_FetchesWholeRangeOnce — a miss spends exactly
// one unit on the whole requested range and caches it as one window, rather than
// iterating trading days.
func TestGetStockBrokerSummaryRange_FetchesWholeRangeOnce(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &rangeFetcher{}
	uc := newRangeTestUC(t, db, f)

	ticker := "TESTG"
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
	if len(f.calls) != 1 {
		t.Fatalf("fetcher calls = %d, want 1 (the whole range costs one unit)", len(f.calls))
	}
	if !f.calls[0].from.Equal(start) || !f.calls[0].to.Equal(end) {
		t.Errorf("fetched window = %s..%s, want %s..%s",
			f.calls[0].from.Format("2006-01-02"), f.calls[0].to.Format("2006-01-02"),
			start.Format("2006-01-02"), end.Format("2006-01-02"))
	}
	if res.Fetched != 1 || res.ServedFrom != servedFromUpstream {
		t.Errorf("res = fetched %d served %q, want 1/upstream", res.Fetched, res.ServedFrom)
	}
	if len(res.Windows) != 1 || res.Windows[0].Grain != grainWindow {
		t.Fatalf("windows = %+v, want one window entry", res.Windows)
	}
	if res.Windows[0].From != "2026-08-10" || res.Windows[0].To != "2026-08-12" {
		t.Errorf("window range = %s..%s, want 2026-08-10..2026-08-12", res.Windows[0].From, res.Windows[0].To)
	}

	// Stored as one window, not three day rows.
	totals, stored, err := uc.Repo.FindStoredWindow(db, ticker, broker.InvestorForeign, start, end)
	if err != nil {
		t.Fatalf("FindStoredWindow: %v", err)
	}
	if len(stored) != 2 {
		t.Errorf("persisted rows = %d, want 2 (one window)", len(stored))
	}
	if totals.Source != entity.BrokerSourceWindow {
		t.Errorf("persisted source = %q, want window", totals.Source)
	}

	// The repeat costs nothing.
	if _, err := uc.GetStockBrokerSummaryRange(context.Background(), ticker, start, end); err != nil {
		t.Fatalf("GetStockBrokerSummaryRange (cached): %v", err)
	}
	if len(f.calls) != 1 {
		t.Errorf("fetcher calls = %d after the repeat, want 1", len(f.calls))
	}
}

// TestGetStockBrokerSummaryRange_PartialCoverFetchesWholeRange — a stored day row
// inside the request does not make the gap fillable; the whole request is
// fetched, because a partial sum would be a different, smaller range.
func TestGetStockBrokerSummaryRange_PartialCoverFetchesWholeRange(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &rangeFetcher{}
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

	day := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	seedHistoryDay(t, db, uc.Repo, ticker, day, 1_000_000_000)

	start := day
	end := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	res, err := uc.GetStockBrokerSummaryRange(context.Background(), ticker, start, end)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryRange: %v", err)
	}
	if len(f.calls) != 1 || !f.calls[0].from.Equal(start) || !f.calls[0].to.Equal(end) {
		t.Fatalf("fetcher calls = %+v, want one call for the whole range", f.calls)
	}
	if res.ServedFrom != servedFromUpstream || res.Fetched != 1 {
		t.Errorf("res = served %q fetched %d, want upstream/1", res.ServedFrom, res.Fetched)
	}
}

func TestGetStockBrokerSummaryRange_EmptyCountedSeparately(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)

	ticker := "TESTM"
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC))
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})

	// An empty answer writes nothing and is counted as empty, not failed.
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	emptyUC := NewBrokerStockSummaryUseCase(
		db, log, validator.New(), &emptyWindowFetcher{},
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)

	start := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	res, err := emptyUC.GetStockBrokerSummaryRange(context.Background(), ticker, start, end)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryRange: %v", err)
	}
	if res.Empty != 1 || res.Fetched != 0 || len(res.Windows) != 0 {
		t.Errorf("res = empty %d fetched %d windows %d, want 1/0/0", res.Empty, res.Fetched, len(res.Windows))
	}
}

type emptyWindowFetcher struct{}

func (f *emptyWindowFetcher) Fetch(ctx context.Context, ticker string, date time.Time) (*broker.Result, error) {
	return f.FetchRange(ctx, ticker, date, date, broker.InvestorForeign)
}

func (f *emptyWindowFetcher) FetchRange(_ context.Context, _ string, _, _ time.Time, _ string) (*broker.Result, error) {
	return &broker.Result{}, nil
}

// TestGetStockBrokerSummaryRange_ExactCoverComposesForFree — two stored day
// windows exactly tile the request, so the answer is one SQL-composed window at
// zero quota.
func TestGetStockBrokerSummaryRange_ExactCoverComposesForFree(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	f := &rangeFetcher{}
	uc := newRangeTestUC(t, db, f)

	ticker := "TESTD"
	d1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	seedTickerAndPrice(t, db, ticker, d1)
	seedTickerAndPrice(t, db, ticker, d2)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})
	seedHistoryDay(t, db, uc.Repo, ticker, d1, 1_000_000_000)
	seedHistoryDay(t, db, uc.Repo, ticker, d2, 2_000_000_000)

	res, err := uc.GetStockBrokerSummaryRange(context.Background(), ticker, d1, d2)
	if err != nil {
		t.Fatalf("GetStockBrokerSummaryRange: %v", err)
	}
	if len(f.calls) != 0 {
		t.Errorf("fetcher calls = %d, want 0 (an exact cover costs nothing)", len(f.calls))
	}
	if res.ServedFrom != servedFromStore || res.Fetched != 0 {
		t.Errorf("res = served %q fetched %d, want store/0", res.ServedFrom, res.Fetched)
	}
	if len(res.Windows) != 1 {
		t.Fatalf("windows = %d, want one composed entry", len(res.Windows))
	}
	win := res.Windows[0]
	if win.From != "2026-08-10" || win.To != "2026-08-11" || win.Grain != grainWindow {
		t.Errorf("composed range = %+v, want 2026-08-10..2026-08-11 window", win)
	}
	if win.TotalBuyValue != 3_000_000_000 || win.TotalSellValue != 1_500_000_000 {
		t.Errorf("composed gross = %d/%d, want 3B/1.5B", win.TotalBuyValue, win.TotalSellValue)
	}
	if win.Totals.FNVal == nil || *win.Totals.FNVal != 1_500_000_000 {
		t.Errorf("composed f_nval = %v, want 1.5B", win.Totals.FNVal)
	}
	if win.Totals.TVal != nil {
		t.Errorf("composed t_val = %v, want null in the foreign lane", *win.Totals.TVal)
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
