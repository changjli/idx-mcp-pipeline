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

// fakeFetcher returns a canned IPOT result without touching the network.
type fakeFetcher struct {
	res        *ipot.Result
	err        error
	lastTicker string
	lastDate   time.Time
}

func (f *fakeFetcher) Fetch(_ context.Context, ticker string, date time.Time) (*ipot.Result, error) {
	f.lastTicker = ticker
	f.lastDate = date
	return f.res, f.err
}

func newTestUC(t *testing.T, db *sqlx.DB) (*BrokerStockSummaryUseCase, *fakeFetcher) {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	f := &fakeFetcher{}
	uc := NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)
	return uc, f
}

func seedTickerAndPrice(t *testing.T, db *sqlx.DB, ticker string, day time.Time) {
	t.Helper()
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true) ON CONFLICT (code) DO NOTHING", ticker, ticker)
	db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)
		VALUES ($1, $2, 100, 101, 99, 100, 1000, 100000, 10, 'idx')
		ON CONFLICT (ticker, trading_day) DO NOTHING`, ticker, day)
}

func TestGetStockBrokerSummary_FetchPersistReturn(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc, f := newTestUC(t, db)

	ticker := "TESTA"
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	f.res = &ipot.Result{
		Buyers: []ipot.Row{
			{BrokerCode: "AK", Lot: 169544, Value: 15_000_000_000, AvgPrice: 883, Rank: 1},
			{BrokerCode: "YP", Lot: 40722, Value: 3_600_000_000, AvgPrice: 878, Rank: 6},
		},
		Sellers: []ipot.Row{
			{BrokerCode: "XL", Lot: 139188, Value: 12_300_000_000, AvgPrice: 881, Rank: 1},
		},
		Totals: ipot.Totals{TVal: 71_200_000_000, FNVal: 11_100_000_000, TLot: 808975, Avg: 880},
	}

	res, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}

	if res.Ticker != ticker {
		t.Errorf("res.Ticker = %q, want %q", res.Ticker, ticker)
	}
	if res.TradingDay != "2026-08-12" {
		t.Errorf("res.TradingDay = %q, want 2026-08-12", res.TradingDay)
	}
	if len(res.Buyers) != 2 || len(res.Sellers) != 1 {
		t.Errorf("expected 2 buyers 1 seller, got %d/%d", len(res.Buyers), len(res.Sellers))
	}
	if res.Buyers[0].BrokerCode != "AK" || res.Buyers[0].Value != 15_000_000_000 {
		t.Errorf("buyers[0] = %+v, want AK/15000000000", res.Buyers[0])
	}
	if res.Totals.TVal != 71_200_000_000 {
		t.Errorf("Totals.TVal = %d, want 71200000000", res.Totals.TVal)
	}

	// Issue 03 aggregates: total buy/sell = footer t_val; others_net = the
	// unlisted tail (Σ sellers − Σ buyers = 12.3B − 18.6B = −6.3B).
	if res.TotalBuyValue != 71_200_000_000 {
		t.Errorf("TotalBuyValue = %d, want 71200000000 (footer t_val)", res.TotalBuyValue)
	}
	if res.TotalSellValue != 71_200_000_000 {
		t.Errorf("TotalSellValue = %d, want 71200000000 (footer t_val)", res.TotalSellValue)
	}
	if res.OthersNet != -6_300_000_000 {
		t.Errorf("OthersNet = %d, want -6300000000 (12.3B sellers − 18.6B buyers)", res.OthersNet)
	}

	// Persisted: rows present in DB.
	stored, err := uc.Repo.FindByTickerAndDay(db, ticker, day)
	if err != nil {
		t.Fatalf("FindByTickerAndDay: %v", err)
	}
	if len(stored) != 3 {
		t.Errorf("expected 3 persisted rows, got %d", len(stored))
	}

	// Persisted: others_net written to the totals row.
	ptotals, err := uc.Repo.FindTotalsByTickerAndDay(db, ticker, day)
	if err != nil {
		t.Fatalf("FindTotalsByTickerAndDay: %v", err)
	}
	if ptotals.OthersNet == nil || *ptotals.OthersNet != -6_300_000_000 {
		t.Errorf("persisted others_net = %v, want -6300000000", ptotals.OthersNet)
	}
}

// TestGetStockBrokerSummary_AllTopN — the listed top-N covers the whole market
// (Σ buyers = Σ sellers = t_val), so the tail is empty and others_net = 0.
func TestGetStockBrokerSummary_AllTopN(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc, f := newTestUC(t, db)

	ticker := "TESTW"
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	f.res = &ipot.Result{
		Buyers: []ipot.Row{
			{BrokerCode: "AK", Lot: 100000, Value: 10_000_000_000, AvgPrice: 100, Rank: 1},
			{BrokerCode: "YP", Lot: 10000, Value: 1_000_000_000, AvgPrice: 100, Rank: 2},
		},
		Sellers: []ipot.Row{
			{BrokerCode: "XL", Lot: 80000, Value: 8_000_000_000, AvgPrice: 100, Rank: 1},
			{BrokerCode: "MG", Lot: 30000, Value: 3_000_000_000, AvgPrice: 100, Rank: 2},
		},
		Totals: ipot.Totals{TVal: 11_000_000_000, FNVal: 2_000_000_000, TLot: 220000, Avg: 100},
	}

	res, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}

	// Σ buyers = Σ sellers = t_val = 11B → nothing unlisted → tail nets to zero.
	if res.OthersNet != 0 {
		t.Errorf("OthersNet = %d, want 0 (top-N covers the whole market)", res.OthersNet)
	}
	if res.TotalBuyValue != 11_000_000_000 || res.TotalSellValue != 11_000_000_000 {
		t.Errorf("totals = %d/%d, want 11000000000/11000000000", res.TotalBuyValue, res.TotalSellValue)
	}
}

func TestGetStockBrokerSummary_EmptyResultWritesNothing(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc, f := newTestUC(t, db)

	ticker := "TESTB"
	day := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	f.res = &ipot.Result{} // empty — non-trading day

	res, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary on empty result should not error, got: %v", err)
	}
	if len(res.Buyers) != 0 || len(res.Sellers) != 0 {
		t.Errorf("expected empty response, got %d buyers %d sellers", len(res.Buyers), len(res.Sellers))
	}

	var count int
	db.Get(&count, "SELECT COUNT(*) FROM broker_stock_summaries WHERE ticker = $1", ticker)
	if count != 0 {
		t.Errorf("empty result wrote %d rows, want 0", count)
	}
}

func TestGetStockBrokerSummary_DateDefaultsToLatestTradingDay(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc, f := newTestUC(t, db)

	ticker := "TESTC"
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	seedTickerAndPrice(t, db, ticker, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC))
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})

	f.res = &ipot.Result{Buyers: []ipot.Row{{BrokerCode: "AK", Lot: 1, Value: 100, AvgPrice: 100, Rank: 1}}}

	if _, err := uc.GetStockBrokerSummary(context.Background(), ticker, nil); err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}
	if f.lastTicker != ticker {
		t.Errorf("fetcher ticker = %q, want %q", f.lastTicker, ticker)
	}
	if f.lastDate.Format("2006-01-02") != "2026-08-11" {
		t.Errorf("fetcher date = %s, want latest trading day 2026-08-11", f.lastDate.Format("2006-01-02"))
	}
}

// dateAwareFetcher returns a canned result for configured dates and empty for
// everything else — simulating IPOT publish lag for a specific day.
type dateAwareFetcher struct {
	withData map[string]*ipot.Result
}

func (f *dateAwareFetcher) Fetch(_ context.Context, _ string, date time.Time) (*ipot.Result, error) {
	if res, ok := f.withData[date.Format("2006-01-02")]; ok {
		return res, nil
	}
	return &ipot.Result{}, nil
}

func TestGetStockBrokerSummary_FallbackProbesBackward(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ticker := "TESTF"
	day11 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	day12 := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	seedTickerAndPrice(t, db, ticker, day11)
	seedTickerAndPrice(t, db, ticker, day12)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})

	// 8/12 not yet published (empty); 8/11 has data.
	f := &dateAwareFetcher{withData: map[string]*ipot.Result{
		"2026-08-11": {
			Buyers:  []ipot.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
			Sellers: []ipot.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
			Totals:  ipot.Totals{TVal: 1_500_000_000, FNVal: 100_000_000, TLot: 150, Avg: 100},
		},
	}}
	uc := NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)

	res, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day12)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}

	if res.AsOf != "2026-08-11" {
		t.Errorf("res.AsOf = %q, want 2026-08-11 (fallback to prior published day)", res.AsOf)
	}
	if res.Cause != "not_yet_published" {
		t.Errorf("res.Cause = %q, want not_yet_published", res.Cause)
	}
	if len(res.Buyers) != 1 || res.Buyers[0].BrokerCode != "AK" {
		t.Errorf("expected fallback day's buyers, got %+v", res.Buyers)
	}

	// Fallback day persisted; empty requested day not written.
	var count int
	db.Get(&count, "SELECT COUNT(*) FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2", ticker, day11)
	if count != 2 {
		t.Errorf("expected 2 rows for fallback day 8/11, got %d", count)
	}
	db.Get(&count, "SELECT COUNT(*) FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2", ticker, day12)
	if count != 0 {
		t.Errorf("expected 0 rows for empty requested day 8/12, got %d", count)
	}
}

func TestGetStockBrokerSummary_FallbackNonTradingDay(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ticker := "TESTN"
	day := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC) // Saturday, no daily_price
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})

	f := &dateAwareFetcher{} // empty everywhere
	uc := NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)

	res, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}
	if res.Cause != "non_trading_day" {
		t.Errorf("res.Cause = %q, want non_trading_day", res.Cause)
	}
	if len(res.Buyers) != 0 || len(res.Sellers) != 0 {
		t.Errorf("expected empty response, got %d buyers %d sellers", len(res.Buyers), len(res.Sellers))
	}
}

func TestGetStockBrokerSummary_InvalidTicker(t *testing.T) {
	uc := &BrokerStockSummaryUseCase{}
	_, err := uc.GetStockBrokerSummary(context.Background(), "bad ticker!", nil)
	if !errors.Is(err, ErrInvalidTicker) {
		t.Errorf("expected ErrInvalidTicker, got %v", err)
	}
}

func TestGetStockBrokerSummary_NoTradingDay(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc, _ := newTestUC(t, db)

	_, err := uc.GetStockBrokerSummary(context.Background(), "ZZZZ", nil)
	if !errors.Is(err, ErrNoTradingDay) {
		t.Errorf("expected ErrNoTradingDay, got %v", err)
	}
}
