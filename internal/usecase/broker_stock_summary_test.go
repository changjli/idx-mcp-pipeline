package usecase

import (
	"context"
	"errors"
	"fmt"
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

// fakeFetcher returns a canned broker result without touching the network and
// records the windows the read path asked for.
type fakeFetcher struct {
	res        *broker.Result
	err        error
	lastTicker string
	lastFrom   time.Time
	lastTo     time.Time
	calls      int
}

func (f *fakeFetcher) Fetch(ctx context.Context, ticker string, date time.Time) (*broker.Result, error) {
	return f.FetchRange(ctx, ticker, date, date, broker.InvestorForeign)
}

func (f *fakeFetcher) FetchRange(_ context.Context, ticker string, from, to time.Time, _ string) (*broker.Result, error) {
	f.calls++
	f.lastTicker = ticker
	f.lastFrom = from
	f.lastTo = to
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

func cleanTicker(t *testing.T, db *sqlx.DB, ticker string) {
	t.Helper()
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
	})
}

// foreignResult is an f-lane payload: no all-investor totals, gross foreign
// buy/sell as the only figures.
func foreignResult() *broker.Result {
	return &broker.Result{
		Investor: broker.InvestorForeign,
		Buyers: []broker.Row{
			{BrokerCode: "AK", Lot: 169544, Value: 15_000_000_000, AvgPrice: 883, Rank: 1},
			{BrokerCode: "YP", Lot: 40722, Value: 3_600_000_000, AvgPrice: 878, Rank: 2},
		},
		Sellers: []broker.Row{
			{BrokerCode: "XL", Lot: 139188, Value: 12_300_000_000, AvgPrice: 881, Rank: 1},
		},
	}
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
	cleanTicker(t, db, ticker)
	f.res = foreignResult()

	res, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}
	if res.Ticker != ticker || res.From != "2026-08-12" || res.To != "2026-08-12" || res.Grain != grainDay {
		t.Errorf("response range = %+v, want TESTA 2026-08-12..2026-08-12 day", res)
	}
	if res.ServedFrom != servedFromUpstream {
		t.Errorf("ServedFrom = %q, want upstream (a miss spends one unit)", res.ServedFrom)
	}
	if res.Coverage.Lane != "foreign" || !res.Coverage.Complete {
		t.Errorf("coverage = %+v, want a structurally complete foreign window", res.Coverage)
	}
	// Foreign gross is the rows' own sum; t_val is unobserved in the f lane.
	if res.TotalBuyValue != 18_600_000_000 || res.TotalSellValue != 12_300_000_000 {
		t.Errorf("foreign gross = %d/%d, want 18.6B/12.3B", res.TotalBuyValue, res.TotalSellValue)
	}
	if res.Totals.TVal != nil || res.Totals.TLot != nil || res.Totals.Avg != nil {
		t.Errorf("f totals = %+v, want null day totals", res.Totals)
	}
	if res.Totals.FNVal == nil || *res.Totals.FNVal != 6_300_000_000 {
		t.Errorf("f_nval = %v, want 6.3B (buy − sell)", res.Totals.FNVal)
	}

	// The second lookup is the zero-unit path: no fetch, served from the store.
	f.res = nil
	again, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary (cached): %v", err)
	}
	if f.calls != 1 {
		t.Errorf("fetcher calls = %d, want 1 (the second lookup must cost zero units)", f.calls)
	}
	if again.ServedFrom != servedFromStore {
		t.Errorf("ServedFrom = %q, want store", again.ServedFrom)
	}
	if again.Totals.FNVal == nil || *again.Totals.FNVal != 6_300_000_000 {
		t.Errorf("stored f_nval = %v, want 6.3B", again.Totals.FNVal)
	}

	// Persisted as the foreign lane's one-day live window.
	totals, rows, err := uc.Repo.FindStoredWindow(db, ticker, broker.InvestorForeign, day, day)
	if err != nil {
		t.Fatalf("FindStoredWindow: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("persisted rows = %d, want 3", len(rows))
	}
	if totals.Source != entity.BrokerSourceLive {
		t.Errorf("persisted source = %q, want live", totals.Source)
	}
}

func TestGetStockBrokerSummary_ProjectionCapsAtTen(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc, f := newTestUC(t, db)

	ticker := "TESTP"
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	cleanTicker(t, db, ticker)

	buyers := make([]broker.Row, 15)
	sellers := make([]broker.Row, 15)
	for i := 0; i < 15; i++ {
		code := fmt.Sprintf("%c%c", 'A'+i, 'A'+i)
		buyers[i] = broker.Row{BrokerCode: code, Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: i + 1}
		sellers[i] = broker.Row{BrokerCode: code, Lot: 100, Value: 900_000_000, AvgPrice: 100, Rank: i + 1}
	}
	f.res = &broker.Result{Investor: broker.InvestorForeign, Buyers: buyers, Sellers: sellers}

	res, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}
	if len(res.Buyers) != brokerSummaryTopN || len(res.Sellers) != brokerSummaryTopN {
		t.Errorf("response = %d buyers/%d sellers, want top %d per side", len(res.Buyers), len(res.Sellers), brokerSummaryTopN)
	}
	if res.Buyers[0].Rank != 1 || res.Buyers[len(res.Buyers)-1].Rank != brokerSummaryTopN {
		t.Errorf("ranks = %d..%d, want dense 1..%d", res.Buyers[0].Rank, res.Buyers[len(res.Buyers)-1].Rank, brokerSummaryTopN)
	}
	if !res.Coverage.Complete || res.Coverage.Brokers != 15 {
		t.Errorf("coverage = %+v, want complete over all 15 stored brokers", res.Coverage)
	}

	totals, stored, err := uc.Repo.FindStoredWindow(db, ticker, broker.InvestorForeign, day, day)
	if err != nil {
		t.Fatalf("FindStoredWindow: %v", err)
	}
	if len(stored) != 30 {
		t.Errorf("persisted rows = %d, want 30 (full set, not top ten per side)", len(stored))
	}
	if totals.FNVal == nil || *totals.FNVal != 1_500_000_000 {
		t.Errorf("f_nval = %v, want 1.5B", totals.FNVal)
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
	cleanTicker(t, db, ticker)

	f.res = &broker.Result{} // empty — non-trading day

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

	f.res = &broker.Result{Investor: broker.InvestorForeign,
		Buyers: []broker.Row{{BrokerCode: "AK", Lot: 1, Value: 100, AvgPrice: 100, Rank: 1}}}

	if _, err := uc.GetStockBrokerSummary(context.Background(), ticker, nil); err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}
	if f.lastTicker != ticker {
		t.Errorf("fetcher ticker = %q, want %q", f.lastTicker, ticker)
	}
	if f.lastTo.Format("2006-01-02") != "2026-08-11" {
		t.Errorf("fetcher window end = %s, want latest trading day 2026-08-11", f.lastTo.Format("2006-01-02"))
	}
}

// dateAwareFetcher returns a canned result for configured window ends and empty
// for everything else — simulating a publish lag for a specific day.
type dateAwareFetcher struct {
	withData map[string]*broker.Result
	calls    []string
}

func (f *dateAwareFetcher) Fetch(ctx context.Context, ticker string, date time.Time) (*broker.Result, error) {
	return f.FetchRange(ctx, ticker, date, date, broker.InvestorForeign)
}

func (f *dateAwareFetcher) FetchRange(_ context.Context, _ string, from, to time.Time, _ string) (*broker.Result, error) {
	f.calls = append(f.calls, to.Format("2006-01-02"))
	if res, ok := f.withData[to.Format("2006-01-02")]; ok {
		return res, nil
	}
	return &broker.Result{}, nil
}

// TestGetStockBrokerSummary_FallbackReadsStoredPriorDay — an empty day falls
// back over stored prior days only. The probe itself never spends quota.
func TestGetStockBrokerSummary_FallbackReadsStoredPriorDay(t *testing.T) {
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

	f := &dateAwareFetcher{withData: map[string]*broker.Result{
		"2026-08-11": {
			Investor: broker.InvestorForeign,
			Buyers:   []broker.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
			Sellers:  []broker.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
		},
	}}
	uc := NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)

	// Today is empty; the prior window is already stored.
	if _, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day11); err != nil {
		t.Fatalf("seed day 11: %v", err)
	}
	callsBefore := len(f.calls)

	res, err := uc.GetStockBrokerSummary(context.Background(), ticker, &day12)
	if err != nil {
		t.Fatalf("GetStockBrokerSummary: %v", err)
	}
	if len(f.calls) != callsBefore+1 {
		t.Errorf("fetcher calls = %d, want %d (only the requested day; no prior-day probe)", len(f.calls), callsBefore+1)
	}
	if res.AsOf != "2026-08-11" {
		t.Errorf("res.AsOf = %q, want 2026-08-11 (stored fallback)", res.AsOf)
	}
	if res.Cause != "not_yet_published" {
		t.Errorf("res.Cause = %q, want not_yet_published", res.Cause)
	}
	if res.ServedFrom != servedFromStore {
		t.Errorf("res.ServedFrom = %q, want store", res.ServedFrom)
	}
	if len(res.Buyers) != 1 || res.Buyers[0].BrokerCode != "AK" {
		t.Errorf("expected fallback day's buyers, got %+v", res.Buyers)
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
	cleanTicker(t, db, ticker)

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

// TestGetStockBrokerSummary_FetchErrorKeepsItsClass — the usecase wraps the
// fetcher error with context but must not erase the structured class the MCP
// boundary maps to an error code.
func TestGetStockBrokerSummary_FetchErrorKeepsItsClass(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	f := &fakeFetcher{err: fmt.Errorf("upstream: %w", broker.ErrQuotaExhausted)}
	uc := NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)
	day := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	cleanTicker(t, db, "BBRI")

	_, err := uc.GetStockBrokerSummary(context.Background(), "BBRI", &day)
	if !errors.Is(err, broker.ErrQuotaExhausted) {
		t.Fatalf("err = %v, want broker.ErrQuotaExhausted in the chain", err)
	}
}

// TestPersist_ForeignLaneWritesNullDayTotals is ticket 10a's writer rule, kept
// by the shared window writer: an f result stores its rows and derives f_nval,
// but t_val/t_lot/avg stay NULL.
func TestPersist_ForeignLaneWritesNullDayTotals(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc, _ := newTestUC(t, db)

	ticker := "TESTX"
	day := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	cleanTicker(t, db, ticker)

	res := &broker.Result{
		Investor: broker.InvestorForeign,
		Buyers:   []broker.Row{{BrokerCode: "HD", Lot: 100, Value: 1_000_000, AvgPrice: 100, Rank: 1}},
		Sellers:  []broker.Row{{BrokerCode: "CC", Lot: 90, Value: 900_000, AvgPrice: 100, Rank: 1}},
	}
	if err := persistBrokerWindow(db, uc.Repo, ticker, day, day, res, broker.InvestorForeign, entity.BrokerSourceLive); err != nil {
		t.Fatalf("persist(f): %v", err)
	}

	totals, _, err := uc.Repo.FindStoredWindow(db, ticker, broker.InvestorForeign, day, day)
	if err != nil {
		t.Fatalf("read f window: %v", err)
	}
	if totals.TVal != nil || totals.TLot != nil || totals.Avg != nil {
		t.Errorf("f day totals = %v/%v/%v, want NULL", totals.TVal, totals.TLot, totals.Avg)
	}
	if totals.FNVal == nil || *totals.FNVal != 100_000 {
		t.Errorf("f_nval = %v, want 100000 (derived from rows)", totals.FNVal)
	}
	if totals.Investor != broker.InvestorForeign || totals.Market != broker.MarketRegular {
		t.Errorf("lane/market = %q/%q, want f/RG", totals.Investor, totals.Market)
	}
}
