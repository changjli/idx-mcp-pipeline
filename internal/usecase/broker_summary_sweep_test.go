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

// ─── fakes ─────────────────────────────────────────────────────────────────

// sweepBudgetFake is a fixed quota view. EnsureSeeded records the free re-seed
// the sweep performs before planning.
type sweepBudgetFake struct {
	available int
	perKey    int
	seeded    int
	seedErr   error
}

func (b *sweepBudgetFake) Available() int        { return b.available }
func (b *sweepBudgetFake) BatchCapacity() int    { return b.perKey }
func (b *sweepBudgetFake) NextReset() *time.Time { return nil }
func (b *sweepBudgetFake) EnsureSeeded(context.Context) error {
	b.seeded++
	return b.seedErr
}

// sweepBatchFetcher answers batches from per-ticker behaviour maps. failAfter
// makes the call after N successes fail with failErr, simulating a quota stop
// mid-run. calls records the tickers in each requested batch.
type sweepBatchFetcher struct {
	invalid   map[string]bool
	empty     map[string]bool
	failAfter int
	failErr   error
	calls     [][]string
}

func (f *sweepBatchFetcher) Fetch(_ context.Context, _ string, _ time.Time) (*broker.Result, error) {
	return sweepBatchResult(), nil
}

func (f *sweepBatchFetcher) FetchBatch(_ context.Context, tickers []string, _ time.Time) (*broker.Batch, error) {
	f.calls = append(f.calls, append([]string(nil), tickers...))
	if f.failErr != nil && f.failAfter >= 0 && len(f.calls) > f.failAfter {
		return nil, f.failErr
	}
	b := &broker.Batch{
		Results: make(map[string]*broker.Result, len(tickers)),
		Invalid: make(map[string]error),
	}
	for _, t := range tickers {
		switch {
		case f.invalid[t]:
			b.Invalid[t] = errors.New("Σbuy ≠ Σsell")
		case f.empty[t]:
			// absent from the response = an empty day
		default:
			b.Results[t] = sweepBatchResult()
		}
	}
	return b, nil
}

func sweepBatchResult() *broker.Result {
	return &broker.Result{
		Buyers:  []broker.Row{{BrokerCode: "AK", Lot: 100, Value: 1_000_000_000, AvgPrice: 100, Rank: 1}},
		Sellers: []broker.Row{{BrokerCode: "XL", Lot: 50, Value: 500_000_000, AvgPrice: 99, Rank: 1}},
		Totals:  broker.Totals{TVal: 1_500_000_000, TLot: 150, Avg: 100},
	}
}

func newSweepTestUC(t *testing.T, db *sqlx.DB, f *sweepBatchFetcher, b *sweepBudgetFake) *BrokerStockSummaryUseCase {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return NewBrokerStockSummaryUseCase(
		db, log, validator.New(), f, // the single-ticker fetcher is unused by the sweep
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
		WithBatchSource(f, b),
	)
}

// ─── fixtures ──────────────────────────────────────────────────────────────

// sweepAnchor is a far-future Friday: no real market rows exist on these dates,
// so the sweep is isolated on a shared DB.
var sweepAnchor = time.Date(2099, 2, 20, 0, 0, 0, 0, time.UTC)

// sweepWindowDays are 9 trading days ending on the anchor, inside the sweep's
// trailing-21-day eligibility window.
var sweepWindowDays = []time.Time{
	time.Date(2099, 2, 9, 0, 0, 0, 0, time.UTC),
	time.Date(2099, 2, 10, 0, 0, 0, 0, time.UTC),
	time.Date(2099, 2, 11, 0, 0, 0, 0, time.UTC),
	time.Date(2099, 2, 12, 0, 0, 0, 0, time.UTC),
	time.Date(2099, 2, 13, 0, 0, 0, 0, time.UTC),
	time.Date(2099, 2, 16, 0, 0, 0, 0, time.UTC),
	time.Date(2099, 2, 17, 0, 0, 0, 0, time.UTC),
	time.Date(2099, 2, 18, 0, 0, 0, 0, time.UTC),
	time.Date(2099, 2, 19, 0, 0, 0, 0, time.UTC),
}

const sweepADTVFloor = 5_000_000_000

// seedSweepTicker seeds an eligible ticker: 9 trading days of a given daily
// value inside the window.
func seedSweepTicker(t *testing.T, db *sqlx.DB, ticker string, dailyValue int64) {
	t.Helper()
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true) ON CONFLICT (code) DO NOTHING", ticker, ticker)
	for _, d := range sweepWindowDays {
		db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)
			VALUES ($1, $2, 100, 101, 99, 100, 1000, $3, 10, 'idx')
			ON CONFLICT (ticker, trading_day) DO NOTHING`, ticker, d, dailyValue)
	}
}

// seedObserved records a prior stored broker day for a ticker, which is what
// the rotation orders on.
func seedObserved(t *testing.T, db *sqlx.DB, ticker string, day time.Time) {
	t.Helper()
	db.MustExec(`INSERT INTO broker_stock_summary_totals (ticker, trading_day, win_from, investor, market, t_val, f_nval, t_lot, avg)
		VALUES ($1, $2, $2, 'all', 'RG', 1, NULL, 1, 1)
		ON CONFLICT (ticker, trading_day, win_from, investor) DO NOTHING`, ticker, day)
}

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

func connectSweepDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	return sqlx.MustConnect("pgx", dsn)
}

// ─── pure planning rules ───────────────────────────────────────────────────

func TestNextChunkSize(t *testing.T) {
	tests := []struct {
		name                          string
		perKey, budgetLeft, candsLeft int
		want                          int
	}{
		{"vendor cap", 150, 200, 200, broker.MaxBatchTickers},
		{"per-key cap", 7, 200, 200, 7},
		{"budget cap", 150, 12, 200, 12},
		{"candidate cap", 150, 200, 3, 3},
		{"no usable key", 0, 200, 200, 0},
		{"budget spent", 50, 0, 200, 0},
		{"no candidates", 50, 200, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextChunkSize(tt.perKey, tt.budgetLeft, tt.candsLeft); got != tt.want {
				t.Errorf("nextChunkSize(%d,%d,%d) = %d, want %d", tt.perKey, tt.budgetLeft, tt.candsLeft, got, tt.want)
			}
		})
	}
}

func TestOrderSweepCandidates_LRUThenLiquidity(t *testing.T) {
	cands := []repository.LiquidityTicker{
		{Ticker: "HIGH", ADTV: 10},
		{Ticker: "OLD", ADTV: 1},
		{Ticker: "NEW", ADTV: 20},
		{Ticker: "LOW", ADTV: 2},
	}
	lastSeen := map[string]time.Time{
		"OLD": sweepAnchor.AddDate(0, 0, -10),
		"NEW": sweepAnchor.AddDate(0, 0, -1),
	}
	got := orderSweepCandidates(cands, lastSeen)
	// Never observed first (HIGH then LOW, ADTV desc), then oldest, then newest.
	want := []string{"HIGH", "LOW", "OLD", "NEW"}
	if len(got) != len(want) {
		t.Fatalf("ordered = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ordered = %v, want %v", got, want)
		}
	}
}

// ─── DB-backed behaviour ───────────────────────────────────────────────────

func TestSweepAnchorDay_FetchesAndPersists(t *testing.T) {
	db := connectSweepDB(t)
	tickers := []string{"SWPA", "SWPB", "SWPC"}
	for _, tk := range tickers {
		seedSweepTicker(t, db, tk, 10_000_000_000)
	}
	sweepCleanup(t, db, tickers)
	f := &sweepBatchFetcher{}
	b := &sweepBudgetFake{available: 100, perKey: 50}
	uc := newSweepTestUC(t, db, f, b)

	res, err := uc.SweepAnchorDay(context.Background(), sweepAnchor, SweepOptions{MinADTV: sweepADTVFloor})
	if err != nil {
		t.Fatalf("SweepAnchorDay: %v", err)
	}
	if res.Eligible != 3 || res.Stored != 0 || res.Planned != 3 || res.Attempted != 3 || res.Fetched != 3 {
		t.Errorf("res = %+v, want eligible=3 planned=3 attempted=3 fetched=3", res)
	}
	if res.Chunks != 1 || res.UnitsSpent != 3 {
		t.Errorf("chunks=%d units=%d, want 1 chunk / 3 units", res.Chunks, res.UnitsSpent)
	}
	if b.seeded != 1 {
		t.Errorf("EnsureSeeded calls = %d, want 1 (free re-seed before planning)", b.seeded)
	}
	if len(f.calls) != 1 || len(f.calls[0]) != 3 {
		t.Fatalf("calls = %v, want one batch of 3", f.calls)
	}

	var rows int
	if err := db.Get(&rows, "SELECT COUNT(*) FROM broker_stock_summaries WHERE trading_day = $1", sweepAnchor); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 6 {
		t.Errorf("persisted rows = %d, want 6 (3 tickers × 2 sides)", rows)
	}
	var sources []string
	if err := db.Select(&sources, "SELECT DISTINCT source FROM broker_stock_summaries WHERE trading_day = $1", sweepAnchor); err != nil {
		t.Fatalf("read sources: %v", err)
	}
	if len(sources) != 1 || sources[0] != entity.BrokerSourceSweep {
		t.Errorf("row provenance = %v, want only %q", sources, entity.BrokerSourceSweep)
	}
}

func TestSweepAnchorDay_SkipsStoredTickerDays(t *testing.T) {
	db := connectSweepDB(t)
	tickers := []string{"SWPA", "SWPB"}
	for _, tk := range tickers {
		seedSweepTicker(t, db, tk, 10_000_000_000)
	}
	sweepCleanup(t, db, tickers)
	f := &sweepBatchFetcher{}
	uc := newSweepTestUC(t, db, f, &sweepBudgetFake{available: 100, perKey: 50})

	if _, err := uc.SweepAnchorDay(context.Background(), sweepAnchor, SweepOptions{MinADTV: sweepADTVFloor}); err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	calls := len(f.calls)

	res, err := uc.SweepAnchorDay(context.Background(), sweepAnchor, SweepOptions{MinADTV: sweepADTVFloor})
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if res.Stored != 2 || res.Planned != 0 || res.Attempted != 0 || res.Fetched != 0 {
		t.Errorf("second sweep = %+v, want stored=2 planned=0 attempted=0", res)
	}
	if len(f.calls) != calls {
		t.Errorf("second sweep made %d extra batch calls", len(f.calls)-calls)
	}
}

func TestSweepAnchorDay_BudgetCapsAndPrefersLiquidity(t *testing.T) {
	db := connectSweepDB(t)
	// Five eligible names, descending liquidity. None observed → liquidity order.
	seed := map[string]int64{
		"SWP1": 12_000_000_000,
		"SWP2": 11_000_000_000,
		"SWP3": 10_000_000_000,
		"SWP4": 9_000_000_000,
		"SWP5": 8_000_000_000,
	}
	tickers := make([]string, 0, len(seed))
	for tk, v := range seed {
		seedSweepTicker(t, db, tk, v)
		tickers = append(tickers, tk)
	}
	sweepCleanup(t, db, tickers)
	f := &sweepBatchFetcher{}
	uc := newSweepTestUC(t, db, f, &sweepBudgetFake{available: 2, perKey: 50})

	res, err := uc.SweepAnchorDay(context.Background(), sweepAnchor, SweepOptions{MinADTV: sweepADTVFloor})
	if err != nil {
		t.Fatalf("SweepAnchorDay: %v", err)
	}
	if res.Eligible != 5 || res.Planned != 2 || res.Attempted != 2 || res.Shortfall != 0 {
		t.Errorf("res = %+v, want eligible=5 planned=2 attempted=2", res)
	}
	if res.Candidates != 5 || res.Uncovered != 3 {
		t.Errorf("candidates=%d uncovered=%d, want 5/3 (rotation leaves a coverage gap)", res.Candidates, res.Uncovered)
	}
	if len(f.calls) != 1 || len(f.calls[0]) != 2 {
		t.Fatalf("calls = %v, want one batch of 2", f.calls)
	}
	if f.calls[0][0] != "SWP1" || f.calls[0][1] != "SWP2" {
		t.Errorf("batch = %v, want the two most liquid [SWP1 SWP2]", f.calls[0])
	}
}

func TestSweepAnchorDay_RotationPrefersLeastRecentlyObserved(t *testing.T) {
	db := connectSweepDB(t)
	// SWPA was observed yesterday (anchor-1); SWPB never. Budget 1 → SWPB.
	seedSweepTicker(t, db, "SWPA", 12_000_000_000) // more liquid, but recently seen
	seedSweepTicker(t, db, "SWPB", 8_000_000_000)
	seedObserved(t, db, "SWPA", sweepAnchor.AddDate(0, 0, -1))
	sweepCleanup(t, db, []string{"SWPA", "SWPB"})
	f := &sweepBatchFetcher{}
	uc := newSweepTestUC(t, db, f, &sweepBudgetFake{available: 1, perKey: 50})

	res, err := uc.SweepAnchorDay(context.Background(), sweepAnchor, SweepOptions{MinADTV: sweepADTVFloor})
	if err != nil {
		t.Fatalf("SweepAnchorDay: %v", err)
	}
	if res.Planned != 1 || len(f.calls) != 1 || len(f.calls[0]) != 1 {
		t.Fatalf("res=%+v calls=%v, want one ticker", res, f.calls)
	}
	if f.calls[0][0] != "SWPB" {
		t.Errorf("rotation picked %s, want the never-observed SWPB", f.calls[0][0])
	}
}

func TestSweepAnchorDay_IsolatesInvalidAndEmpty(t *testing.T) {
	db := connectSweepDB(t)
	tickers := []string{"SWPA", "SWPB", "SWPC"}
	for _, tk := range tickers {
		seedSweepTicker(t, db, tk, 10_000_000_000)
	}
	sweepCleanup(t, db, tickers)
	f := &sweepBatchFetcher{
		invalid: map[string]bool{"SWPB": true},
		empty:   map[string]bool{"SWPC": true},
	}
	uc := newSweepTestUC(t, db, f, &sweepBudgetFake{available: 100, perKey: 50})

	res, err := uc.SweepAnchorDay(context.Background(), sweepAnchor, SweepOptions{MinADTV: sweepADTVFloor})
	if err != nil {
		t.Fatalf("SweepAnchorDay: %v", err)
	}
	if res.Fetched != 1 || res.Invalid != 1 || res.Empty != 1 {
		t.Errorf("res = %+v, want fetched=1 invalid=1 empty=1", res)
	}
	if res.Attempted != 3 || res.Shortfall != 0 {
		t.Errorf("attempted=%d shortfall=%d, want 3/0 (the whole batch was paid for)", res.Attempted, res.Shortfall)
	}
	var rows int
	if err := db.Get(&rows, "SELECT COUNT(*) FROM broker_stock_summaries WHERE trading_day = $1", sweepAnchor); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 2 {
		t.Errorf("persisted rows = %d, want 2 (only the valid ticker)", rows)
	}
}

func TestSweepAnchorDay_QuotaStopKeepsEarlierBatches(t *testing.T) {
	db := connectSweepDB(t)
	tickers := []string{"SWP1", "SWP2", "SWP3", "SWP4", "SWP5"}
	for _, tk := range tickers {
		seedSweepTicker(t, db, tk, 10_000_000_000)
	}
	sweepCleanup(t, db, tickers)
	// perKey=2 → batches of 2; the second call hits the quota wall.
	f := &sweepBatchFetcher{failAfter: 1, failErr: broker.ErrQuotaExhausted}
	uc := newSweepTestUC(t, db, f, &sweepBudgetFake{available: 10, perKey: 2})

	res, err := uc.SweepAnchorDay(context.Background(), sweepAnchor, SweepOptions{MinADTV: sweepADTVFloor})
	if err != nil {
		t.Fatalf("SweepAnchorDay returned an error for a quota stop: %v", err)
	}
	if !res.QuotaStopped || res.Planned != 5 || res.Attempted != 2 || res.Shortfall != 3 {
		t.Errorf("res = %+v, want quota_stopped planned=5 attempted=2 shortfall=3", res)
	}
	if res.Uncovered != 3 {
		t.Errorf("uncovered = %d, want 3", res.Uncovered)
	}
	if res.Fetched != 2 {
		t.Errorf("fetched = %d, want 2 (first batch kept)", res.Fetched)
	}
	var rows int
	if err := db.Get(&rows, "SELECT COUNT(*) FROM broker_stock_summaries WHERE trading_day = $1", sweepAnchor); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 4 {
		t.Errorf("persisted rows = %d, want 4 (first batch intact)", rows)
	}
}

func TestSweepAnchorDay_NoBudget(t *testing.T) {
	db := connectSweepDB(t)
	tickers := []string{"SWPA", "SWPB"}
	for _, tk := range tickers {
		seedSweepTicker(t, db, tk, 10_000_000_000)
	}
	sweepCleanup(t, db, tickers)
	f := &sweepBatchFetcher{}
	uc := newSweepTestUC(t, db, f, &sweepBudgetFake{available: 0, perKey: 50})

	res, err := uc.SweepAnchorDay(context.Background(), sweepAnchor, SweepOptions{MinADTV: sweepADTVFloor})
	if err != nil {
		t.Fatalf("SweepAnchorDay: %v", err)
	}
	if !res.NoCapacity || res.Planned != 0 || res.Attempted != 0 {
		t.Errorf("res = %+v, want no_capacity planned=0", res)
	}
	if res.Uncovered != 2 {
		t.Errorf("uncovered = %d, want 2 (both candidates left for a later run)", res.Uncovered)
	}
	if len(f.calls) != 0 {
		t.Errorf("calls = %v, want none", f.calls)
	}
}
