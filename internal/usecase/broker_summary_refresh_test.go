package usecase

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// fakeBatchFetcher records the windows the writers request and returns canned
// per-ticker results. It satisfies BrokerSummaryBatchFetcher.
type fakeBatchFetcher struct {
	results map[string]*broker.Result
	invalid map[string]error
	err     error

	calls   int
	tickers [][]string
	from    time.Time
	to      time.Time
	lane    string
}

func (f *fakeBatchFetcher) FetchBatch(_ context.Context, tickers []string, _ time.Time) (*broker.Batch, error) {
	return f.FetchBatchRange(context.Background(), tickers, time.Time{}, time.Time{}, broker.InvestorAll)
}

func (f *fakeBatchFetcher) FetchBatchRange(_ context.Context, tickers []string, from, to time.Time, lane string) (*broker.Batch, error) {
	f.calls++
	f.tickers = append(f.tickers, append([]string(nil), tickers...))
	f.from, f.to, f.lane = from, to, lane
	if f.err != nil {
		return nil, f.err
	}
	batch := &broker.Batch{Results: map[string]*broker.Result{}, Invalid: map[string]error{}}
	for _, ticker := range tickers {
		if err, bad := f.invalid[ticker]; bad {
			batch.Invalid[ticker] = err
			continue
		}
		if res, ok := f.results[ticker]; ok {
			batch.Results[ticker] = res
		}
	}
	return batch, nil
}

type fakeSweepBudget struct {
	available int
	perKey    int
}

func (b fakeSweepBudget) Available() int                     { return b.available }
func (b fakeSweepBudget) BatchCapacity() int                 { return b.perKey }
func (b fakeSweepBudget) NextReset() *time.Time              { return nil }
func (b fakeSweepBudget) EnsureSeeded(context.Context) error { return nil }

type fakeUsageCounter struct {
	used map[string]int
}

func (f *fakeUsageCounter) Reserve(_ *sqlx.DB, _ time.Time, feature string, requested, cap int) (int, int, error) {
	if f.used == nil {
		f.used = map[string]int{}
	}
	left := cap - f.used[feature]
	if left < 0 {
		left = 0
	}
	granted := requested
	if granted > left {
		granted = left
	}
	f.used[feature] += granted
	return granted, left - granted, nil
}

func (f *fakeUsageCounter) Units(_ *sqlx.DB, _ time.Time, feature string) (int, error) {
	return f.used[feature], nil
}

// TestRefreshAnomalyDay_OrdersCapsAndSkipsStored — the daily refresher selects
// anomaly tickers by magnitude, skips an already-stored anchor day, and spends
// only the granted daily units. DB-backed because skip-if-stored and persist are
// real queries.
func TestRefreshAnomalyDay_OrdersCapsAndSkipsStored(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := repository.NewBrokerStockSummaryRepository(log)
	anomalyRepo := repository.NewAnomalyRepository(log)
	dailyRepo := repository.NewDailyPriceRepository(log)

	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	tickers := []string{"RFA", "RFB", "RFC", "RFD", "RFE"}
	t.Cleanup(func() {
		for _, ticker := range tickers {
			db.MustExec("DELETE FROM anomalies WHERE ticker = $1 AND trading_day = $2", ticker, day)
			db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
			db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
			db.MustExec("DELETE FROM daily_prices WHERE ticker = $1 AND trading_day = $2", ticker, day)
			db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
		}
	})
	for i, ticker := range tickers {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $1, true) ON CONFLICT (code) DO NOTHING", ticker)
		db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, close, value, source)
			VALUES ($1, $2, 100, 10_000_000_000, 'idx')`, ticker, day)
		// Magnitude descends with the slice: RFA first, RFE last.
		db.MustExec(`INSERT INTO anomalies (ticker, trading_day, type, direction, magnitude_pct)
			VALUES ($1, $2, 'volume', 'up', $3)`, ticker, day, float64(500-i*50))
	}

	// RFB is already stored: it must be skipped even though it ranks second.
	if err := persistBrokerWindow(db, repo, "RFB", day, day, &broker.Result{
		Investor: broker.InvestorForeign,
		Buyers:   []broker.Row{{BrokerCode: "AK", Lot: 1, Value: 2, AvgPrice: 2, Rank: 1}},
	}, broker.InvestorForeign, "sweep"); err != nil {
		t.Fatalf("seed stored day: %v", err)
	}

	results := map[string]*broker.Result{}
	for _, ticker := range tickers {
		results[ticker] = &broker.Result{
			Investor: broker.InvestorForeign,
			Buyers:   []broker.Row{{BrokerCode: "AK", Lot: 1, Value: 2, AvgPrice: 2, Rank: 1}},
		}
	}
	fetcher := &fakeBatchFetcher{results: results}
	usage := &fakeUsageCounter{}
	uc := NewBrokerStockSummaryUseCase(db, log, nil, nil, repo, dailyRepo,
		WithBatchSource(fetcher, fakeSweepBudget{available: 100, perKey: 50}),
		WithAnomalySource(anomalyRepo),
		WithFetchUsage(usage),
	)

	res, err := uc.RefreshAnomalyDay(context.Background(), day, BrokerRefreshOptions{MaxUnits: 3})
	if err != nil {
		t.Fatalf("RefreshAnomalyDay: %v", err)
	}
	if res.Candidates != 5 || res.Stored != 1 {
		t.Fatalf("candidates/stored = %d/%d, want 5/1", res.Candidates, res.Stored)
	}
	if res.Planned != 3 || res.Attempted != 3 || res.Fetched != 3 {
		t.Fatalf("planned/attempted/fetched = %d/%d/%d, want 3/3/3", res.Planned, res.Attempted, res.Fetched)
	}
	// RFB is skipped, so the granted 3 are RFA, RFC, RFD in magnitude order.
	if len(fetcher.tickers) != 1 || len(fetcher.tickers[0]) != 3 {
		t.Fatalf("fetcher calls = %v, want one batch of 3", fetcher.tickers)
	}
	want := []string{"RFA", "RFC", "RFD"}
	for i, ticker := range want {
		if fetcher.tickers[0][i] != ticker {
			t.Errorf("fetch order[%d] = %s, want %s", i, fetcher.tickers[0][i], ticker)
		}
	}
	if !fetcher.from.Equal(day) || !fetcher.to.Equal(day) || fetcher.lane != broker.InvestorForeign {
		t.Errorf("fetched %s..%s lane %s, want one day on the f lane", fetcher.from, fetcher.to, fetcher.lane)
	}

	totals, _, err := repo.FindStoredWindow(db, "RFA", broker.InvestorForeign, day, day)
	if err != nil {
		t.Fatalf("stored day read: %v", err)
	}
	if totals.Source != "sweep" {
		t.Errorf("source = %s, want sweep", totals.Source)
	}
}
