package usecase

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// seedTradingDays inserts one daily_prices row per weekday in [from, to] and
// returns the days. The value clears the Rp5bn floor.
func seedTradingDays(t *testing.T, db *sqlx.DB, ticker string, from, to time.Time) []time.Time {
	t.Helper()
	var days []time.Time
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, close, value, source)
			VALUES ($1, $2, 100, 10_000_000_000, 'idx')
			ON CONFLICT (ticker, trading_day) DO UPDATE SET value = EXCLUDED.value`,
			ticker, d)
		days = append(days, d)
	}
	return days
}

// TestArchiveClosedMonths_FetchesThenSkipsAndDerives — the archiver writes a
// calendar-month chunk, is idempotent on a second run, and derives a month from
// exact-covering day rows for free. DB-backed.
func TestArchiveClosedMonths_FetchesThenSkipsAndDerives(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := repository.NewBrokerStockSummaryRepository(log)
	dailyRepo := repository.NewDailyPriceRepository(log)

	fetchedTicker := "ARCA"
	derivedTicker := "ARCB"
	tickers := []string{fetchedTicker, derivedTicker}
	t.Cleanup(func() {
		for _, ticker := range tickers {
			db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
			db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
			db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
			db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
		}
	})
	for _, ticker := range tickers {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $1, true) ON CONFLICT (code) DO NOTHING", ticker)
	}

	// Enough history for the 21-day ADTV predicate ending 2026-09-30.
	augStart := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	sepEnd := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	seedTradingDays(t, db, fetchedTicker, augStart, sepEnd)
	sepDays := seedTradingDays(t, db, derivedTicker, augStart, sepEnd)

	// The derived ticker has exact-covering day rows for September.
	sepStart := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, day := range sepDays {
		if day.Before(sepStart) {
			continue
		}
		if err := persistBrokerWindow(db, repo, derivedTicker, day, day, &broker.Result{
			Investor: broker.InvestorForeign,
			Buyers:   []broker.Row{{BrokerCode: "AK", Lot: 10, Value: 1_000, AvgPrice: 100, Rank: 1}},
		}, broker.InvestorForeign, entity.BrokerSourceSweep); err != nil {
			t.Fatalf("seed day row %s: %v", day.Format("2006-01-02"), err)
		}
	}

	fetcher := &fakeBatchFetcher{results: map[string]*broker.Result{
		fetchedTicker: {
			Investor: broker.InvestorForeign,
			Buyers:   []broker.Row{{BrokerCode: "AK", Lot: 10, Value: 1_000, AvgPrice: 100, Rank: 1}},
			Sellers:  []broker.Row{{BrokerCode: "XL", Lot: 4, Value: 400, AvgPrice: 100, Rank: 1}},
		},
	}}
	uc := NewBrokerStockSummaryUseCase(db, log, nil, nil, repo, dailyRepo,
		WithBatchSource(fetcher, fakeSweepBudget{available: 100, perKey: 50}),
	)

	runDate := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	res, err := uc.ArchiveClosedMonths(context.Background(), runDate, BrokerArchiveOptions{MaxMonths: 1})
	if err != nil {
		t.Fatalf("ArchiveClosedMonths: %v", err)
	}
	if len(res.Months) != 1 {
		t.Fatalf("months = %d, want 1", len(res.Months))
	}
	m := res.Months[0]
	if m.Month != "2026-09" || m.TradingDays == 0 {
		t.Fatalf("month = %+v, want a populated 2026-09", m)
	}
	if m.Fetched != 1 || m.Derived != 1 || m.Stored != 0 {
		t.Fatalf("fetched/derived/stored = %d/%d/%d, want 1/1/0", m.Fetched, m.Derived, m.Stored)
	}
	if m.UnitsSpent != 1 || res.UnitsSpent != 1 {
		t.Fatalf("units = %d/%d, want 1/1", m.UnitsSpent, res.UnitsSpent)
	}

	fetchedTotals, _, err := repo.FindStoredWindow(db, fetchedTicker, broker.InvestorForeign, sepStart, sepEnd)
	if err != nil {
		t.Fatalf("read fetched chunk: %v", err)
	}
	if fetchedTotals.Source != entity.BrokerSourceArchive {
		t.Errorf("fetched source = %s, want archive", fetchedTotals.Source)
	}
	derivedTotals, derivedRows, err := repo.FindStoredWindow(db, derivedTicker, broker.InvestorForeign, sepStart, sepEnd)
	if err != nil {
		t.Fatalf("read derived chunk: %v", err)
	}
	if derivedTotals.Source != entity.BrokerSourceArchive || len(derivedRows) != 1 {
		t.Errorf("derived chunk = source %s rows %d, want archive/1", derivedTotals.Source, len(derivedRows))
	}

	// Second run: every Tier-2 ticker is covered, so nothing is fetched.
	before := fetcher.calls
	res2, err := uc.ArchiveClosedMonths(context.Background(), runDate, BrokerArchiveOptions{MaxMonths: 1})
	if err != nil {
		t.Fatalf("second ArchiveClosedMonths: %v", err)
	}
	if res2.Months[0].Stored != 2 || res2.Months[0].Fetched != 0 || res2.Months[0].Derived != 0 {
		t.Fatalf("second run = %+v, want both stored", res2.Months[0])
	}
	if fetcher.calls != before {
		t.Errorf("second run fetched %d more windows, want 0", fetcher.calls-before)
	}
}
