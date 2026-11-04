package usecase

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestGetPipelineStatus_BrokerStalenessInTradingDays — the broker source's
// freshness is the age of its newest stored window end, counted in market
// trading days: age 0–1 is fresh (normal before the day's writer runs), 2+ is
// stale. Wall-clock last_success_at does not decide it.
func TestGetPipelineStatus_BrokerStalenessInTradingDays(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	priceRepo := repository.NewDailyPriceRepository(log)
	brokerRepo := repository.NewBrokerStockSummaryRepository(log)
	statusRepo := repository.NewSourceStatusRepository(log)

	uc := NewPipelineUseCase(db, log, nil, statusRepo, repository.NewAlertRepository(log), brokerRepo, priceRepo)

	// A far-future calendar keeps these days the market's newest regardless of
	// the developer database's real history.
	ticker := "TESTPZ"
	days := []time.Time{
		time.Date(2099, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2099, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2099, 1, 4, 0, 0, 0, 0, time.UTC),
	}
	db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM source_status WHERE source = $1", BrokerStockSummarySource)
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $1, true) ON CONFLICT (code) DO NOTHING", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM source_status WHERE source = $1", BrokerStockSummarySource)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})
	for _, day := range days {
		if err := priceRepo.Upsert(db, &entity.DailyPrice{
			Ticker: ticker, TradingDay: day,
			Open: f64p(100), High: f64p(110), Low: f64p(90), Close: f64p(105),
			Volume: i64p(1_000_000), Value: i64p(10_000_000_000), Frequency: i32p(1000),
			Source: "idx",
		}); err != nil {
			t.Fatalf("seed price %s: %v", day.Format("2006-01-02"), err)
		}
	}

	// The stored watermark is deliberately old; the live newest window end is
	// what decides.
	old := time.Date(2099, 1, 3, 0, 0, 0, 0, time.UTC)
	if err := statusRepo.Upsert(db, &entity.SourceStatus{
		Source:        BrokerStockSummarySource,
		LastSuccessAt: &old,
		MaxAgeSeconds: 86400,
		HighWaterMark: &old,
	}); err != nil {
		t.Fatalf("seed source status: %v", err)
	}

	seedForeignWindow := func(day time.Time) {
		t.Helper()
		if err := brokerRepo.UpsertWindow(db, nil, &entity.BrokerStockSummaryTotals{
			Ticker: ticker, TradingDay: day, WinFrom: day,
			Investor: broker.InvestorForeign, Market: broker.MarketRegular,
			FNVal: i64p(1_000_000), Source: entity.BrokerSourceLive,
		}); err != nil {
			t.Fatalf("seed f window %s: %v", day.Format("2006-01-02"), err)
		}
	}

	// Age 2: the newest window ends two trading days before the latest session.
	seedForeignWindow(days[0])
	status, err := uc.GetPipelineStatus(context.Background())
	if err != nil {
		t.Fatalf("GetPipelineStatus: %v", err)
	}
	health := brokerHealth(t, status)
	if health.StaleTradingDays == nil || *health.StaleTradingDays != 2 || !health.Stale {
		t.Errorf("health = %+v, want age 2 and stale", health)
	}

	// Age 0: a window ending on the latest session makes it fresh.
	seedForeignWindow(days[2])
	status, err = uc.GetPipelineStatus(context.Background())
	if err != nil {
		t.Fatalf("GetPipelineStatus: %v", err)
	}
	health = brokerHealth(t, status)
	if health.StaleTradingDays == nil || *health.StaleTradingDays != 0 || health.Stale {
		t.Errorf("health = %+v, want age 0 and fresh", health)
	}
	if health.HighWaterMark == nil || *health.HighWaterMark != "2099-01-04" {
		t.Errorf("high_water_mark = %v, want 2099-01-04", health.HighWaterMark)
	}
}

func brokerHealth(t *testing.T, status *PipelineStatusData) SourceHealth {
	t.Helper()
	for _, s := range status.Sources {
		if s.Source == BrokerStockSummarySource {
			return s
		}
	}
	t.Fatalf("broker source missing from %+v", status.Sources)
	return SourceHealth{}
}
