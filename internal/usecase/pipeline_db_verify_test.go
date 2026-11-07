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

// TestGetPipelineStatus_BrokerFreshnessBar — broker freshness is grain-aware:
// the newest stored window end must reach the last stored trading day of the
// previous calendar month. A month-grain archive is fresh; a missing closed
// month is stale even when a window exists inside the older month.
func TestGetPipelineStatus_BrokerFreshnessBar(t *testing.T) {
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

	// Seed the previous calendar month's weekdays; its last weekday is the
	// freshness bar.
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	firstOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	prevStart := firstOfMonth.AddDate(0, -1, 0)
	prevEnd := firstOfMonth.AddDate(0, 0, -1)

	ticker := "TESTPZ"
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

	var days []time.Time
	for d := prevStart; !d.After(prevEnd); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		days = append(days, d)
		if err := priceRepo.Upsert(db, &entity.DailyPrice{
			Ticker: ticker, TradingDay: d,
			Open: f64p(100), High: f64p(110), Low: f64p(90), Close: f64p(105),
			Volume: i64p(1_000_000), Value: i64p(10_000_000_000), Frequency: i32p(1000),
			Source: "idx",
		}); err != nil {
			t.Fatalf("seed price %s: %v", d.Format("2006-01-02"), err)
		}
	}
	if len(days) < 2 {
		t.Fatalf("seeded %d previous-month days, need at least 2", len(days))
	}
	last, penultimate := days[len(days)-1], days[len(days)-2]

	old := penultimate
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

	// One trading day short of the closed month's last session → stale.
	seedForeignWindow(penultimate)
	status, err := uc.GetPipelineStatus(context.Background())
	if err != nil {
		t.Fatalf("GetPipelineStatus: %v", err)
	}
	health := brokerHealth(t, status)
	if !health.Stale {
		t.Errorf("health = %+v, want stale before the closed month is archived", health)
	}
	if health.StaleTradingDays == nil || *health.StaleTradingDays < 1 {
		t.Errorf("stale_trading_days = %v, want at least 1", health.StaleTradingDays)
	}

	// The closed month's last session → fresh.
	seedForeignWindow(last)
	status, err = uc.GetPipelineStatus(context.Background())
	if err != nil {
		t.Fatalf("GetPipelineStatus: %v", err)
	}
	health = brokerHealth(t, status)
	if health.Stale {
		t.Errorf("health = %+v, want fresh at the closed month end", health)
	}
	if health.HighWaterMark == nil || *health.HighWaterMark != last.Format("2006-01-02") {
		t.Errorf("high_water_mark = %v, want %s", health.HighWaterMark, last.Format("2006-01-02"))
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
