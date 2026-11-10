package usecase

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestFinancialsRead_DB drives get_financials against real Postgres rows: the
// period views, the attributable/net split, per/pbv from the close on or before
// the period end, last_price, coverage and staleness. Skipped unless
// IDX_MCP_DB_DSN is set; fixture dates are in 2089–2090 so production rows are
// never touched.
func TestFinancialsRead_DB(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	const ticker = "TESTFR"
	cleanup := func() {
		db.MustExec(`DELETE FROM daily_prices WHERE ticker = $1`, ticker)
		db.MustExec(`DELETE FROM financial_statement_versions WHERE ticker = $1`, ticker)
		db.MustExec(`DELETE FROM financial_statements WHERE ticker = $1`, ticker)
		db.MustExec(`DELETE FROM tickers WHERE code = $1`, ticker)
	}
	cleanup()
	t.Cleanup(cleanup)

	db.MustExec(`INSERT INTO tickers (code, name, active) VALUES ($1, 'Financials read verify', true)`, ticker)

	period := time.Date(2089, time.September, 30, 0, 0, 0, 0, time.UTC)
	prior := time.Date(2089, time.June, 30, 0, 0, 0, 0, time.UTC)
	assets := int64(287_000_000_000_000)
	equity := int64(150_000_000_000_000)
	revenue := int64(112_000_000_000_000)
	netIncl := int64(24_000_000_000_000)
	netParent := int64(23_000_000_000_000)
	eps := 100.0
	book := 1000.0

	repo := repository.NewFinancialsRepository(log)
	snap := &financials.Snapshot{
		Month:           time.Date(2089, time.October, 1, 0, 0, 0, 0, time.UTC),
		Status:          financials.StatusOK,
		RowCount:        2,
		DistinctPeriods: 2,
		Statements: []financials.Statement{
			{
				Ticker: ticker, PeriodEnd: period,
				FiscalYearEnd:  time.Date(2089, time.December, 31, 0, 0, 0, 0, time.UTC),
				DurationMonths: 9,
				Assets:         &assets, Equity: &equity, Revenue: &revenue,
				NetProfit: &netIncl, NetProfitParent: &netParent,
				EPS: &eps, BookValue: &book, ROE: finPtrF(18.6), ROA: finPtrF(9.7),
				NPM: finPtrF(20.5), DER: finPtrF(0.91),
				Source: financials.SourceRatioSnapshot, ValueBasis: financials.ValueBasisIDX,
				SourceFSDate: period,
			},
			{
				Ticker: ticker, PeriodEnd: prior,
				FiscalYearEnd:  time.Date(2089, time.December, 31, 0, 0, 0, 0, time.UTC),
				DurationMonths: 6,
				Assets:         &assets, Equity: &equity, Revenue: &revenue,
				NetProfit: &netIncl, NetProfitParent: &netParent,
				EPS: &eps, BookValue: &book,
				Source: financials.SourceRatioSnapshot, ValueBasis: financials.ValueBasisIDX,
				SourceFSDate: prior,
			},
		},
	}
	if _, err := repo.IngestSnapshot(db, snap, nil); err != nil {
		t.Fatalf("seed statements: %v", err)
	}

	// The period end is a holiday: the covering close is the prior trading day.
	prices := []entity.DailyPrice{
		{Ticker: ticker, TradingDay: time.Date(2089, time.September, 29, 0, 0, 0, 0, time.UTC), Close: finPtrF(2500), Source: "idx"},
		{Ticker: ticker, TradingDay: time.Date(2089, time.June, 28, 0, 0, 0, 0, time.UTC), Close: finPtrF(2000), Source: "idx"},
		{Ticker: ticker, TradingDay: time.Date(2090, time.January, 5, 0, 0, 0, 0, time.UTC), Close: finPtrF(3000), Source: "idx"},
	}
	priceRepo := repository.NewDailyPriceRepository(log)
	for i := range prices {
		if err := priceRepo.Upsert(db, &prices[i]); err != nil {
			t.Fatalf("seed price %s: %v", prices[i].TradingDay.Format("2006-01-02"), err)
		}
	}

	uc := NewFinancialsUseCase(db, log, repo, priceRepo)
	uc.Now = func() time.Time { return time.Date(2090, time.February, 1, 0, 0, 0, 0, time.UTC) }

	resp, err := uc.GetFinancials(context.Background(), ticker, FinancialPeriodRecent)
	if err != nil {
		t.Fatalf("GetFinancials: %v", err)
	}
	if len(resp.Statements) != 2 {
		t.Fatalf("statements = %d, want 2", len(resp.Statements))
	}
	if resp.LatestPeriodEnd != "2089-09-30" {
		t.Errorf("latest_period_end = %q, want 2089-09-30", resp.LatestPeriodEnd)
	}
	if resp.PeriodStale {
		t.Error("period_stale = true, want false (latest period inside a year)")
	}

	newest := resp.Statements[0]
	if newest.PeriodLabel != "9M 2089" || !newest.IsInterim {
		t.Errorf("newest label/interim = %q/%v, want 9M 2089/true", newest.PeriodLabel, newest.IsInterim)
	}
	if newest.NetProfit == nil || *newest.NetProfit != float64(netParent) {
		t.Errorf("net_profit = %v, want attributable %d", newest.NetProfit, netParent)
	}
	if newest.NetProfitInclNCI == nil || *newest.NetProfitInclNCI != float64(netIncl) {
		t.Errorf("net_profit_incl_nci = %v, want %d", newest.NetProfitInclNCI, netIncl)
	}
	if newest.PriceAsOf != "2089-09-29" {
		t.Errorf("price_as_of = %q, want 2089-09-29", newest.PriceAsOf)
	}
	if newest.PER == nil || *newest.PER != 25 {
		t.Errorf("per = %v, want 25", newest.PER)
	}
	if newest.PBV == nil || *newest.PBV != 2.5 {
		t.Errorf("pbv = %v, want 2.5", newest.PBV)
	}
	if newest.Cash != nil || newest.DebtToEquity != nil {
		t.Error("cash/debt_to_equity must be nil at Tier 1")
	}
	if newest.LiabilitiesToEquity == nil || *newest.LiabilitiesToEquity != 0.91 {
		t.Errorf("liabilities_to_equity = %v, want 0.91", newest.LiabilitiesToEquity)
	}

	if resp.Coverage["revenue"].Status != FinancialCoverageObserved {
		t.Errorf("revenue coverage = %+v, want observed 2/2", resp.Coverage["revenue"])
	}
	if resp.Coverage["cash"].Status != FinancialCoverageUnknown || resp.Coverage["cash"].ObservedRows != 0 {
		t.Errorf("cash coverage = %+v, want unknown 0/2", resp.Coverage["cash"])
	}

	if resp.LastPrice == nil || *resp.LastPrice != 3000 || resp.LastPriceAsOf != "2090-01-05" {
		t.Errorf("last_price = %v as of %q, want 3000 as of 2090-01-05", resp.LastPrice, resp.LastPriceAsOf)
	}

	// The annual view has no 12M row in the fixture: a structured empty list.
	annual, err := uc.GetFinancials(context.Background(), ticker, FinancialPeriodAnnual)
	if err != nil {
		t.Fatalf("annual GetFinancials: %v", err)
	}
	if len(annual.Statements) != 0 || annual.LatestPeriodEnd != "2089-09-30" {
		t.Errorf("annual statements/latest = %d/%q, want 0/2089-09-30", len(annual.Statements), annual.LatestPeriodEnd)
	}

	// A restatement in a later snapshot must be served from the canonical row,
	// with the prior observation kept in the versions trail.
	revised := netParent + 1_000_000_000
	snap2 := *snap
	snap2.Month = time.Date(2089, time.November, 1, 0, 0, 0, 0, time.UTC)
	snap2.Statements = []financials.Statement{snap.Statements[0]}
	snap2.Statements[0].NetProfitParent = &revised
	if _, err := repo.IngestSnapshot(db, &snap2, nil); err != nil {
		t.Fatalf("restate statement: %v", err)
	}
	revisedResp, err := uc.GetFinancials(context.Background(), ticker, FinancialPeriodRecent)
	if err != nil {
		t.Fatalf("GetFinancials after restatement: %v", err)
	}
	if got := revisedResp.Statements[0].NetProfit; got == nil || *got != float64(revised) {
		t.Errorf("net_profit after restatement = %v, want %d (latest version)", got, revised)
	}
	versions, err := repo.FindVersions(db, ticker, period)
	if err != nil {
		t.Fatalf("FindVersions: %v", err)
	}
	if len(versions) != 2 {
		t.Errorf("versions = %d, want 2 (first sight + restatement)", len(versions))
	}
}
