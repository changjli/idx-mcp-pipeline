package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// fakeFinancialStatements implements FinancialStatementsReader.
type fakeFinancialStatements struct {
	rows  []entity.FinancialStatement
	err   error
	calls int
}

func (f *fakeFinancialStatements) FindByTicker(db *sqlx.DB, ticker string) ([]entity.FinancialStatement, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := make([]entity.FinancialStatement, len(f.rows))
	copy(out, f.rows)
	return out, nil
}

// fakeFinancialPrices implements FinancialPriceReader from in-memory maps keyed
// by "ticker|YYYY-MM-DD" (period close) and ticker (latest close).
type fakeFinancialPrices struct {
	closes map[string]entity.DailyPrice
	latest map[string]entity.DailyPrice
	err    error
}

func (f *fakeFinancialPrices) FindCloseOnOrBefore(db *sqlx.DB, ticker string, day time.Time) (*entity.DailyPrice, error) {
	if f.err != nil {
		return nil, f.err
	}
	price, ok := f.closes[ticker+"|"+day.Format("2006-01-02")]
	if !ok {
		return nil, nil
	}
	return &price, nil
}

func (f *fakeFinancialPrices) FindLatestClose(db *sqlx.DB, ticker string) (*entity.DailyPrice, error) {
	if f.err != nil {
		return nil, f.err
	}
	price, ok := f.latest[ticker]
	if !ok {
		return nil, nil
	}
	return &price, nil
}

func finDate(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func finPtrI(v int64) *int64     { return &v }
func finPtrF(v float64) *float64 { return &v }

// finStatement builds a minimal stored row for one period.
func finStatement(ticker, periodEnd string, duration int) entity.FinancialStatement {
	st := entity.FinancialStatement{
		Ticker:         ticker,
		PeriodEnd:      finDate(periodEnd),
		FiscalYearEnd:  finDate(periodEnd[:4] + "-12-31"),
		DurationMonths: duration,
		Source:         "idx_ratio_snapshot",
		ValueBasis:     "IDR (IDX-converted at an undisclosed rate)",
	}
	return st
}

// newFinancialsTestUseCase wires the read path over fakes with a fixed clock.
func newFinancialsTestUseCase(rows []entity.FinancialStatement, prices *fakeFinancialPrices) *FinancialsUseCase {
	if prices == nil {
		prices = &fakeFinancialPrices{}
	}
	return &FinancialsUseCase{
		Log:    logrus.New(),
		Repo:   &fakeFinancialStatements{rows: rows},
		Prices: prices,
		Now:    func() time.Time { return finDate("2026-11-08") },
	}
}

func finPeriodEnds(views []FinancialStatementView) []string {
	out := make([]string, 0, len(views))
	for _, v := range views {
		out = append(out, v.PeriodEnd)
	}
	return out
}

// TestGetFinancialsViews pins the three period views: recent is the newest ~2
// years (all durations), quarterly is every interim period over full history
// (3M/6M/9M — not Q1 only), annual is every 12M period.
func TestGetFinancialsViews(t *testing.T) {
	rows := []entity.FinancialStatement{
		finStatement("TLKM", "2026-09-30", 9),
		finStatement("TLKM", "2026-06-30", 6),
		finStatement("TLKM", "2026-03-31", 3),
		finStatement("TLKM", "2025-12-31", 12),
		finStatement("TLKM", "2025-09-30", 9),
		finStatement("TLKM", "2024-06-30", 6),
		finStatement("TLKM", "2018-12-31", 12),
	}
	uc := newFinancialsTestUseCase(rows, nil)

	t.Run("recent defaults and keeps all durations inside two years", func(t *testing.T) {
		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Period != FinancialPeriodRecent {
			t.Errorf("period = %q, want recent", resp.Period)
		}
		want := []string{"2026-09-30", "2026-06-30", "2026-03-31", "2025-12-31", "2025-09-30"}
		got := finPeriodEnds(resp.Statements)
		if len(got) != len(want) {
			t.Fatalf("periods = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("period[%d] = %s, want %s", i, got[i], want[i])
			}
		}
		if resp.LatestPeriodEnd != "2026-09-30" {
			t.Errorf("latest_period_end = %q, want 2026-09-30", resp.LatestPeriodEnd)
		}
	})

	t.Run("quarterly is every interim period across full history", func(t *testing.T) {
		resp, err := uc.GetFinancials(context.Background(), "TLKM", "quarterly")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"2026-09-30", "2026-06-30", "2026-03-31", "2025-09-30", "2024-06-30"}
		got := finPeriodEnds(resp.Statements)
		if len(got) != len(want) {
			t.Fatalf("periods = %v, want %v", got, want)
		}
		for i, v := range resp.Statements {
			if v.DurationMonths == 12 {
				t.Errorf("quarterly returned a 12M period: %s", v.PeriodEnd)
			}
			if v.DurationMonths != map[string]int{
				"2026-09-30": 9, "2026-06-30": 6, "2026-03-31": 3,
				"2025-09-30": 9, "2024-06-30": 6,
			}[got[i]] {
				t.Errorf("duration_months = %d for %s", v.DurationMonths, v.PeriodEnd)
			}
		}
	})

	t.Run("annual is every 12M period across full history", func(t *testing.T) {
		resp, err := uc.GetFinancials(context.Background(), "TLKM", "annual")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"2025-12-31", "2018-12-31"}
		got := finPeriodEnds(resp.Statements)
		if len(got) != len(want) {
			t.Fatalf("periods = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("period[%d] = %s, want %s", i, got[i], want[i])
			}
		}
		if resp.LatestPeriodEnd != "2026-09-30" {
			t.Errorf("latest_period_end = %q, want the newest stored period 2026-09-30", resp.LatestPeriodEnd)
		}
	})

	t.Run("period is case-insensitive", func(t *testing.T) {
		resp, err := uc.GetFinancials(context.Background(), "tlkm", " ANNUAL ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Period != FinancialPeriodAnnual || resp.Ticker != "TLKM" {
			t.Errorf("resp = %+v, want annual/TLKM", resp)
		}
	})
}

// TestGetFinancialsFieldMapping checks the mapping contract: attributable net
// profit, the incl-NCI sibling, the absolute/ratio fields Tier 1 carries, and
// the fields it must leave unknown (never zero).
func TestGetFinancialsFieldMapping(t *testing.T) {
	st := finStatement("TLKM", "2026-09-30", 9)
	assets := int64(287_000_000_000_000)
	equity := int64(150_000_000_000_000)
	revenue := int64(112_000_000_000_000)
	netIncl := int64(24_000_000_000_000)
	netParent := int64(23_000_000_000_000)
	st.Assets = &assets
	st.Equity = &equity
	st.Revenue = &revenue
	st.NetProfit = &netIncl
	st.NetProfitParent = &netParent
	st.EPS = finPtrF(232.4)
	st.BookValue = finPtrF(1515.2)
	st.ROE = finPtrF(18.6)
	st.ROA = finPtrF(9.7)
	st.NPM = finPtrF(20.5)
	st.DER = finPtrF(0.91)

	uc := newFinancialsTestUseCase([]entity.FinancialStatement{st}, nil)
	resp, err := uc.GetFinancials(context.Background(), "TLKM", FinancialPeriodRecent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Statements) != 1 {
		t.Fatalf("statements = %d, want 1", len(resp.Statements))
	}
	v := resp.Statements[0]

	if v.PeriodLabel != "9M 2026" {
		t.Errorf("period_label = %q, want 9M 2026", v.PeriodLabel)
	}
	if v.DurationMonths != 9 || !v.IsInterim || v.IsForecast {
		t.Errorf("duration/interim/forecast = %d/%v/%v, want 9/true/false", v.DurationMonths, v.IsInterim, v.IsForecast)
	}
	if v.NetProfit == nil || *v.NetProfit != float64(netParent) {
		t.Errorf("net_profit = %v, want attributable %d", v.NetProfit, netParent)
	}
	if v.NetProfitInclNCI == nil || *v.NetProfitInclNCI != float64(netIncl) {
		t.Errorf("net_profit_incl_nci = %v, want %d", v.NetProfitInclNCI, netIncl)
	}
	if v.TotalAssets == nil || *v.TotalAssets != float64(assets) {
		t.Errorf("total_assets = %v, want %d", v.TotalAssets, assets)
	}
	if v.TotalEquity == nil || *v.TotalEquity != float64(equity) {
		t.Errorf("total_equity = %v, want %d", v.TotalEquity, equity)
	}
	if v.Revenue == nil || *v.Revenue != float64(revenue) {
		t.Errorf("revenue = %v, want %d", v.Revenue, revenue)
	}
	if v.EPS == nil || *v.EPS != 232.4 || v.ROE == nil || *v.ROE != 18.6 ||
		v.ROA == nil || *v.ROA != 9.7 || v.NPM == nil || *v.NPM != 20.5 ||
		v.BookValue == nil || *v.BookValue != 1515.2 {
		t.Errorf("stored ratios not returned as stored: %+v", v)
	}
	if v.LiabilitiesToEquity == nil || *v.LiabilitiesToEquity != 0.91 {
		t.Errorf("liabilities_to_equity = %v, want stored der 0.91", v.LiabilitiesToEquity)
	}
	if v.DebtToEquity != nil {
		t.Errorf("debt_to_equity = %v, want nil (der is not total debt)", *v.DebtToEquity)
	}
	for name, field := range map[string]*float64{
		"cash": v.Cash, "gross_profit": v.GrossProfit, "operating_profit": v.OperatingProfit,
		"ebitda": v.EBITDA, "interest_expense": v.InterestExpense, "total_debt": v.TotalDebt,
		"short_term_borrowing": v.ShortTermBorrowing, "long_term_borrowing": v.LongTermBorrowing,
		"gross_profit_margin": v.GrossProfitMargin,
	} {
		if field != nil {
			t.Errorf("%s = %v, want nil", name, *field)
		}
	}
	if resp.Currency != "IDR" || resp.ValueBasis == "" {
		t.Errorf("currency/value_basis = %q/%q, want IDR and a basis note", resp.Currency, resp.ValueBasis)
	}
}

// TestGetFinancialsZeroIsAValue pins the nil/zero distinction: a reported zero
// is returned as zero and counted as observed, never dropped as "not
// computable". Only the per/pbv denominator rule treats a zero as no multiple.
func TestGetFinancialsZeroIsAValue(t *testing.T) {
	st := finStatement("TLKM", "2026-09-30", 9)
	zero := int64(0)
	zeroF := 0.0
	st.Revenue = &zero
	st.NetProfitParent = &zero
	st.EPS = &zeroF

	uc := newFinancialsTestUseCase([]entity.FinancialStatement{st}, &fakeFinancialPrices{
		closes: map[string]entity.DailyPrice{
			"TLKM|2026-09-30": {Ticker: "TLKM", TradingDay: finDate("2026-09-30"), Close: finPtrF(1000)},
		},
	})
	resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := resp.Statements[0]
	if v.Revenue == nil || *v.Revenue != 0 {
		t.Errorf("revenue = %v, want a reported zero", v.Revenue)
	}
	if v.NetProfit == nil || *v.NetProfit != 0 {
		t.Errorf("net_profit = %v, want a reported zero", v.NetProfit)
	}
	if v.PER != nil {
		t.Errorf("per = %v, want null for an eps of zero", *v.PER)
	}
	if c := resp.Coverage["eps"]; c.Status != FinancialCoverageObserved || c.ObservedRows != 1 {
		t.Errorf("eps coverage = %+v, want observed (zero is a value)", c)
	}
}

// TestGetFinancialsValuation checks per/pbv: calculated from the close on or
// before period_end, null when not computable, and negative values preserved.
func TestGetFinancialsValuation(t *testing.T) {
	st := finStatement("TLKM", "2026-09-30", 9)
	st.EPS = finPtrF(100)
	st.BookValue = finPtrF(1000)
	latest := entity.DailyPrice{Ticker: "TLKM", TradingDay: finDate("2026-09-29"), Close: finPtrF(2500)}
	prices := &fakeFinancialPrices{
		// The period end is a holiday; the close on or before it is 09-29.
		closes: map[string]entity.DailyPrice{"TLKM|2026-09-30": latest},
		latest: map[string]entity.DailyPrice{"TLKM": latest},
	}
	uc := newFinancialsTestUseCase([]entity.FinancialStatement{st}, prices)
	resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v := resp.Statements[0]
	if v.PriceAsOf != "2026-09-29" {
		t.Errorf("price_as_of = %q, want 2026-09-29", v.PriceAsOf)
	}
	if v.PER == nil || *v.PER != 25 {
		t.Errorf("per = %v, want 25", v.PER)
	}
	if v.PBV == nil || *v.PBV != 2.5 {
		t.Errorf("pbv = %v, want 2.5", v.PBV)
	}
	if resp.LastPrice == nil || *resp.LastPrice != 2500 || resp.LastPriceAsOf != "2026-09-29" {
		t.Errorf("last_price = %v as of %q, want 2500 as of 2026-09-29", resp.LastPrice, resp.LastPriceAsOf)
	}
}

func TestGetFinancialsValuationEdgeCases(t *testing.T) {
	close := entity.DailyPrice{Ticker: "TLKM", TradingDay: finDate("2026-09-29"), Close: finPtrF(2500)}

	t.Run("zero denominators and no close yield null multiples", func(t *testing.T) {
		st := finStatement("TLKM", "2026-09-30", 9)
		st.EPS = finPtrF(0)
		st.BookValue = finPtrF(0)
		uc := newFinancialsTestUseCase([]entity.FinancialStatement{st}, &fakeFinancialPrices{
			closes: map[string]entity.DailyPrice{"TLKM|2026-09-30": close},
		})
		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		v := resp.Statements[0]
		if v.PER != nil || v.PBV != nil {
			t.Errorf("per/pbv = %v/%v, want null for zero denominators", v.PER, v.PBV)
		}
		if v.PriceAsOf != "2026-09-29" {
			t.Errorf("price_as_of = %q, want the covering close", v.PriceAsOf)
		}
	})

	t.Run("no covering close yields null multiples and no price_as_of", func(t *testing.T) {
		st := finStatement("TLKM", "2026-09-30", 9)
		st.EPS = finPtrF(100)
		st.BookValue = finPtrF(1000)
		uc := newFinancialsTestUseCase([]entity.FinancialStatement{st}, nil)
		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		v := resp.Statements[0]
		if v.PER != nil || v.PBV != nil || v.PriceAsOf != "" {
			t.Errorf("per/pbv/price_as_of = %v/%v/%q, want null/null/empty", v.PER, v.PBV, v.PriceAsOf)
		}
	})

	t.Run("negative denominators keep the negative multiple", func(t *testing.T) {
		st := finStatement("TLKM", "2026-09-30", 9)
		st.EPS = finPtrF(-50)
		st.BookValue = finPtrF(500)
		uc := newFinancialsTestUseCase([]entity.FinancialStatement{st}, &fakeFinancialPrices{
			closes: map[string]entity.DailyPrice{"TLKM|2026-09-30": close},
		})
		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v := resp.Statements[0]; v.PER == nil || *v.PER != -50 {
			t.Errorf("per = %v, want -50", v.PER)
		}
	})
}

// TestGetFinancialsCoverage checks the per-field coverage declaration: carried
// fields observed only when every returned row has them; uncarried fields
// always unknown, with counts so a partial field stays visible.
func TestGetFinancialsCoverage(t *testing.T) {
	withEPS := finStatement("TLKM", "2026-06-30", 6)
	withEPS.EPS = finPtrF(100)
	withEPS.Revenue = finPtrI(1_000)
	withoutEPS := finStatement("TLKM", "2026-03-31", 3)
	withoutEPS.Revenue = finPtrI(500)

	uc := newFinancialsTestUseCase([]entity.FinancialStatement{withEPS, withoutEPS}, nil)
	resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	revenue := resp.Coverage["revenue"]
	if revenue.Status != FinancialCoverageObserved || revenue.ObservedRows != 2 || revenue.ReturnedRows != 2 {
		t.Errorf("revenue coverage = %+v, want observed 2/2", revenue)
	}
	eps := resp.Coverage["eps"]
	if eps.Status != FinancialCoverageUnknown || eps.ObservedRows != 1 || eps.ReturnedRows != 2 {
		t.Errorf("eps coverage = %+v, want unknown 1/2", eps)
	}
	for _, field := range []string{"cash", "gross_profit", "operating_profit", "ebitda", "total_debt", "debt_to_equity"} {
		c := resp.Coverage[field]
		if c.Status != FinancialCoverageUnknown || c.ObservedRows != 0 || c.ReturnedRows != 2 {
			t.Errorf("%s coverage = %+v, want unknown 0/2", field, c)
		}
	}
	per := resp.Coverage["per"]
	if per.Status != FinancialCoverageUnknown || per.ObservedRows != 0 {
		t.Errorf("per coverage = %+v, want unknown (no prices wired)", per)
	}
}

// TestGetFinancialsNoStatements checks the structured empty result: no error,
// empty statement list, empty latest period, stale, and a coverage block that
// says everything is unknown.
func TestGetFinancialsNoStatements(t *testing.T) {
	uc := newFinancialsTestUseCase(nil, nil)
	resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Statements == nil || len(resp.Statements) != 0 {
		t.Errorf("statements = %#v, want an empty (non-nil) list", resp.Statements)
	}
	if resp.LatestPeriodEnd != "" {
		t.Errorf("latest_period_end = %q, want empty", resp.LatestPeriodEnd)
	}
	if !resp.PeriodStale {
		t.Error("data_stale = false, want true with no stored statement")
	}
	if c := resp.Coverage["revenue"]; c.Status != FinancialCoverageUnknown || c.ReturnedRows != 0 {
		t.Errorf("revenue coverage = %+v, want unknown 0/0", c)
	}
	if resp.LastPrice != nil {
		t.Errorf("last_price = %v, want nil", *resp.LastPrice)
	}
}

// TestGetFinancialsStaleness checks the ticker-level freshness signal: a newest
// statement over a year old (or absent) is stale, a recent one is not.
func TestGetFinancialsStaleness(t *testing.T) {
	t.Run("a 2019 statement is stale", func(t *testing.T) {
		rows := []entity.FinancialStatement{
			finStatement("OLD", "2019-12-31", 12),
			finStatement("OLD", "2019-09-30", 9),
		}
		uc := newFinancialsTestUseCase(rows, nil)
		resp, err := uc.GetFinancials(context.Background(), "OLD", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.PeriodStale {
			t.Error("data_stale = false, want true for a 2019 statement")
		}
		if resp.LatestPeriodEnd != "2019-12-31" {
			t.Errorf("latest_period_end = %q, want 2019-12-31", resp.LatestPeriodEnd)
		}
	})

	t.Run("a recent statement is fresh", func(t *testing.T) {
		uc := newFinancialsTestUseCase([]entity.FinancialStatement{finStatement("NEW", "2026-06-30", 6)}, nil)
		resp, err := uc.GetFinancials(context.Background(), "NEW", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.PeriodStale {
			t.Error("data_stale = true, want false for a statement inside the year")
		}
	})
}

// TestGetFinancialsValidation covers the argument contract.
func TestGetFinancialsValidation(t *testing.T) {
	t.Run("unknown period is rejected before any read", func(t *testing.T) {
		statements := &fakeFinancialStatements{}
		uc := &FinancialsUseCase{Log: logrus.New(), Repo: statements, Prices: &fakeFinancialPrices{}}
		_, err := uc.GetFinancials(context.Background(), "TLKM", "monthly")
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
		if statements.calls != 0 {
			t.Error("repository read despite invalid period")
		}
	})

	t.Run("invalid ticker is rejected", func(t *testing.T) {
		uc := newFinancialsTestUseCase(nil, nil)
		if _, err := uc.GetFinancials(context.Background(), "TLKM.JK!", ""); !errors.Is(err, ErrInvalidTicker) {
			t.Errorf("err = %v, want ErrInvalidTicker", err)
		}
	})

	t.Run("repository errors surface", func(t *testing.T) {
		uc := &FinancialsUseCase{
			Log:    logrus.New(),
			Repo:   &fakeFinancialStatements{err: errors.New("boom")},
			Prices: &fakeFinancialPrices{},
		}
		if _, err := uc.GetFinancials(context.Background(), "TLKM", ""); err == nil {
			t.Fatal("err = nil, want the repository failure")
		}
	})

	t.Run("price errors surface", func(t *testing.T) {
		uc := &FinancialsUseCase{
			Log:    logrus.New(),
			Repo:   &fakeFinancialStatements{rows: []entity.FinancialStatement{finStatement("TLKM", "2026-09-30", 9)}},
			Prices: &fakeFinancialPrices{err: errors.New("boom")},
		}
		if _, err := uc.GetFinancials(context.Background(), "TLKM", ""); err == nil {
			t.Fatal("err = nil, want the price failure")
		}
	})
}

// TestSortStatementsNewestFirst keeps the view ordering independent of the
// repository's ORDER BY.
func TestSortStatementsNewestFirst(t *testing.T) {
	rows := []entity.FinancialStatement{
		finStatement("A", "2024-03-31", 3),
		finStatement("A", "2026-09-30", 9),
		finStatement("A", "2025-12-31", 12),
	}
	sortStatementsNewestFirst(rows)
	want := []string{"2026-09-30", "2025-12-31", "2024-03-31"}
	for i, w := range want {
		if got := rows[i].PeriodEnd.Format("2006-01-02"); got != w {
			t.Errorf("rows[%d] = %s, want %s", i, got, w)
		}
	}
}
