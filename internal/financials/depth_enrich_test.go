package financials

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func depthDate(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func depthAmounts(values map[Concept]float64) map[Concept]Value {
	out := make(map[Concept]Value, len(values))
	for c, amount := range values {
		v := amount
		out[c] = Value{
			Amount:     &v,
			Currency:   "IDR",
			Code:       "TEST",
			Source:     ValueRestated,
			PeriodType: PeriodAnnual,
			AsOfDate:   depthDate("2026-12-31"),
		}
	}
	return out
}

func depthStatement(taxonomy Taxonomy, periodType, periodEnd string, values map[Concept]float64) DepthStatement {
	return DepthStatement{
		Ticker:         "TEST",
		Taxonomy:       taxonomy,
		PeriodType:     periodType,
		PeriodEnd:      depthDate(periodEnd),
		Currency:       "IDR",
		Values:         depthAmounts(values),
		Coverage:       map[Concept]CoverageState{},
		Methodology:    MethodologyMorningstar,
		SignConvention: SignConventionExpensesNegative,
	}
}

func depthTierOne(periodEnd string, duration int, values map[string]float64) TierOneDepthPeriod {
	fye := depthDate(periodEnd[:4] + "-12-31")
	return TierOneDepthPeriod{
		PeriodEnd:       depthDate(periodEnd),
		FiscalYearEnd:   fye,
		DurationMonths:  duration,
		Revenue:         floatOrNil(values["revenue"]),
		NetProfitParent: floatOrNil(values["net_profit"]),
		Assets:          floatOrNil(values["assets"]),
		Liabilities:     floatOrNil(values["liabilities"]),
		Equity:          floatOrNil(values["equity"]),
	}
}

func floatOrNil(v float64) *float64 {
	if v == 0 {
		return nil
	}
	return &v
}

func almostEqual(a, b float64) bool {
	scale := math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	return math.Abs(a-b) <= scale*1e-9
}

func findDepthRow(rows []DepthRow, period string) *DepthRow {
	for i := range rows {
		if rows[i].PeriodEnd.Format("2006-01-02") == period {
			return &rows[i]
		}
	}
	return nil
}

// TestBuildDepthRows_AnnualEnrichesOnlyExisting12M pins the safe annual path:
// the 12M statement fills the matching 12M row's depth columns, with the
// original values kept for audit and the row basis labelled annual.
func TestBuildDepthRows_AnnualEnrichesOnlyExisting12M(t *testing.T) {
	statements := []DepthStatement{
		depthStatement(TaxonomyGeneral154, PeriodAnnual, "2025-12-31", map[Concept]float64{
			ConceptCash:                 100,
			ConceptGrossProfit:          200,
			ConceptTotalOperatingProfit: 150,
			ConceptEBITDA:               175,
			ConceptCFO:                  180,
			ConceptCapEx:                40,
			ConceptFCF:                  140,
			ConceptTotalDebt:            300,
		}),
	}
	tier1 := []TierOneDepthPeriod{
		depthTierOne("2025-12-31", 12, map[string]float64{
			"revenue": 1000, "net_profit": 90, "assets": 2000, "liabilities": 800, "equity": 1200,
		}),
	}

	rows := BuildDepthRows("TEST", TaxonomyGeneral154, statements, tier1, DefaultUSDIDRRate, depthDate("2026-11-01"))
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.PeriodBasis != PeriodBasisAnnual12M {
		t.Errorf("period_basis = %q, want %q", row.PeriodBasis, PeriodBasisAnnual12M)
	}
	if row.Values[DepthGrossProfit] == nil || *row.Values[DepthGrossProfit] != 200 {
		t.Errorf("gross_profit = %v, want 200", row.Values[DepthGrossProfit])
	}
	if row.Values[DepthTotalDebt] == nil || *row.Values[DepthTotalDebt] != 300 {
		t.Errorf("total_debt = %v, want 300", row.Values[DepthTotalDebt])
	}
	if row.Coverage[string(DepthCash)] != CoverageObserved {
		t.Errorf("cash coverage = %q, want observed", row.Coverage[string(DepthCash)])
	}
	if row.Coverage[string(DepthInterestExpense)] != CoverageUnknown {
		t.Errorf("interest_expense coverage = %q, want unknown (no concept mapped)", row.Coverage[string(DepthInterestExpense)])
	}
	if row.FXRate != 1 || row.Currency != "IDR" {
		t.Errorf("fx = %v %s, want 1 IDR", row.FXRate, row.Currency)
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(row.SnapshotJSON), &raw); err != nil {
		t.Fatalf("snapshot JSON: %v", err)
	}
	if raw["ticker"] != "TEST" || raw["period_basis"] != PeriodBasisAnnual12M {
		t.Errorf("snapshot = %v", raw)
	}
}

// TestBuildDepthRows_QuarterlyCumulativeYTD pins the discrete→cumulative rule:
// 3M values are summed into 3M/6M/9M rows, the discrete quarter is retained in
// *_q, and a missing quarter never produces a partial cumulative value.
func TestBuildDepthRows_QuarterlyCumulativeYTD(t *testing.T) {
	q1 := depthStatement(TaxonomyGeneral154, PeriodQuarterly, "2026-03-31", map[Concept]float64{
		ConceptRevenue: 100, ConceptGrossProfit: 20, ConceptCFO: 30, ConceptCapEx: 5,
		ConceptFCF: 25, ConceptCash: 50, ConceptTotalDebt: 60, ConceptTotalOperatingProfit: 15,
	})
	q2 := depthStatement(TaxonomyGeneral154, PeriodQuarterly, "2026-06-30", map[Concept]float64{
		ConceptRevenue: 110, ConceptGrossProfit: 22, ConceptCFO: 35, ConceptCapEx: 6,
		ConceptFCF: 29, ConceptCash: 55, ConceptTotalDebt: 65, ConceptTotalOperatingProfit: 18,
	})
	q3 := depthStatement(TaxonomyGeneral154, PeriodQuarterly, "2026-09-30", map[Concept]float64{
		ConceptRevenue: 120, ConceptGrossProfit: 24, ConceptCFO: 40, ConceptCapEx: 7,
		ConceptFCF: 33, ConceptCash: 60, ConceptTotalDebt: 70, ConceptTotalOperatingProfit: 21,
	})
	tier1 := []TierOneDepthPeriod{
		depthTierOne("2026-03-31", 3, map[string]float64{"revenue": 100, "assets": 1000, "liabilities": 400, "equity": 600}),
		depthTierOne("2026-06-30", 6, map[string]float64{"revenue": 210, "assets": 1100, "liabilities": 450, "equity": 650}),
		depthTierOne("2026-09-30", 9, map[string]float64{"revenue": 330, "assets": 1200, "liabilities": 500, "equity": 700}),
	}

	rows := BuildDepthRows("TEST", TaxonomyGeneral154, []DepthStatement{q1, q2, q3}, tier1, DefaultUSDIDRRate, depthDate("2026-11-01"))
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	q3Row := findDepthRow(rows, "2026-09-30")
	if q3Row == nil {
		t.Fatal("missing Q3 row")
	}
	// FCF is not a direct concept in the fixture, so it stays nil; revenue is
	// not a depth field. Use gross profit and CFO for the sum assertion.
	if got := q3Row.Values[DepthGrossProfit]; got == nil || !almostEqual(*got, 66) {
		t.Errorf("9M gross_profit = %v, want 66", got)
	}
	if got := q3Row.Values[DepthCFO]; got == nil || !almostEqual(*got, 105) {
		t.Errorf("9M cfo = %v, want 105", got)
	}
	if got := q3Row.Quarters[DepthGrossProfit]; got == nil || *got != 24 {
		t.Errorf("Q3 gross_profit_q = %v, want 24", got)
	}
	if q3Row.PeriodBasis != PeriodBasisCumulativeYTD {
		t.Errorf("period_basis = %q, want cumulative_ytd", q3Row.PeriodBasis)
	}

	// Drop Q2: the 6M and 9M cumulative values must be unknown, not a partial
	// sum, while the Q3 discrete value remains observed.
	rows = BuildDepthRows("TEST", TaxonomyGeneral154, []DepthStatement{q1, q3}, tier1, DefaultUSDIDRRate, depthDate("2026-11-01"))
	q3Row = findDepthRow(rows, "2026-09-30")
	if q3Row == nil {
		t.Fatal("missing Q3 row after dropping Q2")
	}
	if q3Row.Values[DepthGrossProfit] != nil {
		t.Errorf("9M gross_profit = %v, want nil when a quarter is missing", *q3Row.Values[DepthGrossProfit])
	}
	if q3Row.Coverage[string(DepthGrossProfit)] != CoverageUnknown {
		t.Errorf("9M gross_profit coverage = %q, want unknown", q3Row.Coverage[string(DepthGrossProfit)])
	}
	if q3Row.Quarters[DepthGrossProfit] == nil || *q3Row.Quarters[DepthGrossProfit] != 24 {
		t.Errorf("Q3 gross_profit_q = %v, want 24", q3Row.Quarters[DepthGrossProfit])
	}
}

// TestBuildDepthRows_TTMOnMatchingRow pins that TTM never becomes a duration
// row: it lands in *_ttm on the row whose period_end matches its asOfDate.
func TestBuildDepthRows_TTMOnMatchingRow(t *testing.T) {
	annual := depthStatement(TaxonomyGeneral154, PeriodAnnual, "2025-12-31", map[Concept]float64{
		ConceptGrossProfit: 200,
	})
	ttm := depthStatement(TaxonomyGeneral154, PeriodTTM, "2026-09-30", map[Concept]float64{
		ConceptGrossProfit: 230,
	})
	q1 := depthStatement(TaxonomyGeneral154, PeriodQuarterly, "2026-03-31", map[Concept]float64{ConceptCash: 10})
	q2 := depthStatement(TaxonomyGeneral154, PeriodQuarterly, "2026-06-30", map[Concept]float64{ConceptCash: 11})
	q3 := depthStatement(TaxonomyGeneral154, PeriodQuarterly, "2026-09-30", map[Concept]float64{ConceptCash: 12})
	tier1 := []TierOneDepthPeriod{
		depthTierOne("2025-12-31", 12, map[string]float64{"revenue": 1000, "assets": 2000, "liabilities": 800, "equity": 1200}),
		depthTierOne("2026-09-30", 9, map[string]float64{"revenue": 900, "assets": 2100, "liabilities": 900, "equity": 1200}),
	}
	rows := BuildDepthRows("TEST", TaxonomyGeneral154, []DepthStatement{annual, ttm, q1, q2, q3}, tier1, DefaultUSDIDRRate, depthDate("2026-11-01"))
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (annual and the existing 9M row; TTM is not a statement row)", len(rows))
	}
	row := findDepthRow(rows, "2026-09-30")
	if row == nil {
		t.Fatal("missing the 9M row")
	}
	if row.DurationMonths != 9 {
		t.Fatalf("row duration = %d, want the existing 9M row", row.DurationMonths)
	}
	if row.TTM[DepthGrossProfit] == nil || *row.TTM[DepthGrossProfit] != 230 {
		t.Errorf("gross_profit_ttm = %v, want 230", row.TTM[DepthGrossProfit])
	}
	if row.Values[DepthGrossProfit] != nil {
		t.Errorf("9M main gross_profit = %v, want nil (no quarterly data)", *row.Values[DepthGrossProfit])
	}
}

// TestBuildDepthRows_CurrencyConversionAndConflict pins the IDR contract: a
// USD statement is converted at the rate implied by the Tier-1 row's own IDR
// values, the original values remain in the raw audit document, and a
// divergence beyond tolerance is recorded without touching Tier 1.
func TestBuildDepthRows_CurrencyConversionAndConflict(t *testing.T) {
	annualValues := map[Concept]float64{
		ConceptRevenue:     100, // USD
		ConceptGrossProfit: 40,
		ConceptAssets:      500,
		ConceptEquity:      300,
		ConceptLiabilities: 200,
		ConceptCash:        50,
	}
	annual := depthStatement(TaxonomyGeneral154, PeriodAnnual, "2025-12-31", annualValues)
	annual.Currency = "USD"
	for c, v := range annual.Values {
		v.Currency = "USD"
		annual.Values[c] = v
	}
	// Tier-1 revenue 1,600,000 IDR => implied 16,000 IDR/USD. Equity and
	// liabilities imply the same rate, so the median is 16,000. The profit
	// check is intentionally omitted from the fixture.
	tier1 := []TierOneDepthPeriod{
		{
			PeriodEnd:       depthDate("2025-12-31"),
			FiscalYearEnd:   depthDate("2025-12-31"),
			DurationMonths:  12,
			Revenue:         floatPtrTest(1600000),
			Assets:          floatPtrTest(8000000),
			Liabilities:     floatPtrTest(3200000),
			Equity:          floatPtrTest(4800000),
			NetProfitParent: floatPtrTest(999999), // not comparable: no Yahoo NI
		},
	}
	rows := BuildDepthRows("TEST", TaxonomyGeneral154, []DepthStatement{annual}, tier1, DefaultUSDIDRRate, depthDate("2026-11-01"))
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	row := rows[0]
	if !almostEqual(row.FXRate, 16000) {
		t.Errorf("fx_rate = %v, want 16000", row.FXRate)
	}
	if row.Currency != "USD" {
		t.Errorf("currency = %q, want USD", row.Currency)
	}
	if got := row.Values[DepthGrossProfit]; got == nil || !almostEqual(*got, 640000) {
		t.Errorf("gross_profit IDR = %v, want 640000", got)
	}
	if got := row.Values[DepthCash]; got == nil || !almostEqual(*got, 800000) {
		t.Errorf("cash IDR = %v, want 800000", got)
	}
	if raw, ok := row.Raw[string(DepthGrossProfit)].(map[string]any); !ok || raw["currency"] != "USD" {
		t.Errorf("raw gross_profit = %v, want the original USD value", row.Raw[string(DepthGrossProfit)])
	}
	if len(row.Conflicts) != 0 {
		t.Errorf("conflicts = %+v, want none (Tier-1 values agree with the implied rate)", row.Conflicts)
	}

	// A real divergence is recorded and never overwrites the Tier-1 field.
	tier1[0].Revenue = floatPtrTest(1800000) // 12.5% away from 1,600,000
	rows = BuildDepthRows("TEST", TaxonomyGeneral154, []DepthStatement{annual}, tier1, DefaultUSDIDRRate, depthDate("2026-11-01"))
	if len(rows[0].Conflicts) != 1 || rows[0].Conflicts[0].Field != "revenue" {
		t.Fatalf("conflicts = %+v, want one revenue conflict", rows[0].Conflicts)
	}
}

// TestBuildDepthRows_BankGrossProfitNotApplicable pins the tri-state: a bank's
// absent gross profit is not_applicable, while a general-list omission stays
// unknown.
func TestBuildDepthRows_BankGrossProfitNotApplicable(t *testing.T) {
	bank := depthStatement(TaxonomyBank108, PeriodAnnual, "2025-12-31", map[Concept]float64{
		ConceptCash: 100,
	})
	general := depthStatement(TaxonomyGeneral154, PeriodAnnual, "2025-12-31", map[Concept]float64{
		ConceptCash: 100,
	})
	tier1 := []TierOneDepthPeriod{depthTierOne("2025-12-31", 12, map[string]float64{"revenue": 1000})}

	bankRows := BuildDepthRows("BANK", TaxonomyBank108, []DepthStatement{bank}, tier1, DefaultUSDIDRRate, depthDate("2026-11-01"))
	if got := bankRows[0].Coverage[string(DepthGrossProfit)]; got != CoverageNotApplicable {
		t.Errorf("bank gross_profit coverage = %q, want not_applicable", got)
	}
	generalRows := BuildDepthRows("GEN", TaxonomyGeneral154, []DepthStatement{general}, tier1, DefaultUSDIDRRate, depthDate("2026-11-01"))
	if got := generalRows[0].Coverage[string(DepthGrossProfit)]; got != CoverageUnknown {
		t.Errorf("general gross_profit coverage = %q, want unknown", got)
	}
}

func floatPtrTest(v float64) *float64 { return &v }
