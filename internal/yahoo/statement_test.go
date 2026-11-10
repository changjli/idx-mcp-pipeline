package yahoo

import (
	"strings"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

func TestBuildStatements_PTBA_General(t *testing.T) {
	stmts := loadFixtureStatements(t, "ptba")
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")

	if stmt.Taxonomy != financials.TaxonomyGeneral154 {
		t.Fatalf("taxonomy = %s, want general_154", stmt.Taxonomy)
	}
	if stmt.Ticker != "PTBA" {
		t.Errorf("ticker = %q, want PTBA", stmt.Ticker)
	}
	if stmt.Currency != "IDR" {
		t.Errorf("currency = %q, want IDR", stmt.Currency)
	}
	if stmt.Methodology != financials.MethodologyMorningstar {
		t.Errorf("methodology = %q", stmt.Methodology)
	}
	if stmt.SignConvention != financials.SignConventionExpensesNegative {
		t.Errorf("sign convention = %q", stmt.SignConvention)
	}

	assertAmount(t, stmt, financials.ConceptRevenue, 42651724000000)
	assertAmount(t, stmt, financials.ConceptCostOfRevenue, -36394624000000)
	assertAmount(t, stmt, financials.ConceptGrossProfit, 6257100000000)
	assertAmount(t, stmt, financials.ConceptSGNA, -2852536000000)
	assertAmount(t, stmt, financials.ConceptDepreciationAmortization, -300136000000)
	assertAmount(t, stmt, financials.ConceptPretaxIncome, 3773209000000)
	assertAmount(t, stmt, financials.ConceptIncomeTax, -813973000000)
	assertAmount(t, stmt, financials.ConceptNetIncomeContinuing, 2959236000000)
	assertAmount(t, stmt, financials.ConceptNCI, -29279000000)
	assertAmount(t, stmt, financials.ConceptNetIncomeAfterNCI, 2929957000000)
	assertAmount(t, stmt, financials.ConceptEBIT, 3776358000000)
	assertAmount(t, stmt, financials.ConceptEBITDA, 5787227000000)
	assertAmount(t, stmt, financials.ConceptCFO, 6260828000000)
	assertAmount(t, stmt, financials.ConceptCapEx, 3505367000000)
	assertAmount(t, stmt, financials.ConceptFCF, 2930080835676)
	assertAmount(t, stmt, financials.ConceptCash, 4522194000000)
	assertAmount(t, stmt, financials.ConceptAssets, 43917063000000)
	assertAmount(t, stmt, financials.ConceptLiabilities, 21300262000000)
	assertAmount(t, stmt, financials.ConceptEquity, 22616801000000)
	assertAmount(t, stmt, financials.ConceptCommonEquity, 22483714000000)
	assertAmount(t, stmt, financials.ConceptTotalDebt, 3857183000000)

	// EPS is computed from NI / WASO, not taken from the rounded source code.
	wantEPS := 2929957000000.0 / 11520659250.0
	assertAmount(t, stmt, financials.ConceptEPSBasic, wantEPS)
	assertAmount(t, stmt, financials.ConceptEPSDiluted, wantEPS)
	eps := stmt.Values[financials.ConceptEPSBasic]
	if eps.Source != financials.ValueDerived || !strings.Contains(eps.Formula, "waso_basic") {
		t.Errorf("EPS provenance = %+v, want derived via waso_basic", eps)
	}

	// The expense sign is preserved, not normalised away.
	if v := amount(t, stmt, financials.ConceptCostOfRevenue); v >= 0 {
		t.Errorf("cost of revenue = %v, want negative", v)
	}
	if v := amount(t, stmt, financials.ConceptIncomeTax); v >= 0 {
		t.Errorf("income tax = %v, want negative expense", v)
	}

	// PTBA is general-industry but does not report interest income; that is
	// unknown, not not_applicable.
	assertCoverage(t, stmt, financials.ConceptInterestIncome, financials.CoverageUnknown)
	if len(stmt.AliasConflicts) != 0 {
		t.Errorf("alias conflicts = %+v, want none", stmt.AliasConflicts)
	}
	for concept, value := range stmt.Values {
		if value.Currency != "IDR" {
			t.Errorf("%s currency = %q, want IDR", concept, value.Currency)
		}
	}
}

func TestBuildStatements_BBRI_Bank(t *testing.T) {
	stmts := loadFixtureStatements(t, "bbri")
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")

	if stmt.Taxonomy != financials.TaxonomyBank108 {
		t.Fatalf("taxonomy = %s, want bank_108", stmt.Taxonomy)
	}

	// Gross profit and cost of revenue are structurally absent from the bank
	// template: not_applicable, not unknown and not zero.
	assertCoverage(t, stmt, financials.ConceptGrossProfit, financials.CoverageNotApplicable)
	assertCoverage(t, stmt, financials.ConceptCostOfRevenue, financials.CoverageNotApplicable)
	if _, ok := stmt.Amount(financials.ConceptGrossProfit); ok {
		t.Error("bank gross profit should not carry a value")
	}

	assertAmount(t, stmt, financials.ConceptRevenue, 186876558000000)
	assertAmount(t, stmt, financials.ConceptAssets, 2135371105000000)
	assertAmount(t, stmt, financials.ConceptEquity, 330941434000000)
	assertAmount(t, stmt, financials.ConceptCash, 122736347000000)
	assertAmount(t, stmt, financials.ConceptTotalDebt, 1709675564000000)
	assertAmount(t, stmt, financials.ConceptInterestIncome, 192859238000000)
	assertAmount(t, stmt, financials.ConceptCFO, 14617023000000)
	assertAmount(t, stmt, financials.ConceptCapEx, 6344276000000)

	// Yahoo has no direct FCF for this bank; the fallback is CFO - CapEx and
	// carries its own formula tag.
	assertAmount(t, stmt, financials.ConceptFCF, 14617023000000-6344276000000)
	fcf := stmt.Values[financials.ConceptFCF]
	if fcf.Source != financials.ValueDerived || fcf.Formula != "cfo - abs(capex)" {
		t.Errorf("FCF provenance = %+v, want derived cfo-abs(capex)", fcf)
	}

	wantEPS := 56652384000000.0 / 150602160685.0
	assertAmount(t, stmt, financials.ConceptEPSBasic, wantEPS)

	// EBIT/EBITDA are absent from the bank fixture, and there is no net-finance
	// component to derive EBIT from: unknown, not fabricated.
	assertCoverage(t, stmt, financials.ConceptEBIT, financials.CoverageUnknown)
	assertCoverage(t, stmt, financials.ConceptEBITDA, financials.CoverageUnknown)
}

func TestBuildStatements_ABDA_Insurance(t *testing.T) {
	stmts := loadFixtureStatements(t, "abda")
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")

	if stmt.Taxonomy != financials.TaxonomyInsurance74 {
		t.Fatalf("taxonomy = %s, want insurance_74", stmt.Taxonomy)
	}
	assertCoverage(t, stmt, financials.ConceptGrossProfit, financials.CoverageNotApplicable)
	assertCoverage(t, stmt, financials.ConceptCostOfRevenue, financials.CoverageNotApplicable)

	assertAmount(t, stmt, financials.ConceptRevenue, 993213672000)
	assertAmount(t, stmt, financials.ConceptPretaxIncome, 93608818000)
	// A positive tax under the negative-expense convention is a tax benefit.
	assertAmount(t, stmt, financials.ConceptIncomeTax, 6165464000)
	assertAmount(t, stmt, financials.ConceptNetIncomeContinuing, 99774282000)
	assertAmount(t, stmt, financials.ConceptNCI, -2117000)
	assertAmount(t, stmt, financials.ConceptNetIncomeAfterNCI, 99772165000)
	assertAmount(t, stmt, financials.ConceptEBIT, 93608818000)
	assertAmount(t, stmt, financials.ConceptEBITDA, 98407405000)
	assertAmount(t, stmt, financials.ConceptFCF, 1684316000)

	// A reported zero is observed, not missing.
	assertAmount(t, stmt, financials.ConceptTotalDebt, 0)

	wantEPS := 99772165000.0 / 620806680.0
	assertAmount(t, stmt, financials.ConceptEPSBasic, wantEPS)
}

func TestBuildStatements_AADI_USD(t *testing.T) {
	stmts := loadFixtureStatements(t, "aadi")
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")

	if stmt.Taxonomy != financials.TaxonomyGeneral154 {
		t.Fatalf("taxonomy = %s, want general_154", stmt.Taxonomy)
	}
	if stmt.Currency != "USD" {
		t.Fatalf("currency = %q, want USD", stmt.Currency)
	}
	assertAmount(t, stmt, financials.ConceptRevenue, 4910263000)
	assertAmount(t, stmt, financials.ConceptGrossProfit, 1261695000)
	assertAmount(t, stmt, financials.ConceptPretaxIncome, 1055346000)
	assertAmount(t, stmt, financials.ConceptIncomeTax, -206162000)
	assertAmount(t, stmt, financials.ConceptEBIT, 1053128000)
	assertAmount(t, stmt, financials.ConceptEBITDA, 1177056000)
	assertAmount(t, stmt, financials.ConceptFCF, 576056405.25)
	assertAmount(t, stmt, financials.ConceptTotalDebt, 826140000)

	wantEPS := 760180000.0 / 7786892000.0
	assertAmount(t, stmt, financials.ConceptEPSBasic, wantEPS)

	for concept, value := range stmt.Values {
		if value.Currency != "USD" {
			t.Errorf("%s currency = %q, want USD", concept, value.Currency)
		}
	}
}

func TestBuildStatements_QuarterlyAndTTM(t *testing.T) {
	stmts := loadFixtureStatements(t, "ptba")

	q := findStatement(t, stmts, financials.PeriodQuarterly, "2026-09-30")
	assertAmount(t, q, financials.ConceptRevenue, 13173484000000)
	assertAmount(t, q, financials.ConceptGrossProfit, 3019057000000)
	assertAmount(t, q, financials.ConceptEBITDA, 2833591000000)

	ttm := findStatement(t, stmts, financials.PeriodTTM, "2026-09-30")
	assertAmount(t, ttm, financials.ConceptRevenue, 46522813000000)
	assertAmount(t, ttm, financials.ConceptEBITDA, 9401089000000)

	if q.PeriodType != financials.PeriodQuarterly || ttm.PeriodType != financials.PeriodTTM {
		t.Errorf("period types = %q/%q", q.PeriodType, ttm.PeriodType)
	}
}

func TestBuildStatements_UnknownVersusNotApplicable(t *testing.T) {
	// General template with no gross-profit fact: unknown, because the concept
	// exists but the issuer did not report it.
	general := BuildStatements("BREN", financials.TaxonomyGeneral154, []Fact{{
		Code: "EQT07", Type: "EQT07_12M", PeriodType: financials.PeriodAnnual,
		AsOfDate: mustDate("2025-12-31"), Currency: "IDR", Value: floatPtr(100), Source: financials.ValueRestated,
	}})
	stmt := findStatement(t, general, financials.PeriodAnnual, "2025-12-31")
	assertCoverage(t, stmt, financials.ConceptGrossProfit, financials.CoverageUnknown)

	// The same absence on a bank is not_applicable.
	bank := BuildStatements("BBRI", financials.TaxonomyBank108, []Fact{{
		Code: "EQT07", Type: "EQT07_12M", PeriodType: financials.PeriodAnnual,
		AsOfDate: mustDate("2025-12-31"), Currency: "IDR", Value: floatPtr(100), Source: financials.ValueRestated,
	}})
	stmt = findStatement(t, bank, financials.PeriodAnnual, "2025-12-31")
	assertCoverage(t, stmt, financials.ConceptGrossProfit, financials.CoverageNotApplicable)
}

func TestBuildStatements_AliasConflict(t *testing.T) {
	facts := []Fact{
		{Code: "EQQR7", Type: "EQQR7_12M", PeriodType: financials.PeriodAnnual, AsOfDate: mustDate("2025-12-31"), Currency: "IDR", Value: floatPtr(-300), Source: financials.ValueRestated},
		{Code: "EQTFV", Type: "EQTFV_12M", PeriodType: financials.PeriodAnnual, AsOfDate: mustDate("2025-12-31"), Currency: "IDR", Value: floatPtr(-300), Source: financials.ValueRestated},
		{Code: "EQK97", Type: "EQK97_12M", PeriodType: financials.PeriodAnnual, AsOfDate: mustDate("2025-12-31"), Currency: "IDR", Value: floatPtr(-250), Source: financials.ValueRestated},
	}
	stmts := BuildStatements("TEST", financials.TaxonomyGeneral154, facts)
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")
	assertAmount(t, stmt, financials.ConceptDepreciationAmortization, -300)
	if len(stmt.AliasConflicts) != 1 || stmt.AliasConflicts[0].OtherCode != "EQK97" {
		t.Errorf("alias conflicts = %+v, want one EQK97 conflict", stmt.AliasConflicts)
	}
}

func TestBuildStatements_CurrencyConflict(t *testing.T) {
	facts := []Fact{
		{Code: "EQT07", Type: "EQT07_12M", PeriodType: financials.PeriodAnnual, AsOfDate: mustDate("2025-12-31"), Currency: "IDR", Value: floatPtr(100), Source: financials.ValueRestated},
		{Code: "EQ46Q", Type: "EQ46Q_12M", PeriodType: financials.PeriodAnnual, AsOfDate: mustDate("2025-12-31"), Currency: "USD", Value: floatPtr(10), Source: financials.ValueRestated},
	}
	stmts := BuildStatements("TEST", financials.TaxonomyGeneral154, facts)
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")
	if !stmt.CurrencyConflict {
		t.Error("currency conflict not recorded")
	}
	if stmt.Currency != "IDR" {
		t.Errorf("statement currency = %q, want first seen IDR", stmt.Currency)
	}
}

func TestBuildStatements_DerivedPretaxAndNCI(t *testing.T) {
	// The formulas are the fallback when the direct combined-preload code is
	// absent: pretax = EQIEA + EQAW7, NI = pretax + tax, NCI = NI after NCI -
	// NI continuing.
	facts := []Fact{
		fact("EQIEA", 1000), fact("EQAW7", 200), fact("EQYDI", -300),
		fact("EQVHZ", 900), fact("EQEO0", 880), fact("EQL8B", -20),
	}
	stmts := BuildStatements("TEST", financials.TaxonomyGeneral154, facts)
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")

	// Pretax is not in the input, so it is derived.
	assertAmount(t, stmt, financials.ConceptPretaxIncome, 1200)
	if got := stmt.Values[financials.ConceptPretaxIncome]; got.Source != financials.ValueDerived {
		t.Errorf("pretax source = %q", got.Source)
	}
	assertAmount(t, stmt, financials.ConceptNetIncomeContinuing, 900)
	// NCI is not in the input; derived as NI after NCI - NI continuing.
	assertAmount(t, stmt, financials.ConceptNCI, -20)
	assertAmount(t, stmt, financials.ConceptNetIncomeAfterNCI, 880)
	assertAmount(t, stmt, financials.ConceptNetIncomeCommon, 880)
}

func TestBuildStatements_EBITAndFCFDerivations(t *testing.T) {
	facts := []Fact{
		fact("EQHE5", 1100), fact("EQSX8", -100),
		fact("EQ6JP", 600), fact("EQUZ0", 100),
	}
	stmts := BuildStatements("TEST", financials.TaxonomyGeneral154, facts)
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")

	// EBIT = pretax - net finance = 1100 - (-100) = 1200.
	assertAmount(t, stmt, financials.ConceptEBIT, 1200)
	if got := stmt.Values[financials.ConceptEBIT]; got.Source != financials.ValueDerived {
		t.Errorf("EBIT source = %q", got.Source)
	}
	// FCF = CFO - |CapEx| = 500.
	assertAmount(t, stmt, financials.ConceptFCF, 500)
	if got := stmt.Values[financials.ConceptFCF]; !strings.Contains(got.Formula, "cfo - abs(capex)") {
		t.Errorf("FCF formula = %q", got.Formula)
	}
}

func TestBuildStatements_EBITDADirectWinsOverFormula(t *testing.T) {
	facts := []Fact{
		fact("EQHE5", 1000), fact("EQSX8", 0), fact("EQQR7", -100),
		fact("EQHEI", 1300),
	}
	stmts := BuildStatements("TEST", financials.TaxonomyGeneral154, facts)
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")
	assertAmount(t, stmt, financials.ConceptEBITDA, 1300)
	if got := stmt.Values[financials.ConceptEBITDA]; got.Source == financials.ValueDerived {
		t.Errorf("direct EBITDA was overridden: %+v", got)
	}
}

func fact(code string, value float64) Fact {
	return Fact{
		Code:       code,
		Type:       code + "_12M",
		PeriodType: financials.PeriodAnnual,
		AsOfDate:   mustDate("2025-12-31"),
		Currency:   "IDR",
		Value:      floatPtr(value),
		Source:     financials.ValueRestated,
	}
}

func TestBuildStatements_ZeroAmountIsObserved(t *testing.T) {
	stmts := BuildStatements("TEST", financials.TaxonomyGeneral154, []Fact{fact("EQRIO", 0)})
	stmt := findStatement(t, stmts, financials.PeriodAnnual, "2025-12-31")
	assertAmount(t, stmt, financials.ConceptTotalDebt, 0)
	if got := stmt.CoverageOf(financials.ConceptTotalDebt); got != financials.CoverageObserved {
		t.Errorf("coverage = %q", got)
	}
}

func TestSameAmount(t *testing.T) {
	if !sameAmount(1000000000000, 1000000000000) {
		t.Error("identical amounts should match")
	}
	if sameAmount(1000000000000, 1100000000000) {
		t.Error("materially different amounts should not match")
	}
	if !sameAmount(0, 0) {
		t.Error("zeros should match")
	}
}
