package financials

import "time"

// Taxonomy identifies the Yahoo/Morningstar statement template an issuer is
// served. The template is a property of the issuer as Morningstar classifies
// it, not of the IDX sector: KREN (Technology) is bank-shaped and SMMA (a
// financial holding) is insurance-shaped.
//
// The template decides which line items exist, which is why it is part of the
// data model rather than an adapter implementation detail: an absent gross
// profit for a bank is not_applicable, while an absent gross profit on the
// general template is unknown (BREN's omitted line).
type Taxonomy string

const (
	// TaxonomyGeneral154 is the general-industry template (154 income types).
	TaxonomyGeneral154 Taxonomy = "general_154"
	// TaxonomyBank108 is the bank template (108 income types).
	TaxonomyBank108 Taxonomy = "bank_108"
	// TaxonomyInsurance74 is the insurance template (74 income types).
	TaxonomyInsurance74 Taxonomy = "insurance_74"
	// TaxonomyOther is the catch-all for a ticker whose probe resolves to none
	// of the three known shapes. It is not a synonym for general.
	TaxonomyOther Taxonomy = "other"
)

// Size returns the template's income type-list length. It is 0 for other,
// because "other" carries no discovered list.
func (t Taxonomy) Size() int {
	switch t {
	case TaxonomyGeneral154:
		return 154
	case TaxonomyBank108:
		return 108
	case TaxonomyInsurance74:
		return 74
	}
	return 0
}

// Valid reports whether t is one of the four stored values.
func (t Taxonomy) Valid() bool {
	switch t {
	case TaxonomyGeneral154, TaxonomyBank108, TaxonomyInsurance74, TaxonomyOther:
		return true
	}
	return false
}

// Concept is a normalized statement line item. The string values are part of
// the Tier-2 contract (they will be persisted as depth column names), so they
// are stable and snake_case.
type Concept string

const (
	ConceptRevenue                  Concept = "revenue"
	ConceptCostOfRevenue            Concept = "cost_of_revenue"
	ConceptGrossProfit              Concept = "gross_profit"
	ConceptSGNA                     Concept = "sga"
	ConceptDepreciationAmortization Concept = "depreciation_amortization"
	ConceptAccretionARO             Concept = "accretion_aro"
	ConceptProvisionExpense         Concept = "provision_expense"
	ConceptOperatingIncomeExpenses  Concept = "operating_income_expenses"
	ConceptTotalOperatingProfit     Concept = "total_operating_profit"
	ConceptNetFinanceIncome         Concept = "net_finance_income"
	ConceptNetInvestmentIncome      Concept = "net_investment_income"
	ConceptNonOperatingTotal        Concept = "non_operating_total"
	ConceptPretaxIncome             Concept = "pretax_income"
	ConceptIncomeTax                Concept = "income_tax"
	ConceptNetIncomeContinuing      Concept = "net_income_continuing"
	ConceptNetIncomeAfterNCI        Concept = "net_income_after_nci"
	ConceptNCI                      Concept = "nci"
	ConceptNetIncomeCommon          Concept = "net_income_common"
	ConceptEPSBasic                 Concept = "eps_basic"
	ConceptEPSDiluted               Concept = "eps_diluted"
	// WASO is stored in full shares. Yahoo's page displays thousands of shares
	// (PTBA 11,520,659.25k = 11,520,659,250 full shares), so the raw code values
	// are already full units and must not be multiplied again.
	ConceptWASOBasic      Concept = "waso_basic"
	ConceptWASODiluted    Concept = "waso_diluted"
	ConceptEBIT           Concept = "ebit"
	ConceptEBITDA         Concept = "ebitda"
	ConceptCash           Concept = "cash"
	ConceptAssets         Concept = "assets"
	ConceptLiabilities    Concept = "liabilities"
	ConceptEquity         Concept = "equity"
	ConceptCommonEquity   Concept = "common_equity"
	ConceptTotalDebt      Concept = "total_debt"
	ConceptNetDebt        Concept = "net_debt"
	ConceptCFO            Concept = "cfo"
	ConceptCFI            Concept = "cfi"
	ConceptCFF            Concept = "cff"
	ConceptCapEx          Concept = "capex"
	ConceptFCF            Concept = "fcf"
	ConceptEndCash        Concept = "end_cash"
	ConceptInterestIncome Concept = "interest_income"
)

// CoreConcepts is the resolved concept set ticket 04 requires. net_income_common
// and the EPS/WASO and EBIT/EBITDA rows are page-computed; the adapter derives
// them when the direct combined-preload code is absent.
var CoreConcepts = []Concept{
	ConceptRevenue,
	ConceptCostOfRevenue,
	ConceptGrossProfit,
	ConceptSGNA,
	ConceptDepreciationAmortization,
	ConceptAccretionARO,
	ConceptProvisionExpense,
	ConceptOperatingIncomeExpenses,
	ConceptTotalOperatingProfit,
	ConceptNetFinanceIncome,
	ConceptNetInvestmentIncome,
	ConceptNonOperatingTotal,
	ConceptPretaxIncome,
	ConceptIncomeTax,
	ConceptNetIncomeContinuing,
	ConceptNetIncomeAfterNCI,
	ConceptNCI,
	ConceptNetIncomeCommon,
	ConceptEPSBasic,
	ConceptEPSDiluted,
	ConceptWASOBasic,
	ConceptWASODiluted,
	ConceptEBIT,
	ConceptEBITDA,
	ConceptCash,
	ConceptAssets,
	ConceptLiabilities,
	ConceptEquity,
	ConceptCommonEquity,
	ConceptTotalDebt,
	ConceptNetDebt,
	ConceptCFO,
	ConceptCFI,
	ConceptCFF,
	ConceptCapEx,
	ConceptFCF,
	ConceptEndCash,
	ConceptInterestIncome,
}

// CoverageState is the tri-state a depth field carries. It is deliberately
// distinct from a nil amount: a bank's absent gross profit is not_applicable,
// a general-list issuer's omitted line is unknown, and neither is zero.
type CoverageState string

const (
	CoverageObserved      CoverageState = "observed"
	CoverageUnknown       CoverageState = "unknown"
	CoverageNotApplicable CoverageState = "not_applicable"
)

// ValueSource records where a value came from. restatedValue is the normal
// Morningstar case; reportedValue appears for a minority of facts; derived
// means the adapter computed the row (the page-computed aggregates).
type ValueSource string

const (
	ValueRestated ValueSource = "restated"
	ValueReported ValueSource = "reported"
	ValueDerived  ValueSource = "derived"
)

// Provenance constants stamped on every Tier-2 statement.
const (
	// MethodologyMorningstar is the post-2026-11-09 dataset. The legacy
	// human-readable types on the same endpoint carry the old methodology and
	// different EBIT/EBITDA values; never mix the two.
	MethodologyMorningstar = "morningstar_post_1109"

	// SignConventionExpensesNegative records that Yahoo/Morningstar reports
	// expenses (cost of revenue, SG&A, D&A, tax) as negative numbers. The
	// adapter preserves the sign rather than smoothing it away, so arithmetic
	// follows the reported convention.
	SignConventionExpensesNegative = "expenses_negative"
)

// Period types accepted by the endpoint.
const (
	PeriodAnnual    = "12M"
	PeriodQuarterly = "3M"
	PeriodTTM       = "TTM"
)

// Value is one observed or derived concept value. Amount is in the issuer's
// reporting currency, in full units (not thousands or millions), with the
// source sign convention. A nil Amount means the value was not observed; the
// Coverage map is the authority on why.
type Value struct {
	Amount     *float64
	Currency   string
	Code       string // source code; empty for a derived value
	Source     ValueSource
	Formula    string // human-readable formula for derived values
	AsOfDate   time.Time
	PeriodType string
}

// AliasConflict records two codes in the same alias group that returned
// different values for one period. The primary code wins; the conflict is
// surfaced so the mapping can be corrected instead of silently collapsed.
type AliasConflict struct {
	Concept   Concept
	Code      string
	Amount    float64
	OtherCode string
	Other     float64
}

// DepthStatement is one issuer period after concept resolution, alias
// selection, derived-row computation and tri-state coverage. Tier 2 enrichment
// consumes this shape; it never sees a raw Yahoo code.
type DepthStatement struct {
	Ticker         string
	Taxonomy       Taxonomy
	PeriodType     string
	PeriodEnd      time.Time
	Currency       string
	Values         map[Concept]Value
	Coverage       map[Concept]CoverageState
	AliasConflicts []AliasConflict
	// CurrencyConflict reports that facts in this period disagreed on the
	// currency. The majority/first currency is on Currency; each Value keeps
	// its own Currency.
	CurrencyConflict bool
	Methodology      string
	SignConvention   string
}

// Amount returns a concept's amount and whether it was observed.
func (s DepthStatement) Amount(c Concept) (*float64, bool) {
	v, ok := s.Values[c]
	if !ok || v.Amount == nil {
		return nil, false
	}
	return v.Amount, true
}

// CoverageOf returns the concept's coverage state; a concept absent from the
// map is unknown, never observed.
func (s DepthStatement) CoverageOf(c Concept) CoverageState {
	if state, ok := s.Coverage[c]; ok {
		return state
	}
	return CoverageUnknown
}
