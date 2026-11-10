package yahoo

import "github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"

// conceptSpec binds one normalized concept to its Morningstar data-ID codes.
// The first code is the primary; the rest are aliases. Alias groups are
// recorded here and checked per issuer at build time — the endpoint returns
// byte-identical series for the PTBA/BBRI/ABDA/AADI fixtures, but the mapping
// still surfaces a conflict instead of silently collapsing a divergent alias.
type conceptSpec struct {
	Concept financials.Concept
	Codes   []string
}

// conceptSpecs is the core concept map required by ticket 04.
//
// Codes are global (EQT07 is revenue everywhere); the taxonomy decides which
// exist. A concept absent from a bank/insurance template is marked
// not_applicable in applicable() below. The page-computed rows (EPS, WASO,
// EBIT, EBITDA, pretax, net income, NCI) also have direct codes in the combined
// 500-type preload; the adapter uses those when present and derives them from
// components when the direct code is missing.
var conceptSpecs = []conceptSpec{
	{financials.ConceptRevenue, []string{"EQT07"}},
	{financials.ConceptCostOfRevenue, []string{"EQ8Y4"}},
	{financials.ConceptGrossProfit, []string{"EQ46Q"}},
	{financials.ConceptSGNA, []string{"EQYWG"}},
	{financials.ConceptDepreciationAmortization, []string{"EQQR7", "EQTFV", "EQK97"}},
	{financials.ConceptAccretionARO, []string{"EQ7E3"}},
	{financials.ConceptProvisionExpense, []string{"EQ0YW", "EQG1P"}},
	{financials.ConceptOperatingIncomeExpenses, []string{"EQNIG"}},
	{financials.ConceptTotalOperatingProfit, []string{"EQIEA"}},
	{financials.ConceptNetFinanceIncome, []string{"EQSX8", "EQLTG"}},
	{financials.ConceptNetInvestmentIncome, []string{"EQYSV"}},
	{financials.ConceptNonOperatingTotal, []string{"EQAW7"}},
	{financials.ConceptPretaxIncome, []string{"EQHE5"}},
	{financials.ConceptIncomeTax, []string{"EQYDI"}},
	{financials.ConceptNetIncomeContinuing, []string{"EQVHZ", "EQGH5"}},
	{financials.ConceptNetIncomeAfterNCI, []string{"EQEO0", "EQUVT", "EQ7WE"}},
	{financials.ConceptNCI, []string{"EQL8B"}},
	{financials.ConceptNetIncomeCommon, nil}, // derived: net income after NCI
	{financials.ConceptEPSBasic, []string{"EQ4P8", "EQBQ4", "EQOD7", "EQ72T"}},
	{financials.ConceptEPSDiluted, []string{"EQ4P8", "EQBQ4", "EQOD7", "EQ72T"}},
	{financials.ConceptWASOBasic, []string{"EQAFC"}},
	{financials.ConceptWASODiluted, []string{"EQEZO"}},
	{financials.ConceptEBIT, []string{"EQQWX"}},
	{financials.ConceptEBITDA, []string{"EQHEI", "EQR8M"}},
	{financials.ConceptCash, []string{"EQWXW"}},
	{financials.ConceptAssets, []string{"EQYZU"}},
	{financials.ConceptLiabilities, []string{"EQRG7", "EQA1D"}},
	{financials.ConceptEquity, []string{"EQE13", "EQ4RH"}},
	{financials.ConceptCommonEquity, []string{"EQ1HC", "EQTCA"}},
	{financials.ConceptTotalDebt, []string{"EQRIO"}},
	{financials.ConceptNetDebt, []string{"EQV4N"}},
	{financials.ConceptCFO, []string{"EQ6JP", "EQ65B"}},
	{financials.ConceptCFI, []string{"EQBDY", "EQ1HW"}},
	{financials.ConceptCFF, []string{"EQPNK", "EQZNP"}},
	{financials.ConceptCapEx, []string{"EQUZ0"}},
	{financials.ConceptFCF, []string{"EQ58O"}},
	{financials.ConceptEndCash, []string{"EQU9Z"}},
	{financials.ConceptInterestIncome, []string{"EQDBM"}},
}

// codeToConcept is the reverse index used by BuildStatements.
var codeToConcept = func() map[string]financials.Concept {
	m := make(map[string]financials.Concept)
	for _, spec := range conceptSpecs {
		for _, code := range spec.Codes {
			if _, exists := m[code]; !exists {
				m[code] = spec.Concept
			}
		}
	}
	return m
}()

// ConceptCodes returns every data-ID the adapter must request, deduplicated.
// Derived-only concepts contribute no code.
func ConceptCodes() []string {
	codes := make([]string, 0, len(codeToConcept))
	seen := make(map[string]struct{}, len(codeToConcept))
	for _, spec := range conceptSpecs {
		for _, code := range spec.Codes {
			if _, ok := seen[code]; ok {
				continue
			}
			seen[code] = struct{}{}
			codes = append(codes, code)
		}
	}
	return codes
}

// ConceptForCode resolves a data-ID to its normalized concept.
func ConceptForCode(code string) (financials.Concept, bool) {
	c, ok := codeToConcept[code]
	return c, ok
}

// AliasesFor returns the code group for a concept (primary first).
func AliasesFor(c financials.Concept) []string {
	for _, spec := range conceptSpecs {
		if spec.Concept == c {
			return append([]string(nil), spec.Codes...)
		}
	}
	return nil
}

// Applicable reports whether a concept exists in a template. A false result is
// the not_applicable state; true only means "could exist", not "was reported".
// The rule lives in internal/financials because the depth store must make the
// same distinction without importing this adapter.
func Applicable(taxonomy financials.Taxonomy, concept financials.Concept) bool {
	return financials.Applicable(taxonomy, concept)
}
