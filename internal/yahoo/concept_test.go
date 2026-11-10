package yahoo

import (
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

func TestConceptForCode_AliasGroups(t *testing.T) {
	groups := map[financials.Concept][]string{
		financials.ConceptDepreciationAmortization: {"EQQR7", "EQTFV", "EQK97"},
		financials.ConceptLiabilities:              {"EQRG7", "EQA1D"},
		financials.ConceptEquity:                   {"EQE13", "EQ4RH"},
		financials.ConceptCommonEquity:             {"EQ1HC", "EQTCA"},
		financials.ConceptNetFinanceIncome:         {"EQSX8", "EQLTG"},
		financials.ConceptCFO:                      {"EQ6JP", "EQ65B"},
		financials.ConceptCFI:                      {"EQBDY", "EQ1HW"},
		financials.ConceptCFF:                      {"EQPNK", "EQZNP"},
	}
	for concept, codes := range groups {
		for _, code := range codes {
			got, ok := ConceptForCode(code)
			if !ok {
				t.Errorf("%s: code %s not mapped", concept, code)
				continue
			}
			if got != concept {
				t.Errorf("code %s = %s, want %s", code, got, concept)
			}
		}
	}
}

func TestConceptCodes_NoDuplicates(t *testing.T) {
	seen := make(map[string]struct{})
	for _, code := range ConceptCodes() {
		if _, ok := seen[code]; ok {
			t.Errorf("duplicate code %s", code)
		}
		seen[code] = struct{}{}
	}
	if len(seen) < 40 {
		t.Errorf("concept codes = %d, want the full core map", len(seen))
	}
}

func TestApplicable(t *testing.T) {
	cases := []struct {
		taxonomy financials.Taxonomy
		concept  financials.Concept
		want     bool
	}{
		{financials.TaxonomyGeneral154, financials.ConceptGrossProfit, true},
		{financials.TaxonomyBank108, financials.ConceptGrossProfit, false},
		{financials.TaxonomyInsurance74, financials.ConceptGrossProfit, false},
		{financials.TaxonomyBank108, financials.ConceptCostOfRevenue, false},
		{financials.TaxonomyInsurance74, financials.ConceptCostOfRevenue, false},
		{financials.TaxonomyOther, financials.ConceptGrossProfit, true},
		{financials.TaxonomyBank108, financials.ConceptRevenue, true},
		{financials.TaxonomyBank108, financials.ConceptSGNA, true},
	}
	for _, tc := range cases {
		if got := Applicable(tc.taxonomy, tc.concept); got != tc.want {
			t.Errorf("Applicable(%s, %s) = %v, want %v", tc.taxonomy, tc.concept, got, tc.want)
		}
	}
}

func TestSelectFact_AliasConflict(t *testing.T) {
	facts := map[string][]Fact{
		"EQQR7": {{Code: "EQQR7", Value: floatPtr(-300)}},
		"EQTFV": {{Code: "EQTFV", Value: floatPtr(-300)}},
		"EQK97": {{Code: "EQK97", Value: floatPtr(-250)}},
	}
	spec := conceptSpec{Concept: financials.ConceptDepreciationAmortization, Codes: []string{"EQQR7", "EQTFV", "EQK97"}}
	chosen, ok, conflicts := selectFact(spec, facts)
	if !ok || chosen.Code != "EQQR7" {
		t.Fatalf("chosen = %+v, %v", chosen, ok)
	}
	if len(conflicts) != 1 || conflicts[0].OtherCode != "EQK97" {
		t.Fatalf("conflicts = %+v, want one EQK97 conflict", conflicts)
	}
}
