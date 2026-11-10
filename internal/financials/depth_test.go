package financials

import "testing"

func TestTaxonomy_ValidAndSize(t *testing.T) {
	cases := []struct {
		taxonomy Taxonomy
		valid    bool
		size     int
	}{
		{TaxonomyGeneral154, true, 154},
		{TaxonomyBank108, true, 108},
		{TaxonomyInsurance74, true, 74},
		{TaxonomyOther, true, 0},
		{Taxonomy(""), false, 0},
		{Taxonomy("bogus"), false, 0},
	}
	for _, tc := range cases {
		if got := tc.taxonomy.Valid(); got != tc.valid {
			t.Errorf("%q.Valid() = %v, want %v", tc.taxonomy, got, tc.valid)
		}
		if got := tc.taxonomy.Size(); got != tc.size {
			t.Errorf("%q.Size() = %d, want %d", tc.taxonomy, got, tc.size)
		}
	}
}

func TestDepthStatement_AmountAndCoverage(t *testing.T) {
	zero := 0.0
	stmt := DepthStatement{
		Values: map[Concept]Value{
			ConceptRevenue:   {Amount: floatPtr(100)},
			ConceptTotalDebt: {Amount: &zero},
		},
		Coverage: map[Concept]CoverageState{
			ConceptRevenue:     CoverageObserved,
			ConceptTotalDebt:   CoverageObserved,
			ConceptGrossProfit: CoverageNotApplicable,
		},
	}

	if v, ok := stmt.Amount(ConceptRevenue); !ok || *v != 100 {
		t.Errorf("revenue = %v, %v", v, ok)
	}
	if v, ok := stmt.Amount(ConceptTotalDebt); !ok || *v != 0 {
		t.Errorf("total debt zero = %v, %v", v, ok)
	}
	if _, ok := stmt.Amount(ConceptGrossProfit); ok {
		t.Error("gross profit should not be observed")
	}
	if got := stmt.CoverageOf(ConceptGrossProfit); got != CoverageNotApplicable {
		t.Errorf("gross profit coverage = %q", got)
	}
	if got := stmt.CoverageOf(ConceptCFO); got != CoverageUnknown {
		t.Errorf("missing coverage = %q, want unknown", got)
	}
}

func TestCoreConcepts_Unique(t *testing.T) {
	seen := make(map[Concept]struct{}, len(CoreConcepts))
	for _, c := range CoreConcepts {
		if _, ok := seen[c]; ok {
			t.Errorf("duplicate concept %q", c)
		}
		seen[c] = struct{}{}
	}
	if len(seen) != 38 {
		t.Errorf("core concepts = %d, want 38", len(seen))
	}
}

func floatPtr(v float64) *float64 { return &v }
