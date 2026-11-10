package yahoo

import (
	"strings"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

// probeResult builds a Result that reports data for the named codes in both
// 12M and TTM, mirroring what the classifier actually sees.
func probeResult(codes ...string) *Result {
	res := &Result{Facts: map[string][]Fact{}, Status: map[string]TypeStatus{}}
	for _, typ := range ProbeTypes() {
		res.Status[typ] = TypeAbsent
	}
	for _, code := range codes {
		for _, period := range []string{financials.PeriodAnnual, financials.PeriodTTM} {
			typ := code + "_" + period
			v := floatPtr(1)
			res.Facts[typ] = []Fact{{
				Code:       code,
				Type:       typ,
				PeriodType: period,
				AsOfDate:   mustDate("2025-12-31"),
				Currency:   "IDR",
				Value:      v,
				Source:     financials.ValueRestated,
			}}
			res.Status[typ] = TypeObserved
		}
	}
	return res
}

func TestClassifyResult_KnownSet(t *testing.T) {
	cases := []struct {
		ticker string
		codes  []string
		want   financials.Taxonomy
	}{
		{"BBCA", []string{"EQFK8", "EQRD7"}, financials.TaxonomyBank108},
		{"BBRI", []string{"EQFK8", "EQRD7", "EQDBM"}, financials.TaxonomyBank108},
		{"BBNI", []string{"EQFK8"}, financials.TaxonomyBank108},
		{"BMRI", []string{"EQRD7"}, financials.TaxonomyBank108},

		{"ABDA", []string{"EQDBM"}, financials.TaxonomyInsurance74},
		{"ASDM", []string{"EQDBM"}, financials.TaxonomyInsurance74},
		{"AMAG", []string{"EQF4S", "EQDBM"}, financials.TaxonomyInsurance74},
		{"SMMA", []string{"EQRYP", "EQDBM"}, financials.TaxonomyInsurance74},
		{"TUGU", []string{"EQDBM"}, financials.TaxonomyInsurance74},

		{"PTBA", []string{"EQ46Q", "EQ25Z"}, financials.TaxonomyGeneral154},
		{"AADI", []string{"EQ46Q", "EQ9HN"}, financials.TaxonomyGeneral154},
		{"AALI", []string{"EQ46Q", "EQLYB"}, financials.TaxonomyGeneral154},
		{"ADMF", []string{"EQ46Q"}, financials.TaxonomyGeneral154},
		{"LPPS", []string{"EQ46Q", "EQYSV"}, financials.TaxonomyGeneral154},
		{"BREN", []string{"EQ25Z", "EQLYB", "EQYSV", "EQAW7"}, financials.TaxonomyGeneral154},

		{"UNKNOWN", nil, financials.TaxonomyOther},
	}

	for _, tc := range cases {
		t.Run(tc.ticker, func(t *testing.T) {
			got := ClassifyResult(probeResult(tc.codes...))
			if got.Taxonomy != tc.want {
				t.Errorf("taxonomy = %q, want %q (evidence %v)", got.Taxonomy, tc.want, got.Evidence)
			}
			if got.Method != MethodDiscriminantProbe {
				t.Errorf("method = %q", got.Method)
			}
		})
	}
}

func TestClassifyResult_Priority(t *testing.T) {
	// A bank probe must beat a general marker even if both are populated.
	got := ClassifyResult(probeResult("EQFK8", "EQ46Q", "EQDBM"))
	if got.Taxonomy != financials.TaxonomyBank108 {
		t.Errorf("taxonomy = %q, want bank", got.Taxonomy)
	}

	// A general marker must beat the insurance fallback, so a general issuer
	// reporting interest income is not mistaken for an insurer.
	got = ClassifyResult(probeResult("EQ46Q", "EQDBM"))
	if got.Taxonomy != financials.TaxonomyGeneral154 {
		t.Errorf("taxonomy = %q, want general", got.Taxonomy)
	}

	// BREN: general list, no gross profit, but general operating markers.
	got = ClassifyResult(probeResult("EQDBM"))
	if got.Taxonomy != financials.TaxonomyInsurance74 {
		t.Errorf("interest-only = %q, want insurance", got.Taxonomy)
	}
}

func TestClassifyResult_IgnoresStaleProbeFacts(t *testing.T) {
	res := probeResult("EQDBM")
	// An insurer can carry an old general-market code; it must not outvote the
	// current insurance shape.
	res.Facts["EQ46Q_12M"] = []Fact{{
		Code:       "EQ46Q",
		Type:       "EQ46Q_12M",
		PeriodType: financials.PeriodAnnual,
		AsOfDate:   mustDate("2018-12-31"),
		Currency:   "IDR",
		Value:      floatPtr(1),
		Source:     financials.ValueRestated,
	}}
	got := ClassifyResult(res)
	if got.Taxonomy != financials.TaxonomyInsurance74 {
		t.Errorf("taxonomy = %q, want insurance; stale EQ46Q should be ignored", got.Taxonomy)
	}
	if got.Evidence["EQ46Q"] {
		t.Error("stale EQ46Q counted as evidence")
	}
}

func TestProbeTypes_IncludeAnnualAndTTM(t *testing.T) {
	types := ProbeTypes()
	joined := strings.Join(types, ",")
	for _, code := range []string{"EQRD7", "EQFK8", "EQDBM", "EQ46Q", "EQ25Z"} {
		if !strings.Contains(joined, code+"_12M") || !strings.Contains(joined, code+"_TTM") {
			t.Errorf("probe types missing %s 12M/TTM: %v", code, types)
		}
	}
}

func TestClassify_FromFixtures(t *testing.T) {
	cases := map[string]financials.Taxonomy{
		"ptba": financials.TaxonomyGeneral154,
		"bbri": financials.TaxonomyBank108,
		"abda": financials.TaxonomyInsurance74,
		"aadi": financials.TaxonomyGeneral154,
	}
	for ticker, want := range cases {
		t.Run(ticker, func(t *testing.T) {
			res := loadFixtureResult(t, ticker)
			got := ClassifyResult(res)
			if got.Taxonomy != want {
				t.Errorf("taxonomy = %q, want %q (evidence %v)", got.Taxonomy, want, got.Evidence)
			}
		})
	}
}
