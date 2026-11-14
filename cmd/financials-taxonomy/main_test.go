package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/yahoo"
)

func TestParseTickerText(t *testing.T) {
	got := parseTickerText(`
# covered set, 2026-11-09
bbri
PTBA.JK, aadi
BBRI
`)
	want := []string{"BBRI", "PTBA", "AADI"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvidenceStringIsSorted(t *testing.T) {
	got := evidenceString(map[string]bool{"EQ46Q": true, "EQRD7": false, "EQ8Y4": true})
	want := "EQ46Q=1;EQ8Y4=1;EQRD7=0"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestCodeSetStripsPeriodsAndSorts(t *testing.T) {
	got := codeSet([]string{"EQ46Q_12M", "EQ46Q_TTM", "EQRD7_3M", "EQ8Y4_12M", "EQ46Q_12M"})
	want := "EQ46Q,EQ8Y4,EQRD7"
	if strings.Join(got, ",") != want {
		t.Fatalf("got %v, want %s", got, want)
	}
}

func TestWriteTaxonomyCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	results := []usecase.TaxonomyResult{
		{Ticker: "BBRI", Taxonomy: financials.TaxonomyBank108, Method: "discriminant_probe", Evidence: map[string]bool{"EQRD7": true}, Applied: true},
		{Ticker: "ABCD", Taxonomy: financials.TaxonomyOther, Method: "discriminant_probe"},
	}
	if err := writeTaxonomyCSV(path, results); err != nil {
		t.Fatalf("writeTaxonomyCSV: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if !strings.Contains(text, "ticker,taxonomy,method,evidence,applied,error") {
		t.Fatalf("missing header:\n%s", text)
	}
	if !strings.Contains(text, "BBRI,bank_108,discriminant_probe,EQRD7=1,true,") {
		t.Fatalf("missing BBRI row:\n%s", text)
	}
}

func TestReadUncoveredSkipsHeaderAndBlankTicker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "uncovered.csv")
	body := "ticker|sector\nACRO|Consumer_Cyclicals\nAHAP|Financials\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	rows, err := readUncovered(path)
	if err != nil {
		t.Fatalf("readUncovered: %v", err)
	}
	if len(rows) != 2 || rows[0].Ticker != "ACRO" || rows[1].Sector != "Financials" {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestParseEvidence(t *testing.T) {
	got := parseEvidence("EQ46Q=1;EQRD7=0;EQ8Y4=1")
	if len(got) != 3 || !got["EQ46Q"] || got["EQRD7"] || !got["EQ8Y4"] {
		t.Fatalf("evidence = %v", got)
	}
	if parseEvidence("") != nil {
		t.Fatal("empty evidence should be nil")
	}
}

func TestReadTaxonomyCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "evidence.csv")
	body := `ticker,taxonomy,method,evidence,applied,error
BBRI,bank_108,discriminant_probe,EQRD7=1,true,
BAD,bogus,discriminant_probe,,false,
FAIL,bank_108,discriminant_probe,,false,probe failed
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	rows, err := readTaxonomyCSV(path)
	if err != nil {
		t.Fatalf("readTaxonomyCSV: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[0].Ticker != "BBRI" || !rows[0].Evidence["EQRD7"] {
		t.Errorf("BBRI = %+v", rows[0])
	}
	if rows[1].Err == nil {
		t.Error("invalid taxonomy should be recorded as an error")
	}
	if rows[2].Err == nil {
		t.Error("row error should be preserved")
	}
}

func TestTaxonomyFromTypeList(t *testing.T) {
	makeList := func(sizes ...int) *yahoo.TypeList {
		list := &yahoo.TypeList{}
		for _, n := range sizes {
			types := make([]string, n)
			for i := range types {
				types[i] = "EQT07_12M"
			}
			list.Groups = append(list.Groups, yahoo.TypeListGroup{Types: types})
		}
		return list
	}
	cases := []struct {
		list *yahoo.TypeList
		want financials.Taxonomy
		ok   bool
	}{
		{makeList(154, 500), financials.TaxonomyGeneral154, true},
		{makeList(108, 500), financials.TaxonomyBank108, true},
		{makeList(74, 500), financials.TaxonomyInsurance74, true},
		{makeList(50, 500), financials.TaxonomyOther, false},
		{&yahoo.TypeList{}, financials.TaxonomyOther, false},
	}
	for _, tc := range cases {
		got, ok := taxonomyFromTypeList(tc.list)
		if got != tc.want || ok != tc.ok {
			t.Errorf("sizes -> (%s, %v), want (%s, %v)", got, ok, tc.want, tc.ok)
		}
	}
}

func TestApplicabilityCell(t *testing.T) {
	codes := map[string]bool{"EQT07": true}
	if got := applicabilityCell(financials.TaxonomyBank108, codes, financials.ConceptRevenue); got != "yes" {
		t.Errorf("present alias = %q", got)
	}
	if got := applicabilityCell(financials.TaxonomyBank108, codes, financials.ConceptGrossProfit); got != "N/A" {
		t.Errorf("structural N/A = %q", got)
	}
	if got := applicabilityCell(financials.TaxonomyBank108, codes, financials.ConceptSGNA); got != "unmapped" {
		t.Errorf("absent mapped code = %q", got)
	}
	if got := applicabilityCell(financials.TaxonomyGeneral154, codes, financials.ConceptNetIncomeCommon); !strings.HasPrefix(got, "yes") {
		t.Errorf("derived concept = %q", got)
	}
}
