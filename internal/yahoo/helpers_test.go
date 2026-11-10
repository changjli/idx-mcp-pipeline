package yahoo

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

func mustDate(s string) time.Time {
	d, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return d
}

// fixtureTypes asks the parser for every concept type the adapter knows about
// plus the probe types. The fixture simply lacks the types that Yahoo returned
// meta-only or did not return at all.
func fixtureTypes() []string {
	types := append([]string(nil), ProbeTypes()...)
	for _, code := range ConceptCodes() {
		for _, period := range []string{financials.PeriodAnnual, financials.PeriodQuarterly, financials.PeriodTTM} {
			types = append(types, code+"_"+period)
		}
	}
	return uniqueStrings(types)
}

func loadFixtureResult(t *testing.T, ticker string) *Result {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", ticker+"_facts.json"))
	if err != nil {
		t.Fatalf("read fixture %s: %v", ticker, err)
	}
	res, err := parseTimeseries(body, fixtureTypes())
	if err != nil {
		t.Fatalf("parse fixture %s: %v", ticker, err)
	}
	return res
}

func loadFixtureStatements(t *testing.T, ticker string) []financials.DepthStatement {
	t.Helper()
	res := loadFixtureResult(t, ticker)
	cls := ClassifyResult(res)
	return BuildStatements(strings.ToUpper(ticker), cls.Taxonomy, res.Flatten())
}

func findStatement(t *testing.T, stmts []financials.DepthStatement, periodType, periodEnd string) financials.DepthStatement {
	t.Helper()
	for _, stmt := range stmts {
		if stmt.PeriodType == periodType && stmt.PeriodEnd.Format("2006-01-02") == periodEnd {
			return stmt
		}
	}
	t.Fatalf("statement %s %s not found in %d statements", periodType, periodEnd, len(stmts))
	return financials.DepthStatement{}
}

func amount(t *testing.T, stmt financials.DepthStatement, c financials.Concept) float64 {
	t.Helper()
	v, ok := stmt.Amount(c)
	if !ok {
		t.Fatalf("%s/%s: concept %s not observed", stmt.Ticker, stmt.PeriodEnd.Format("2006-01-02"), c)
	}
	return *v
}

func assertAmount(t *testing.T, stmt financials.DepthStatement, c financials.Concept, want float64) {
	t.Helper()
	got := amount(t, stmt, c)
	scale := math.Max(1, math.Abs(want))
	if math.Abs(got-want) > scale*1e-9 {
		t.Errorf("%s: got %.10f, want %.10f", c, got, want)
	}
	if state := stmt.CoverageOf(c); state != financials.CoverageObserved {
		t.Errorf("%s coverage = %q, want observed", c, state)
	}
}

func assertCoverage(t *testing.T, stmt financials.DepthStatement, c financials.Concept, want financials.CoverageState) {
	t.Helper()
	if got := stmt.CoverageOf(c); got != want {
		t.Errorf("%s coverage = %q, want %q", c, got, want)
	}
}
