package ipot

import (
	"os"
	"testing"
)

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	return b
}

// fval derefs an optional float for assertions.
func fval(t *testing.T, p *float64) float64 {
	t.Helper()
	if p == nil {
		t.Fatal("expected non-nil value")
	}
	return *p
}

// TestParseFundamentalTLKM parses the captured TLKM quarter=5 response and
// checks the normalized output against the values visible in the raw HTML.
func TestParseFundamentalTLKM(t *testing.T) {
	fin, err := ParseFundamental(loadFixture(t, "fundamental-tlkm-q5.html"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if fin.Currency != "IDR" {
		t.Errorf("currency = %q, want IDR", fin.Currency)
	}
	// 8 columns: Anlz + [6M] + 6 reported.
	if len(fin.Periods) != 8 {
		t.Fatalf("periods = %d, want 8 (legacy second table must not leak in)", len(fin.Periods))
	}
	if fin.LastPrice == nil || *fin.LastPrice != 2610 {
		t.Errorf("last price = %v, want 2610", fin.LastPrice)
	}

	// Column order: Anlz 2026 (forecast), [6M] 2026 (interim), then reported.
	first := fin.Periods[0]
	if !first.IsForecast || first.Label != "Anlz 2026" {
		t.Errorf("first period = %+v, want Anlz 2026 forecast", first)
	}
	if first.PeriodEnd != "2026-12-31" {
		t.Errorf("Anlz period end = %q, want 2026-12-31", first.PeriodEnd)
	}
	second := fin.Periods[1]
	if !second.IsInterim || second.Label != "[6M] 2026" || second.PeriodEnd != "2026-06-30" {
		t.Errorf("second period = %+v, want [6M] 2026 interim", second)
	}

	// 3M 2026 column (index 2), from the raw cells:
	// Revenue 37.2T, Net.Profit 4.3T, Cash 37.5T, S.T.Borrowing 26.7T,
	// L.T.Borrowing 42.0T, Total Equity 155.8T, Debt/Equity 0.44, ROE 2.79.
	p := fin.Periods[2]
	if p.Label != "3M 2026" || p.PeriodEnd != "2026-03-31" || p.DurationMonths != 3 {
		t.Errorf("third period = %+v", p)
	}
	if got := fval(t, p.Revenue); got != 37.2e12 {
		t.Errorf("revenue = %v, want 3.72e13", got)
	}
	if got := fval(t, p.NetProfit); got != 4.3e12 {
		t.Errorf("net profit = %v, want 4.3e12", got)
	}
	if got := fval(t, p.Cash); got != 37.5e12 {
		t.Errorf("cash = %v, want 3.75e13", got)
	}
	if got := fval(t, p.TotalEquity); got != 155.8e12 {
		t.Errorf("total equity = %v, want 1.558e14", got)
	}
	if got := fval(t, p.TotalDebt); got != 26.7e12+42.0e12 {
		t.Errorf("total debt = %v, want ST+LT", got)
	}
	if got := fval(t, p.DebtToEquity); got != 0.44 {
		t.Errorf("debt/equity = %v, want 0.44", got)
	}
	if got := fval(t, p.ROE); got != 2.79 {
		t.Errorf("roe = %v, want 2.79", got)
	}

	// The second (legacy) table's values must not overwrite the live ones:
	// its Total Asset for the first column is 154,050B, live is 303.1T.
	if got := fval(t, first.TotalAssets); got != 303.1e12 {
		t.Errorf("total assets = %v, want 3.031e14 (legacy table leaked)", got)
	}

	// Unmapped line items land in Extra keyed by the source label: the
	// 3M 2026 column has Share Out 99.1B, Market Cap 303.1T, Deviden 0,
	// BVPS 1,572.85, EV/EBITDA 18.97.
	for _, want := range []struct {
		key string
		val float64
	}{
		{"Share Out", 99.1e9},
		{"Market Cap", 303.1e12},
		{"Deviden", 0},
		{"BVPS", 1572.85},
		{"EV/EBITDA", 18.97},
	} {
		got, ok := p.Extra[want.key]
		if !ok {
			t.Errorf("extra[%q] missing; have %v", want.key, p.Extra)
			continue
		}
		if got != want.val {
			t.Errorf("extra[%q] = %v, want %v", want.key, got, want.val)
		}
	}
	if _, ok := first.Extra["Last Price"]; !ok {
		t.Error("Last Price should also ride in extra per period")
	}
}

// TestParseFundamentalAnnualView checks the quarter=4 fixture: the annual
// view keeps the tagged forecast/interim columns plus reported 12M columns.
func TestParseFundamentalAnnualView(t *testing.T) {
	fin, err := ParseFundamental(loadFixture(t, "fundamental-tlkm-q4.html"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	annual := filterView(fin, ViewAnnual)
	if len(annual.Periods) == 0 {
		t.Fatal("no annual periods after filter")
	}
	if !annual.Periods[0].IsForecast {
		t.Errorf("first period %q should be the forecast column", annual.Periods[0].Label)
	}
	reported := 0
	for _, p := range annual.Periods {
		if p.IsForecast || p.IsInterim {
			continue
		}
		reported++
		if p.DurationMonths != 12 {
			t.Errorf("reported period %q has duration %d, want 12", p.Label, p.DurationMonths)
		}
	}
	if reported == 0 {
		t.Error("no reported 12M columns in annual view")
	}
	if annual.Periods[0].Label != "Anlz 2026" {
		t.Errorf("newest period = %q, want Anlz 2026", annual.Periods[0].Label)
	}
}

// TestParseFundamentalQuarterlyView checks quarter=1 yields only 3M periods.
func TestParseFundamentalQuarterlyView(t *testing.T) {
	fin, err := ParseFundamental(loadFixture(t, "fundamental-tlkm-q1.html"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q := filterView(fin, ViewQuarterly)
	for _, p := range q.Periods {
		if p.DurationMonths != 3 {
			t.Errorf("period %+v has duration %d, want 3", p.Label, p.DurationMonths)
		}
	}
	if len(q.Periods) < 5 {
		t.Errorf("quarterly periods = %d, want >= 5", len(q.Periods))
	}
}

// TestParseFundamentalNoTable checks a response without the fundamental table
// errors instead of returning an empty shell.
func TestParseFundamentalNoTable(t *testing.T) {
	if _, err := ParseFundamental(loadFixture(t, "empty.html")); err == nil {
		t.Fatal("expected error for response without fundamental table")
	}
}

func TestParseFinancialValue(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"54.6 T", 54.6e12, true},
		{"27,256B", 27256e9, true},
		{"2,610", 2610, true},
		{"12.17 x", 12.17, true},
		{"7.01 %", 7.01, true},
		{"0", 0, true},
		{"(123)", -123, true},
		{"-", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		got, ok := parseFinancialValue(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("parseFinancialValue(%q) = (%v, %v), want (%v, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}
