package yahoo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

// TestLiveDepthPTBA is an opt-in smoke test against the real post-1109
// endpoint. It classifies PTBA, fetches its 12M/3M/TTM statements, and runs the
// depth builder over a synthetic Tier-1 row shape to prove the full mapping
// (annual plus cumulative Q3 plus TTM) works on live data.
//
// Run with YAHOO_LIVE=1 go test ./internal/yahoo -run TestLiveDepthPTBA -v.
func TestLiveDepthPTBA(t *testing.T) {
	if os.Getenv("YAHOO_LIVE") != "1" {
		t.Skip("YAHOO_LIVE not set; skipping live Yahoo smoke test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := NewClient()
	taxonomy, method, err := c.ClassifyDepth(ctx, "PTBA")
	if err != nil {
		t.Fatalf("ClassifyDepth: %v", err)
	}
	if taxonomy != financials.TaxonomyGeneral154 {
		t.Fatalf("taxonomy = %s (%s), want general_154", taxonomy, method)
	}
	statements, err := c.FetchDepth(ctx, "PTBA", taxonomy)
	if err != nil {
		t.Fatalf("FetchDepth: %v", err)
	}
	if len(statements) == 0 {
		t.Fatal("no statements")
	}
	t.Logf("PTBA: taxonomy=%s method=%s statements=%d", taxonomy, method, len(statements))

	periods := []financials.TierOneDepthPeriod{
		{
			PeriodEnd:      time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
			FiscalYearEnd:  time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
			DurationMonths: 12,
			Revenue:        livePtr(42651724000000),
		},
		{
			PeriodEnd:      time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
			FiscalYearEnd:  time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			DurationMonths: 9,
			Revenue:        livePtr(33000000000000),
		},
	}
	rows := financials.BuildDepthRows("PTBA", taxonomy, statements, periods, financials.DefaultUSDIDRRate, time.Now())
	if len(rows) == 0 {
		t.Fatal("builder produced no depth rows")
	}
	var annual, q3 *financials.DepthRow
	for i := range rows {
		switch rows[i].PeriodEnd.Format("2006-01-02") {
		case "2025-12-31":
			annual = &rows[i]
		case "2026-09-30":
			q3 = &rows[i]
		}
	}
	if annual == nil {
		t.Fatal("missing annual depth row")
	}
	if annual.Values[financials.DepthGrossProfit] == nil || *annual.Values[financials.DepthGrossProfit] <= 0 {
		t.Errorf("annual gross profit = %v, want observed positive", annual.Values[financials.DepthGrossProfit])
	}
	if annual.Values[financials.DepthCash] == nil {
		t.Error("annual cash not observed")
	}
	if annual.PeriodBasis != financials.PeriodBasisAnnual12M {
		t.Errorf("annual period_basis = %q", annual.PeriodBasis)
	}
	if q3 == nil {
		t.Fatal("missing Q3 depth row")
	}
	if q3.Values[financials.DepthGrossProfit] == nil {
		t.Error("9M cumulative gross profit not observed")
	}
	if q3.Quarters[financials.DepthGrossProfit] == nil {
		t.Error("Q3 discrete gross profit not observed")
	}
	if q3.TTM[financials.DepthGrossProfit] == nil {
		t.Error("TTM gross profit not observed")
	}
}

func livePtr(v float64) *float64 { return &v }
