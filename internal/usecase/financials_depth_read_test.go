package usecase

import (
	"context"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// TestGetFinancialsDepthFieldsAndCoverage pins the read-side Tier-2 contract:
// depth columns fill the existing response fields, a bank's absent gross profit
// reads as not_applicable (not unknown), and the derived margin / debt ratios
// are computed only when both operands are present.
func TestGetFinancialsDepthFieldsAndCoverage(t *testing.T) {
	row := finStatement("BANK", "2026-09-30", 9)
	row.Revenue = finPtrI(1000)
	row.Equity = finPtrI(800)
	row.Cash = finPtrF(500)
	row.CFO = finPtrF(100)
	row.TotalDebt = finPtrF(400)
	// GrossProfit deliberately nil: a bank has no gross-profit line.
	coverage := `{"cash":"observed","gross_profit":"not_applicable","operating_profit":"not_applicable","ebitda":"not_applicable","cfo":"observed","capex":"not_applicable","fcf":"not_applicable","total_debt":"observed","interest_expense":"not_applicable"}`
	row.DepthCoverage = &coverage

	uc := newFinancialsTestUseCase([]entity.FinancialStatement{row}, nil)
	resp, err := uc.GetFinancials(context.Background(), "BANK", "")
	if err != nil {
		t.Fatalf("GetFinancials: %v", err)
	}
	if len(resp.Statements) != 1 {
		t.Fatalf("statements = %d, want 1", len(resp.Statements))
	}
	view := resp.Statements[0]
	if view.Cash == nil || *view.Cash != 500 {
		t.Errorf("cash = %v, want 500", view.Cash)
	}
	if view.TotalDebt == nil || *view.TotalDebt != 400 {
		t.Errorf("total_debt = %v, want 400", view.TotalDebt)
	}
	if view.GrossProfit != nil {
		t.Errorf("gross_profit = %v, want nil", *view.GrossProfit)
	}
	if view.GrossProfitMargin != nil {
		t.Errorf("gross_profit_margin = %v, want nil (no gross profit)", *view.GrossProfitMargin)
	}
	if view.DebtToEquity == nil || *view.DebtToEquity != 0.5 {
		t.Errorf("debt_to_equity = %v, want 0.5", view.DebtToEquity)
	}

	if got := resp.Coverage["cash"].Status; got != FinancialCoverageObserved {
		t.Errorf("cash coverage = %q, want observed", got)
	}
	if got := resp.Coverage["gross_profit"].Status; got != FinancialCoverageNotApplicable {
		t.Errorf("gross_profit coverage = %q, want not_applicable", got)
	}
	if got := resp.Coverage["interest_expense"].Status; got != FinancialCoverageNotApplicable {
		t.Errorf("interest_expense coverage = %q, want not_applicable", got)
	}
	if got := resp.Coverage["debt_to_equity"].Status; got != FinancialCoverageObserved {
		t.Errorf("debt_to_equity coverage = %q, want observed", got)
	}
}
