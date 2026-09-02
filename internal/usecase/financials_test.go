package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
)

// fakeFinancialsFetcher records calls and returns canned results.
type fakeFinancialsFetcher struct {
	calls  int
	ticker string
	view   ipot.FinancialView
	result *ipot.Financials
	err    error
}

func (f *fakeFinancialsFetcher) FetchFinancial(ctx context.Context, ticker string, view ipot.FinancialView) (*ipot.Financials, error) {
	f.calls++
	f.ticker, f.view = ticker, view
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

func testFinancials(ticker string) *ipot.Financials {
	f := 37.2e12
	np := 4.3e12
	return &ipot.Financials{
		Ticker:   ticker,
		Currency: "IDR",
		Periods: []ipot.FinancialStatement{
			{Label: "3M 2026", PeriodEnd: "2026-03-31", DurationMonths: 3, Revenue: &f, NetProfit: &np},
			{Label: "3M 2025", PeriodEnd: "2025-03-31", DurationMonths: 3},
		},
	}
}

// TestGetFinancialsDefaultsAndValidation covers the argument contract.
func TestGetFinancialsDefaultsAndValidation(t *testing.T) {
	uc := &FinancialsUseCase{Log: logrus.New()}

	t.Run("default period is recent", func(t *testing.T) {
		fake := &fakeFinancialsFetcher{result: testFinancials("TLKM")}
		uc.Fetcher = fake
		resp, err := uc.GetFinancials(context.Background(), "TLKM", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.Period != FinancialPeriodRecent {
			t.Errorf("period = %q, want recent", resp.Period)
		}
		if fake.view != ipot.ViewRecent {
			t.Errorf("view = %q, want recent", fake.view)
		}
	})

	t.Run("annual maps to the annual view", func(t *testing.T) {
		fake := &fakeFinancialsFetcher{result: testFinancials("TLKM")}
		uc.Fetcher = fake
		if _, err := uc.GetFinancials(context.Background(), "TLKM", FinancialPeriodAnnual); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fake.view != ipot.ViewAnnual {
			t.Errorf("view = %q, want annual", fake.view)
		}
	})

	t.Run("invalid period is rejected before any fetch", func(t *testing.T) {
		fake := &fakeFinancialsFetcher{}
		uc.Fetcher = fake
		_, err := uc.GetFinancials(context.Background(), "TLKM", "monthly")
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
		if fake.calls != 0 {
			t.Error("fetcher called despite invalid period")
		}
	})

	t.Run("invalid ticker is rejected", func(t *testing.T) {
		uc.Fetcher = &fakeFinancialsFetcher{}
		if _, err := uc.GetFinancials(context.Background(), "TLKM.JK!", ""); !errors.Is(err, ErrInvalidTicker) {
			t.Errorf("err = %v, want ErrInvalidTicker", err)
		}
	})
}

// TestGetFinancialsResponseShape checks the response fields an AI reads:
// latest period end (feeds last_good_date), currency, statements.
func TestGetFinancialsResponseShape(t *testing.T) {
	uc := &FinancialsUseCase{
		Log:     logrus.New(),
		Fetcher: &fakeFinancialsFetcher{result: testFinancials("TLKM")},
	}
	resp, err := uc.GetFinancials(context.Background(), " tlkm ", FinancialPeriodQuarterly)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Ticker != "TLKM" {
		t.Errorf("ticker = %q, want TLKM", resp.Ticker)
	}
	if resp.LatestPeriodEnd != "2026-03-31" {
		t.Errorf("latest period end = %q, want 2026-03-31", resp.LatestPeriodEnd)
	}
	if resp.Currency != "IDR" {
		t.Errorf("currency = %q, want IDR", resp.Currency)
	}
	if len(resp.Statements) != 2 {
		t.Errorf("statements = %d, want 2", len(resp.Statements))
	}
}

// TestGetFinancialsUpstreamErrorPassthrough checks upstream failures surface
// unwrapped enough for exceptionToEnvelope to classify (429 sentinel).
func TestGetFinancialsUpstreamErrorPassthrough(t *testing.T) {
	uc := &FinancialsUseCase{
		Log:     logrus.New(),
		Fetcher: &fakeFinancialsFetcher{err: ipot.ErrUpstream429},
	}
	_, err := uc.GetFinancials(context.Background(), "TLKM", "")
	if err == nil || !strings.Contains(err.Error(), "rate limited") {
		t.Errorf("err = %v, want wrapped ErrUpstream429", err)
	}
}
