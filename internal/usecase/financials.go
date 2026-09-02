package usecase

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
)

// FinancialViewParam values accepted by GetFinancials.
const (
	FinancialPeriodRecent    = "recent"
	FinancialPeriodQuarterly = "quarterly"
	FinancialPeriodAnnual    = "annual"
)

// FinancialsFetcher fetches normalized financial statements from IPOT.
// *ipot.Client satisfies this; tests use a fake.
type FinancialsFetcher interface {
	FetchFinancial(ctx context.Context, ticker string, view ipot.FinancialView) (*ipot.Financials, error)
}

// FinancialsUseCase orchestrates the on-demand get_financials flow: validate,
// fetch from IPOT, return. It is a temporary live-fetch route (issue 07) —
// nothing is persisted; the persisted financial-statements pipeline is issue
// 07b.
type FinancialsUseCase struct {
	DB  *sqlx.DB
	Log *logrus.Logger

	Fetcher FinancialsFetcher
}

func NewFinancialsUseCase(db *sqlx.DB, log *logrus.Logger, fetcher FinancialsFetcher) *FinancialsUseCase {
	return &FinancialsUseCase{DB: db, Log: log, Fetcher: fetcher}
}

// FinancialsResponse is the structured MCP tool result. DataStale is always
// false (the data was fetched live) and LastGoodDate is the newest statement
// period end, not a pipeline timestamp.
type FinancialsResponse struct {
	Ticker          string                    `json:"ticker"`
	Period          string                    `json:"period"`
	Currency        string                    `json:"currency"`
	LastPrice       *float64                  `json:"last_price,omitempty"`
	LatestPeriodEnd string                    `json:"latest_period_end"`
	Statements      []ipot.FinancialStatement `json:"statements"`
}

// GetFinancials returns the normalized financial statements for a ticker.
// period defaults to "recent" (~2 years, all report types + forecast +
// interim); "quarterly" selects uniform Q1 columns, "annual" audited
// full-year columns.
func (uc *FinancialsUseCase) GetFinancials(ctx context.Context, ticker, period string) (*FinancialsResponse, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}
	if period == "" {
		period = FinancialPeriodRecent
	}
	var view ipot.FinancialView
	switch period {
	case FinancialPeriodRecent:
		view = ipot.ViewRecent
	case FinancialPeriodQuarterly:
		view = ipot.ViewQuarterly
	case FinancialPeriodAnnual:
		view = ipot.ViewAnnual
	default:
		return nil, fmt.Errorf("%w: period must be %q, %q or %q, got %q",
			ErrInvalidArgument, FinancialPeriodRecent, FinancialPeriodQuarterly, FinancialPeriodAnnual, period)
	}

	fin, err := uc.Fetcher.FetchFinancial(ctx, ticker, view)
	if err != nil {
		return nil, fmt.Errorf("ipot financials: %w", err)
	}

	resp := &FinancialsResponse{
		Ticker:     fin.Ticker,
		Period:     period,
		Currency:   fin.Currency,
		LastPrice:  fin.LastPrice,
		Statements: fin.Periods,
	}
	if len(fin.Periods) > 0 {
		resp.LatestPeriodEnd = fin.Periods[0].PeriodEnd
	}
	return resp, nil
}
