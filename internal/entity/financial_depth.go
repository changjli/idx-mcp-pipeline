package entity

import "time"

// TickerFinancialTaxonomy records the Yahoo/Morningstar statement template an
// issuer is served and the latest depth state. The full-universe population is
// ticket 06's job; the monthly depth sweep writes the rows it needs and uses
// depth_status='not_covered' to stop retrying a ticker the vendor does not
// carry.
type TickerFinancialTaxonomy struct {
	Ticker         string     `db:"ticker"`
	Taxonomy       string     `db:"taxonomy"`
	DiscoveredAt   time.Time  `db:"discovered_at"`
	Source         string     `db:"source"`
	DepthStatus    string     `db:"depth_status"`
	LastDepthMonth *time.Time `db:"last_depth_month"`
	LastError      *string    `db:"last_error"`
}

// FinancialDepthRun is one ticker's ledger row for one depth run month. The
// pair is the resume cursor: terminal statuses are skipped on a repeat run,
// failed/yahoo_lagging are retried.
type FinancialDepthRun struct {
	RunMonth    time.Time `db:"run_month"`
	Ticker      string    `db:"ticker"`
	Status      string    `db:"status"`
	RowsWritten int       `db:"rows_written"`
	Error       *string   `db:"error"`
	Attempts    int       `db:"attempts"`
	UpdatedAt   time.Time `db:"updated_at"`
}

// FinancialStatementDepth is the write shape for the depth columns on an
// existing canonical statement row. Every value is nullable and already
// converted to IDR; DepthCoverage/DepthRaw/DepthConflicts are JSON documents.
//
// ObservedMonth and DepthSnapshot are only used by the append-only depth
// version trail (financial_statement_depth_versions), not by the canonical
// UPDATE.
type FinancialStatementDepth struct {
	Ticker    string    `db:"ticker"`
	PeriodEnd time.Time `db:"period_end"`

	Cash            *float64 `db:"cash"`
	GrossProfit     *float64 `db:"gross_profit"`
	OperatingProfit *float64 `db:"operating_profit"`
	EBITDA          *float64 `db:"ebitda"`
	CFO             *float64 `db:"cfo"`
	CapEx           *float64 `db:"capex"`
	FCF             *float64 `db:"fcf"`
	TotalDebt       *float64 `db:"total_debt"`
	InterestExpense *float64 `db:"interest_expense"`

	CashQ            *float64 `db:"cash_q"`
	GrossProfitQ     *float64 `db:"gross_profit_q"`
	OperatingProfitQ *float64 `db:"operating_profit_q"`
	EBITDAq          *float64 `db:"ebitda_q"`
	CFOQ             *float64 `db:"cfo_q"`
	CapExQ           *float64 `db:"capex_q"`
	FCFQ             *float64 `db:"fcf_q"`
	TotalDebtQ       *float64 `db:"total_debt_q"`
	InterestExpenseQ *float64 `db:"interest_expense_q"`

	CashTTM            *float64 `db:"cash_ttm"`
	GrossProfitTTM     *float64 `db:"gross_profit_ttm"`
	OperatingProfitTTM *float64 `db:"operating_profit_ttm"`
	EBITDATTM          *float64 `db:"ebitda_ttm"`
	CFOTTM             *float64 `db:"cfo_ttm"`
	CapExTTM           *float64 `db:"capex_ttm"`
	FCFTTM             *float64 `db:"fcf_ttm"`
	TotalDebtTTM       *float64 `db:"total_debt_ttm"`
	InterestExpenseTTM *float64 `db:"interest_expense_ttm"`

	Currency    *string    `db:"currency"`
	FXRate      *float64   `db:"fx_rate"`
	FXDate      *time.Time `db:"fx_date"`
	PeriodBasis *string    `db:"period_basis"`
	Methodology *string    `db:"methodology"`

	DepthCoverage  *string `db:"depth_coverage"`
	DepthRaw       *string `db:"depth_raw"`
	DepthConflicts *string `db:"depth_conflicts"`

	DepthStatus      *string    `db:"depth_status"`
	DepthFetchedAt   *time.Time `db:"depth_fetched_at"`
	DepthContentHash *string    `db:"depth_content_hash"`

	// Append-only depth version trail only.
	ObservedMonth *time.Time `db:"observed_month"`
	DepthSnapshot *string    `db:"depth_snapshot"`
}

// FinancialStatementDepthVersion is one append-only observation of a depth
// snapshot. DepthSnapshot is the JSON document written to the trail; it is
// selected as text so callers do not need a JSON codec.
type FinancialStatementDepthVersion struct {
	Ticker        string    `db:"ticker"`
	PeriodEnd     time.Time `db:"period_end"`
	ObservedMonth time.Time `db:"observed_month"`
	DepthSnapshot string    `db:"depth_snapshot"`
	ContentHash   string    `db:"content_hash"`
	RecordedAt    time.Time `db:"recorded_at"`
}
