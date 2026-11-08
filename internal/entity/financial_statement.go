package entity

import "time"

// FinancialStatement is one canonical (ticker, period_end) reported statement.
// Money fields are raw rupiah, stored as NUMERIC; ratios are IDX's TTM values
// stored exactly as reported. FirstSeenMonth / LastSeenMonth are the first day
// of the snapshot month that first/last carried the statement.
type FinancialStatement struct {
	Ticker         string    `db:"ticker"`
	PeriodEnd      time.Time `db:"period_end"`
	FiscalYearEnd  time.Time `db:"fiscal_year_end"`
	DurationMonths int       `db:"duration_months"`

	Assets          *int64 `db:"assets"`
	Liabilities     *int64 `db:"liabilities"`
	Equity          *int64 `db:"equity"`
	Revenue         *int64 `db:"revenue"`
	EBT             *int64 `db:"ebt"`
	NetProfit       *int64 `db:"net_profit"`
	NetProfitParent *int64 `db:"net_profit_parent"`

	EPS       *float64 `db:"eps"`
	BookValue *float64 `db:"book_value"`
	ROA       *float64 `db:"roa"`
	ROE       *float64 `db:"roe"`
	NPM       *float64 `db:"npm"`
	DER       *float64 `db:"der"`

	AuditStatus  *string   `db:"audit_status"`
	Opinion      *string   `db:"opinion"`
	Source       string    `db:"source"`
	ValueBasis   string    `db:"value_basis"`
	SourceFSDate time.Time `db:"source_fs_date"`

	FirstSeenMonth time.Time `db:"first_seen_month"`
	LastSeenMonth  time.Time `db:"last_seen_month"`
	RevisionCount  int       `db:"revision_count"`
	ContentHash    string    `db:"content_hash"`
	StoredAt       time.Time `db:"stored_at"`
	UpdatedAt      time.Time `db:"updated_at"`
}

// TableName implements repository.Tabler (the generic repository is not used
// for this table, but the interface documents the name).
func (FinancialStatement) TableName() string { return "financial_statements" }

// FinancialStatementVersion is one append-only point-in-time observation of a
// statement: written on first sight and on every value change, not on every
// monthly sweep. SeenMonth is the snapshot month that carried it.
type FinancialStatementVersion struct {
	Ticker         string    `db:"ticker"`
	PeriodEnd      time.Time `db:"period_end"`
	SeenMonth      time.Time `db:"seen_month"`
	FiscalYearEnd  time.Time `db:"fiscal_year_end"`
	DurationMonths int       `db:"duration_months"`

	Assets          *int64 `db:"assets"`
	Liabilities     *int64 `db:"liabilities"`
	Equity          *int64 `db:"equity"`
	Revenue         *int64 `db:"revenue"`
	EBT             *int64 `db:"ebt"`
	NetProfit       *int64 `db:"net_profit"`
	NetProfitParent *int64 `db:"net_profit_parent"`

	EPS       *float64 `db:"eps"`
	BookValue *float64 `db:"book_value"`
	ROA       *float64 `db:"roa"`
	ROE       *float64 `db:"roe"`
	NPM       *float64 `db:"npm"`
	DER       *float64 `db:"der"`

	AuditStatus *string   `db:"audit_status"`
	Opinion     *string   `db:"opinion"`
	ContentHash string    `db:"content_hash"`
	RecordedAt  time.Time `db:"recorded_at"`
}

// TableName implements repository.Tabler.
func (FinancialStatementVersion) TableName() string { return "financial_statement_versions" }

// FinancialSnapshotRun is one month's ingestion ledger row. SnapshotMonth is
// the first day of the month; Status is ok | suspect | empty | failed.
// QuarterEndShare is nil for an empty month, where the share is undefined.
type FinancialSnapshotRun struct {
	SnapshotMonth   time.Time `db:"snapshot_month"`
	Source          string    `db:"source"`
	Status          string    `db:"status"`
	RowCount        int       `db:"row_count"`
	DistinctPeriods int       `db:"distinct_periods"`
	QuarterEndShare *float64  `db:"quarter_end_share"`
	ContentHash     *string   `db:"content_hash"`
	Error           *string   `db:"error"`
	FetchedAt       time.Time `db:"fetched_at"`
}

// TableName implements repository.Tabler.
func (FinancialSnapshotRun) TableName() string { return "financial_snapshot_runs" }
