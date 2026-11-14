package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// FinancialPeriod* are the period views accepted by GetFinancials.
const (
	// FinancialPeriodRecent — the latest ~2 years of statements, all durations.
	FinancialPeriodRecent = "recent"
	// FinancialPeriodQuarterly — every interim statement (3M / 6M / 9M) over
	// the full stored history. The view is not "Q1 only": Tier 1 has 6M and 9M
	// too, and every row carries duration_months so only same-duration columns
	// are comparable.
	FinancialPeriodQuarterly = "quarterly"
	// FinancialPeriodAnnual — every full-year statement (12M) over the full
	// stored history.
	FinancialPeriodAnnual = "annual"
)

// Field coverage statuses. They describe what a response field holds for the
// returned statements, so a null never has to be interpreted:
//   - observed: every returned statement carries the value;
//   - unknown: the value is unobserved (not carried by Tier 1, or missing on at
//     least one returned row) — never read it as zero;
//   - not_applicable: the concept does not exist for the issuer (a Tier-2
//     distinction: a bank has no gross-profit line). Tier 1 emits unknown for
//     every line it cannot supply, because the snapshot cannot distinguish an
//     absent concept from an uncarried one.
const (
	FinancialCoverageObserved      = "observed"
	FinancialCoverageUnknown       = "unknown"
	FinancialCoverageNotApplicable = "not_applicable"
)

// financialsRecentYears is the lookback of the default `recent` view, anchored
// on the newest stored statement (not wall-clock time), so a ticker that
// stopped filing still returns its last two years of statements.
const financialsRecentYears = 2

// financialsStaleMonths is how old the newest stored statement may be before a
// ticker reads as stale. An active issuer files at least quarterly (annual at
// worst, three months after year-end) and the snapshot adds ~1 month of lag, so
// a year is a safe ceiling. A ticker whose latest statement is 2019 is stale.
const financialsStaleMonths = 12

// FinancialStatementsReader is the statement seam the read path depends on.
// *repository.FinancialsRepository satisfies it; tests use a fake.
type FinancialStatementsReader interface {
	FindByTicker(db *sqlx.DB, ticker string) ([]entity.FinancialStatement, error)
}

// FinancialPriceReader is the price seam per/pbv and last_price read through.
// *repository.DailyPriceRepository satisfies it.
type FinancialPriceReader interface {
	FindCloseOnOrBefore(db *sqlx.DB, ticker string, day time.Time) (*entity.DailyPrice, error)
	FindLatestClose(db *sqlx.DB, ticker string) (*entity.DailyPrice, error)
}

// FinancialDepthRequester is the on-demand Tier-2 seam (issue 07). It reports
// the ticker's response-level depth_status and, when depth is missing and the
// cooldown allows, enqueues one async enrichment job. It never performs an
// upstream call, so the read path never blocks on Yahoo.
// *usecase.FinancialDepthRequestUseCase satisfies it.
type FinancialDepthRequester interface {
	RequireDepth(ctx context.Context, ticker string, depthMissing bool) (string, error)
}

// FinancialsUseCase answers get_financials from the persisted statement store.
// It is a pure DB read: the MCP request path never touches IPOT or IDX (Heroku
// H12 constraint, ADR-0009). The store is populated by the monthly snapshot
// ingestion (financials-v2 issue 01) and its history backfill (issue 02).
type FinancialsUseCase struct {
	DB   *sqlx.DB
	Log  *logrus.Logger
	Repo FinancialStatementsReader
	// Prices supplies the close on/before each statement's period end (per/pbv)
	// and the ticker's latest stored close (last_price).
	Prices FinancialPriceReader
	// Depth is the on-demand Tier-2 trigger. Nil disables the trigger (the
	// status is then derived from coverage alone); the MCP server always wires
	// it.
	Depth FinancialDepthRequester
	// Now is the clock per-ticker staleness is judged against; nil means
	// time.Now.
	Now func() time.Time
}

// FinancialsOption configures the read usecase.
type FinancialsOption func(*FinancialsUseCase)

// WithFinancialDepthRequester wires the on-demand Tier-2 trigger.
func WithFinancialDepthRequester(depth FinancialDepthRequester) FinancialsOption {
	return func(uc *FinancialsUseCase) {
		uc.Depth = depth
	}
}

func NewFinancialsUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	repo FinancialStatementsReader,
	prices FinancialPriceReader,
	opts ...FinancialsOption,
) *FinancialsUseCase {
	uc := &FinancialsUseCase{DB: db, Log: log, Repo: repo, Prices: prices}
	for _, opt := range opts {
		if opt != nil {
			opt(uc)
		}
	}
	return uc
}

// FinancialFieldCoverage is one field's coverage across the returned
// statements. ObservedRows / ReturnedRows keep a partially observed field from
// reading as either fully observed or wholly absent.
type FinancialFieldCoverage struct {
	Status       string `json:"status"`
	ObservedRows int    `json:"observed_rows"`
	ReturnedRows int    `json:"returned_rows"`
}

// FinancialStatementView is one stored statement projected into the tool's
// response shape. Money is raw rupiah, ratios are IDX's TTM values, and every
// field Tier 1 cannot supply is null — never zero. Per/pbv are calculated at
// read time from the close on or before PeriodEnd; nil means not computable
// (no covering close or a zero denominator), never zero.
type FinancialStatementView struct {
	PeriodLabel    string `json:"period_label"` // e.g. "3M 2026"
	PeriodEnd      string `json:"period_end"`   // YYYY-MM-DD
	DurationMonths int    `json:"duration_months"`
	IsForecast     bool   `json:"is_forecast"` // always false: Tier 1 has no forecasts
	IsInterim      bool   `json:"is_interim"`  // duration 3M / 6M / 9M
	// PriceAsOf is the trading day whose close valued this row; empty when no
	// stored close covers the period, in which case per/pbv are null.
	PriceAsOf string `json:"price_as_of,omitempty"`

	Revenue            *float64 `json:"revenue"`
	GrossProfit        *float64 `json:"gross_profit"`
	OperatingProfit    *float64 `json:"operating_profit"`
	NetProfit          *float64 `json:"net_profit"` // attributable to owners
	NetProfitInclNCI   *float64 `json:"net_profit_incl_nci"`
	EBITDA             *float64 `json:"ebitda"`
	InterestExpense    *float64 `json:"interest_expense"`
	Cash               *float64 `json:"cash"`
	TotalAssets        *float64 `json:"total_assets"`
	ShortTermBorrowing *float64 `json:"short_term_borrowing"`
	LongTermBorrowing  *float64 `json:"long_term_borrowing"`
	TotalDebt          *float64 `json:"total_debt"`
	TotalEquity        *float64 `json:"total_equity"`
	GrossProfitMargin  *float64 `json:"gross_profit_margin"`

	EPS       *float64 `json:"eps"`        // IDR per share, TTM
	BookValue *float64 `json:"book_value"` // IDR per share
	ROE       *float64 `json:"roe"`        // percent, TTM
	ROA       *float64 `json:"roa"`        // percent, TTM
	NPM       *float64 `json:"npm"`        // percent, TTM profit / YTD sales (IDX's mixed basis)
	PER       *float64 `json:"per"`        // close at period end / eps
	PBV       *float64 `json:"pbv"`        // close at period end / book_value
	// DebtToEquity is computed from Tier-2 total debt over Tier-1 equity when
	// both are present; it stays null at Tier 1 because the snapshot's der is
	// liabilities/equity, not total-debt/equity, and must not be mislabelled.
	DebtToEquity *float64 `json:"debt_to_equity"`
	// LiabilitiesToEquity is the stored der, under its real meaning.
	LiabilitiesToEquity *float64 `json:"liabilities_to_equity"`

	Extra map[string]float64 `json:"extra,omitempty"`

	// depthCoverage is the per-row Tier-2 coverage map used by
	// financialCoverage; it is not part of the response shape.
	depthCoverage map[string]string
}

// FinancialsResponse is the structured MCP tool result. Statements are newest
// period first. PeriodStale is the per-ticker freshness signal (the newest
// statement is over a year old, or the ticker has no statement at all); the
// handler ORs it into the standard envelope and exposes LatestPeriodEnd as
// last_good_date.
type FinancialsResponse struct {
	Ticker          string                            `json:"ticker"`
	Period          string                            `json:"period"`
	Currency        string                            `json:"currency"`
	ValueBasis      string                            `json:"value_basis"`
	LastPrice       *float64                          `json:"last_price,omitempty"`
	LastPriceAsOf   string                            `json:"last_price_as_of,omitempty"`
	LatestPeriodEnd string                            `json:"latest_period_end"`
	Statements      []FinancialStatementView          `json:"statements"`
	Coverage        map[string]FinancialFieldCoverage `json:"coverage"`

	// DepthStatus is the Tier-2 on-demand state (issue 07): observed when the
	// newest statement already carries depth, pending while an enrichment job
	// is queued or cooling down, failed after a failed attempt, not_covered
	// when the vendor carries no data for the ticker. Empty when the ticker has
	// no stored statements.
	DepthStatus string `json:"depth_status,omitempty"`

	PeriodStale bool `json:"-"`
}

// GetFinancials returns a ticker's stored statements in the requested period
// view. period defaults to "recent" (latest ~2 years, all durations);
// "quarterly" selects every interim statement (3M/6M/9M) over full history;
// "annual" selects every 12M statement. Periods are cumulative YTD and
// comparable only within the same duration_months. The statement data is a
// pure DB read; the only side effect is the Tier-2 trigger (issue 07), which
// may enqueue one depth enrichment job when the newest statement has no depth
// yet — it never fetches upstream.
func (uc *FinancialsUseCase) GetFinancials(ctx context.Context, ticker, period string) (*FinancialsResponse, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}
	view, err := normalizeFinancialPeriod(period)
	if err != nil {
		return nil, err
	}

	rows, err := uc.Repo.FindByTicker(uc.DB, ticker)
	if err != nil {
		return nil, fmt.Errorf("read financial statements: %w", err)
	}
	// The repository orders newest first; keep the invariant local too so the
	// recent cutoff and latest_period_end never depend on that detail.
	sortStatementsNewestFirst(rows)

	latest := time.Time{}
	valueBasis := financials.ValueBasisIDX
	if len(rows) > 0 {
		latest = rows[0].PeriodEnd
		if rows[0].ValueBasis != "" {
			valueBasis = rows[0].ValueBasis
		}
	}

	selected := selectFinancialStatements(rows, view)
	statements := make([]FinancialStatementView, 0, len(selected))
	for _, st := range selected {
		price, err := uc.Prices.FindCloseOnOrBefore(uc.DB, ticker, st.PeriodEnd)
		if err != nil {
			return nil, fmt.Errorf("read close for %s %s: %w", ticker, st.PeriodEnd.Format("2006-01-02"), err)
		}
		statements = append(statements, financialStatementView(st, price))
	}

	resp := &FinancialsResponse{
		Ticker:     ticker,
		Period:     view,
		Currency:   "IDR",
		ValueBasis: valueBasis,
		Statements: statements,
		Coverage:   financialCoverage(statements),
	}
	if !latest.IsZero() {
		resp.LatestPeriodEnd = latest.Format("2006-01-02")
	}

	lastPrice, err := uc.Prices.FindLatestClose(uc.DB, ticker)
	if err != nil {
		return nil, fmt.Errorf("read latest close for %s: %w", ticker, err)
	}
	if lastPrice != nil && lastPrice.Close != nil {
		close := *lastPrice.Close
		resp.LastPrice = &close
		resp.LastPriceAsOf = lastPrice.TradingDay.Format("2006-01-02")
	}

	now := time.Now()
	if uc.Now != nil {
		now = uc.Now()
	}
	resp.PeriodStale = latest.IsZero() || latest.AddDate(0, financialsStaleMonths, 0).Before(now)

	// Tier-2 on-demand trigger (issue 07). The read stays a store read: when
	// the newest stored statement has no depth yet, this enqueues one async
	// enrichment job (subject to the per-ticker cooldown) and reports pending.
	// The client polls the same call; a later poll is a store hit.
	if len(rows) > 0 {
		depthMissing := financialStatementsDepthMissing(rows)
		resp.DepthStatus = FinancialDepthObserved
		if depthMissing {
			resp.DepthStatus = FinancialDepthPending
		}
		if uc.Depth != nil {
			status, derr := uc.Depth.RequireDepth(ctx, ticker, depthMissing)
			if derr != nil && uc.Log != nil {
				uc.Log.Warnf("get_financials: depth request %s: %v", ticker, derr)
			}
			if status != "" {
				resp.DepthStatus = status
			}
		}
	}
	return resp, nil
}

// normalizeFinancialPeriod maps the caller's period string to a stored view.
// Empty means the default `recent`; anything else unrecognised is an argument
// error.
func normalizeFinancialPeriod(period string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(period)) {
	case "", FinancialPeriodRecent:
		return FinancialPeriodRecent, nil
	case FinancialPeriodQuarterly:
		return FinancialPeriodQuarterly, nil
	case FinancialPeriodAnnual:
		return FinancialPeriodAnnual, nil
	default:
		return "", fmt.Errorf("%w: period must be %q, %q or %q, got %q",
			ErrInvalidArgument, FinancialPeriodRecent, FinancialPeriodQuarterly, FinancialPeriodAnnual, period)
	}
}

// selectFinancialStatements applies one period view to the newest-first rows.
func selectFinancialStatements(rows []entity.FinancialStatement, view string) []entity.FinancialStatement {
	if len(rows) == 0 {
		return nil
	}
	out := make([]entity.FinancialStatement, 0, len(rows))
	switch view {
	case FinancialPeriodQuarterly:
		for _, st := range rows {
			if st.DurationMonths == 3 || st.DurationMonths == 6 || st.DurationMonths == 9 {
				out = append(out, st)
			}
		}
	case FinancialPeriodAnnual:
		for _, st := range rows {
			if st.DurationMonths == 12 {
				out = append(out, st)
			}
		}
	default: // recent
		cutoff := rows[0].PeriodEnd.AddDate(-financialsRecentYears, 0, 0)
		for _, st := range rows {
			if !st.PeriodEnd.Before(cutoff) {
				out = append(out, st)
			}
		}
	}
	return out
}

// financialStatementView maps one stored statement to the response shape. The
// mapping is deliberately explicit: net_profit is the attributable figure
// (profitAttrOwner), consistent with eps/roe, and the total including
// non-controlling interests is exposed separately.
func financialStatementView(st entity.FinancialStatement, price *entity.DailyPrice) FinancialStatementView {
	view := FinancialStatementView{
		PeriodLabel:         fmt.Sprintf("%dM %d", st.DurationMonths, st.PeriodEnd.Year()),
		PeriodEnd:           st.PeriodEnd.Format("2006-01-02"),
		DurationMonths:      st.DurationMonths,
		IsForecast:          false,
		IsInterim:           st.DurationMonths != 12,
		Revenue:             rupiahFloat(st.Revenue),
		NetProfit:           rupiahFloat(st.NetProfitParent),
		NetProfitInclNCI:    rupiahFloat(st.NetProfit),
		TotalAssets:         rupiahFloat(st.Assets),
		TotalEquity:         rupiahFloat(st.Equity),
		Cash:                copyFloat(st.Cash),
		GrossProfit:         copyFloat(st.GrossProfit),
		OperatingProfit:     copyFloat(st.OperatingProfit),
		EBITDA:              copyFloat(st.EBITDA),
		InterestExpense:     copyFloat(st.InterestExpense),
		TotalDebt:           copyFloat(st.TotalDebt),
		EPS:                 copyFloat(st.EPS),
		BookValue:           copyFloat(st.BookValue),
		ROE:                 copyFloat(st.ROE),
		ROA:                 copyFloat(st.ROA),
		NPM:                 copyFloat(st.NPM),
		LiabilitiesToEquity: copyFloat(st.DER),
		depthCoverage:       parseDepthCoverage(st.DepthCoverage),
	}
	// Gross margin and debt/equity are derived at read time from the values
	// actually held, so a missing denominator stays null rather than zero.
	view.GrossProfitMargin = safeRatio(view.GrossProfit, view.Revenue)
	view.DebtToEquity = safeRatio(view.TotalDebt, view.TotalEquity)
	// Cash, operating profit, EBITDA, interest expense and debt are unknown at
	// Tier 1 — nil, never zero. Tier 2 fills them when depth has run.
	if price == nil || price.Close == nil {
		return view
	}
	view.PriceAsOf = price.TradingDay.Format("2006-01-02")
	close := *price.Close
	if st.EPS != nil && *st.EPS != 0 {
		per := close / *st.EPS
		view.PER = &per
	}
	if st.BookValue != nil && *st.BookValue != 0 {
		pbv := close / *st.BookValue
		view.PBV = &pbv
	}
	return view
}

// financialCoverage declares, per response field, what the returned statements
// actually hold. Carried fields are observed only when every returned row has a
// value; fields the snapshot does not carry are unknown by construction. The
// returned rows also carry the value itself (or null), so a partially observed
// field is both countable and visible.
func financialCoverage(views []FinancialStatementView) map[string]FinancialFieldCoverage {
	cov := make(map[string]FinancialFieldCoverage, 24)
	add := func(field string, value func(FinancialStatementView) *float64) {
		observed := 0
		for _, v := range views {
			if value(v) != nil {
				observed++
			}
		}
		status := FinancialCoverageUnknown
		if len(views) > 0 && observed == len(views) {
			status = FinancialCoverageObserved
		}
		cov[field] = FinancialFieldCoverage{
			Status:       status,
			ObservedRows: observed,
			ReturnedRows: len(views),
		}
	}
	add("revenue", func(v FinancialStatementView) *float64 { return v.Revenue })
	add("net_profit", func(v FinancialStatementView) *float64 { return v.NetProfit })
	add("net_profit_incl_nci", func(v FinancialStatementView) *float64 { return v.NetProfitInclNCI })
	add("total_assets", func(v FinancialStatementView) *float64 { return v.TotalAssets })
	add("total_equity", func(v FinancialStatementView) *float64 { return v.TotalEquity })
	add("eps", func(v FinancialStatementView) *float64 { return v.EPS })
	add("book_value", func(v FinancialStatementView) *float64 { return v.BookValue })
	add("roe", func(v FinancialStatementView) *float64 { return v.ROE })
	add("roa", func(v FinancialStatementView) *float64 { return v.ROA })
	add("npm", func(v FinancialStatementView) *float64 { return v.NPM })
	add("liabilities_to_equity", func(v FinancialStatementView) *float64 { return v.LiabilitiesToEquity })
	add("per", func(v FinancialStatementView) *float64 { return v.PER })
	add("pbv", func(v FinancialStatementView) *float64 { return v.PBV })

	// Tier-2 fields are coverage-aware: a bank's gross profit is
	// not_applicable, while a general-list issuer that simply omitted the line
	// stays unknown. A field is observed only when every returned row carries
	// it; the per-row value and its coverage are both visible.
	addDepth := func(field string, value func(FinancialStatementView) *float64) {
		observed := 0
		notApplicable := 0
		for _, v := range views {
			if value(v) != nil {
				observed++
				continue
			}
			if v.depthCoverage[field] == FinancialCoverageNotApplicable {
				notApplicable++
			}
		}
		status := FinancialCoverageUnknown
		switch {
		case len(views) > 0 && observed == len(views):
			status = FinancialCoverageObserved
		case len(views) > 0 && observed == 0 && notApplicable == len(views):
			status = FinancialCoverageNotApplicable
		}
		cov[field] = FinancialFieldCoverage{
			Status:       status,
			ObservedRows: observed,
			ReturnedRows: len(views),
		}
	}
	addDepth("gross_profit", func(v FinancialStatementView) *float64 { return v.GrossProfit })
	addDepth("operating_profit", func(v FinancialStatementView) *float64 { return v.OperatingProfit })
	addDepth("ebitda", func(v FinancialStatementView) *float64 { return v.EBITDA })
	addDepth("interest_expense", func(v FinancialStatementView) *float64 { return v.InterestExpense })
	addDepth("cash", func(v FinancialStatementView) *float64 { return v.Cash })
	addDepth("total_debt", func(v FinancialStatementView) *float64 { return v.TotalDebt })
	addDepth("gross_profit_margin", func(v FinancialStatementView) *float64 { return v.GrossProfitMargin })
	addDepth("debt_to_equity", func(v FinancialStatementView) *float64 { return v.DebtToEquity })
	for _, field := range []string{"short_term_borrowing", "long_term_borrowing"} {
		cov[field] = FinancialFieldCoverage{
			Status:       FinancialCoverageUnknown,
			ObservedRows: 0,
			ReturnedRows: len(views),
		}
	}

	priceAsOf := 0
	for _, v := range views {
		if v.PriceAsOf != "" {
			priceAsOf++
		}
	}
	status := FinancialCoverageUnknown
	if len(views) > 0 && priceAsOf == len(views) {
		status = FinancialCoverageObserved
	}
	cov["price_as_of"] = FinancialFieldCoverage{Status: status, ObservedRows: priceAsOf, ReturnedRows: len(views)}
	return cov
}

// sortStatementsNewestFirst orders rows by period end descending in place.
func sortStatementsNewestFirst(rows []entity.FinancialStatement) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && rows[j].PeriodEnd.After(rows[j-1].PeriodEnd); j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}

// rupiahFloat converts a raw-rupiah integer into the response's float64. The
// magnitudes (up to ~1e15) stay exact well inside float64's 2^53 integer
// range.
func rupiahFloat(v *int64) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}

// copyFloat copies a nullable ratio so the response never aliases a repository
// row.
func copyFloat(v *float64) *float64 {
	if v == nil {
		return nil
	}
	f := *v
	return &f
}

// safeRatio returns num/den when both are present and the denominator is
// non-zero; nil means "not computable", never zero.
func safeRatio(num, den *float64) *float64 {
	if num == nil || den == nil || *den == 0 {
		return nil
	}
	v := *num / *den
	return &v
}

// parseDepthCoverage decodes the stored per-field depth coverage. A malformed
// or absent document degrades to no coverage information (unknown), never to
// observed.
func parseDepthCoverage(raw *string) map[string]string {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return nil
	}
	var cov map[string]string
	if err := json.Unmarshal([]byte(*raw), &cov); err != nil {
		return nil
	}
	return cov
}

// financialStatementsDepthMissing reports whether the ticker's newest stored
// statement row still lacks usable Tier-2 depth. The depth coverage document is
// written by the depth store on every row it enriches, so its presence alone
// would mark a row that only carries unknown/not_applicable concepts; a usable
// value is at least one `observed` entry. Rows are newest-first.
func financialStatementsDepthMissing(rows []entity.FinancialStatement) bool {
	if len(rows) == 0 {
		return false
	}
	coverage := parseDepthCoverage(rows[0].DepthCoverage)
	if coverage == nil {
		return true
	}
	for _, state := range coverage {
		if state == FinancialCoverageObserved {
			return false
		}
	}
	return true
}

// compile-time check: the concrete repository satisfies the read seams.
var (
	_ FinancialStatementsReader = (*repository.FinancialsRepository)(nil)
	_ FinancialPriceReader      = (*repository.DailyPriceRepository)(nil)
)
