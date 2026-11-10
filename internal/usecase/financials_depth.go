package usecase

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// YahooFinancialsSource is the source_status key for Tier-2 depth. It is
// separate from idx_financials so the standard data_stale / last_good_date
// envelope reports depth freshness as well as Tier-1 breadth.
const YahooFinancialsSource = "yahoo:financials"

// YahooFinancialsMaxAgeSeconds is the depth freshness window. The source is
// monthly (Yahoo's statement updates lag the IDX filing by weeks); 45 days lets
// one publication slip without flagging stale while a two-month gap is visible.
const YahooFinancialsMaxAgeSeconds int32 = 45 * 86400

// FinancialDepthSource is the only network seam of the depth enrichment.
// *yahoo.Client satisfies it through the adapter methods in
// internal/yahoo/depth.go; tests use a fake and never touch the network.
type FinancialDepthSource interface {
	ClassifyDepth(ctx context.Context, ticker string) (financials.Taxonomy, string, error)
	FetchDepth(ctx context.Context, ticker string, taxonomy financials.Taxonomy) ([]financials.DepthStatement, error)
}

// FinancialDepthStore is the store seam for the depth sweep.
// *repository.FinancialDepthRepository satisfies it.
type FinancialDepthStore interface {
	ListDepthSweepTickers(db *sqlx.DB, includeNotCovered bool) ([]string, error)
	FindTaxonomy(db *sqlx.DB, ticker string) (*entity.TickerFinancialTaxonomy, error)
	UpsertTaxonomy(db *sqlx.DB, row *entity.TickerFinancialTaxonomy) error
	FindDepthRun(db *sqlx.DB, month time.Time, ticker string) (*entity.FinancialDepthRun, error)
	UpsertDepthRun(db *sqlx.DB, row *entity.FinancialDepthRun) error
	ApplyDepth(db *sqlx.DB, observedMonth time.Time, rows []entity.FinancialStatementDepth) (int, error)
}

// FinancialDepthUseCase owns the monthly Tier-2 sweep: classify the covered
// set (once), fetch each ticker's 12M/3M/TTM statements in one small call, and
// apply the depth update to the existing Tier-1 rows. It is never called from
// the MCP request path.
type FinancialDepthUseCase struct {
	DB         *sqlx.DB
	Log        *logrus.Logger
	Source     FinancialDepthSource
	Repo       FinancialDepthStore
	Statements FinancialStatementsReader

	// Now is the clock used for classification/fetch timestamps.
	Now func() time.Time
	// Concurrency is the number of tickers in flight (default 4).
	Concurrency int
	// Spacing is the per-worker delay between ticker fetches (default 800ms).
	Spacing time.Duration
	// FallbackFXRate is used only when a non-IDR row has no Tier-1 value to
	// imply the conversion rate from. The rate and date are always recorded.
	FallbackFXRate float64
}

// FinancialDepthOption configures the depth usecase.
type FinancialDepthOption func(*FinancialDepthUseCase)

// WithDepthPacing sets the sweep's worker count and per-worker spacing.
func WithDepthPacing(concurrency int, spacing time.Duration) FinancialDepthOption {
	return func(uc *FinancialDepthUseCase) {
		if concurrency > 0 {
			uc.Concurrency = concurrency
		}
		if spacing >= 0 {
			uc.Spacing = spacing
		}
	}
}

// WithDepthFallbackFXRate sets the last-resort rate for non-IDR issuers.
func WithDepthFallbackFXRate(rate float64) FinancialDepthOption {
	return func(uc *FinancialDepthUseCase) {
		if rate > 0 {
			uc.FallbackFXRate = rate
		}
	}
}

// WithDepthClock injects the clock.
func WithDepthClock(now func() time.Time) FinancialDepthOption {
	return func(uc *FinancialDepthUseCase) {
		if now != nil {
			uc.Now = now
		}
	}
}

// NewFinancialDepthUseCase wires the monthly depth sweep.
func NewFinancialDepthUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	source FinancialDepthSource,
	repo FinancialDepthStore,
	statements FinancialStatementsReader,
	opts ...FinancialDepthOption,
) *FinancialDepthUseCase {
	uc := &FinancialDepthUseCase{
		DB:             db,
		Log:            log,
		Source:         source,
		Repo:           repo,
		Statements:     statements,
		Now:            time.Now,
		Concurrency:    4,
		Spacing:        800 * time.Millisecond,
		FallbackFXRate: financials.DefaultUSDIDRRate,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(uc)
		}
	}
	return uc
}

// FinancialDepthTickerResult is one ticker's outcome in a sweep.
type FinancialDepthTickerResult struct {
	Ticker        string
	Status        string
	Taxonomy      financials.Taxonomy
	RowsWritten   int
	YahooLatest   string
	TierOneLatest string
	Lagging       bool
	Skipped       bool
	Err           error
}

// FinancialDepthSweepResult summarizes one monthly sweep.
type FinancialDepthSweepResult struct {
	Month          string
	Tickers        int
	Processed      int
	Skipped        int
	Observed       int
	Partial        int
	NotCovered     int
	YahooLagging   int
	Failed         int
	RowsWritten    int
	LatestYahoo    string
	TickersResults []FinancialDepthTickerResult
}

// EnrichTicker classifies (if needed), fetches and applies one ticker's depth
// for a run month. A terminal ledger row (observed/partial/not_covered) makes
// the call a no-op unless force is set. A failed fetch or apply leaves the
// prior depth columns intact and records the error; the next run retries.
func (uc *FinancialDepthUseCase) EnrichTicker(ctx context.Context, ticker string, month time.Time, force bool) (*FinancialDepthTickerResult, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	month = financials.FirstOfMonth(month)
	res := &FinancialDepthTickerResult{Ticker: ticker}

	run, err := uc.Repo.FindDepthRun(uc.DB, month, ticker)
	if err != nil {
		return nil, err
	}
	if run != nil && !force && isTerminalDepthStatus(run.Status) {
		res.Status = run.Status
		res.Skipped = true
		return res, nil
	}

	rows, err := uc.Statements.FindByTicker(uc.DB, ticker)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		res.Skipped = true
		return res, nil
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].PeriodEnd.After(rows[j].PeriodEnd) })
	tierOneLatest := rows[0].PeriodEnd
	res.TierOneLatest = tierOneLatest.Format("2006-01-02")

	taxonomy, source, err := uc.taxonomyFor(ctx, ticker)
	if err != nil {
		uc.recordFailure(ticker, month, "discriminant_probe", err)
		res.Status = financials.DepthStatusFailed
		res.Err = err
		return res, err
	}
	res.Taxonomy = taxonomy

	statements, err := uc.Source.FetchDepth(ctx, ticker, taxonomy)
	if err != nil {
		uc.recordFailure(ticker, month, source, err)
		res.Status = financials.DepthStatusFailed
		res.Err = err
		return res, err
	}
	if len(statements) == 0 {
		res.Status = financials.DepthStatusNotCovered
		uc.recordTickerStatus(ticker, month, taxonomy, source, financials.DepthStatusNotCovered, 0, nil)
		return res, nil
	}

	latestYahoo := latestDepthPeriod(statements)
	res.YahooLatest = latestYahoo.Format("2006-01-02")
	res.Lagging = latestYahoo.Before(tierOneLatest)

	periods := tierOnePeriods(rows)
	prepared := financials.BuildDepthRows(
		ticker, taxonomy, statements, periods, uc.FallbackFXRate, month,
	)
	depthRows := make([]entity.FinancialStatementDepth, 0, len(prepared))
	now := uc.Now()
	for _, row := range prepared {
		depthRows = append(depthRows, depthEntity(ticker, row, month, now))
	}

	written, err := uc.Repo.ApplyDepth(uc.DB, month, depthRows)
	if err != nil {
		uc.recordFailure(ticker, month, source, err)
		res.Status = financials.DepthStatusFailed
		res.Err = err
		return res, err
	}
	res.RowsWritten = written

	status := financials.DepthStatusObserved
	if len(prepared) == 0 {
		status = financials.DepthStatusPartial
	}
	if res.Lagging {
		status = financials.DepthStatusYahooLagging
	}
	res.Status = status
	uc.recordTickerStatus(ticker, month, taxonomy, source, status, written, nil)
	return res, nil
}

// Sweep walks the Tier-1 universe for one run month with a bounded worker pool.
// Each ticker's ledger row is written before moving on, so a killed run resumes
// from the first unprocessed ticker. Terminal rows are skipped on a repeat run.
func (uc *FinancialDepthUseCase) Sweep(ctx context.Context, month time.Time, includeNotCovered bool) (*FinancialDepthSweepResult, error) {
	month = financials.FirstOfMonth(month)
	tickers, err := uc.Repo.ListDepthSweepTickers(uc.DB, includeNotCovered)
	if err != nil {
		return nil, err
	}
	res := &FinancialDepthSweepResult{
		Month:          month.Format("2006-01"),
		Tickers:        len(tickers),
		TickersResults: make([]FinancialDepthTickerResult, len(tickers)),
	}
	uc.Log.Infof("financial depth: sweeping %d ticker(s) for %s (concurrency=%d, spacing=%s)",
		len(tickers), res.Month, uc.Concurrency, uc.Spacing)

	workers := uc.Concurrency
	if workers < 1 {
		workers = 1
	}
	if workers > len(tickers) && len(tickers) > 0 {
		workers = len(tickers)
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	var gate <-chan time.Time
	var ticker *time.Ticker
	if uc.Spacing > 0 {
		ticker = time.NewTicker(uc.Spacing)
		defer ticker.Stop()
		gate = ticker.C
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := uc.waitPace(ctx, gate); err != nil {
					return
				}
				result, err := uc.EnrichTicker(ctx, tickers[idx], month, includeNotCovered)
				if result == nil {
					result = &FinancialDepthTickerResult{Ticker: tickers[idx], Status: financials.DepthStatusFailed, Err: err}
				}
				res.TickersResults[idx] = *result
			}
		}()
	}
	for i := range tickers {
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return res, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()

	var latestYahoo time.Time
	for _, r := range res.TickersResults {
		if r.Ticker == "" {
			continue // the run was cancelled before this worker started
		}
		if r.Skipped {
			res.Skipped++
			continue
		}
		res.Processed++
		switch r.Status {
		case financials.DepthStatusObserved:
			res.Observed++
		case financials.DepthStatusPartial:
			res.Partial++
		case financials.DepthStatusNotCovered:
			res.NotCovered++
		case financials.DepthStatusYahooLagging:
			res.YahooLagging++
		default:
			res.Failed++
		}
		res.RowsWritten += r.RowsWritten
		if r.YahooLatest != "" {
			if t, err := time.Parse("2006-01-02", r.YahooLatest); err == nil && t.After(latestYahoo) {
				latestYahoo = t
			}
		}
	}
	if !latestYahoo.IsZero() {
		res.LatestYahoo = latestYahoo.Format("2006-01-02")
	}
	uc.Log.Infof("financial depth: sweep %s complete: processed=%d skipped=%d observed=%d partial=%d not_covered=%d yahoo_lagging=%d failed=%d rows_written=%d",
		res.Month, res.Processed, res.Skipped, res.Observed, res.Partial,
		res.NotCovered, res.YahooLagging, res.Failed, res.RowsWritten)
	return res, nil
}

func (uc *FinancialDepthUseCase) taxonomyFor(ctx context.Context, ticker string) (financials.Taxonomy, string, error) {
	stored, err := uc.Repo.FindTaxonomy(uc.DB, ticker)
	if err != nil {
		return "", "", err
	}
	if stored != nil {
		taxonomy := financials.Taxonomy(stored.Taxonomy)
		if taxonomy.Valid() {
			return taxonomy, stored.Source, nil
		}
	}
	taxonomy, method, err := uc.Source.ClassifyDepth(ctx, ticker)
	if err != nil {
		return "", "", err
	}
	now := uc.Now()
	row := &entity.TickerFinancialTaxonomy{
		Ticker:       ticker,
		Taxonomy:     string(taxonomy),
		DiscoveredAt: now,
		Source:       method,
		DepthStatus:  financials.DepthStatusPartial,
	}
	if err := uc.Repo.UpsertTaxonomy(uc.DB, row); err != nil {
		return "", "", err
	}
	return taxonomy, method, nil
}

func (uc *FinancialDepthUseCase) recordFailure(ticker string, month time.Time, source string, cause error) {
	errText := cause.Error()
	// Only update an existing classification. A classification failure must not
	// persist a bogus `other` row that the next run would trust; leaving it
	// absent makes the next run retry the discriminant probe.
	if stored, err := uc.Repo.FindTaxonomy(uc.DB, ticker); err != nil {
		uc.Log.Errorf("financial depth: read taxonomy on failure %s: %v", ticker, err)
	} else if stored != nil {
		stored.DepthStatus = financials.DepthStatusFailed
		stored.LastDepthMonth = &month
		stored.LastError = &errText
		if source != "" {
			stored.Source = source
		}
		if err := uc.Repo.UpsertTaxonomy(uc.DB, stored); err != nil {
			uc.Log.Errorf("financial depth: record taxonomy failure %s: %v", ticker, err)
		}
	}
	uc.upsertRun(ticker, month, financials.DepthStatusFailed, 0, cause)
}

func (uc *FinancialDepthUseCase) recordTickerStatus(ticker string, month time.Time, taxonomy financials.Taxonomy, source, status string, rows int, cause error) {
	now := uc.Now()
	row := &entity.TickerFinancialTaxonomy{
		Ticker:         ticker,
		Taxonomy:       string(taxonomy),
		DiscoveredAt:   now,
		Source:         source,
		DepthStatus:    status,
		LastDepthMonth: &month,
	}
	if cause != nil {
		errText := cause.Error()
		row.LastError = &errText
	}
	if err := uc.Repo.UpsertTaxonomy(uc.DB, row); err != nil {
		uc.Log.Errorf("financial depth: record taxonomy %s: %v", ticker, err)
	}
	uc.upsertRun(ticker, month, status, rows, cause)
}

func (uc *FinancialDepthUseCase) upsertRun(ticker string, month time.Time, status string, rows int, cause error) {
	run := &entity.FinancialDepthRun{
		RunMonth:    month,
		Ticker:      ticker,
		Status:      status,
		RowsWritten: rows,
		Attempts:    1,
	}
	if cause != nil {
		errText := cause.Error()
		run.Error = &errText
	}
	if err := uc.Repo.UpsertDepthRun(uc.DB, run); err != nil {
		uc.Log.Errorf("financial depth: record run %s %s: %v", month.Format("2006-01"), ticker, err)
	}
}

func (uc *FinancialDepthUseCase) waitPace(ctx context.Context, gate <-chan time.Time) error {
	if gate == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate:
		return nil
	}
}

func isTerminalDepthStatus(status string) bool {
	switch status {
	case financials.DepthStatusObserved, financials.DepthStatusPartial, financials.DepthStatusNotCovered:
		return true
	}
	return false
}

func latestDepthPeriod(statements []financials.DepthStatement) time.Time {
	var latest time.Time
	for _, st := range statements {
		if st.PeriodEnd.After(latest) {
			latest = st.PeriodEnd
		}
	}
	return latest
}

func tierOnePeriods(rows []entity.FinancialStatement) []financials.TierOneDepthPeriod {
	out := make([]financials.TierOneDepthPeriod, 0, len(rows))
	for _, row := range rows {
		out = append(out, financials.TierOneDepthPeriod{
			PeriodEnd:       row.PeriodEnd,
			FiscalYearEnd:   row.FiscalYearEnd,
			DurationMonths:  row.DurationMonths,
			Revenue:         int64ToFloat(row.Revenue),
			NetProfitParent: int64ToFloat(row.NetProfitParent),
			Assets:          int64ToFloat(row.Assets),
			Liabilities:     int64ToFloat(row.Liabilities),
			Equity:          int64ToFloat(row.Equity),
		})
	}
	return out
}

func int64ToFloat(v *int64) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}

func depthEntity(ticker string, row financials.DepthRow, month, now time.Time) entity.FinancialStatementDepth {
	coverage, _ := json.Marshal(row.Coverage)
	raw, _ := json.Marshal(row.Raw)
	conflicts, _ := json.Marshal(row.Conflicts)
	currency := row.Currency
	basis := row.PeriodBasis
	methodology := row.Methodology
	status := row.Status
	hash := row.ContentHash
	snapshot := row.SnapshotJSON
	fetchedAt := now
	observedMonth := month
	return entity.FinancialStatementDepth{
		Ticker:             ticker,
		PeriodEnd:          row.PeriodEnd,
		Cash:               row.Values[financials.DepthCash],
		GrossProfit:        row.Values[financials.DepthGrossProfit],
		OperatingProfit:    row.Values[financials.DepthOperatingProfit],
		EBITDA:             row.Values[financials.DepthEBITDA],
		CFO:                row.Values[financials.DepthCFO],
		CapEx:              row.Values[financials.DepthCapEx],
		FCF:                row.Values[financials.DepthFCF],
		TotalDebt:          row.Values[financials.DepthTotalDebt],
		InterestExpense:    row.Values[financials.DepthInterestExpense],
		CashQ:              row.Quarters[financials.DepthCash],
		GrossProfitQ:       row.Quarters[financials.DepthGrossProfit],
		OperatingProfitQ:   row.Quarters[financials.DepthOperatingProfit],
		EBITDAq:            row.Quarters[financials.DepthEBITDA],
		CFOQ:               row.Quarters[financials.DepthCFO],
		CapExQ:             row.Quarters[financials.DepthCapEx],
		FCFQ:               row.Quarters[financials.DepthFCF],
		TotalDebtQ:         row.Quarters[financials.DepthTotalDebt],
		InterestExpenseQ:   row.Quarters[financials.DepthInterestExpense],
		CashTTM:            row.TTM[financials.DepthCash],
		GrossProfitTTM:     row.TTM[financials.DepthGrossProfit],
		OperatingProfitTTM: row.TTM[financials.DepthOperatingProfit],
		EBITDATTM:          row.TTM[financials.DepthEBITDA],
		CFOTTM:             row.TTM[financials.DepthCFO],
		CapExTTM:           row.TTM[financials.DepthCapEx],
		FCFTTM:             row.TTM[financials.DepthFCF],
		TotalDebtTTM:       row.TTM[financials.DepthTotalDebt],
		InterestExpenseTTM: row.TTM[financials.DepthInterestExpense],
		Currency:           &currency,
		FXRate:             &row.FXRate,
		FXDate:             &row.FXDate,
		PeriodBasis:        &basis,
		Methodology:        &methodology,
		DepthCoverage:      strPtr(string(coverage)),
		DepthRaw:           strPtr(string(raw)),
		DepthConflicts:     strPtr(string(conflicts)),
		DepthStatus:        &status,
		DepthFetchedAt:     &fetchedAt,
		DepthContentHash:   &hash,
		ObservedMonth:      &observedMonth,
		DepthSnapshot:      &snapshot,
	}
}

// compile-time check: the concrete depth store satisfies the seam. The source
// check lives in internal/yahoo/depth.go, where the adapter methods are defined.
var _ FinancialDepthStore = (*repository.FinancialDepthRepository)(nil)
