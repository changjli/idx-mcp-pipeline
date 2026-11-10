package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

type fakeDepthSource struct {
	classifyCalls int
	fetchCalls    int
	taxonomy      financials.Taxonomy
	method        string
	classifyErr   error
	statements    []financials.DepthStatement
	fetchErr      error
}

func (f *fakeDepthSource) ClassifyDepth(ctx context.Context, ticker string) (financials.Taxonomy, string, error) {
	f.classifyCalls++
	if f.classifyErr != nil {
		return "", "", f.classifyErr
	}
	return f.taxonomy, f.method, nil
}

func (f *fakeDepthSource) FetchDepth(ctx context.Context, ticker string, taxonomy financials.Taxonomy) ([]financials.DepthStatement, error) {
	f.fetchCalls++
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	out := make([]financials.DepthStatement, len(f.statements))
	copy(out, f.statements)
	return out, nil
}

type fakeDepthStore struct {
	tickers    []string
	taxonomies map[string]entity.TickerFinancialTaxonomy
	runs       map[string]entity.FinancialDepthRun
	applied    []entity.FinancialStatementDepth
	applyErr   error
}

func newFakeDepthStore() *fakeDepthStore {
	return &fakeDepthStore{
		taxonomies: map[string]entity.TickerFinancialTaxonomy{},
		runs:       map[string]entity.FinancialDepthRun{},
	}
}

func (f *fakeDepthStore) ListDepthSweepTickers(db *sqlx.DB, includeNotCovered bool) ([]string, error) {
	if includeNotCovered {
		return f.tickers, nil
	}
	out := make([]string, 0, len(f.tickers))
	for _, t := range f.tickers {
		if tax, ok := f.taxonomies[t]; ok && tax.DepthStatus == financials.DepthStatusNotCovered {
			continue
		}
		out = append(out, t)
	}
	return out, nil
}

func (f *fakeDepthStore) FindTaxonomy(db *sqlx.DB, ticker string) (*entity.TickerFinancialTaxonomy, error) {
	row, ok := f.taxonomies[ticker]
	if !ok {
		return nil, nil
	}
	return &row, nil
}

func (f *fakeDepthStore) UpsertTaxonomy(db *sqlx.DB, row *entity.TickerFinancialTaxonomy) error {
	f.taxonomies[row.Ticker] = *row
	return nil
}

func (f *fakeDepthStore) FindDepthRun(db *sqlx.DB, month time.Time, ticker string) (*entity.FinancialDepthRun, error) {
	row, ok := f.runs[depthRunKey(month, ticker)]
	if !ok {
		return nil, nil
	}
	return &row, nil
}

func (f *fakeDepthStore) UpsertDepthRun(db *sqlx.DB, row *entity.FinancialDepthRun) error {
	key := depthRunKey(row.RunMonth, row.Ticker)
	if prior, ok := f.runs[key]; ok {
		row.Attempts += prior.Attempts
	}
	f.runs[key] = *row
	return nil
}

func (f *fakeDepthStore) ApplyDepth(db *sqlx.DB, observedMonth time.Time, rows []entity.FinancialStatementDepth) (int, error) {
	if f.applyErr != nil {
		return 0, f.applyErr
	}
	f.applied = append(f.applied, rows...)
	return len(rows), nil
}

func depthRunKey(month time.Time, ticker string) string {
	return month.Format("2006-01") + "|" + ticker
}

type fakeDepthStatements struct {
	rows map[string][]entity.FinancialStatement
}

func (f *fakeDepthStatements) FindByTicker(db *sqlx.DB, ticker string) ([]entity.FinancialStatement, error) {
	return f.rows[ticker], nil
}

func depthPtr(v float64) *float64 { return &v }

func depthIntPtr(v int64) *int64 { return &v }

func depthStmt(ticker string, periodEnd string, duration int) entity.FinancialStatement {
	period := finDate(periodEnd)
	return entity.FinancialStatement{
		Ticker:          ticker,
		PeriodEnd:       period,
		FiscalYearEnd:   finDate(periodEnd[:4] + "-12-31"),
		DurationMonths:  duration,
		Revenue:         depthIntPtr(1000),
		NetProfitParent: depthIntPtr(100),
		Assets:          depthIntPtr(2000),
		Liabilities:     depthIntPtr(800),
		Equity:          depthIntPtr(1200),
	}
}

func depthSourceStatement(ticker, periodType, periodEnd string, values map[financials.Concept]float64) financials.DepthStatement {
	vals := make(map[financials.Concept]financials.Value, len(values))
	for c, v := range values {
		amt := v
		vals[c] = financials.Value{
			Amount: &amt, Currency: "IDR", Code: "TEST", Source: financials.ValueRestated,
			PeriodType: periodType, AsOfDate: finDate(periodEnd),
		}
	}
	return financials.DepthStatement{
		Ticker: ticker, Taxonomy: financials.TaxonomyGeneral154, PeriodType: periodType,
		PeriodEnd: finDate(periodEnd), Currency: "IDR", Values: vals,
		Methodology: financials.MethodologyMorningstar,
	}
}

func newDepthTestUseCase(source *fakeDepthSource, store *fakeDepthStore, statements *fakeDepthStatements) *FinancialDepthUseCase {
	return NewFinancialDepthUseCase(
		nil, logrus.New(), source, store, statements,
		WithDepthPacing(1, 0),
		WithDepthClock(func() time.Time { return finDate("2026-11-10") }),
	)
}

// TestFinancialDepth_EnrichTickerClassifiesAndPersists drives the happy path:
// classification is cached, one fetch produces two existing periods, and the
// ledger makes a repeat run of the same month a no-op.
func TestFinancialDepth_EnrichTickerClassifiesAndPersists(t *testing.T) {
	source := &fakeDepthSource{
		taxonomy: financials.TaxonomyGeneral154,
		method:   "discriminant_probe",
		statements: []financials.DepthStatement{
			depthSourceStatement("TESTD", financials.PeriodAnnual, "2025-12-31", map[financials.Concept]float64{
				financials.ConceptGrossProfit: 200,
				financials.ConceptCash:        100,
			}),
			depthSourceStatement("TESTD", financials.PeriodQuarterly, "2026-03-31", map[financials.Concept]float64{
				financials.ConceptGrossProfit: 50,
				financials.ConceptCash:        110,
			}),
			depthSourceStatement("TESTD", financials.PeriodQuarterly, "2026-06-30", map[financials.Concept]float64{
				financials.ConceptGrossProfit: 55,
				financials.ConceptCash:        120,
			}),
			depthSourceStatement("TESTD", financials.PeriodQuarterly, "2026-09-30", map[financials.Concept]float64{
				financials.ConceptGrossProfit: 60,
				financials.ConceptCash:        130,
			}),
			depthSourceStatement("TESTD", financials.PeriodTTM, "2026-09-30", map[financials.Concept]float64{
				financials.ConceptGrossProfit: 230,
			}),
		},
	}
	store := newFakeDepthStore()
	store.tickers = []string{"TESTD"}
	statements := &fakeDepthStatements{rows: map[string][]entity.FinancialStatement{
		"TESTD": {
			depthStmt("TESTD", "2026-09-30", 9),
			depthStmt("TESTD", "2026-06-30", 6),
			depthStmt("TESTD", "2026-03-31", 3),
			depthStmt("TESTD", "2025-12-31", 12),
		},
	}}
	uc := newDepthTestUseCase(source, store, statements)
	month := finDate("2026-11-01")

	res, err := uc.EnrichTicker(context.Background(), "testd", month, false)
	if err != nil {
		t.Fatalf("EnrichTicker: %v", err)
	}
	if res.Status != financials.DepthStatusObserved {
		t.Errorf("status = %q, want observed", res.Status)
	}
	if source.classifyCalls != 1 || source.fetchCalls != 1 {
		t.Errorf("calls classify=%d fetch=%d, want 1/1", source.classifyCalls, source.fetchCalls)
	}
	if len(store.applied) != 4 {
		t.Fatalf("applied rows = %d, want 4 existing periods", len(store.applied))
	}
	if store.applied[0].Ticker != "TESTD" {
		t.Errorf("applied ticker = %q", store.applied[0].Ticker)
	}
	tax := store.taxonomies["TESTD"]
	if tax.Taxonomy != string(financials.TaxonomyGeneral154) || tax.DepthStatus != financials.DepthStatusObserved {
		t.Errorf("taxonomy = %+v", tax)
	}
	run := store.runs[depthRunKey(month, "TESTD")]
	if run.Status != financials.DepthStatusObserved {
		t.Errorf("run = %+v, want observed", run)
	}

	// A repeat run of the same month is a no-op: the ledger is terminal.
	beforeFetch := source.fetchCalls
	res, err = uc.EnrichTicker(context.Background(), "TESTD", month, false)
	if err != nil {
		t.Fatalf("repeat EnrichTicker: %v", err)
	}
	if !res.Skipped || source.fetchCalls != beforeFetch {
		t.Errorf("repeat run was not skipped (skipped=%t fetch_calls=%d)", res.Skipped, source.fetchCalls)
	}
}

// TestFinancialDepth_NotCoveredAndLagging pins the two non-success states: a
// ticker with no Yahoo data is recorded not_covered (and excluded from the
// sweep), while a ticker whose Yahoo latest period is older than Tier 1 is
// marked yahoo_lagging and retried next run.
func TestFinancialDepth_NotCoveredAndLagging(t *testing.T) {
	store := newFakeDepthStore()
	statements := &fakeDepthStatements{rows: map[string][]entity.FinancialStatement{
		"NOS": {depthStmt("NOS", "2026-09-30", 9)},
		"LAG": {depthStmt("LAG", "2026-09-30", 9)},
	}}
	uc := newDepthTestUseCase(&fakeDepthSource{taxonomy: financials.TaxonomyOther}, store, statements)
	month := finDate("2026-11-01")

	res, err := uc.EnrichTicker(context.Background(), "NOS", month, false)
	if err != nil {
		t.Fatalf("not_covered: %v", err)
	}
	if res.Status != financials.DepthStatusNotCovered {
		t.Errorf("status = %q, want not_covered", res.Status)
	}
	if store.taxonomies["NOS"].DepthStatus != financials.DepthStatusNotCovered {
		t.Errorf("taxonomy = %+v, want not_covered", store.taxonomies["NOS"])
	}

	store.tickers = []string{"NOS", "LAG"}
	lagSource := &fakeDepthSource{
		taxonomy: financials.TaxonomyGeneral154,
		statements: []financials.DepthStatement{
			depthSourceStatement("LAG", financials.PeriodQuarterly, "2026-06-30", map[financials.Concept]float64{
				financials.ConceptGrossProfit: 50,
			}),
		},
	}
	uc = newDepthTestUseCase(lagSource, store, statements)
	res, err = uc.EnrichTicker(context.Background(), "LAG", month, false)
	if err != nil {
		t.Fatalf("lagging: %v", err)
	}
	if res.Status != financials.DepthStatusYahooLagging || !res.Lagging {
		t.Errorf("result = %+v, want yahoo_lagging", res)
	}
	beforeFetch := lagSource.fetchCalls
	if _, err := uc.EnrichTicker(context.Background(), "LAG", month, false); err != nil {
		t.Fatalf("lagging retry: %v", err)
	}
	if lagSource.fetchCalls != beforeFetch+1 {
		t.Errorf("yahoo_lagging was not retried (fetch calls %d -> %d)", beforeFetch, lagSource.fetchCalls)
	}

	// The sweep excludes the not_covered ticker but processes LAG again.
	store.runs = map[string]entity.FinancialDepthRun{}
	sweep, err := uc.Sweep(context.Background(), month, false)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if sweep.Tickers != 1 || sweep.TickersResults[0].Ticker != "LAG" {
		t.Errorf("sweep results = %+v, want only LAG", sweep.TickersResults)
	}
}

// TestFinancialDepth_FetchFailureLeavesPriorData verifies the failure contract:
// a failed fetch records failed, writes no depth row, and leaves whatever the
// store already held intact.
func TestFinancialDepth_FetchFailureLeavesPriorData(t *testing.T) {
	store := newFakeDepthStore()
	prior := entity.FinancialStatementDepth{Ticker: "FAIL", PeriodEnd: finDate("2026-09-30")}
	store.applied = append(store.applied, prior)
	statements := &fakeDepthStatements{rows: map[string][]entity.FinancialStatement{
		"FAIL": {depthStmt("FAIL", "2026-09-30", 9)},
	}}
	source := &fakeDepthSource{fetchErr: errors.New("yahoo: http 503")}
	uc := newDepthTestUseCase(source, store, statements)
	month := finDate("2026-11-01")

	_, err := uc.EnrichTicker(context.Background(), "FAIL", month, false)
	if err == nil {
		t.Fatal("expected the fetch error")
	}
	if len(store.applied) != 1 || store.applied[0].Ticker != "FAIL" {
		t.Errorf("prior depth rows were changed: %+v", store.applied)
	}
	run := store.runs[depthRunKey(month, "FAIL")]
	if run.Status != financials.DepthStatusFailed || run.Error == nil {
		t.Errorf("run = %+v, want failed with an error", run)
	}
}

// TestFinancialDepth_ClassificationFailureDoesNotPersistTaxonomy pins that a
// probe failure is retried on the next run instead of writing a bogus `other`
// classification that later runs would trust.
func TestFinancialDepth_ClassificationFailureDoesNotPersistTaxonomy(t *testing.T) {
	store := newFakeDepthStore()
	statements := &fakeDepthStatements{rows: map[string][]entity.FinancialStatement{
		"CLS": {depthStmt("CLS", "2026-09-30", 9)},
	}}
	source := &fakeDepthSource{classifyErr: errors.New("yahoo: http 429")}
	uc := newDepthTestUseCase(source, store, statements)

	_, err := uc.EnrichTicker(context.Background(), "CLS", finDate("2026-11-01"), false)
	if err == nil {
		t.Fatal("expected the classification error")
	}
	if _, ok := store.taxonomies["CLS"]; ok {
		t.Errorf("classification failure persisted taxonomy: %+v", store.taxonomies["CLS"])
	}
	run := store.runs[depthRunKey(finDate("2026-11-01"), "CLS")]
	if run.Status != financials.DepthStatusFailed {
		t.Errorf("run = %+v, want failed", run)
	}
}
