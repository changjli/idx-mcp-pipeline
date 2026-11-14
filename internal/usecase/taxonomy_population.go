package usecase

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TaxonomySource is the classify-only seam: one discriminant probe per ticker,
// no statement fetch. *yahoo.Client satisfies it through the adapter in
// cmd/financials-taxonomy; tests use a fake.
type TaxonomySource interface {
	ClassifyTaxonomy(ctx context.Context, ticker string) (financials.Taxonomy, string, map[string]bool, error)
}

// TaxonomyStore is the write seam. *repository.FinancialDepthRepository
// satisfies it. SeedTaxonomy writes the classification only: it preserves the
// depth state the monthly sweep owns, so re-running the population never resets
// an observed ticker to pending.
type TaxonomyStore interface {
	SeedTaxonomy(db *sqlx.DB, row *entity.TickerFinancialTaxonomy) error
}

// TaxonomyResult is one ticker's outcome. Evidence is the probe's
// code -> populated map, retained so the CSV artifact explains the decision.
type TaxonomyResult struct {
	Ticker   string
	Taxonomy financials.Taxonomy
	Method   string
	Evidence map[string]bool
	Applied  bool
	Err      error
}

// TaxonomyPopulation walks a ticker list with the discriminant probe and
// optionally seeds ticker_financial_taxonomy. It deliberately does not fetch
// statements or touch the depth columns: ticket 06 owns the classification
// population, ticket 05's sweep owns depth_status and the depth values.
type TaxonomyPopulation struct {
	DB     *sqlx.DB
	Log    *logrus.Logger
	Source TaxonomySource
	Store  TaxonomyStore

	// Now is the classification timestamp written to discovered_at.
	Now func() time.Time
	// Concurrency is the number of probes in flight (default 4).
	Concurrency int
	// Spacing is the shared delay between probes (default 800ms), matching the
	// depth sweep's pacing so a population run and a sweep look the same to
	// Yahoo.
	Spacing time.Duration
}

// TaxonomyPopulationOption configures the population run.
type TaxonomyPopulationOption func(*TaxonomyPopulation)

// WithTaxonomyPacing sets the worker count and shared probe spacing.
func WithTaxonomyPacing(concurrency int, spacing time.Duration) TaxonomyPopulationOption {
	return func(uc *TaxonomyPopulation) {
		if concurrency > 0 {
			uc.Concurrency = concurrency
		}
		if spacing >= 0 {
			uc.Spacing = spacing
		}
	}
}

// WithTaxonomyClock injects the clock used for discovered_at.
func WithTaxonomyClock(now func() time.Time) TaxonomyPopulationOption {
	return func(uc *TaxonomyPopulation) {
		if now != nil {
			uc.Now = now
		}
	}
}

// NewTaxonomyPopulation wires the classify-only population.
func NewTaxonomyPopulation(
	db *sqlx.DB,
	log *logrus.Logger,
	source TaxonomySource,
	store TaxonomyStore,
	opts ...TaxonomyPopulationOption,
) *TaxonomyPopulation {
	uc := &TaxonomyPopulation{
		DB:          db,
		Log:         log,
		Source:      source,
		Store:       store,
		Now:         time.Now,
		Concurrency: 4,
		Spacing:     800 * time.Millisecond,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(uc)
		}
	}
	return uc
}

// Populate classifies every ticker and, when apply is true, upserts the
// confirmed taxonomies. A probe that resolves to "other" is reported in the
// results but never persisted: "other" means no discriminant code was found,
// which is indistinguishable from a transient empty probe, and persisting it
// would stop the monthly sweep from re-probing. The evidence CSV keeps the
// row, so a human can still see and act on it.
//
// Per-ticker failures are recorded on the result and do not abort the run; the
// returned error is only the context cancellation (or a caller-level failure).
func (uc *TaxonomyPopulation) Populate(ctx context.Context, tickers []string, apply bool) ([]TaxonomyResult, error) {
	tickers = uniqueTickers(tickers)
	results := make([]TaxonomyResult, len(tickers))
	if len(tickers) == 0 {
		return results, nil
	}

	workers := uc.Concurrency
	if workers < 1 {
		workers = 1
	}
	if workers > len(tickers) {
		workers = len(tickers)
	}

	uc.Log.Infof("financial taxonomy: classifying %d ticker(s) (apply=%t, concurrency=%d, spacing=%s)",
		len(tickers), apply, workers, uc.Spacing)

	jobs := make(chan int)
	var wg sync.WaitGroup
	var gate <-chan time.Time
	var pace *time.Ticker
	if uc.Spacing > 0 {
		pace = time.NewTicker(uc.Spacing)
		defer pace.Stop()
		gate = pace.C
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if ctx.Err() != nil {
					return
				}
				if !uc.waitPace(ctx, gate) {
					return
				}
				results[idx] = uc.classifyOne(ctx, tickers[idx], apply)
			}
		}()
	}

	for i := range tickers {
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return results, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return results, err
	}
	uc.logSummary(results, apply)
	return results, nil
}

func (uc *TaxonomyPopulation) classifyOne(ctx context.Context, ticker string, apply bool) TaxonomyResult {
	res := TaxonomyResult{Ticker: ticker}
	taxonomy, method, evidence, err := uc.Source.ClassifyTaxonomy(ctx, ticker)
	if err != nil {
		res.Err = err
		return res
	}
	res.Taxonomy = taxonomy
	res.Method = method
	res.Evidence = evidence
	if !apply {
		return res
	}
	return uc.persist(res)
}

// ApplyResults persists an already-classified evidence set without touching the
// network. The operator path is: classify (dry run) -> review the CSV ->
// classify --from-csv ... --apply --yes. A failed upsert is recorded on the
// result and does not abort the rest of the batch.
func (uc *TaxonomyPopulation) ApplyResults(ctx context.Context, results []TaxonomyResult) ([]TaxonomyResult, error) {
	out := make([]TaxonomyResult, len(results))
	for i, r := range results {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		out[i] = uc.persist(r)
	}
	uc.logSummary(out, true)
	return out, nil
}

// persist writes one confirmed classification. "other" is never persisted: it
// means no discriminant code was found, which is indistinguishable from a
// transient empty probe, and would stop the monthly sweep re-probing. A caller
// that wants the four-value enum stored can do it deliberately outside this
// path.
func (uc *TaxonomyPopulation) persist(res TaxonomyResult) TaxonomyResult {
	res.Applied = false
	if res.Err != nil || res.Taxonomy == "" || res.Taxonomy == financials.TaxonomyOther {
		return res
	}
	row := &entity.TickerFinancialTaxonomy{
		Ticker:       res.Ticker,
		Taxonomy:     string(res.Taxonomy),
		DiscoveredAt: uc.Now(),
		Source:       res.Method,
		// 05 owns depth_status. A classification probe proves the template, not
		// that depth was observed or is fresh, so the sweep still measures it.
		DepthStatus: financials.DepthStatusPending,
	}
	if err := uc.Store.SeedTaxonomy(uc.DB, row); err != nil {
		res.Err = err
		return res
	}
	res.Applied = true
	return res
}

func (uc *TaxonomyPopulation) waitPace(ctx context.Context, gate <-chan time.Time) bool {
	if gate == nil {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-gate:
		return true
	}
}

func (uc *TaxonomyPopulation) logSummary(results []TaxonomyResult, apply bool) {
	var general, bank, insurance, other, failed, applied int
	for _, r := range results {
		if r.Err != nil {
			failed++
			continue
		}
		switch r.Taxonomy {
		case financials.TaxonomyGeneral154:
			general++
		case financials.TaxonomyBank108:
			bank++
		case financials.TaxonomyInsurance74:
			insurance++
		default:
			other++
		}
		if r.Applied {
			applied++
		}
	}
	uc.Log.Infof("financial taxonomy: done: general=%d bank=%d insurance=%d other=%d failed=%d applied=%d (apply=%t)",
		general, bank, insurance, other, failed, applied, apply)
}

// uniqueTickers normalizes and deduplicates a ticker list. It accepts PTBA or
// PTBA.JK, uppercases, trims, and drops blanks, preserving first-seen order.
func uniqueTickers(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		t := strings.ToUpper(strings.TrimSpace(raw))
		t = strings.TrimSuffix(t, ".JK")
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// The concrete depth store satisfies the taxonomy write seam; the same check
// as FinancialDepthStore in financials_depth.go.
var _ TaxonomyStore = (*repository.FinancialDepthRepository)(nil)
