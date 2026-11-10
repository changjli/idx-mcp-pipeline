package yahoo

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

// FetchOption configures one statement fetch.
type FetchOption func(*fetchOptions)

type fetchOptions struct {
	taxonomy    financials.Taxonomy
	periodTypes []string
}

// WithTaxonomy supplies an already-discovered taxonomy, skipping the probe.
// Ticket 05 will pass the cached classification from ticker_financial_taxonomy.
func WithTaxonomy(t financials.Taxonomy) FetchOption {
	return func(o *fetchOptions) { o.taxonomy = t }
}

// WithPeriodTypes limits the fetch to the given Yahoo period types (12M, 3M,
// TTM). The default requests all three; an invalid type is ignored.
func WithPeriodTypes(periods ...string) FetchOption {
	return func(o *fetchOptions) {
		if len(periods) == 0 {
			return
		}
		o.periodTypes = o.periodTypes[:0]
		for _, p := range periods {
			switch p {
			case financials.PeriodAnnual, financials.PeriodQuarterly, financials.PeriodTTM:
				o.periodTypes = append(o.periodTypes, p)
			}
		}
	}
}

// FetchStatements classifies the ticker (unless a taxonomy was supplied),
// requests the core concept codes for the requested period types, and returns
// one normalized DepthStatement per (period type, period end).
//
// It performs at most two small API calls per ticker: the discriminant probe
// when no taxonomy is cached, then the concept fetch. It never fetches the
// ~1 MB statement page; DiscoverTypeList is the explicit discovery path.
//
// A caller that needs the classification even when the ticker has no facts
// (ticket 05 records not_covered tickers) should call Classify first and pass
// WithTaxonomy, rather than relying on the statements' Taxonomy field.
func (c *Client) FetchStatements(ctx context.Context, ticker string, opts ...FetchOption) ([]financials.DepthStatement, error) {
	o := fetchOptions{periodTypes: []string{financials.PeriodAnnual, financials.PeriodQuarterly, financials.PeriodTTM}}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if len(o.periodTypes) == 0 {
		o.periodTypes = []string{financials.PeriodAnnual, financials.PeriodQuarterly, financials.PeriodTTM}
	}

	taxonomy := o.taxonomy
	if !taxonomy.Valid() {
		cls, err := c.Classify(ctx, ticker)
		if err != nil {
			return nil, err
		}
		taxonomy = cls.Taxonomy
	}

	types := make([]string, 0, len(ConceptCodes())*len(o.periodTypes))
	for _, code := range ConceptCodes() {
		for _, period := range o.periodTypes {
			types = append(types, code+"_"+period)
		}
	}

	res, err := c.FetchTypes(ctx, ticker, types)
	if err != nil {
		return nil, fmt.Errorf("yahoo: fetch statements for %s: %w", ticker, err)
	}
	return BuildStatements(ticker, taxonomy, res.Flatten()), nil
}

// BuildStatements turns decoded facts into normalized period statements. It is
// pure so the mapping and derived formulas can be tested without a network.
func BuildStatements(ticker string, taxonomy financials.Taxonomy, facts []Fact) []financials.DepthStatement {
	type key struct {
		periodType string
		periodEnd  time.Time
	}
	groups := make(map[key][]Fact)
	for _, f := range facts {
		if f.Value == nil || f.PeriodType == "" || f.AsOfDate.IsZero() {
			continue
		}
		k := key{periodType: f.PeriodType, periodEnd: f.AsOfDate}
		groups[k] = append(groups[k], f)
	}

	out := make([]financials.DepthStatement, 0, len(groups))
	for k, group := range groups {
		out = append(out, buildStatement(ticker, taxonomy, k.periodType, k.periodEnd, group))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].PeriodEnd.Equal(out[j].PeriodEnd) {
			return out[i].PeriodEnd.Before(out[j].PeriodEnd)
		}
		return periodRank(out[i].PeriodType) < periodRank(out[j].PeriodType)
	})
	return out
}

func periodRank(p string) int {
	switch p {
	case financials.PeriodAnnual:
		return 0
	case financials.PeriodQuarterly:
		return 1
	case financials.PeriodTTM:
		return 2
	}
	return 3
}

func buildStatement(ticker string, taxonomy financials.Taxonomy, periodType string, periodEnd time.Time, facts []Fact) financials.DepthStatement {
	stmt := financials.DepthStatement{
		Ticker:         bareTicker(ticker),
		Taxonomy:       taxonomy,
		PeriodType:     periodType,
		PeriodEnd:      periodEnd,
		Values:         make(map[financials.Concept]financials.Value, len(conceptSpecs)),
		Coverage:       make(map[financials.Concept]financials.CoverageState, len(financials.CoreConcepts)),
		Methodology:    financials.MethodologyMorningstar,
		SignConvention: financials.SignConventionExpensesNegative,
	}

	byCode := make(map[string][]Fact, len(conceptSpecs))
	for _, f := range facts {
		byCode[f.Code] = append(byCode[f.Code], f)
		if f.Currency != "" {
			if stmt.Currency == "" {
				stmt.Currency = f.Currency
			} else if f.Currency != stmt.Currency {
				stmt.CurrencyConflict = true
			}
		}
	}

	for _, spec := range conceptSpecs {
		chosen, ok, conflicts := selectFact(spec, byCode)
		if len(conflicts) > 0 {
			stmt.AliasConflicts = append(stmt.AliasConflicts, conflicts...)
		}
		if ok {
			stmt.Values[spec.Concept] = financials.Value{
				Amount:     floatPtr(*chosen.Value),
				Currency:   chosen.Currency,
				Code:       chosen.Code,
				Source:     chosen.Source,
				AsOfDate:   chosen.AsOfDate,
				PeriodType: chosen.PeriodType,
			}
			stmt.Coverage[spec.Concept] = financials.CoverageObserved
			continue
		}
		if !Applicable(taxonomy, spec.Concept) {
			stmt.Coverage[spec.Concept] = financials.CoverageNotApplicable
		} else {
			stmt.Coverage[spec.Concept] = financials.CoverageUnknown
		}
	}

	deriveMetrics(&stmt)
	return stmt
}

// selectFact picks the first code in the alias group that has a value and
// records any divergent alias values. The primary code always wins.
func selectFact(spec conceptSpec, byCode map[string][]Fact) (Fact, bool, []financials.AliasConflict) {
	var chosen Fact
	found := false
	var conflicts []financials.AliasConflict
	for _, code := range spec.Codes {
		for _, f := range byCode[code] {
			if f.Value == nil {
				continue
			}
			if !found {
				chosen = f
				found = true
				continue
			}
			if !sameAmount(*chosen.Value, *f.Value) {
				conflicts = append(conflicts, financials.AliasConflict{
					Concept:   spec.Concept,
					Code:      chosen.Code,
					Amount:    *chosen.Value,
					OtherCode: f.Code,
					Other:     *f.Value,
				})
			}
		}
	}
	return chosen, found, conflicts
}

// sameAmount treats an alias pair as identical when the relative difference is
// negligible. Yahoo returns identical float64 values for the verified alias
// groups; the tolerance only absorbs serialization noise.
func sameAmount(a, b float64) bool {
	scale := math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	return math.Abs(a-b) <= scale*1e-6
}

// deriveMetrics fills the page-computed rows with documented formulas and a
// methodology tag. Direct combined-preload values win where they exist; the
// formulas are the fallback and the cross-check.
//
// Sign convention: expenses are negative. For example, pretax =
// total operating profit + non-operating total, and net income =
// pretax + tax where tax is negative for a tax expense and positive for a
// benefit.
func deriveMetrics(stmt *financials.DepthStatement) {
	// Pretax: EQIEA (total operating profit/loss) + EQAW7 (non-operating
	// income/expenses, total).
	if _, ok := stmt.Amount(financials.ConceptPretaxIncome); !ok {
		if a, okA := stmt.Amount(financials.ConceptTotalOperatingProfit); okA {
			if b, okB := stmt.Amount(financials.ConceptNonOperatingTotal); okB {
				setDerived(stmt, financials.ConceptPretaxIncome, *a+*b, "total_operating_profit + non_operating_total")
			}
		}
	}

	// Net income from continuing operations: pretax + tax.
	if _, ok := stmt.Amount(financials.ConceptNetIncomeContinuing); !ok {
		if a, okA := stmt.Amount(financials.ConceptPretaxIncome); okA {
			if b, okB := stmt.Amount(financials.ConceptIncomeTax); okB {
				setDerived(stmt, financials.ConceptNetIncomeContinuing, *a+*b, "pretax_income + income_tax")
			}
		}
	}

	// NCI: net income after NCI - net income from continuing operations.
	if _, ok := stmt.Amount(financials.ConceptNCI); !ok {
		if a, okA := stmt.Amount(financials.ConceptNetIncomeAfterNCI); okA {
			if b, okB := stmt.Amount(financials.ConceptNetIncomeContinuing); okB {
				setDerived(stmt, financials.ConceptNCI, *a-*b, "net_income_after_nci - net_income_continuing")
			}
		}
	}

	// Net income after NCI: continuing operations + NCI (NCI is negative for a
	// minority interest deduction).
	if _, ok := stmt.Amount(financials.ConceptNetIncomeAfterNCI); !ok {
		if a, okA := stmt.Amount(financials.ConceptNetIncomeContinuing); okA {
			if b, okB := stmt.Amount(financials.ConceptNCI); okB {
				setDerived(stmt, financials.ConceptNetIncomeAfterNCI, *a+*b, "net_income_continuing + nci")
			}
		}
	}

	// Net income available to common: NI after NCI (no preferred dividend code
	// is mapped in this phase).
	if _, ok := stmt.Amount(financials.ConceptNetIncomeCommon); !ok {
		if v, ok := stmt.Amount(financials.ConceptNetIncomeAfterNCI); ok {
			setDerived(stmt, financials.ConceptNetIncomeCommon, *v, "net_income_after_nci")
		}
	}

	// WASO fallback: NI / reported EPS. Only used when the direct share-count
	// code is missing; EPS is rounded at the source, so the direct code wins.
	if _, ok := stmt.Amount(financials.ConceptWASOBasic); !ok {
		if ni, okNI := stmt.Amount(financials.ConceptNetIncomeCommon); okNI {
			if eps, okEPS := stmt.Amount(financials.ConceptEPSBasic); okEPS && *eps != 0 {
				setDerived(stmt, financials.ConceptWASOBasic, *ni/(*eps), "net_income_common / eps_basic")
			}
		}
	}
	if _, ok := stmt.Amount(financials.ConceptWASODiluted); !ok {
		if ni, okNI := stmt.Amount(financials.ConceptNetIncomeCommon); okNI {
			if eps, okEPS := stmt.Amount(financials.ConceptEPSDiluted); okEPS && *eps != 0 {
				setDerived(stmt, financials.ConceptWASODiluted, *ni/(*eps), "net_income_common / eps_diluted")
			}
		}
	}

	// EPS is always computed when NI and WASO are available. This is
	// deliberate: Yahoo's own EPS is rounded (RAJA reports 0.00 for a
	// USD 0.001289/share result), so the derived value is the precise one.
	if ni, okNI := stmt.Amount(financials.ConceptNetIncomeCommon); okNI {
		if waso, okW := stmt.Amount(financials.ConceptWASOBasic); okW && *waso != 0 {
			setDerived(stmt, financials.ConceptEPSBasic, *ni/(*waso), "net_income_common / waso_basic")
		}
		if waso, okW := stmt.Amount(financials.ConceptWASODiluted); okW && *waso != 0 {
			setDerived(stmt, financials.ConceptEPSDiluted, *ni/(*waso), "net_income_common / waso_diluted")
		}
	}

	// EBIT: pretax - net finance income/expense. With expenses negative, a
	// net finance expense is subtracted, which adds it back.
	if _, ok := stmt.Amount(financials.ConceptEBIT); !ok {
		if pretax, okP := stmt.Amount(financials.ConceptPretaxIncome); okP {
			if nf, okF := stmt.Amount(financials.ConceptNetFinanceIncome); okF {
				setDerived(stmt, financials.ConceptEBIT, *pretax-*nf, "pretax_income - net_finance_income")
			}
		}
	}

	// EBITDA fallback. Yahoo's direct code (EQHEI/EQR8M) is the primary value;
	// this formula only runs when it is absent. It adds back the income-
	// statement D&A line, which can be narrower than the broader charge the
	// direct EBITDA row uses, so the formula is tagged for consumers.
	if _, ok := stmt.Amount(financials.ConceptEBITDA); !ok {
		if ebit, okE := stmt.Amount(financials.ConceptEBIT); okE {
			if dna, okD := stmt.Amount(financials.ConceptDepreciationAmortization); okD {
				setDerived(stmt, financials.ConceptEBITDA, *ebit+*dna,
					"ebit + depreciation_amortization (income-statement D&A; direct Morningstar EBITDA preferred when present)")
			}
		}
	}

	// FCF fallback: CFO - CapEx. CapEx is positive on the new page; use the
	// absolute value so a negative presentation cannot silently add to FCF.
	// Yahoo's direct Free Cash Flow to Firm (EQ58O) is not exactly CFO-CapEx,
	// so it always wins when present.
	if _, ok := stmt.Amount(financials.ConceptFCF); !ok {
		if cfo, okC := stmt.Amount(financials.ConceptCFO); okC {
			if capex, okX := stmt.Amount(financials.ConceptCapEx); okX {
				setDerived(stmt, financials.ConceptFCF, *cfo-math.Abs(*capex), "cfo - abs(capex)")
			}
		}
	}
}

func setDerived(stmt *financials.DepthStatement, concept financials.Concept, amount float64, formula string) {
	stmt.Values[concept] = financials.Value{
		Amount:     floatPtr(amount),
		Currency:   stmt.Currency,
		Source:     financials.ValueDerived,
		Formula:    formula,
		AsOfDate:   stmt.PeriodEnd,
		PeriodType: stmt.PeriodType,
	}
	stmt.Coverage[concept] = financials.CoverageObserved
}

func floatPtr(v float64) *float64 { return &v }
