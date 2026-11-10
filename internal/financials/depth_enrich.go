package financials

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"time"
)

// Depth run statuses. They are written to financial_depth_runs and to
// ticker_financial_taxonomy.depth_status, so the strings are part of the store
// contract. observed/partial/not_covered are terminal for one run month; failed
// and yahoo_lagging are retried on the next run of the same month.
const (
	DepthStatusObserved     = "observed"
	DepthStatusPartial      = "partial"
	DepthStatusFailed       = "failed"
	DepthStatusNotCovered   = "not_covered"
	DepthStatusYahooLagging = "yahoo_lagging"
)

// Period bases persisted on a depth-enriched row. The main columns are
// cumulative YTD on 3M/6M/9M rows and 12M on annual rows; the *_q and *_ttm
// column families carry the other two bases without changing the row key.
const (
	PeriodBasisCumulativeYTD = "cumulative_ytd"
	PeriodBasisAnnual12M     = "annual_12m"
)

const (
	// DepthConflictTolerance is the relative difference above which a Tier-1
	// value and its converted Tier-2 counterpart are recorded as a conflict.
	// Tier 1 always keeps its own revenue/profit/equity/liabilities; the
	// conflict is evidence, not an overwrite path.
	DepthConflictTolerance = 0.01

	// DefaultUSDIDRRate is the last-resort rate used only when a non-IDR row
	// has no Tier-1 value to imply the rate from. It is the rate observed in
	// the AADI Q1 2026 snapshot (16,999 IDR/USD); the actual rate and date are
	// always written next to the values so a reader can audit the conversion.
	DefaultUSDIDRRate = 16999.0
)

// DepthField is one Tier-2 line persisted on the canonical statement row.
type DepthField string

const (
	DepthCash            DepthField = "cash"
	DepthGrossProfit     DepthField = "gross_profit"
	DepthOperatingProfit DepthField = "operating_profit"
	DepthEBITDA          DepthField = "ebitda"
	DepthCFO             DepthField = "cfo"
	DepthCapEx           DepthField = "capex"
	DepthFCF             DepthField = "fcf"
	DepthTotalDebt       DepthField = "total_debt"
	DepthInterestExpense DepthField = "interest_expense"
)

// DepthFields is the stable field order used for snapshots and hashes.
var DepthFields = []DepthField{
	DepthCash,
	DepthGrossProfit,
	DepthOperatingProfit,
	DepthEBITDA,
	DepthCFO,
	DepthCapEx,
	DepthFCF,
	DepthTotalDebt,
	DepthInterestExpense,
}

// depthConcept binds a stored depth field to the source-neutral concept. An
// empty concept means the adapter has no mapping yet (interest expense on the
// post-1109 income list): the column exists and is persisted as unknown rather
// than guessed from net finance income.
var depthConcept = map[DepthField]Concept{
	DepthCash:            ConceptCash,
	DepthGrossProfit:     ConceptGrossProfit,
	DepthOperatingProfit: ConceptTotalOperatingProfit,
	DepthEBITDA:          ConceptEBITDA,
	DepthCFO:             ConceptCFO,
	DepthCapEx:           ConceptCapEx,
	DepthFCF:             ConceptFCF,
	DepthTotalDebt:       ConceptTotalDebt,
	DepthInterestExpense: "",
}

// DepthConflict records a Tier-1 / Tier-2 disagreement. Tier 1 keeps its value;
// this row makes the disagreement auditable instead of silently replacing it.
type DepthConflict struct {
	Field        string  `json:"field"`
	Tier1        float64 `json:"tier1_idr"`
	Tier2IDR     float64 `json:"tier2_idr"`
	RelativeDiff float64 `json:"relative_diff"`
}

// TierOneDepthPeriod is the subset of an existing (ticker, period_end) row the
// depth builder needs: the period coordinates, the Tier-1 values that must
// never be overwritten, and the fiscal year that decides which discrete
// quarters accumulate into this period.
type TierOneDepthPeriod struct {
	PeriodEnd      time.Time
	FiscalYearEnd  time.Time
	DurationMonths int

	Revenue         *float64
	NetProfitParent *float64
	Assets          *float64
	Liabilities     *float64
	Equity          *float64
}

// DepthRow is one prepared depth update. The maps hold IDR values already
// converted at Ingest; Raw keeps the original reporting-currency values,
// source codes and formulas for audit.
type DepthRow struct {
	PeriodEnd      time.Time
	DurationMonths int
	Currency       string
	FXRate         float64
	FXDate         time.Time
	PeriodBasis    string
	Methodology    string
	Status         string

	Values   map[DepthField]*float64
	Quarters map[DepthField]*float64
	TTM      map[DepthField]*float64

	Coverage  map[string]CoverageState
	Raw       map[string]any
	Conflicts []DepthConflict

	ContentHash  string
	SnapshotJSON string
}

// BuildDepthRows converts the adapter's normalized period statements into
// depth updates for the Tier-1 rows that already exist. It never invents a
// row: a statement whose period is not in tier1 is ignored, so
// duration_months stays in {3,6,9,12}.
//
// Rules:
//   - a 12M statement enriches the matching 12M row (same basis);
//   - 3M statements are discrete quarters and are summed into cumulative YTD
//     for the matching 3M/6M/9M row, but only when every quarter from the
//     fiscal-year start is present — a partial sum is never written;
//   - TTM is written to *_ttm columns on the row whose period_end equals the
//     TTM asOfDate, never as a new duration row;
//   - a non-IDR issuer is converted at an implied rate from the Tier-1 values
//     (IDX's own conversion) when possible, else the fallback rate, always
//     recording the rate and date.
func BuildDepthRows(
	ticker string,
	taxonomy Taxonomy,
	statements []DepthStatement,
	tier1 []TierOneDepthPeriod,
	fallbackFXRate float64,
	runDate time.Time,
) []DepthRow {
	if len(statements) == 0 || len(tier1) == 0 {
		return nil
	}
	if fallbackFXRate <= 0 {
		fallbackFXRate = DefaultUSDIDRRate
	}

	annual := make(map[string]DepthStatement)
	var quarterly []DepthStatement
	ttm := make(map[string]DepthStatement)
	for _, st := range statements {
		key := st.PeriodEnd.Format("2006-01-02")
		switch st.PeriodType {
		case PeriodAnnual:
			annual[key] = st
		case PeriodQuarterly:
			quarterly = append(quarterly, st)
		case PeriodTTM:
			ttm[key] = st
		}
	}
	sort.Slice(quarterly, func(i, j int) bool { return quarterly[i].PeriodEnd.Before(quarterly[j].PeriodEnd) })

	periods := append([]TierOneDepthPeriod(nil), tier1...)
	sort.Slice(periods, func(i, j int) bool { return periods[i].PeriodEnd.Before(periods[j].PeriodEnd) })

	out := make([]DepthRow, 0, len(periods))
	for _, period := range periods {
		key := period.PeriodEnd.Format("2006-01-02")

		var annualStmt *DepthStatement
		var q *DepthStatement
		var quarters []DepthStatement
		var complete bool
		var ttmStmt *DepthStatement

		if st, ok := annual[key]; ok {
			s := st
			annualStmt = &s
		}
		if st, ok := ttm[key]; ok {
			s := st
			ttmStmt = &s
		}
		if period.DurationMonths == 12 {
			if annualStmt == nil && ttmStmt == nil {
				continue
			}
		} else {
			quarters, complete = quartersThrough(quarterly, period)
			if len(quarters) > 0 && quarters[len(quarters)-1].PeriodEnd.Equal(period.PeriodEnd) {
				qst := quarters[len(quarters)-1]
				q = &qst
			}
			if q == nil && ttmStmt == nil {
				continue // no discrete quarter and no TTM for this row
			}
		}

		// basisAmounts is the value set that shares the Tier-1 row's basis:
		// annual for 12M, complete cumulative YTD for interim rows. It drives
		// the implied FX rate and the conflict check; a TTM-only row has no
		// comparable basis and neither runs.
		basisAmounts := map[Concept]*float64{}
		if period.DurationMonths == 12 && annualStmt != nil {
			basisAmounts = statementAmounts(annualStmt)
		} else if period.DurationMonths != 12 && complete {
			basisAmounts = cumulativeAmounts(quarters)
		}

		base := annualStmt
		if base == nil {
			base = q
		}
		if base == nil {
			base = ttmStmt
		}

		row := DepthRow{
			PeriodEnd:      period.PeriodEnd,
			DurationMonths: period.DurationMonths,
			Methodology:    MethodologyMorningstar,
			Values:         make(map[DepthField]*float64, len(DepthFields)),
			Quarters:       make(map[DepthField]*float64, len(DepthFields)),
			TTM:            make(map[DepthField]*float64, len(DepthFields)),
			Coverage:       make(map[string]CoverageState, len(DepthFields)*3),
			Raw:            make(map[string]any),
		}
		if period.DurationMonths == 12 {
			row.PeriodBasis = PeriodBasisAnnual12M
		} else {
			row.PeriodBasis = PeriodBasisCumulativeYTD
		}
		row.Currency = statementCurrency(base)
		rate, fxDate := deriveFXRate(period, basisAmounts, row.Currency, fallbackFXRate, runDate)
		row.FXRate = rate
		row.FXDate = fxDate

		for _, field := range DepthFields {
			concept := depthConcept[field]
			var main, quarter, trailing *float64

			if period.DurationMonths == 12 {
				main = amountOf(annualStmt, concept)
			} else if complete {
				main, _ = sumConcept(quarters, concept)
			}
			quarter = amountOf(q, concept)
			trailing = amountOf(ttmStmt, concept)

			row.Values[field] = convertAmount(main, rate)
			row.Quarters[field] = convertAmount(quarter, rate)
			row.TTM[field] = convertAmount(trailing, rate)

			row.Coverage[string(field)] = coverageFor(taxonomy, concept, row.Values[field])
			row.Coverage[string(field)+"_q"] = coverageFor(taxonomy, concept, row.Quarters[field])
			row.Coverage[string(field)+"_ttm"] = coverageFor(taxonomy, concept, row.TTM[field])

			if period.DurationMonths == 12 {
				if raw := rawValue(annualStmt, concept, "cumulative"); raw != nil {
					row.Raw[string(field)] = raw
				}
			} else if complete {
				if raw := cumulativeRaw(quarters, concept); raw != nil {
					row.Raw[string(field)] = raw
				}
			}
			if raw := rawValue(q, concept, "quarter"); raw != nil {
				row.Raw[string(field)+"_q"] = raw
			}
			if raw := rawValue(ttmStmt, concept, "ttm"); raw != nil {
				row.Raw[string(field)+"_ttm"] = raw
			}
		}

		row.Raw["currency"] = row.Currency
		row.Raw["fx_rate"] = row.FXRate
		row.Raw["fx_date"] = row.FXDate.Format("2006-01-02")
		row.Raw["period_basis"] = row.PeriodBasis
		row.Raw["methodology"] = row.Methodology
		row.Raw["sign_convention"] = SignConventionExpensesNegative
		row.Raw["taxonomy"] = string(taxonomy)

		row.Conflicts = detectConflicts(period, basisAmounts, rate)
		if row.Conflicts == nil {
			row.Conflicts = []DepthConflict{}
		}

		if !rowHasDepth(row) {
			continue
		}
		if rowHasValues(row) {
			row.Status = DepthStatusObserved
		} else {
			row.Status = DepthStatusPartial
		}
		row.ContentHash = depthContentHash(ticker, row)
		row.SnapshotJSON = depthSnapshotJSON(ticker, row)
		out = append(out, row)
	}
	return out
}

// quartersThrough returns the discrete 3M statements from the fiscal-year
// start through period's period_end, oldest first, and whether the sequence is
// complete for the row's duration (3M = 1 quarter, 6M = 2, 9M = 3). A missing
// quarter makes the cumulative sum unavailable, never a partial sum.
func quartersThrough(quarterly []DepthStatement, period TierOneDepthPeriod) ([]DepthStatement, bool) {
	start := period.FiscalYearEnd.AddDate(-1, 0, 0).AddDate(0, 0, 1)
	out := make([]DepthStatement, 0, period.DurationMonths/3)
	for _, q := range quarterly {
		if q.PeriodEnd.Before(start) || q.PeriodEnd.After(period.PeriodEnd) {
			continue
		}
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeriodEnd.Before(out[j].PeriodEnd) })
	return out, len(out) == period.DurationMonths/3
}

func amountOf(stmt *DepthStatement, concept Concept) *float64 {
	if stmt == nil || concept == "" {
		return nil
	}
	v, ok := stmt.Amount(concept)
	if !ok {
		return nil
	}
	return v
}

func sumConcept(quarters []DepthStatement, concept Concept) (*float64, bool) {
	if concept == "" || len(quarters) == 0 {
		return nil, false
	}
	var sum float64
	for _, q := range quarters {
		v, ok := q.Amount(concept)
		if !ok {
			return nil, false
		}
		sum += *v
	}
	return &sum, true
}

func statementCurrency(stmt *DepthStatement) string {
	if stmt == nil || stmt.Currency == "" {
		return "IDR"
	}
	return stmt.Currency
}

// statementAmounts returns a statement's reporting-currency amounts keyed by
// concept. It is the basis used for FX inference and conflict checks on an
// annual row.
func statementAmounts(stmt *DepthStatement) map[Concept]*float64 {
	out := make(map[Concept]*float64, len(stmt.Values))
	if stmt == nil {
		return out
	}
	for c, v := range stmt.Values {
		if v.Amount != nil {
			out[c] = v.Amount
		}
	}
	return out
}

// cumulativeAmounts sums the complete quarter set per concept. A concept
// missing from any quarter is absent from the map, never a partial sum.
func cumulativeAmounts(quarters []DepthStatement) map[Concept]*float64 {
	out := make(map[Concept]*float64, len(CoreConcepts))
	if len(quarters) == 0 {
		return out
	}
	for _, concept := range CoreConcepts {
		if sum, ok := sumConcept(quarters, concept); ok {
			out[concept] = sum
		}
	}
	return out
}

// deriveFXRate recovers the conversion rate from the Tier-1 row's own IDR
// values when the issuer reports in another currency. This is the most honest
// rate available: IDX already converted the same statement. A fallback is used
// only when no comparable concept is present, and its date is the run date
// rather than pretending it was a period-end rate.
func deriveFXRate(period TierOneDepthPeriod, amounts map[Concept]*float64, currency string, fallback float64, runDate time.Time) (float64, time.Time) {
	if currency == "" || currency == "IDR" {
		return 1, period.PeriodEnd
	}
	ratios := make([]float64, 0, 4)
	addRatio := func(tier1 *float64, concept Concept) {
		original := amounts[concept]
		if tier1 == nil || original == nil || *tier1 == 0 || *original == 0 {
			return
		}
		if (*tier1 > 0) != (*original > 0) {
			return
		}
		ratio := *tier1 / *original
		if ratio > 0 && !math.IsInf(ratio, 0) && !math.IsNaN(ratio) {
			ratios = append(ratios, ratio)
		}
	}
	addRatio(period.Revenue, ConceptRevenue)
	addRatio(period.Assets, ConceptAssets)
	addRatio(period.Equity, ConceptEquity)
	addRatio(period.Liabilities, ConceptLiabilities)
	if len(ratios) > 0 {
		sort.Float64s(ratios)
		return ratios[len(ratios)/2], period.PeriodEnd
	}
	return fallback, runDate
}

func convertAmount(v *float64, rate float64) *float64 {
	if v == nil {
		return nil
	}
	converted := *v * rate
	return &converted
}

func coverageFor(taxonomy Taxonomy, concept Concept, value *float64) CoverageState {
	if value != nil {
		return CoverageObserved
	}
	if concept != "" && !Applicable(taxonomy, concept) {
		return CoverageNotApplicable
	}
	return CoverageUnknown
}

func rawValue(stmt *DepthStatement, concept Concept, basis string) map[string]any {
	if stmt == nil || concept == "" {
		return nil
	}
	v, ok := stmt.Values[concept]
	if !ok || v.Amount == nil {
		return nil
	}
	raw := map[string]any{
		"amount":      *v.Amount,
		"currency":    v.Currency,
		"code":        v.Code,
		"source":      string(v.Source),
		"period_type": v.PeriodType,
		"basis":       basis,
	}
	if v.Formula != "" {
		raw["formula"] = v.Formula
	}
	return raw
}

// cumulativeRaw records the original-currency sum behind a cumulative YTD
// value. It is deliberately derived from the same complete quarter set the
// main column sums, so the audit document cannot show a partial sum.
func cumulativeRaw(quarters []DepthStatement, concept Concept) map[string]any {
	sum, ok := sumConcept(quarters, concept)
	if !ok {
		return nil
	}
	currency := ""
	if len(quarters) > 0 {
		currency = quarters[len(quarters)-1].Currency
	}
	return map[string]any{
		"amount":   *sum,
		"currency": currency,
		"basis":    "cumulative_ytd",
		"formula":  "sum of discrete 3M quarters",
		"quarters": len(quarters),
	}
}

func detectConflicts(period TierOneDepthPeriod, amounts map[Concept]*float64, rate float64) []DepthConflict {
	if len(amounts) == 0 {
		return nil
	}
	var out []DepthConflict
	add := func(field string, tier1 *float64, concept Concept) {
		if tier1 == nil {
			return
		}
		original := amounts[concept]
		if original == nil {
			return
		}
		tier2 := *original * rate
		denom := math.Max(math.Abs(*tier1), math.Abs(tier2))
		if denom == 0 {
			return
		}
		diff := math.Abs(*tier1-tier2) / denom
		if diff > DepthConflictTolerance {
			out = append(out, DepthConflict{
				Field:        field,
				Tier1:        *tier1,
				Tier2IDR:     tier2,
				RelativeDiff: diff,
			})
		}
	}
	add("revenue", period.Revenue, ConceptRevenue)
	add("net_profit", period.NetProfitParent, ConceptNetIncomeCommon)
	add("equity", period.Equity, ConceptEquity)
	add("liabilities", period.Liabilities, ConceptLiabilities)
	return out
}

func rowHasValues(row DepthRow) bool {
	for _, field := range DepthFields {
		if row.Values[field] != nil || row.Quarters[field] != nil || row.TTM[field] != nil {
			return true
		}
	}
	return false
}

func rowHasDepth(row DepthRow) bool {
	if rowHasValues(row) {
		return true
	}
	for _, state := range row.Coverage {
		if state == CoverageNotApplicable {
			return true
		}
	}
	return false
}

func depthContentHash(ticker string, row DepthRow) string {
	h := sha256.New()
	writeStr := func(v string) {
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	writeFloat := func(v *float64) {
		if v == nil {
			writeStr("-")
			return
		}
		if *v == 0 {
			writeStr("0")
			return
		}
		writeStr(strconv.FormatFloat(*v, 'f', 4, 64))
	}
	writeStr(ticker)
	writeStr(row.PeriodEnd.Format("2006-01-02"))
	writeStr(strconv.Itoa(row.DurationMonths))
	writeStr(row.Currency)
	writeStr(strconv.FormatFloat(row.FXRate, 'f', 8, 64))
	writeStr(row.FXDate.Format("2006-01-02"))
	writeStr(row.PeriodBasis)
	writeStr(row.Methodology)
	for _, field := range DepthFields {
		writeFloat(row.Values[field])
		writeFloat(row.Quarters[field])
		writeFloat(row.TTM[field])
		writeStr(string(row.Coverage[string(field)]))
		writeStr(string(row.Coverage[string(field)+"_q"]))
		writeStr(string(row.Coverage[string(field)+"_ttm"]))
	}
	for _, c := range row.Conflicts {
		writeStr(c.Field)
		writeStr(strconv.FormatFloat(c.RelativeDiff, 'f', 6, 64))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func depthSnapshotJSON(ticker string, row DepthRow) string {
	values := make(map[string]any, len(DepthFields)*3)
	for _, field := range DepthFields {
		values[string(field)] = row.Values[field]
		values[string(field)+"_q"] = row.Quarters[field]
		values[string(field)+"_ttm"] = row.TTM[field]
	}
	snapshot := map[string]any{
		"ticker":          ticker,
		"period_end":      row.PeriodEnd.Format("2006-01-02"),
		"duration_months": row.DurationMonths,
		"currency":        row.Currency,
		"fx_rate":         row.FXRate,
		"fx_date":         row.FXDate.Format("2006-01-02"),
		"period_basis":    row.PeriodBasis,
		"methodology":     row.Methodology,
		"status":          row.Status,
		"values":          values,
		"coverage":        row.Coverage,
		"raw":             row.Raw,
		"conflicts":       row.Conflicts,
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}
