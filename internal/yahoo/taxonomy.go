package yahoo

import (
	"context"
	"fmt"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

// Classification method names, recorded by ticket 06 in
// ticker_financial_taxonomy.source.
const (
	MethodDiscriminantProbe = "discriminant_probe"
	MethodPageTypeList      = "page_type_list"
)

// Discriminant code groups. They are deliberately small and value-based, not
// page-based: the first page-burst attempt tripped Yahoo's limiter and, under
// load, the page served a generic 154/500 template for every ticker, including
// banks and insurers. These groups were validated against the 16-ticker sample
// (BBCA/BBRI/BBNI/BMRI, ABDA/ASDM/AMAG/SMMA, PTBA/AADI/AALI/ADMF/LPPS/BREN)
// plus TUGU.
var (
	// bankProbeCodes are present in the bank (108) income list and absent from
	// the general and insurance lists. They are not all exclusive to real
	// banks: the 2026-11-10 650-ticker population found 17 general-list issuers
	// that populate EQOBD/EQBTD/EQ4LG, so the general markers are checked first.
	//
	// EQ70F/EQO3Z were added in the same run: BRIS and BTPS are 108-shaped but
	// populate none of the original probes, while both codes are present in the
	// bank-only income list and absent from the general/insurance lists and
	// preloads. The recency filter below is what keeps ABDA's stale 2018 EQOBD
	// TTM value from outvoting its current insurance shape.
	bankProbeCodes = []string{"EQRD7", "EQFK8", "EQOBD", "EQ4LG", "EQBTD", "EQ70F", "EQO3Z"}

	// insuranceProbeCodes are populated only by some insurance issuers
	// (AMAG/SMMA in the sample). They are positive evidence but not necessary:
	// ABDA/ASDM/TUGU do not populate them.
	insuranceProbeCodes = []string{"EQF4S", "EQRYP", "EQPVP"}

	// interestIncomeCode (EQDBM) is populated by banks and insurers and absent
	// from general issuers in the sample. It is the insurance fallback once the
	// bank probes and the general markers are ruled out.
	interestIncomeCode = "EQDBM"

	// generalProbeCodes are populated by the general template: gross profit
	// plus a handful of general-only operating lines. Gross profit alone can be
	// absent for a general-list issuer (BREN), so the extra markers matter.
	generalProbeCodes = []string{"EQ46Q", "EQ25Z", "EQ9HN", "EQLYB", "EQYSV", "EQAW7"}
)

// ProbeTypes returns the exact timeseries tokens the classifier requests. Both
// annual and TTM are probed because a ticker can have TTM data while its
// annual series is empty.
func ProbeTypes() []string {
	codes := make([]string, 0, len(bankProbeCodes)+len(insuranceProbeCodes)+1+len(generalProbeCodes))
	codes = append(codes, bankProbeCodes...)
	codes = append(codes, insuranceProbeCodes...)
	codes = append(codes, interestIncomeCode)
	codes = append(codes, generalProbeCodes...)

	types := make([]string, 0, len(codes)*2)
	for _, code := range uniqueStrings(codes) {
		types = append(types, code+"_"+financials.PeriodAnnual, code+"_"+financials.PeriodTTM)
	}
	return types
}

// Classification is the result of one probe. Evidence records which
// discriminant codes were populated, so ticket 06 can persist the decision and
// a later reader can see why it was made.
type Classification struct {
	Taxonomy financials.Taxonomy
	Method   string
	Evidence map[string]bool
}

// Ambiguous reports whether the probe result is not strong enough to persist
// without a page type-list check. The probe cannot separate an EQDBM-only bank
// from an EQDBM-only insurer (KREN vs ABDA on 2026-11-10), and it has no
// evidence at all for a bank that populates none of the bank codes. The
// population tool uses this to fetch one page type list for the ambiguous
// tickers; the default Classify path never does.
func (c *Classification) Ambiguous() bool {
	if c == nil {
		return true
	}
	if c.Taxonomy == financials.TaxonomyOther {
		return true
	}
	if c.Taxonomy != financials.TaxonomyBank108 && c.Taxonomy != financials.TaxonomyInsurance74 {
		return false
	}
	if c.Taxonomy == financials.TaxonomyInsurance74 && !anyTrue(c.Evidence, insuranceProbeCodes) {
		// EQDBM-only: could be an insurer (ABDA) or a bank that populates none
		// of the bank codes (KREN).
		return true
	}
	// A bank/insurer tie: both probe groups are populated and there is no
	// general marker. CASA and PNLF hit this on 2026-11-10.
	return anyTrue(c.Evidence, bankProbeCodes) && anyTrue(c.Evidence, insuranceProbeCodes)
}

// Classify fetches the discriminant probe and classifies the ticker. It makes
// one small timeseries request — never a page fetch.
func (c *Client) Classify(ctx context.Context, ticker string) (*Classification, error) {
	res, err := c.FetchTypes(ctx, ticker, ProbeTypes())
	if err != nil {
		return nil, fmt.Errorf("yahoo: classify %s: %w", ticker, err)
	}
	return ClassifyResult(res), nil
}

// ClassifyResult applies the classification rules to an already-fetched probe.
// It is pure and is what the tests exercise with fixture data. Probe facts
// older than probeWindow relative to the ticker's latest probe fact are
// ignored: a stale legacy code (ABDA carries a 2018 EQOBD TTM value) must not
// outvote the current statement shape.
func ClassifyResult(res *Result) *Classification {
	cutoff := probeCutoff(res)
	evidence := make(map[string]bool)
	for _, code := range allProbeCodes() {
		evidence[code] = hasDataSince(res, code, cutoff)
	}

	cls := &Classification{
		Taxonomy: financials.TaxonomyOther,
		Method:   MethodDiscriminantProbe,
		Evidence: evidence,
	}

	// 1. General markers win. The original order put a bank probe first, but the
	//    2026-11-10 650-ticker population showed that EQOBD/EQBTD/EQ4LG are also
	//    populated by 17 general-list issuers (all with gross profit and the
	//    other general markers). Gross profit is a stronger signal: a bank or
	//    insurer does not report it. BREN (general list, no gross profit) is
	//    still caught by the other general markers.
	if anyTrue(evidence, generalProbeCodes) {
		cls.Taxonomy = financials.TaxonomyGeneral154
		return cls
	}

	// 2. Bank probes. These are populated by the 108 shape and absent from the
	//    general/insurance lists, but they are not all exclusive to real banks
	//    (see Ambiguous for the bank/insurer tie).
	if anyTrue(evidence, bankProbeCodes) {
		cls.Taxonomy = financials.TaxonomyBank108
		return cls
	}

	// 3. Insurance-specific codes, or the bank/insurance interest-income line
	//    once the two stronger shapes are ruled out.
	if anyTrue(evidence, insuranceProbeCodes) || evidence[interestIncomeCode] {
		cls.Taxonomy = financials.TaxonomyInsurance74
		return cls
	}

	return cls
}

func allProbeCodes() []string {
	codes := make([]string, 0, len(bankProbeCodes)+len(insuranceProbeCodes)+1+len(generalProbeCodes))
	codes = append(codes, bankProbeCodes...)
	codes = append(codes, insuranceProbeCodes...)
	codes = append(codes, interestIncomeCode)
	codes = append(codes, generalProbeCodes...)
	return uniqueStrings(codes)
}

// probeWindow bounds how old a probe fact may be relative to the ticker's
// latest fact. Five years is deliberately generous: the point is to drop a
// legacy straggler, not to require freshness (ticket 05 owns freshness).
const probeWindow = 5

func probeCutoff(res *Result) time.Time {
	if res == nil {
		return time.Time{}
	}
	var latest time.Time
	for _, facts := range res.Facts {
		for _, f := range facts {
			if f.Value != nil && f.AsOfDate.After(latest) {
				latest = f.AsOfDate
			}
		}
	}
	if latest.IsZero() {
		return time.Time{}
	}
	return latest.AddDate(-probeWindow, 0, 0)
}

func hasDataSince(res *Result, code string, cutoff time.Time) bool {
	if res == nil {
		return false
	}
	for typ, facts := range res.Facts {
		if codeFromType(typ) != code {
			continue
		}
		for _, f := range facts {
			if f.Value == nil {
				continue
			}
			if cutoff.IsZero() || !f.AsOfDate.Before(cutoff) {
				return true
			}
		}
	}
	return false
}

func anyTrue(evidence map[string]bool, codes []string) bool {
	for _, code := range codes {
		if evidence[code] {
			return true
		}
	}
	return false
}
