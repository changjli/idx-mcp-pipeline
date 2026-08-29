package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// DisclosureFilterLookbackDays is the anomaly-gate lookback window: a
// disclosure's market impact can lag its announcement by days, so the gate
// matches anomalies whose trading_day falls within the window starting at the
// announcement date and extending forward this many days. This constant is the
// single definition of the window (ADR-0006); both gate paths inherit it:
//   - write path: DisclosureFilter evaluates disclosures against
//     AnomalyRepository.ExistsForTickerInWindow with this lookback;
//   - read path: AnomalyRepository.FindByDateWithDisclosures is parameterized
//     by it (callers pass this constant) instead of hard-coding its own copy.
const DisclosureFilterLookbackDays = 7

// Disclosure whitelist categories (ticket 11, layer 2): material-event titles
// kept for extraction. Titles are matched case-insensitively as substrings —
// real IDX titles are longer than the category names ("Pemanggilan RUPS
// Tahunan" contains "Pemanggilan RUPS"). All matched categories are stored.
var disclosureWhitelistKeywords = []string{
	"Informasi dan Fakta Material",
	"Pemanggilan RUPS",
	"Pembelian/Penjualan Efek",
	"Dividen",
	"Right Issue",
	"Stock Split",
	"Bonus Share",
	"Perubahan Papan Pencatatan",
	"Suspensi/Penundaan Pencatatan",
}

// disclosureExclusionKeywords are routine filings excluded even when a
// whitelist keyword also matches (exclusion wins). Laporan Keuangan covers
// routine quarterly financials.
var disclosureExclusionKeywords = []string{
	"Laporan Keuangan",
}

// Keyword pairs precomputed once at init — titles are compared
// case-insensitively, and recomputing strings.ToLower per keyword per
// disclosure would redo the same work on every row of every run. Each keyword
// keeps its lowercase form in one value so matching and canonical-category
// storage cannot drift out of lockstep.
type keywordPair struct{ original, lower string }

var (
	disclosureWhitelistPairs = lowerAll(disclosureWhitelistKeywords)
	disclosureExclusionPairs = lowerAll(disclosureExclusionKeywords)
)

func lowerAll(keywords []string) []keywordPair {
	pairs := make([]keywordPair, len(keywords))
	for i, kw := range keywords {
		pairs[i] = keywordPair{original: kw, lower: strings.ToLower(kw)}
	}
	return pairs
}

// DisclosureSource supplies the pending rows the filter evaluates and records
// each verdict. Consumer-side interface (ADR-0006): satisfied by the
// sqlx-backed DisclosureRepository via NewSQLDisclosureSource; tests provide
// the second adapter.
type DisclosureSource interface {
	FindPendingForFilter(today time.Time) ([]entity.Disclosure, error)
	MarkFiltered(id int64, passed bool, categories []string) error
}

// AnomalyGate answers the anomaly-gate predicate (layer 1): whether the
// disclosure's ticker has an anomaly within the gate window after the
// announcement date. The filter owns the window (DisclosureFilterLookbackDays)
// and passes it on every query. Satisfied by AnomalyRepository via
// NewSQLAnomalyGate.
type AnomalyGate interface {
	ExistsForTickerInWindow(ticker string, announcementDate time.Time, lookbackDays int) (bool, error)
}

// SQLDisclosureSource adapts DisclosureRepository to DisclosureSource.
type SQLDisclosureSource struct {
	repo *repository.DisclosureRepository
	db   *sqlx.DB
}

// NewSQLDisclosureSource binds a disclosure repository to its database.
func NewSQLDisclosureSource(repo *repository.DisclosureRepository, db *sqlx.DB) *SQLDisclosureSource {
	return &SQLDisclosureSource{repo: repo, db: db}
}

// FindPendingForFilter returns the filter's work set for the run date,
// parameterized by the filter's own lookback window.
func (s *SQLDisclosureSource) FindPendingForFilter(today time.Time) ([]entity.Disclosure, error) {
	return s.repo.FindPendingForFilter(s.db, today, DisclosureFilterLookbackDays)
}

// MarkFiltered records the filter verdict. categories is stored only for
// passing rows; rejected rows get NULL.
func (s *SQLDisclosureSource) MarkFiltered(id int64, passed bool, categories []string) error {
	return s.repo.MarkFiltered(s.db, id, passed, categories)
}

// SQLAnomalyGate adapts AnomalyRepository to AnomalyGate.
type SQLAnomalyGate struct {
	repo *repository.AnomalyRepository
	db   *sqlx.DB
}

// NewSQLAnomalyGate binds an anomaly repository to its database.
func NewSQLAnomalyGate(repo *repository.AnomalyRepository, db *sqlx.DB) *SQLAnomalyGate {
	return &SQLAnomalyGate{repo: repo, db: db}
}

// ExistsForTickerInWindow delegates the gate query to the repository; the
// window bounds are the caller's (the filter's), keeping the definition
// single-owned.
func (s *SQLAnomalyGate) ExistsForTickerInWindow(ticker string, announcementDate time.Time, lookbackDays int) (bool, error) {
	return s.repo.ExistsForTickerInWindow(s.db, ticker, announcementDate, lookbackDays)
}

// FilterStats summarizes one filter run; the handler turns it into the
// disclosure_filtered log event.
type FilterStats struct {
	Total       int
	Passed      int
	Rejected    int
	ReExtracted int
}

// DisclosureFilter applies ADR-0003's 3-layer filter to pending disclosures:
// the anomaly-gate (only tickers with an anomaly in the gate window proceed),
// the keyword whitelist, and the exclusion override. Passing rows are
// enqueued for extraction; the verdict is marked sticky — once true, the gate
// is never re-evaluated.
type DisclosureFilter struct {
	disclosures DisclosureSource
	gate        AnomalyGate
}

// NewDisclosureFilter wires a filter over its disclosure source and gate.
func NewDisclosureFilter(disclosures DisclosureSource, gate AnomalyGate) *DisclosureFilter {
	return &DisclosureFilter{disclosures: disclosures, gate: gate}
}

// Filter applies the anomaly-gate and keyword whitelist to every pending
// disclosure and marks the verdict. enqueue is called once per passing
// disclosure (injected so tests can collect IDs without Redis). Returns the
// run's stats; logging stays with the handler, which owns the task-scoped
// fields.
func (f *DisclosureFilter) Filter(_ context.Context, today time.Time, enqueue func(id int64)) (FilterStats, error) {
	rows, err := f.disclosures.FindPendingForFilter(today)
	if err != nil {
		return FilterStats{}, fmt.Errorf("query pending disclosures: %w", err)
	}

	stats := FilterStats{Total: len(rows)}
	for _, d := range rows {
		// Sticky true: never re-evaluate the gate. Re-enqueue extract only
		// while the row still awaits extraction (self-heal for missed or
		// R2-less runs).
		if d.PassedFilter != nil && *d.PassedFilter {
			if d.ExtractionStatus == "pending" {
				enqueue(d.ID)
				stats.ReExtracted++
			}
			continue
		}

		gate, err := f.anomalyGate(d)
		if err != nil {
			return FilterStats{}, fmt.Errorf("anomaly gate for disclosure %d: %w", d.ID, err)
		}
		ok, categories := evaluateDisclosure(d.Title, gate)
		if err := f.disclosures.MarkFiltered(d.ID, ok, categories); err != nil {
			return FilterStats{}, fmt.Errorf("mark disclosure %d: %w", d.ID, err)
		}
		if ok {
			enqueue(d.ID)
			stats.Passed++
		} else {
			stats.Rejected++
		}
	}
	return stats, nil
}

// anomalyGate reports whether the disclosure's ticker has an anomaly whose
// trading_day falls within the lookback window after the announcement date.
// A disclosure with no ticker can never match.
func (f *DisclosureFilter) anomalyGate(d entity.Disclosure) (bool, error) {
	if d.Ticker == nil {
		return false, nil
	}
	return f.gate.ExistsForTickerInWindow(*d.Ticker, d.AnnouncementDate, DisclosureFilterLookbackDays)
}

// evaluateDisclosure applies the keyword whitelist (layer 2) to a title whose
// anomaly-gate already passed. Exclusion keywords win: a title matching
// "Laporan Keuangan" is rejected even if a whitelist keyword also matches.
// Returns the verdict and every matched whitelist category.
func evaluateDisclosure(title string, gatePasses bool) (bool, []string) {
	if !gatePasses {
		return false, nil
	}
	lower := strings.ToLower(title)
	for _, kw := range disclosureExclusionPairs {
		if strings.Contains(lower, kw.lower) {
			return false, nil
		}
	}
	var categories []string
	for _, kw := range disclosureWhitelistPairs {
		if strings.Contains(lower, kw.lower) {
			categories = append(categories, kw.original)
		}
	}
	return len(categories) > 0, categories
}
