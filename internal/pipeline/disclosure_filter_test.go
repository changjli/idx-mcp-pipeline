package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// fakeDisclosureSource serves a canned pending list and records verdicts.
type fakeDisclosureSource struct {
	rows   []entity.Disclosure
	marked map[int64]filterVerdict
}

type filterVerdict struct {
	passed     bool
	categories []string
}

func (f *fakeDisclosureSource) FindPendingForFilter(today time.Time) ([]entity.Disclosure, error) {
	return f.rows, nil
}

func (f *fakeDisclosureSource) MarkFiltered(id int64, passed bool, categories []string) error {
	if f.marked == nil {
		f.marked = make(map[int64]filterVerdict)
	}
	f.marked[id] = filterVerdict{passed: passed, categories: categories}
	return nil
}

// fakeAnomalyGate records the lookback it was queried with so tests can
// assert the filter passes its own window (the single definition).
type fakeAnomalyGate struct {
	controllers map[string]bool // ticker -> gate verdict
	lookback    int
}

func (f *fakeAnomalyGate) ExistsForTickerInWindow(ticker string, announcementDate time.Time, lookbackDays int) (bool, error) {
	f.lookback = lookbackDays
	return f.controllers[ticker], nil
}

func strPtr(s string) *string { return &s }

func newTestFilter(rows []entity.Disclosure, controllers map[string]bool) (*DisclosureFilter, *fakeDisclosureSource, *fakeAnomalyGate) {
	src := &fakeDisclosureSource{rows: rows}
	gate := &fakeAnomalyGate{controllers: controllers}
	return NewDisclosureFilter(src, gate), src, gate
}

func disclosureRow(id int64, ticker *string, title, extractionStatus string, passed *bool) entity.Disclosure {
	return entity.Disclosure{
		ID:               id,
		Ticker:           ticker,
		Title:            title,
		ExtractionStatus: extractionStatus,
		PassedFilter:     passed,
	}
}

func TestFilter_GateWindowAndVerdicts(t *testing.T) {
	today := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	rups := "Pemanggilan RUPS Tahunan PT ABC"
	rows := []entity.Disclosure{
		// Anomaly + whitelist title -> pass.
		disclosureRow(1, strPtr("GE.TK"), rups, "pending", nil),
		// Gate passes but title hits exclusion -> reject (exclusion wins).
		disclosureRow(2, strPtr("EX.TK"), "Laporan Keuangan dan Informasi dan Fakta Material", "pending", nil),
		// No anomaly (gate returns false) -> reject.
		disclosureRow(3, strPtr("NO.GATE"), rups, "pending", nil),
		// No ticker -> gate never queried, reject.
		disclosureRow(4, nil, rups, "pending", nil),
	}
	filter, src, gate := newTestFilter(rows, map[string]bool{"GE.TK": true, "EX.TK": true})

	var enqueued []int64
	stats, err := filter.Filter(context.Background(), today, func(id int64) { enqueued = append(enqueued, id) })
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}

	// The filter passes its own window on every gate query — the single
	// definition (ADR-0006), shared with the read-path JOIN via parameter.
	if gate.lookback != DisclosureFilterLookbackDays {
		t.Errorf("expected gate lookback %d, got %d", DisclosureFilterLookbackDays, gate.lookback)
	}
	if stats.Total != 4 || stats.Passed != 1 || stats.Rejected != 3 || stats.ReExtracted != 0 {
		t.Errorf("unexpected stats: %+v", stats)
	}
	if enq := enqueued; !reflect.DeepEqual(enq, []int64{1}) {
		t.Errorf("expected only passing disclosure enqueued, got %v", enq)
	}

	v := src.marked[1]
	if !v.passed || !reflect.DeepEqual(v.categories, []string{"Pemanggilan RUPS"}) {
		t.Errorf("expected pass with [Pemanggilan RUPS], got %+v", v)
	}
	for id := int64(2); id <= 4; id++ {
		if v := src.marked[id]; v.passed {
			t.Errorf("disclosure %d: expected rejected", id)
		}
	}
}

func TestFilter_StickyTrueNotReEvaluated(t *testing.T) {
	passed := true
	bulkTitle := "Laporan Bulanan Registrasi Pemegang Efek"
	rows := []entity.Disclosure{
		// Previously passed (categories set by a prior run) but its title is
		// now non-material — re-evaluation would wrongly reject it.
		disclosureRow(7, strPtr("STICK"), bulkTitle, "pending", &passed),
		disclosureRow(8, strPtr("STICK"), bulkTitle, "ok", &passed),
	}
	filter, src, gate := newTestFilter(rows, map[string]bool{})

	var enqueued []int64
	stats, err := filter.Filter(context.Background(), time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC), func(id int64) { enqueued = append(enqueued, id) })
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if gate.lookback != 0 {
		t.Error("sticky rows must not re-evaluate the anomaly gate")
	}
	if stats.Passed != 0 || stats.Rejected != 0 || stats.ReExtracted != 1 || stats.Total != 2 {
		t.Errorf("unexpected stats: %+v", stats)
	}
	if !reflect.DeepEqual(enqueued, []int64{7}) {
		t.Errorf("expected only pending sticky row re-enqueued, got %v", enqueued)
	}
	if _, ok := src.marked[7]; ok {
		t.Error("sticky row must not be re-marked")
	}
}

// evaluateDisclosure tests migrated from tasks/filter_disclosures_test.go.
func TestEvaluateDisclosure_GateFails(t *testing.T) {
	passed, cats := evaluateDisclosure("Pemanggilan RUPS Tahunan", false)
	if passed {
		t.Error("expected rejected when anomaly-gate fails")
	}
	if cats != nil {
		t.Errorf("expected no categories, got %v", cats)
	}
}

func TestEvaluateDisclosure_WhitelistSubstringMatch(t *testing.T) {
	// Real IDX titles are longer than the category name — substring match.
	passed, cats := evaluateDisclosure("Pemanggilan RUPS Tahunan PT ABC", true)
	if !passed {
		t.Fatal("expected passed for RUPS summons")
	}
	if !reflect.DeepEqual(cats, []string{"Pemanggilan RUPS"}) {
		t.Errorf("expected [Pemanggilan RUPS], got %v", cats)
	}
}

func TestEvaluateDisclosure_CaseInsensitive(t *testing.T) {
	passed, cats := evaluateDisclosure("pemanggilan rups tahunan", true)
	if !passed {
		t.Fatal("expected case-insensitive match")
	}
	if !reflect.DeepEqual(cats, []string{"Pemanggilan RUPS"}) {
		t.Errorf("expected canonical category, got %v", cats)
	}
}

func TestEvaluateDisclosure_MultipleCategories(t *testing.T) {
	passed, cats := evaluateDisclosure("Informasi dan Fakta Material dan Pembagian Dividen", true)
	if !passed {
		t.Fatal("expected passed")
	}
	want := []string{"Informasi dan Fakta Material", "Dividen"}
	if !reflect.DeepEqual(cats, want) {
		t.Errorf("expected %v, got %v", want, cats)
	}
}

func TestEvaluateDisclosure_ExclusionWins(t *testing.T) {
	// Laporan Keuangan is excluded even when a whitelist keyword also matches.
	passed, cats := evaluateDisclosure("Laporan Keuangan dan Informasi dan Fakta Material", true)
	if passed {
		t.Error("expected rejected for Laporan Keuangan")
	}
	if cats != nil {
		t.Errorf("expected no categories, got %v", cats)
	}
}

func TestEvaluateDisclosure_NoMatch(t *testing.T) {
	passed, cats := evaluateDisclosure("Laporan Bulanan Registrasi Pemegang Efek", true)
	if passed {
		t.Error("expected rejected for non-material title")
	}
	if cats != nil {
		t.Errorf("expected no categories, got %v", cats)
	}
}

func TestFilter_SourceErrorPropagates(t *testing.T) {
	src := &errDisclosureSource{}
	filter := NewDisclosureFilter(src, &fakeAnomalyGate{})
	if _, err := filter.Filter(context.Background(), time.Now(), func(int64) {}); err == nil {
		t.Error("expected source error to propagate")
	}
}

type errDisclosureSource struct{ fakeDisclosureSource }

func (e *errDisclosureSource) FindPendingForFilter(today time.Time) ([]entity.Disclosure, error) {
	return nil, fmt.Errorf("db down")
}
