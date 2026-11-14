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

type fakeTaxonomySource struct {
	byTicker map[string]fakeTaxonomyClassification
}

type fakeTaxonomyClassification struct {
	taxonomy financials.Taxonomy
	method   string
	evidence map[string]bool
	err      error
}

func (f fakeTaxonomySource) ClassifyTaxonomy(_ context.Context, ticker string) (financials.Taxonomy, string, map[string]bool, error) {
	c, ok := f.byTicker[ticker]
	if !ok {
		return financials.TaxonomyOther, "discriminant_probe", map[string]bool{}, nil
	}
	return c.taxonomy, c.method, c.evidence, c.err
}

type fakeTaxonomyStore struct {
	rows []*entity.TickerFinancialTaxonomy
	err  error
}

func (f *fakeTaxonomyStore) SeedTaxonomy(_ *sqlx.DB, row *entity.TickerFinancialTaxonomy) error {
	if f.err != nil {
		return f.err
	}
	f.rows = append(f.rows, row)
	return nil
}

func newTestTaxonomyPopulation(source TaxonomySource, store TaxonomyStore) *TaxonomyPopulation {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	return NewTaxonomyPopulation(nil, log, source, store,
		WithTaxonomyPacing(1, 0),
		WithTaxonomyClock(func() time.Time { return now }),
	)
}

func TestTaxonomyPopulation_DryRunDoesNotPersist(t *testing.T) {
	source := fakeTaxonomySource{byTicker: map[string]fakeTaxonomyClassification{
		"BBRI": {taxonomy: financials.TaxonomyBank108, method: "discriminant_probe", evidence: map[string]bool{"EQRD7": true}},
		"PTBA": {taxonomy: financials.TaxonomyGeneral154, method: "discriminant_probe", evidence: map[string]bool{"EQ46Q": true}},
	}}
	store := &fakeTaxonomyStore{}
	uc := newTestTaxonomyPopulation(source, store)

	res, err := uc.Populate(context.Background(), []string{"bbri", "PTBA.JK"}, false)
	if err != nil {
		t.Fatalf("Populate: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("results = %d, want 2", len(res))
	}
	if res[0].Ticker != "BBRI" || res[0].Taxonomy != financials.TaxonomyBank108 {
		t.Errorf("first result = %+v", res[0])
	}
	if res[1].Ticker != "PTBA" || res[1].Taxonomy != financials.TaxonomyGeneral154 {
		t.Errorf("second result = %+v", res[1])
	}
	if len(store.rows) != 0 {
		t.Errorf("dry run wrote %d row(s), want 0", len(store.rows))
	}
}

func TestTaxonomyPopulation_ApplyPersistsConfirmedOnly(t *testing.T) {
	source := fakeTaxonomySource{byTicker: map[string]fakeTaxonomyClassification{
		"BBRI": {taxonomy: financials.TaxonomyBank108, method: "discriminant_probe", evidence: map[string]bool{"EQRD7": true}},
		"ABCD": {taxonomy: financials.TaxonomyOther, method: "discriminant_probe", evidence: map[string]bool{}},
	}}
	store := &fakeTaxonomyStore{}
	uc := newTestTaxonomyPopulation(source, store)

	res, err := uc.Populate(context.Background(), []string{"BBRI", "ABCD"}, true)
	if err != nil {
		t.Fatalf("Populate: %v", err)
	}
	if !res[0].Applied {
		t.Error("BBRI should have been applied")
	}
	if res[1].Applied {
		t.Error("other should not be applied")
	}
	if len(store.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(store.rows))
	}
	row := store.rows[0]
	if row.Ticker != "BBRI" || row.Taxonomy != "bank_108" || row.Source != "discriminant_probe" {
		t.Errorf("row = %+v", row)
	}
	if row.DepthStatus != financials.DepthStatusPending {
		t.Errorf("depth_status = %q, want %q", row.DepthStatus, financials.DepthStatusPending)
	}
	if !row.DiscoveredAt.Equal(time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("discovered_at = %v", row.DiscoveredAt)
	}
}

func TestTaxonomyPopulation_ProbeErrorIsRecordedNotFatal(t *testing.T) {
	source := fakeTaxonomySource{byTicker: map[string]fakeTaxonomyClassification{
		"BBRI": {err: errors.New("yahoo: http 503")},
		"PTBA": {taxonomy: financials.TaxonomyGeneral154, method: "discriminant_probe"},
	}}
	store := &fakeTaxonomyStore{}
	uc := newTestTaxonomyPopulation(source, store)

	res, err := uc.Populate(context.Background(), []string{"BBRI", "PTBA"}, true)
	if err != nil {
		t.Fatalf("Populate should not abort on one probe failure: %v", err)
	}
	if res[0].Err == nil {
		t.Error("BBRI error not recorded")
	}
	if len(store.rows) != 1 || store.rows[0].Ticker != "PTBA" {
		t.Errorf("rows = %+v", store.rows)
	}
}

func TestTaxonomyPopulation_StoreErrorIsRecorded(t *testing.T) {
	source := fakeTaxonomySource{byTicker: map[string]fakeTaxonomyClassification{
		"BBRI": {taxonomy: financials.TaxonomyBank108, method: "discriminant_probe"},
	}}
	store := &fakeTaxonomyStore{err: errors.New("db down")}
	uc := newTestTaxonomyPopulation(source, store)

	res, err := uc.Populate(context.Background(), []string{"BBRI"}, true)
	if err != nil {
		t.Fatalf("Populate: %v", err)
	}
	if res[0].Err == nil {
		t.Error("store error not recorded on the result")
	}
	if res[0].Applied {
		t.Error("a failed upsert must not be reported as applied")
	}
}

func TestTaxonomyPopulation_ApplyResultsSkipsProbeAndWritesConfirmed(t *testing.T) {
	// The source must not be called: ApplyResults is the offline path.
	source := fakeTaxonomySource{byTicker: map[string]fakeTaxonomyClassification{}}
	store := &fakeTaxonomyStore{}
	uc := newTestTaxonomyPopulation(source, store)

	in := []TaxonomyResult{
		{Ticker: "BBRI", Taxonomy: financials.TaxonomyBank108, Method: "discriminant_probe"},
		{Ticker: "ABCD", Taxonomy: financials.TaxonomyOther, Method: "discriminant_probe"},
		{Ticker: "PTBA", Taxonomy: financials.TaxonomyGeneral154, Method: "page_type_list"},
		{Ticker: "FAIL", Taxonomy: financials.TaxonomyBank108, Method: "discriminant_probe", Err: errors.New("probe failed")},
	}
	out, err := uc.ApplyResults(context.Background(), in)
	if err != nil {
		t.Fatalf("ApplyResults: %v", err)
	}
	if len(store.rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(store.rows))
	}
	if out[0].Ticker != "BBRI" || !out[0].Applied {
		t.Errorf("BBRI = %+v", out[0])
	}
	if out[1].Applied {
		t.Error("other must not be applied")
	}
	if out[2].Method != "page_type_list" || !out[2].Applied {
		t.Errorf("PTBA = %+v", out[2])
	}
	if out[3].Applied || out[3].Err == nil {
		t.Errorf("FAIL = %+v", out[3])
	}
}

func TestUniqueTickers(t *testing.T) {
	got := uniqueTickers([]string{" bbri ", "BBRI.JK", "", "PTBA", "ptba.jk", " aadi "})
	want := []string{"BBRI", "PTBA", "AADI"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got %v, want %v", got, want)
			break
		}
	}
}
