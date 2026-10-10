package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// ─── Disclosure Grade fakes ──────────────────────────────────────────────────

// fakeDisclosureFilingSource is the Disclosure Grade's seam: it serves a filing
// day's passed and rejected rows from canned maps, so a test's population is
// explicit without a disclosure database.
type fakeDisclosureFilingSource struct {
	passedByDay   map[string][]entity.Disclosure
	rejectedByDay map[string][]entity.Disclosure
	err           error
}

func (f *fakeDisclosureFilingSource) FindByAnnouncementDate(_ *sqlx.DB, day time.Time, passed bool) ([]entity.Disclosure, error) {
	if f.err != nil {
		return nil, f.err
	}
	key := day.Format("2006-01-02")
	if passed {
		return f.passedByDay[key], nil
	}
	return f.rejectedByDay[key], nil
}

// ─── Fixture ─────────────────────────────────────────────────────────────────

// disclosure is one filing row. The fake keys on the passed flag, so the entity
// field is only for the report's own reads.
func disclosure(id int64, ticker string, day time.Time, category, pdf string) entity.Disclosure {
	var categories pq.StringArray
	if category != "" {
		categories = pq.StringArray{category}
	}
	return entity.Disclosure{
		ID:               id,
		Ticker:           &ticker,
		AnnouncementDate: day,
		Title:            "title-" + pdf,
		PdfURL:           pdf,
		Categories:       categories,
	}
}

// disclosureGradeScenario is a hand-computed one-filing-day fixture. Six
// hard-filter candidates carry the Liquidity Baseline; three passed filings
// dedupe to two ticker-days (AAA has two filings, exercising dedupe and the
// category union), and three rejects seed the Reject Sample.
//
//	return  AAA +30%  BBB +10%  CCC -20%  DDD -10%  EEE 0%  NORM -5%
//	baseline median = -2.5%;  passed median = +20%;  Excess = +22.5%.
//
//	passed: AAA (Dividen), AAA (Right Issue), BBB (Stock Split)
//	reject: CCC, DDD, EEE
func disclosureGradeScenario() (time.Time, []time.Time, *fakeMeasurementPrices, *fakeDisclosureFilingSource) {
	anchor := measurementAnchor()
	days := measurementCalendar(anchor, 25)
	targetDays := []time.Time{anchor, days[5], days[10], days[20]}

	returns := map[string]float64{
		"AAA": 0.30, "BBB": 0.10, "CCC": -0.20, "DDD": -0.10, "EEE": 0.00, "NORM": -0.05,
	}
	candidates := make([]repository.ScreenCandidate, 0, len(returns))
	prices := make(map[string][]entity.DailyPrice, len(returns))
	for ticker, ret := range returns {
		candidates = append(candidates, repository.ScreenCandidate{
			Ticker: ticker, Value: i64p(100_000_000_000), Close: f64p(100),
		})
		prices[ticker] = measurementSeries(ticker, targetDays, 100, 100*(1+ret), 100*(1+ret), 100*(1+ret))
	}
	prices["BBCA"] = measurementSeries("BBCA", targetDays, 100, 105, 105, 105)

	priceFake := &fakeMeasurementPrices{calendar: days, candidates: candidates, prices: prices}

	key := anchor.Format("2006-01-02")
	source := &fakeDisclosureFilingSource{
		passedByDay: map[string][]entity.Disclosure{
			key: {
				disclosure(1, "AAA", anchor, "Dividen", "pdf-a"),
				disclosure(2, "AAA", anchor, "Right Issue", "pdf-a2"),
				disclosure(3, "BBB", anchor, "Stock Split", "pdf-b"),
			},
		},
		rejectedByDay: map[string][]entity.Disclosure{
			key: {
				disclosure(4, "CCC", anchor, "", "pdf-c"),
				disclosure(5, "DDD", anchor, "", "pdf-d"),
				disclosure(6, "EEE", anchor, "", "pdf-e"),
			},
		},
	}
	return anchor, days, priceFake, source
}

func newDisclosureGradeEngine(prices MeasurementPriceSource, source MeasurementDisclosureFilingSource) *MeasurementEngine {
	engine := NewMeasurementEngine(nil, logrus.New(), nil, prices, nil, "testrev")
	engine.DisclosureFilings = source
	return engine
}

func findCategory(t *testing.T, buckets []DisclosureCategoryBucket, category string) DisclosureCategoryBucket {
	t.Helper()
	for _, bucket := range buckets {
		if bucket.Category == category {
			return bucket
		}
	}
	t.Fatalf("category %q not in %+v", category, buckets)
	return DisclosureCategoryBucket{}
}

func disclosureRow(t *testing.T, rows []DisclosureGradeRow, id int64) DisclosureGradeRow {
	t.Helper()
	for _, row := range rows {
		if row.DisclosureID == id {
			return row
		}
	}
	t.Fatalf("disclosure %d not in rows", id)
	return DisclosureGradeRow{}
}

// ─── Tests ───────────────────────────────────────────────────────────────────

// TestGradeDisclosures_HorizonsAndCounts — the passed ticker-days' Forward
// Returns vs the Liquidity Baseline, the (ticker, Filing Day) dedupe, and the
// population counts.
func TestGradeDisclosures_HorizonsAndCounts(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if grade.Kind != disclosureGradeKind || grade.GitRevision != "testrev" {
		t.Fatalf("kind/revision = %q/%q", grade.Kind, grade.GitRevision)
	}
	if grade.Counts.Observations != 2 || grade.Counts.Disclosures != 3 {
		t.Fatalf("observations/disclosures = %d/%d, want 2/3 (AAA deduped)",
			grade.Counts.Observations, grade.Counts.Disclosures)
	}
	if grade.Counts.Baseline != 6 || grade.Counts.DistinctTickers != 2 {
		t.Fatalf("baseline/distinct = %d/%d, want 6/2", grade.Counts.Baseline, grade.Counts.DistinctTickers)
	}
	if grade.Counts.RejectedPool != 3 {
		t.Fatalf("reject pool = %d, want 3", grade.Counts.RejectedPool)
	}

	five := horizonExcess(t, grade.Horizons, 5)
	measurementApprox(t, "5d excess", *five, 0.225) // +20% passed - (-2.5%) baseline
	twenty := horizonExcess(t, grade.Horizons, 20)
	measurementApprox(t, "20d excess", *twenty, 0.225)

	for _, stats := range grade.Horizons {
		if stats.Survivor.N != 2 {
			t.Fatalf("%dd passed n = %d, want 2", stats.Horizon, stats.Survivor.N)
		}
		if stats.Survivor.Unresolved != 0 {
			t.Fatalf("%dd unresolved = %d, want 0", stats.Horizon, stats.Survivor.Unresolved)
		}
	}
}

// TestGradeDisclosures_Categories — the keyword split scores each whitelist
// category's ticker-days; buckets are not disjoint, and the union categories
// from a ticker's two filings both count.
func TestGradeDisclosures_Categories(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dividend := findCategory(t, grade.Categories, "Dividen")
	if dividend.N != 1 {
		t.Fatalf("Dividen n = %d, want 1", dividend.N)
	}
	measurementApprox(t, "Dividen 20d excess", *horizonExcess(t, dividend.Horizons, 20), 0.325) // +30% - (-2.5%)

	split := findCategory(t, grade.Categories, "Stock Split")
	if split.N != 1 {
		t.Fatalf("Stock Split n = %d, want 1", split.N)
	}
	measurementApprox(t, "Stock Split 20d excess", *horizonExcess(t, split.Horizons, 20), 0.125) // +10% - (-2.5%)

	// AAA's two filings put it in both of its category buckets, so the split is
	// a per-category score, not a partition.
	rights := findCategory(t, grade.Categories, "Right Issue")
	if rights.N != 1 {
		t.Fatalf("Right Issue n = %d, want 1", rights.N)
	}
}

// TestGradeDisclosures_RowsRetainEveryFiling — the drill-down keeps one row per
// passed disclosure (pdf_url, category) even though the statistics dedupe.
func TestGradeDisclosures_RowsRetainEveryFiling(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(grade.Rows) != 3 {
		t.Fatalf("rows = %d, want 3 (one per passed disclosure)", len(grade.Rows))
	}
	first := disclosureRow(t, grade.Rows, 1)
	if first.Ticker != "AAA" || first.PDFURL != "pdf-a" || first.ReactionAnchor != "2026-06-01" {
		t.Fatalf("row 1 = %+v, want AAA/pdf-a/2026-06-01", first)
	}
	if len(first.Categories) != 1 || first.Categories[0] != "Dividen" {
		t.Fatalf("row 1 categories = %v, want [Dividen]", first.Categories)
	}
	measurementApprox(t, "row 1 5d return", *first.Returns[5], 0.30)
}

// TestGradeDisclosures_RejectSample — the fixed sample is drawn deterministically
// and its verdict slots are left for the agent.
func TestGradeDisclosures_RejectSample(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sample := grade.RejectSampling
	if sample.N != defaultRejectSampleN || sample.Pool != 3 || sample.Sampled != 3 {
		t.Fatalf("sample = n %d / pool %d / sampled %d, want 20/3/3", sample.N, sample.Pool, sample.Sampled)
	}
	if len(sample.Rows) != 3 {
		t.Fatalf("sampled rows = %d, want 3", len(sample.Rows))
	}
	if sample.Procedure == "" {
		t.Fatal("reject sampling procedure is empty")
	}
	for _, row := range sample.Rows {
		if row.Material != nil {
			t.Fatalf("reject %d carries a material verdict %v; the tool must never judge", row.DisclosureID, *row.Material)
		}
	}
	// A rejected row still carries Forward Returns for the reviewer's context.
	measurementApprox(t, "reject CCC 5d return", *sample.Rows[0].Returns[5], -0.20)
}

// TestGradeDisclosures_UnresolvedHorizon — a filing whose 20d target has no
// stored close is counted as unresolved at that horizon, not dropped.
func TestGradeDisclosures_UnresolvedHorizon(t *testing.T) {
	anchor, days, prices, source := disclosureGradeScenario()
	// BBB has no 20d close, so its 20d return is unresolved.
	prices.prices["BBB"] = measurementSeries("BBB", []time.Time{days[0], days[5], days[10]}, 100, 110, 110)

	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	twenty := measurementHorizonByName(t, grade.Horizons, 20)
	if twenty.Survivor.N != 1 || twenty.Survivor.Unresolved != 1 {
		t.Fatalf("20d passed n/unresolved = %d/%d, want 1/1", twenty.Survivor.N, twenty.Survivor.Unresolved)
	}
	if twenty.Baseline.Unresolved != 1 {
		t.Fatalf("20d baseline unresolved = %d, want 1 (BBB)", twenty.Baseline.Unresolved)
	}
	if !strings.Contains(strings.Join(grade.Flags, "\n"), "unresolved: 1 passed ticker-day(s) with no stored close on the 20d target") {
		t.Fatalf("flags missing the unresolved note: %v", grade.Flags)
	}
}

// TestGradeDisclosures_FilingWithoutPostAnchorPrices — a filing whose ticker has
// no post-anchor prices at all is counted (fully unresolved) and reported with
// nil returns, never dropped.
func TestGradeDisclosures_FilingWithoutPostAnchorPrices(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	key := anchor.Format("2006-01-02")
	source.passedByDay[key] = append(source.passedByDay[key], disclosure(7, "ZZZ", anchor, "Dividen", "pdf-z"))

	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if grade.Counts.Observations != 3 {
		t.Fatalf("observations = %d, want 3 (ZZZ counted, not dropped)", grade.Counts.Observations)
	}
	if grade.Counts.Unresolved != 1 {
		t.Fatalf("fully unresolved = %d, want 1 (ZZZ)", grade.Counts.Unresolved)
	}

	row := disclosureRow(t, grade.Rows, 7)
	if row.AnchorClose != nil {
		t.Fatalf("ZZZ anchor close = %v, want nil", *row.AnchorClose)
	}
	if len(row.Unresolved) != 3 {
		t.Fatalf("ZZZ unresolved horizons = %v, want all three", row.Unresolved)
	}
	twenty := measurementHorizonByName(t, grade.Horizons, 20)
	if twenty.Survivor.N != 2 || twenty.Survivor.Unresolved != 1 {
		t.Fatalf("20d n/unresolved = %d/%d, want 2/1", twenty.Survivor.N, twenty.Survivor.Unresolved)
	}
}

// TestGradeDisclosures_ReactionAnchorNormalizesNonTradingDay — a filing on a day
// with no stored row anchors on the next Trading Day, so the reaction is
// measured from a real close.
func TestGradeDisclosures_ReactionAnchorNormalizesNonTradingDay(t *testing.T) {
	base := measurementAnchor()
	raw := measurementCalendar(base, 25)
	gapped := make([]time.Time, 0, len(raw))
	for _, day := range raw {
		if day.Format("2006-01-02") != "2026-06-03" {
			gapped = append(gapped, day)
		}
	}
	filingDay := time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC)
	reaction := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)

	// AAA's series runs from the Reaction Anchor: index 2 of the gapped
	// calendar, with targets 5/10/20 trading days later.
	targetDays := []time.Time{gapped[2], gapped[7], gapped[12], gapped[22]}
	prices := &fakeMeasurementPrices{
		calendar: gapped,
		candidates: []repository.ScreenCandidate{
			{Ticker: "AAA", Value: i64p(100_000_000_000), Close: f64p(100)},
		},
		prices: map[string][]entity.DailyPrice{
			"AAA": measurementSeries("AAA", targetDays, 100, 110, 120, 150),
		},
	}
	source := &fakeDisclosureFilingSource{passedByDay: map[string][]entity.Disclosure{
		"2026-06-03": {disclosure(1, "AAA", filingDay, "Dividen", "pdf-a")},
	}}

	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{filingDay}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(grade.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(grade.Rows))
	}
	row := grade.Rows[0]
	if row.ReactionAnchor != reaction.Format("2006-01-02") {
		t.Fatalf("reaction anchor = %s, want %s", row.ReactionAnchor, reaction.Format("2006-01-02"))
	}
	measurementApprox(t, "5d return", *row.Returns[5], 0.10)
	measurementApprox(t, "10d return", *row.Returns[10], 0.20)
	measurementApprox(t, "20d return", *row.Returns[20], 0.50)
}

// TestGradeDisclosures_PoolsFilingDays — two Filing Days pool their observations
// and retain per-date blocks.
func TestGradeDisclosures_PoolsFilingDays(t *testing.T) {
	anchor, days, prices, source := disclosureGradeScenario()
	second := days[15]
	source.passedByDay[second.Format("2006-01-02")] = []entity.Disclosure{
		disclosure(10, "AAA", second, "Dividen", "pdf-a3"),
	}

	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor, second}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(grade.ReplayDates) != 2 || len(grade.PerDate) != 2 {
		t.Fatalf("replay dates/per-date = %d/%d, want 2/2", len(grade.ReplayDates), len(grade.PerDate))
	}
	if grade.Counts.Observations != 3 {
		t.Fatalf("pooled observations = %d, want 3 (2 at the first filing day, 1 at the second)", grade.Counts.Observations)
	}
}

// TestGradeDisclosures_Guards — a holdout breach, an empty request, an
// out-of-range reject sample, and a mis-wired engine are all structured errors.
func TestGradeDisclosures_Guards(t *testing.T) {
	anchor, days, prices, source := disclosureGradeScenario()
	engine := newDisclosureGradeEngine(prices, source)

	if _, err := engine.GradeDisclosures(context.Background(), DisclosureGradeOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("no anchors err = %v, want ErrInvalidArgument", err)
	}

	seal := days[10]
	if _, err := engine.GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}, HoldoutStart: &seal}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("holdout breach err = %v, want ErrInvalidArgument", err)
	}

	if _, err := engine.GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}, RejectSampleN: 9999}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("bad reject sample err = %v, want ErrInvalidArgument", err)
	}

	bare := newDisclosureGradeEngine(prices, nil)
	if _, err := bare.GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing source err = %v, want ErrInvalidArgument", err)
	}
}

// TestGradeDisclosures_ReadErrorsPropagate — a database failure is an error,
// never an empty report.
func TestGradeDisclosures_ReadErrorsPropagate(t *testing.T) {
	boom := errors.New("db exploded")

	_, _, prices, source := disclosureGradeScenario()
	source.err = boom
	if _, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{measurementAnchor()}}); !errors.Is(err, boom) {
		t.Fatalf("disclosure read err = %v, want the read failure", err)
	}

	_, _, prices2, source2 := disclosureGradeScenario()
	prices2.calendarErr = boom
	if _, err := newDisclosureGradeEngine(prices2, source2).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{measurementAnchor()}}); !errors.Is(err, boom) {
		t.Fatalf("calendar read err = %v, want the read failure", err)
	}

	_, _, prices3, source3 := disclosureGradeScenario()
	prices3.candidatesErr = boom
	if _, err := newDisclosureGradeEngine(prices3, source3).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{measurementAnchor()}}); !errors.Is(err, boom) {
		t.Fatalf("baseline read err = %v, want the read failure", err)
	}
}

// TestGradeDisclosures_EmptyRunFlagged — a filing day with no passed disclosures
// produces a valid report flagged as empty, not an error.
func TestGradeDisclosures_EmptyRunFlagged(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	delete(source.passedByDay, anchor.Format("2006-01-02"))

	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if grade.Counts.Observations != 0 {
		t.Fatalf("observations = %d, want 0", grade.Counts.Observations)
	}
	if !strings.Contains(strings.Join(grade.Flags, "\n"), "no passed-filter disclosures") {
		t.Fatalf("flags missing the empty-run note: %v", grade.Flags)
	}
}

// TestGradeDisclosures_CategoryAnswer — the third success criterion names the
// leading and trailing whitelist categories.
func TestGradeDisclosures_CategoryAnswer(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(grade.SuccessCriteria) != 3 {
		t.Fatalf("criteria = %d, want 3", len(grade.SuccessCriteria))
	}
	third := grade.SuccessCriteria[2].Answer
	if !strings.Contains(third, "Dividen") || !strings.Contains(third, "Stock Split") {
		t.Fatalf("category answer = %q, want the leading and trailing categories", third)
	}
}

// measurementHorizonByName finds a horizon's stats, failing the test if absent.
func measurementHorizonByName(t *testing.T, horizons []HorizonStats, horizon int) HorizonStats {
	t.Helper()
	for _, stats := range horizons {
		if stats.Horizon == horizon {
			return stats
		}
	}
	t.Fatalf("horizon %d not in report: %+v", horizon, horizons)
	return HorizonStats{}
}
