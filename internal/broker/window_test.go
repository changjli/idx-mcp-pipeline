package broker

import (
	"testing"
	"time"
)

func d(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func span(from, to string) Span { return Span{From: d(from), To: d(to)} }

// tradingDays returns market days with a weekend gap, mimicking the stored
// daily_prices calendar.
func tradingDays(days ...string) []time.Time {
	out := make([]time.Time, 0, len(days))
	for _, day := range days {
		out = append(out, d(day))
	}
	return out
}

func TestExactCover_SingleWindow(t *testing.T) {
	days := tradingDays("2026-09-01", "2026-09-02", "2026-09-03")
	got, ok := ExactCover([]Span{span("2026-09-01", "2026-09-03")}, d("2026-09-01"), d("2026-09-03"), days)
	if !ok || len(got) != 1 {
		t.Fatalf("ExactCover = %v, %v; want the one window", got, ok)
	}
}

func TestExactCover_DayRowsTile(t *testing.T) {
	days := tradingDays("2026-09-01", "2026-09-02", "2026-09-03")
	spans := []Span{
		span("2026-09-01", "2026-09-01"),
		span("2026-09-02", "2026-09-02"),
		span("2026-09-03", "2026-09-03"),
	}
	got, ok := ExactCover(spans, d("2026-09-01"), d("2026-09-03"), days)
	if !ok || len(got) != 3 {
		t.Fatalf("ExactCover = %v, %v; want the three day rows", got, ok)
	}
}

func TestExactCover_WeekendGapIsNotAHole(t *testing.T) {
	// Friday 2026-09-04 and Monday 2026-09-07; no trading day in between.
	days := tradingDays("2026-09-04", "2026-09-07")
	spans := []Span{
		span("2026-09-04", "2026-09-04"),
		span("2026-09-07", "2026-09-07"),
	}
	got, ok := ExactCover(spans, d("2026-09-04"), d("2026-09-07"), days)
	if !ok || len(got) != 2 {
		t.Fatalf("ExactCover = %v, %v; want the two day rows across the weekend", got, ok)
	}
}

func TestExactCover_MissingDayFails(t *testing.T) {
	days := tradingDays("2026-09-01", "2026-09-02", "2026-09-03")
	spans := []Span{
		span("2026-09-01", "2026-09-01"),
		span("2026-09-03", "2026-09-03"),
	}
	if _, ok := ExactCover(spans, d("2026-09-01"), d("2026-09-03"), days); ok {
		t.Fatal("ExactCover succeeded with 2026-09-02 missing")
	}
}

func TestExactCover_WiderThanRequestFails(t *testing.T) {
	// The stored chunk starts before the request. It cannot answer a narrower
	// range: aggregation is irreversible.
	days := tradingDays("2026-09-05", "2026-09-06")
	spans := []Span{span("2026-09-01", "2026-09-10")}
	if _, ok := ExactCover(spans, d("2026-09-05"), d("2026-09-06"), days); ok {
		t.Fatal("ExactCover used a window that extends outside the request")
	}
}

func TestExactCover_ChunkWinsOverContainedDays(t *testing.T) {
	days := tradingDays("2026-09-01", "2026-09-02", "2026-09-03")
	spans := []Span{
		span("2026-09-01", "2026-09-01"),
		span("2026-09-02", "2026-09-02"),
		span("2026-09-03", "2026-09-03"),
		span("2026-09-01", "2026-09-03"),
	}
	got, ok := ExactCover(spans, d("2026-09-01"), d("2026-09-03"), days)
	if !ok || len(got) != 1 || got[0] != span("2026-09-01", "2026-09-03") {
		t.Fatalf("ExactCover = %v, %v; want the single chunk", got, ok)
	}
}

func TestExactCover_NoDoubleCount(t *testing.T) {
	// A short span followed by an overlapping long one. Summing both would
	// double-count 2026-09-02, and no disjoint union fills the range, so the
	// answer is a miss (fetch), never an overlapping sum.
	days := tradingDays("2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04")
	spans := []Span{
		span("2026-09-01", "2026-09-02"),
		span("2026-09-02", "2026-09-04"),
	}
	if _, ok := ExactCover(spans, d("2026-09-01"), d("2026-09-04"), days); ok {
		t.Fatal("ExactCover summed overlapping spans")
	}
}

func TestDisjointCover_PicksNonOverlapping(t *testing.T) {
	days := tradingDays("2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04")
	spans := []Span{
		span("2026-09-01", "2026-09-02"),
		span("2026-09-02", "2026-09-04"),
		span("2026-09-03", "2026-09-03"),
	}
	got := DisjointCover(spans, d("2026-09-01"), d("2026-09-04"), days)
	if len(got) != 2 || got[0] != span("2026-09-01", "2026-09-02") || got[1] != span("2026-09-03", "2026-09-03") {
		t.Fatalf("DisjointCover = %v; want 09-01..09-02 then 09-03 (overlap skipped)", got)
	}
}

func TestDisjointCover_SkipsWiderThanRequest(t *testing.T) {
	days := tradingDays("2026-09-05", "2026-09-06")
	got := DisjointCover([]Span{span("2026-09-01", "2026-09-10")}, d("2026-09-05"), d("2026-09-06"), days)
	if len(got) != 0 {
		t.Fatalf("DisjointCover = %v; want nothing (span extends outside the request)", got)
	}
}
