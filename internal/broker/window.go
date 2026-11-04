package broker

import (
	"sort"
	"time"
)

// Span is one stored window's range: WinFrom is its first day, To its window
// end (the table's trading_day). A day row is the degenerate From == To span.
//
// Only a span fully contained in the requested range may answer it: an
// aggregated window cannot be decomposed, so using a wider span would report
// days outside the request as if they belonged to it.
type Span struct {
	From time.Time
	To   time.Time
}

// ExactCover returns spans that exactly tile the requested [from, to] window's
// trading days, or false when no such union exists. It is the answering rule:
// a range is either filled exactly or it is not filled at all.
//
// Only spans fully inside [from, to] are candidates (see Span). A span that
// contains no trading day (a weekend-only window) is ignored. Selection is
// greedy — at each step it takes the contained span covering the next
// uncovered trading day that reaches furthest — so the result is disjoint and
// covers every day in `days` when it succeeds.
func ExactCover(spans []Span, from, to time.Time, days []time.Time) ([]Span, bool) {
	candidates := containedSpans(spans, from, to, days)
	if len(days) == 0 || len(candidates) == 0 {
		return nil, false
	}

	pos := 0
	selected := make([]Span, 0, len(candidates))
	for pos < len(days) {
		best := -1
		bestLast := -1
		for i := range candidates {
			c := candidates[i]
			if c.first > pos {
				break // sorted by first day; every later span starts even later
			}
			// first < pos after the first step means the span overlaps the
			// previously selected one; an overlap can never be summed.
			if c.first != pos {
				continue
			}
			if c.last > bestLast {
				best, bestLast = i, c.last
			}
		}
		if best < 0 {
			return nil, false
		}
		selected = append(selected, candidates[best].span)
		pos = bestLast + 1
	}
	return selected, true
}

// DisjointCover returns a maximal set of non-overlapping spans fully inside
// [from, to], for reads that aggregate whatever the partition holds and declare
// their coverage (the flow tools). It never double-counts: an overlapping span
// is skipped rather than summed. The result is not required to tile the
// requested window — the caller's coverage fields say how much it does.
func DisjointCover(spans []Span, from, to time.Time, days []time.Time) []Span {
	candidates := containedSpans(spans, from, to, days)
	if len(candidates) == 0 {
		return nil
	}
	selected := make([]Span, 0, len(candidates))
	lastEnd := -1
	for _, c := range candidates {
		if c.first <= lastEnd {
			continue // overlaps an already-selected span
		}
		selected = append(selected, c.span)
		lastEnd = c.last
	}
	return selected
}

// indexedSpan is a span resolved against the trading-day calendar.
type indexedSpan struct {
	span        Span
	first, last int
}

// containedSpans resolves fully-contained, non-empty spans to trading-day
// indices and sorts them so a greedy scan can stop once spans start past the
// current position. Same-start spans are ordered longest-first so a chunk wins
// over the day rows it contains.
func containedSpans(spans []Span, from, to time.Time, days []time.Time) []indexedSpan {
	seen := make(map[string]struct{}, len(spans))
	out := make([]indexedSpan, 0, len(spans))
	for _, s := range spans {
		if s.From.IsZero() || s.To.IsZero() {
			continue
		}
		// Calendar containment, not index containment: a chunk that starts
		// before `from` holds days outside the request and cannot answer it.
		if s.From.Before(truncateDay(from)) || s.To.After(truncateDay(to)) {
			continue
		}
		if s.To.Before(s.From) {
			continue
		}
		key := s.From.Format("2006-01-02") + "|" + s.To.Format("2006-01-02")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		first := sort.Search(len(days), func(i int) bool { return !days[i].Before(s.From) })
		last := sort.Search(len(days), func(i int) bool { return days[i].After(s.To) }) - 1
		if first > last || first >= len(days) {
			continue // no trading day inside this span
		}
		out = append(out, indexedSpan{span: s, first: first, last: last})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].first != out[j].first {
			return out[i].first < out[j].first
		}
		return out[i].last > out[j].last
	})
	return out
}

// truncateDay returns the date at UTC midnight, matching the stored DATE
// columns every span value comes from.
func truncateDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
