package indicator

import "math"

// Columns is the set of stored price columns an entry's formula reads *beyond*
// close. Every v1 entry reads close, so the warm-up bar is judged against the
// rows that also carry these.
type Columns uint8

const (
	// UsesHighLow adds high and low (ATR, range position).
	UsesHighLow Columns = 1 << iota
	// UsesVolume adds volume (OBV, volume MA, volume ratio).
	UsesVolume
)

// Series is one ticker's stored price history, ascending by trading day, as
// parallel columns of equal length. Every row carries a stored close — a row
// without one is dropped at the seam, because no registry entry can use it. A
// row whose high, low, or volume was not stored holds NaN there, so an entry
// that reads the column sees a shorter series rather than a zero, which would
// read as a real (and maximally bearish) observation.
type Series struct {
	High   []float64
	Low    []float64
	Close  []float64
	Volume []float64
}

// Len is the number of trading days in the series.
func (s Series) Len() int { return len(s.Close) }

// Rows narrows the series to the rows carrying every column the entry needs,
// keeping the columns parallel. The caller then computes over a dense series
// and never has to re-check a column for a gap.
//
// A malformed series (columns of unequal length) narrows to nothing, so a
// caller's mistake surfaces as insufficient history rather than as a panic
// inside the library's same-size checks.
func (s Series) Rows(needs Columns) Series {
	if !s.wellFormed() {
		return Series{}
	}
	if needs == 0 {
		return s
	}

	kept := s.keptRows(needs)
	highs := make([]float64, 0, len(kept))
	lows := make([]float64, 0, len(kept))
	closes := make([]float64, 0, len(kept))
	volumes := make([]float64, 0, len(kept))
	for _, i := range kept {
		highs = append(highs, s.High[i])
		lows = append(lows, s.Low[i])
		closes = append(closes, s.Close[i])
		volumes = append(volumes, s.Volume[i])
	}

	return Series{High: highs, Low: lows, Close: closes, Volume: volumes}
}

// keptRows returns the indexes of the rows carrying every column listed in
// needs, ascending. Rows narrows to exactly these, so a caller holding values
// computed over the narrowed series can map them back onto the full row axis
// instead of onto a series whose gaps have shifted every index.
func (s Series) keptRows(needs Columns) []int {
	if !s.wellFormed() {
		return nil
	}
	kept := make([]int, 0, len(s.Close))
	for i := range s.Close {
		if s.rowUsable(i, needs) {
			kept = append(kept, i)
		}
	}
	return kept
}

// wellFormed reports whether the columns are parallel, the invariant every
// method here and every library call depends on.
func (s Series) wellFormed() bool {
	n := len(s.Close)
	return len(s.High) == n && len(s.Low) == n && len(s.Volume) == n
}

// rowUsable reports whether row i carries every column the entry needs. A
// missing close always disqualifies the row; an infinite value is treated as
// missing too, since it can only come from a corrupt stored row.
func (s Series) rowUsable(i int, needs Columns) bool {
	if isMissing(s.Close[i]) {
		return false
	}
	if needs&UsesHighLow != 0 && (isMissing(s.High[i]) || isMissing(s.Low[i])) {
		return false
	}
	if needs&UsesVolume != 0 && isMissing(s.Volume[i]) {
		return false
	}
	return true
}

// isMissing marks a column value the row did not carry.
func isMissing(value float64) bool {
	return math.IsNaN(value) || math.IsInf(value, 0)
}
