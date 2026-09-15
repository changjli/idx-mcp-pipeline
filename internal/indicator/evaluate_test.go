package indicator

import (
	"math"
	"testing"
)

// evalSeries is a 60-row series every registered entry can compute over: the
// closes drift with a sine so no window is flat (a flat high-low range would
// leave range_position without a denominator), with high/low around them and a
// volume that varies row to row.
func evalSeries() Series {
	n := 60
	s := Series{
		High:   make([]float64, n),
		Low:    make([]float64, n),
		Close:  make([]float64, n),
		Volume: make([]float64, n),
	}
	for i := 0; i < n; i++ {
		close := 100 + 10*math.Sin(float64(i)/5) + float64(i)
		s.Close[i] = close
		s.High[i] = close + 1
		s.Low[i] = close - 1
		s.Volume[i] = float64(1000 + 50*i)
	}
	return s
}

// The array is the shape a series-mode read hands the AI: one value per row,
// ascending, with the warm-up rows empty instead of carrying the library's
// short-window head values.
func TestEvaluate_ArrayIsParallelToTheRows(t *testing.T) {
	req, err := Parse("sma:3")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	evaluated := Evaluate(req, closeSeries([]float64{1, 2, 3, 4, 5}))
	if got := len(evaluated.Array); got != 5 {
		t.Fatalf("array length = %d, want 5 (one per row)", got)
	}
	// sma:3 settles at row 3: (1+2+3)/3, then (2+3+4)/3, then (3+4+5)/3.
	for i, want := range []float64{math.NaN(), math.NaN(), 2, 3, 4} {
		if math.IsNaN(want) {
			if !math.IsNaN(evaluated.Array[i]) {
				t.Errorf("array[%d] = %v, want NaN (below the 3-row bar)", i, evaluated.Array[i])
			}
			continue
		}
		if diff := evaluated.Array[i] - want; diff > 1e-9 || diff < -1e-9 {
			t.Errorf("array[%d] = %v, want %v", i, evaluated.Array[i], want)
		}
	}
	if evaluated.UsableRows != 5 || evaluated.RequiredRows != 3 || !evaluated.Sufficient() {
		t.Errorf("basis = %+v, want 5 usable rows over a 3-row bar", evaluated)
	}
}

// A hand-computed array for an in-package entry, so the per-row arithmetic is
// pinned and not just the last element.
func TestEvaluate_HandComputedArray(t *testing.T) {
	req, err := Parse("roc:3")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// roc:3 = close[i] against close[i-3], in percent: (4/1-1)*100, (5/2-1)*100.
	evaluated := Evaluate(req, closeSeries([]float64{1, 2, 3, 4, 5}))
	want := []float64{math.NaN(), math.NaN(), math.NaN(), 300, 150}
	for i, w := range want {
		got := evaluated.Array[i]
		if math.IsNaN(w) {
			if !math.IsNaN(got) {
				t.Errorf("array[%d] = %v, want NaN", i, got)
			}
			continue
		}
		if diff := got - w; diff > 1e-9 || diff < -1e-9 {
			t.Errorf("array[%d] = %v, want %v", i, got, w)
		}
	}
	if evaluated.RequiredRows != 4 {
		t.Errorf("required rows = %d, want 4 (period+1)", evaluated.RequiredRows)
	}
}

// History shorter than the bar leaves the array empty of values rather than
// short-window ones — the same rule screen mode applies, read as a whole series.
func TestEvaluate_InsufficientHistoryIsAllEmpty(t *testing.T) {
	req, err := Parse("sma:20")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	evaluated := Evaluate(req, closeSeries([]float64{1, 2, 3}))
	if evaluated.Sufficient() {
		t.Errorf("basis = %+v, want insufficient (3 rows under a 20-row bar)", evaluated)
	}
	for i, got := range evaluated.Array {
		if !math.IsNaN(got) {
			t.Errorf("array[%d] = %v, want NaN (insufficient history)", i, got)
		}
	}
	if _, ok := Value(req, closeSeries([]float64{1, 2, 3})); ok {
		t.Error("Value reported a value on a series below the bar")
	}
}

// A row the entry's columns are missing from is a gap at its own index, not a
// shift: the values after it still line up with the rows they were computed
// from, and the row itself is empty rather than read as a zero volume.
func TestEvaluate_MissingColumnIsAnEmptyRowNotAShift(t *testing.T) {
	req, err := Parse("volume_ma:3")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	series := Series{
		High:   []float64{11, 12, 12, 13, 13},
		Low:    []float64{9, 10, 10, 11, 11},
		Close:  []float64{10, 11, 11, 12, 12},
		Volume: []float64{100, 200, math.NaN(), 400, 500},
	}

	evaluated := Evaluate(req, series)
	if evaluated.UsableRows != 4 {
		t.Errorf("usable rows = %d, want 4 (one row has no volume)", evaluated.UsableRows)
	}
	if !evaluated.Sufficient() {
		t.Errorf("basis = %+v, want sufficient (4 usable rows over a 3-row bar)", evaluated)
	}
	if !math.IsNaN(evaluated.Array[2]) {
		t.Errorf("array[2] = %v, want NaN (that row stored no volume)", evaluated.Array[2])
	}
	// The remaining three values come from the narrowed series [100,200,400,500]:
	// sma(3) = 100, 150, 366.6666667 — the last two land on their own rows.
	if diff := evaluated.Array[3] - 233.3333333; diff > 1e-6 || diff < -1e-6 {
		t.Errorf("array[3] = %v, want 233.3333333 ((200+400+500)/3)", evaluated.Array[3])
	}
	if diff := evaluated.Array[4] - 366.6666667; diff > 1e-6 || diff < -1e-6 {
		t.Errorf("array[4] = %v, want 366.6666667 ((400+500+... ))", evaluated.Array[4])
	}
}

// The invariant behind two response shapes: a screen row's latest value is the
// last element of the same array a series read returns, and every row below the
// entry's bar is empty. A registry entry that broke either would show up as a
// screener filter disagreeing with a deep-dive read of the same ticker.
func TestEvaluate_LastElementIsTheScreenValueForEveryEntry(t *testing.T) {
	series := evalSeries()
	if len(Names()) == 0 {
		t.Fatal("registry is empty")
	}

	for _, name := range Names() {
		t.Run(name, func(t *testing.T) {
			req, err := Parse(name)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			evaluated := Evaluate(req, series)
			if len(evaluated.Array) != series.Len() {
				t.Fatalf("array length = %d, want %d (one per row)", len(evaluated.Array), series.Len())
			}

			required := RequiredRows(req)
			if !evaluated.Sufficient() {
				t.Fatalf("basis = %+v, want sufficient over %d rows", evaluated, series.Len())
			}
			for i, got := range evaluated.Array {
				isEmpty := math.IsNaN(got) || math.IsInf(got, 0)
				if i < required-1 && !isEmpty {
					t.Errorf("array[%d] = %v, want empty (below the %d-row bar)", i, got, required)
				}
				if i >= required-1 && isEmpty {
					t.Errorf("array[%d] = empty, want a value (the %d-row bar is cleared)", i, required)
				}
			}

			value, ok := Value(req, series)
			if !ok {
				t.Fatal("Value reported no value where the array has one")
			}
			if diff := value - last(evaluated.Array); diff > 1e-9 || diff < -1e-9 {
				t.Errorf("Value = %v, array's last = %v — the two shapes disagree", value, last(evaluated.Array))
			}
		})
	}
}

// An unknown name has no array at all, so a caller maps it to the same
// "nothing to report" path as insufficient history.
func TestEvaluate_UnknownNameIsEmpty(t *testing.T) {
	evaluated := Evaluate(Request{Name: "smma", Periods: []int{20}}, evalSeries())
	if len(evaluated.Array) != 0 {
		t.Errorf("array = %v, want empty", evaluated.Array)
	}
	if evaluated.Sufficient() {
		t.Error("an unknown name must not report sufficient history")
	}
}

// A malformed series (columns of unequal length) evaluates to empties rather
// than panicking inside a library's same-size check.
func TestEvaluate_MalformedSeriesReportsNothing(t *testing.T) {
	for _, name := range []string{"sma:20", "atr:14", "obv"} {
		req, err := Parse(name)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		evaluated := Evaluate(req, Series{Close: make([]float64, 5), High: make([]float64, 2)})
		if evaluated.Sufficient() {
			t.Errorf("%s reported sufficient history on a malformed series", name)
		}
		for i, got := range evaluated.Array {
			if !math.IsNaN(got) {
				t.Errorf("%s array[%d] = %v, want NaN", name, i, got)
			}
		}
	}
}
