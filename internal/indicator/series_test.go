package indicator

import (
	"math"
	"testing"
)

// NaN is what a row carries for a column it did not store, so the narrowing
// must be judged per column: a volume-reading entry loses the row, a
// close-reading entry keeps it.
func TestSeriesRows_NarrowsToRowsCarryingTheNeededColumns(t *testing.T) {
	nan := math.NaN()
	s := Series{
		High:   []float64{12, nan, 12, 16, 14},
		Low:    []float64{8, nan, 10, 11, 11},
		Close:  []float64{10, 11, 11, 12, 12},
		Volume: []float64{100, 200, nan, 400, 500},
	}

	cases := []struct {
		name  string
		needs Columns
		want  int
	}{
		{name: "close only keeps every row", needs: 0, want: 5},
		{name: "high/low drops the gap row", needs: UsesHighLow, want: 4},
		{name: "volume drops its gap row", needs: UsesVolume, want: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := s.Rows(tc.needs)
			if rows.Len() != tc.want {
				t.Fatalf("Rows(%v).Len() = %d, want %d", tc.needs, rows.Len(), tc.want)
			}
			// The columns stay parallel and the surviving rows keep their
			// values — a narrowed series is still a series.
			if len(rows.High) != rows.Len() || len(rows.Low) != rows.Len() || len(rows.Volume) != rows.Len() {
				t.Errorf("Rows(%v) columns are not parallel: high=%d low=%d close=%d volume=%d",
					tc.needs, len(rows.High), len(rows.Low), len(rows.Close), len(rows.Volume))
			}
			for i := range rows.Close {
				if math.IsNaN(rows.Close[i]) {
					t.Errorf("Rows(%v) kept a row with no close at index %d", tc.needs, i)
				}
			}
		})
	}

	withHighLow := s.Rows(UsesHighLow)
	if withHighLow.Len() > 1 && withHighLow.High[1] != 12 {
		t.Errorf("narrowed high = %v, want the row after the gap (12)", withHighLow.High)
	}
}

// A malformed series (columns of unequal length) is a caller's bug, and it must
// surface as "no value" rather than as a panic inside the library's same-size
// check — a panic here would take down the MCP server.
func TestValue_MalformedSeriesReportsNoValue(t *testing.T) {
	s := Series{
		High:   []float64{12, 16},
		Low:    []float64{8},
		Close:  []float64{10, 11, 11},
		Volume: []float64{100, 200, 300},
	}
	req, err := Parse("sma:2")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if value, ok := Value(req, s); ok {
		t.Errorf("Value(sma:2) = %v, want no value for a malformed series", value)
	}
}

// Every registered entry declares the columns it reads, and narrowing over
// those columns must leave it enough rows on a full series — a wrong Needs
// declaration would show up as a permanently null column in the screener.
func TestEveryEntry_ComputesOnAFullSeries(t *testing.T) {
	// macdWarmup rows of a rising ramp with every column present.
	n := macdWarmup + 60
	s := Series{
		High:   make([]float64, n),
		Low:    make([]float64, n),
		Close:  make([]float64, n),
		Volume: make([]float64, n),
	}
	for i := 0; i < n; i++ {
		base := 1000 + float64(i)*5
		s.Close[i] = base
		s.High[i] = base + 20
		s.Low[i] = base - 20
		s.Volume[i] = float64(100000 + i*1000)
	}

	for _, name := range Names() {
		t.Run(name, func(t *testing.T) {
			req, err := Parse(name)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", name, err)
			}
			if _, ok := Value(req, s); !ok {
				t.Errorf("Value(%q) reported no value on a %d-row series with every column stored",
					req.Key(), n)
			}
		})
	}
}
