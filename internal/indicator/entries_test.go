package indicator

import (
	"math"
	"testing"
)

// The OHLCV fixture, hand-checked so every formula below stays arithmetic a
// reader can follow:
//
//	row:      1    2    3    4    5
//	high:    12   16   12   16   14
//	low:      8   10   10   11   11
//	close:   10   11   11   12   12
//	volume: 100  200  300  400  500
func ohlcvSeries() Series {
	return Series{
		High:   []float64{12, 16, 12, 16, 14},
		Low:    []float64{8, 10, 10, 11, 11},
		Close:  []float64{10, 11, 11, 12, 12},
		Volume: []float64{100, 200, 300, 400, 500},
	}
}

// macdSeries is the MACD fixture: 20 flat rows at 100, then +1 per row to 120.
// The flat head settles both EMAs at 100, so the histogram is driven purely by
// the ramp and the arithmetic stays checkable.
func macdSeries() Series {
	closes := make([]float64, 40)
	for i := range closes {
		if i < 20 {
			closes[i] = 100
		} else {
			closes[i] = 100 + float64(i-19)
		}
	}
	return closeSeries(closes)
}

// Every v1 entry's value is asserted against arithmetic written out in the
// comment: the values come from the pinned library's documented formulas or,
// where the registry computes in-package, from the definition in the entry's
// Summary.
func TestValue_HandComputedEntries(t *testing.T) {
	cases := []struct {
		name   string
		spec   string
		series Series
		want   float64
		// why documents the arithmetic in the shape the value was derived from.
		why string
	}{
		{
			name:   "atr",
			spec:   "atr:3",
			series: ohlcvSeries(),
			// TR = max(H-L, H-C, C-L) per bar = [4, 6, 2, 5, 3]
			// ATR(3) = SMA of the last three TRs = (2+5+3)/3
			want: 3.3333333,
			why:  "(2+5+3)/3",
		},
		{
			name:   "obv",
			spec:   "obv",
			series: ohlcvSeries(),
			// close 10,11,11,12,12 against volume 100,200,300,400,500:
			// 11>10 +200 = 200; 11==11 = 200; 12>11 +400 = 600; 12==12 = 600
			want: 600,
			why:  "200 + 400",
		},
		{
			name:   "volume_ma",
			spec:   "volume_ma:3",
			series: ohlcvSeries(),
			want:   400,
			why:    "(300+400+500)/3",
		},
		{
			name:   "volume_ratio",
			spec:   "volume_ratio:3",
			series: ohlcvSeries(),
			want:   1.25,
			why:    "500/400",
		},
		{
			name:   "range_position",
			spec:   "range_position:3",
			series: ohlcvSeries(),
			// last three rows: highs 12,16,14 -> 16; lows 10,11,11 -> 10
			want: 0.3333333,
			why:  "(12-10)/(16-10)",
		},
		{
			name:   "roc",
			spec:   "roc:3",
			series: ohlcvSeries(),
			// close[5]=12 against close[2]=11
			want: 9.0909091,
			why:  "(12/11 - 1) * 100",
		},
		{
			name:   "ma_slope",
			spec:   "ma_slope:2",
			series: ohlcvSeries(),
			// MA2 = [_, 10.5, 11, 11.5, 12]; latest against the MA 2 rows back
			want: 9.0909091,
			why:  "(12/11 - 1) * 100",
		},
		{
			name:   "ma_distance",
			spec:   "ma_distance:3:5",
			series: ohlcvSeries(),
			// MA3 latest = (11+12+12)/3 = 11.6666667
			want: 2.8571429,
			why:  "(12/11.6666667 - 1) * 100",
		},
		{
			name:   "ma_spread",
			spec:   "ma_spread:3:5",
			series: ohlcvSeries(),
			// MA3 = 11.6666667 against MA5 = (10+11+11+12+12)/5 = 11.2
			want: 4.1666667,
			why:  "(11.6666667/11.2 - 1) * 100",
		},
		{
			name:   "bb_width",
			spec:   "bb_width:3",
			series: ohlcvSeries(),
			// last three closes 11,12,12: population std = sqrt(2/9) = 0.4714045,
			// width = 4*std/middle = 4*0.4714045/11.6666667
			want: 16.1624407,
			why:  "4*0.4714045/11.6666667 * 100",
		},
		{
			name:   "macd",
			spec:   "macd",
			series: macdSeries(),
			// EMA12 k=2/13, EMA26 k=2/27, signal EMA9 (k=0.2) of the line:
			// row 40: EMA12 = 114.6946926, EMA26 = 110.1818526 -> line 4.5128400
			//         signal = 3.6686956 -> histogram 0.8441444
			want: 0.8441444,
			why:  "4.5128400 - 3.6686956",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := Parse(tc.spec)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.spec, err)
			}
			got, ok := Value(req, tc.series)
			if !ok {
				t.Fatalf("Value(%q) reported no value for %d rows", tc.spec, tc.series.Len())
			}
			if math.Abs(got-tc.want) > 1e-4 {
				t.Errorf("Value(%q) = %v, want %v (%s)", tc.spec, got, tc.want, tc.why)
			}
		})
	}
}

// An entry that reads volume or high/low gets no value when the rows do not
// carry the column — a missing volume must never be read as zero volume, which
// would look like a real collapse in volume_ratio and a real signal in OBV.
func TestValue_MissingColumnIsNotAZero(t *testing.T) {
	closesOnly := closeSeries([]float64{10, 11, 11, 12, 12})

	for _, spec := range []string{"atr:3", "range_position:3", "obv", "volume_ma:3", "volume_ratio:3"} {
		t.Run(spec, func(t *testing.T) {
			req, err := Parse(spec)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", spec, err)
			}
			if value, ok := Value(req, closesOnly); ok {
				t.Errorf("Value(%q) = %v, want no value for a series with no stored %s",
					spec, value, columnName(req.Name))
			}
		})
	}
}

func columnName(name string) string {
	switch name {
	case "atr", "range_position":
		return "high/low"
	default:
		return "volume"
	}
}

// A flat period range has no position within it: the row reports no value
// rather than a fabricated "at the low".
func TestValue_FlatRangeHasNoPosition(t *testing.T) {
	flat := Series{
		High:   []float64{10, 10, 10},
		Low:    []float64{10, 10, 10},
		Close:  []float64{10, 10, 10},
		Volume: []float64{0, 0, 0},
	}
	req, err := Parse("range_position:3")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if value, ok := Value(req, flat); ok {
		t.Errorf("Value(range_position:3) = %v, want no value for a zero-width range", value)
	}
}

// Derived indicators must agree with the MA values they collapse, so the
// screener's `ma_distance > 0` and a caller's own MA reading cannot drift.
func TestDerivedIndicators_AgreeWithTheMAsTheyCollapse(t *testing.T) {
	series := ohlcvSeries()

	fastReq, err := Parse("sma:3")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	slowReq, err := Parse("sma:5")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	distanceReq, err := Parse("ma_distance:3:5")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	spreadReq, err := Parse("ma_spread:3:5")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	fast, ok := Value(fastReq, series)
	if !ok {
		t.Fatal("sma:3 reported no value")
	}
	slow, ok := Value(slowReq, series)
	if !ok {
		t.Fatal("sma:5 reported no value")
	}
	close := last(series.Close)

	wantDistance := (close/fast - 1) * 100
	if got, _ := Value(distanceReq, series); math.Abs(got-wantDistance) > 1e-9 {
		t.Errorf("ma_distance = %v, want %v from sma:3 = %v", got, wantDistance, fast)
	}

	wantSpread := (fast/slow - 1) * 100
	if got, _ := Value(spreadReq, series); math.Abs(got-wantSpread) > 1e-9 {
		t.Errorf("ma_spread = %v, want %v from sma:3 = %v and sma:5 = %v", got, wantSpread, fast, slow)
	}
}

// The percent-scale entries are what a fixed threshold is compared against, so
// their sign convention is the contract the screener relies on.
func TestDerivedIndicators_SignConvention(t *testing.T) {
	// A monotonic ramp, so close is above the fast MA when rising and below it
	// when falling, with no range-position or ATR involvement.
	ramp := func(first, step float64) Series {
		closes := make([]float64, 10)
		for i := range closes {
			closes[i] = first + float64(i)*step
		}
		return Series{
			High:   append([]float64(nil), closes...),
			Low:    append([]float64(nil), closes...),
			Close:  closes,
			Volume: make([]float64, len(closes)),
		}
	}
	rising := ramp(9, 1)    // 9..18
	falling := ramp(18, -1) // 18..9

	for _, tc := range []struct {
		name   string
		spec   string
		series Series
		want   bool // want positive
	}{
		{name: "ma_distance above the fast MA", spec: "ma_distance:3:5", series: rising, want: true},
		{name: "ma_distance below the fast MA", spec: "ma_distance:3:5", series: falling, want: false},
		{name: "ma_spread fast over slow", spec: "ma_spread:3:5", series: rising, want: true},
		{name: "ma_spread fast under slow", spec: "ma_spread:3:5", series: falling, want: false},
		{name: "roc rising", spec: "roc:3", series: rising, want: true},
		{name: "roc falling", spec: "roc:3", series: falling, want: false},
		{name: "ma_slope rising", spec: "ma_slope:3", series: rising, want: true},
		{name: "ma_slope falling", spec: "ma_slope:3", series: falling, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := Parse(tc.spec)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.spec, err)
			}
			got, ok := Value(req, tc.series)
			if !ok {
				t.Fatalf("Value(%q) reported no value", tc.spec)
			}
			if positive := got > 0; positive != tc.want {
				t.Errorf("Value(%q) = %v, want positive = %v", tc.spec, got, tc.want)
			}
		})
	}
}
