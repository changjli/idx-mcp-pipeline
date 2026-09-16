package indicator

import (
	"errors"
	"math"
	"strings"
	"testing"
)

// closeSeries is the close-only series the pure-close entries compute over:
// high, low, and volume carry the NaN that stands for a row which did not
// store them.
func closeSeries(closes []float64) Series {
	missing := make([]float64, len(closes))
	for i := range missing {
		missing[i] = math.NaN()
	}
	return Series{High: missing, Low: missing, Close: closes, Volume: missing}
}

// Fixtures are hand-computed from the pinned library's documented formulas
// (github.com/cinar/indicator v1.3.0), not copied from a run — so a library
// formula change surfaces here as a failure rather than as a silently
// different value in the screener.
func TestValue_HandComputedFixtures(t *testing.T) {
	closes1to5 := []float64{1, 2, 3, 4, 5}
	closes1to20 := make([]float64, 20)
	for i := range closes1to20 {
		closes1to20[i] = float64(i + 1) // 1..20
	}

	// RSI (period 3) over 10,11,12,11,13,12:
	//   gains  = [0,1,1,0,2,0]  (differences from i=1; gains[0] is 0)
	//   losses = [0,0,0,1,0,1]
	//   RMA(3, gains)[5]:  seed 0, 0.5, 0.6666667 (running means), then
	//     (0.6666667*2+0)/3 = 0.4444444
	//     (0.4444444*2+2)/3 = 0.9629630
	//     (0.9629630*2+0)/3 = 0.6419753
	//   RMA(3, losses)[5]: seed 0, 0, 0.3333333 (running means), then
	//     (0.3333333*2+0)/3 = 0.2222222
	//     (0.2222222*2+0)/3 = 0.1481481
	//     (0.1481481*2+1)/3 = 0.4814815
	//   RS  = 0.6419753/0.4814815 = 1.3333333
	//   RSI = 100 - 100/(1+1.3333333) = 57.142857
	// Wilder smoothing is the point of the exact-value check: the plain average
	// of the same gains and losses is 100 - 100/(1+ (3/3)/(1/3)) = 50, so an
	// SMA-smoothed implementation fails here.
	rsiCloses := []float64{10, 11, 12, 11, 13, 12}

	cases := []struct {
		name   string
		spec   string
		closes []float64
		want   float64
	}{
		{
			// (3+4+5)/3
			name:   "sma period 3",
			spec:   "sma:3",
			closes: closes1to5,
			want:   4,
		},
		{
			// Default period 20 over 1..20: (1+..+20)/20
			name:   "sma default period",
			spec:   "sma",
			closes: closes1to20,
			want:   10.5,
		},
		{
			// EMA(3) k = 2/(1+3) = 0.5, seeded at the first close:
			//   1, 1.5, 2.25, 3.125, 4.0625
			name:   "ema period 3",
			spec:   "ema:3",
			closes: closes1to5,
			want:   4.0625,
		},
		{
			name:   "rsi period 3 (Wilder smoothing)",
			spec:   "rsi:3",
			closes: rsiCloses,
			want:   57.142857,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := Parse(tc.spec)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.spec, err)
			}
			got, ok := Value(req, closeSeries(tc.closes))
			if !ok {
				t.Fatalf("Value(%q) reported insufficient history for %d rows", tc.spec, len(tc.closes))
			}
			if math.Abs(got-tc.want) > 1e-4 {
				t.Errorf("Value(%q) = %v, want %v", tc.spec, got, tc.want)
			}
		})
	}
}

// A series shorter than the entry's warm-up bar is never computed on the short
// window — the caller must see ok=false so the row can carry an insufficient
// flag instead of a number the trader would read as fully warmed.
func TestValue_InsufficientWarmup(t *testing.T) {
	cases := []struct {
		name   string
		spec   string
		closes []float64
		wantOK bool
	}{
		{name: "sma exactly at the bar", spec: "sma:3", closes: []float64{1, 2, 3}, wantOK: true},
		{name: "sma one row short", spec: "sma:3", closes: []float64{1, 2}, wantOK: false},
		{name: "rsi exactly at the bar (period+1)", spec: "rsi:3", closes: []float64{10, 11, 12, 11}, wantOK: true},
		{name: "rsi one row short", spec: "rsi:3", closes: []float64{10, 11, 12}, wantOK: false},
		{name: "ema one row short", spec: "ema:3", closes: []float64{1, 2}, wantOK: false},
		{name: "roc exactly at the bar (period+1)", spec: "roc:3", closes: []float64{10, 11, 12, 11}, wantOK: true},
		{name: "roc one row short", spec: "roc:3", closes: []float64{10, 11, 12}, wantOK: false},
		{name: "ma_slope needs two full periods", spec: "ma_slope:3", closes: []float64{10, 11, 12, 11, 13, 12}, wantOK: true},
		{name: "ma_slope one row short of two periods", spec: "ma_slope:3", closes: []float64{10, 11, 12, 11, 13}, wantOK: false},
		{name: "ma_distance needs only the fast MA", spec: "ma_distance:5:50", closes: []float64{10, 11, 12, 11, 13}, wantOK: true},
		{name: "ma_spread needs the slow MA", spec: "ma_spread:5:50", closes: []float64{10, 11, 12, 11, 13}, wantOK: false},
		{name: "macd one row short of the signal warm-up", spec: "macd", closes: make([]float64, macdWarmup-1), wantOK: false},
		{name: "macd exactly at the signal warm-up", spec: "macd", closes: make([]float64, macdWarmup), wantOK: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := Parse(tc.spec)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.spec, err)
			}
			if _, ok := Value(req, closeSeries(tc.closes)); ok != tc.wantOK {
				t.Errorf("Value(%q) ok = %v, want %v (%d rows)", tc.spec, ok, tc.wantOK, len(tc.closes))
			}
		})
	}
}

func TestParse(t *testing.T) {
	cases := []struct {
		spec        string
		wantName    string
		wantPeriods []int
		wantErr     bool
	}{
		{spec: "sma", wantName: "sma", wantPeriods: []int{20}},
		{spec: "sma:50", wantName: "sma", wantPeriods: []int{50}},
		{spec: "  EMA:200  ", wantName: "ema", wantPeriods: []int{200}},
		{spec: "rsi:14", wantName: "rsi", wantPeriods: []int{14}},
		{spec: "rsi", wantName: "rsi", wantPeriods: []int{14}},
		{spec: "ma_distance", wantName: "ma_distance", wantPeriods: []int{20, 50}},
		{spec: "ma_distance:10:30", wantName: "ma_distance", wantPeriods: []int{10, 30}},
		{spec: "MA_SPREAD:5:10", wantName: "ma_spread", wantPeriods: []int{5, 10}},
		{spec: "macd", wantName: "macd"},
		{spec: "obv", wantName: "obv"},
		{spec: "sma:1", wantErr: true},
		{spec: "sma:0", wantErr: true},
		{spec: "sma:401", wantErr: true},
		{spec: "sma:x", wantErr: true},
		{spec: "sma:", wantErr: true},
		{spec: "sma:20:50", wantErr: true}, // single-period entry, two given
		{spec: "ma_distance:10", wantErr: true},
		{spec: "ma_distance:30:10", wantErr: true}, // slow must be the longer one
		{spec: "ma_distance:20:20", wantErr: true},
		{spec: "ma_distance:10:30:50", wantErr: true},
		{spec: "macd:12", wantErr: true}, // fixed-parameter entry
		{spec: "obv:20", wantErr: true},
		{spec: "", wantErr: true},
		{spec: "adx:14", wantErr: true},        // deferred follow-up, not v1
		{spec: "stochastic:14", wantErr: true}, // deferred follow-up, not v1
		{spec: "supertrend:10", wantErr: true}, // deferred follow-up, not v1
		{spec: "nope:20", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			req, err := Parse(tc.spec)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("Parse(%q) error = %v, want ErrInvalid", tc.spec, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.spec, err)
			}
			if req.Name != tc.wantName || !sameInts(req.Periods, tc.wantPeriods) {
				t.Errorf("Parse(%q) = %s/%v, want %s/%v",
					tc.spec, req.Name, req.Periods, tc.wantName, tc.wantPeriods)
			}
		})
	}
}

func sameInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// The unknown-name error must enumerate the valid names — a typo has to be
// fixable from the error alone, without a second call.
func TestLookup_UnknownNameEnumeratesValidNames(t *testing.T) {
	_, err := Lookup("smma")
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Lookup error = %v, want ErrInvalid", err)
	}
	for _, name := range Names() {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not enumerate valid name %q", err.Error(), name)
		}
	}
}

// The key is how a caller reads a value back out of a row, so it must name
// every period the value was computed with — and a fixed-parameter entry has
// none to name.
func TestRequestKey_CarriesEveryPeriod(t *testing.T) {
	cases := []struct {
		spec string
		want string
	}{
		{spec: "sma", want: "sma:20"},
		{spec: "ma_distance", want: "ma_distance:20:50"},
		{spec: "ma_distance:10:30", want: "ma_distance:10:30"},
		{spec: "macd", want: "macd"},
		{spec: "obv", want: "obv"},
	}
	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			req, err := Parse(tc.spec)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if got := req.Key(); got != tc.want {
				t.Errorf("Key() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRequiredRows(t *testing.T) {
	cases := []struct {
		spec string
		want int
	}{
		{spec: "sma:20", want: 20},
		{spec: "ema:50", want: 50},
		{spec: "rsi:14", want: 15},
		{spec: "roc:20", want: 21},
		{spec: "ma_slope:20", want: 40},
		{spec: "bb_width:20", want: 20},
		{spec: "atr:14", want: 14},
		{spec: "obv", want: 2},
		{spec: "macd", want: macdWarmup},
		{spec: "ma_distance:20:50", want: 20},
		{spec: "ma_spread:20:50", want: 50},
	}
	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			req, err := Parse(tc.spec)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			if got := RequiredRows(req); got != tc.want {
				t.Errorf("RequiredRows(%q) = %d, want %d", tc.spec, got, tc.want)
			}
		})
	}
}

// The catalog is what the tool description is generated from, so every entry
// must appear with the spec a caller can type — and the deferred indicators
// must not.
func TestCatalog_ListsEveryRegisteredName(t *testing.T) {
	catalog := Catalog()
	for _, name := range Names() {
		if !strings.Contains(catalog, name) {
			t.Errorf("catalog %q is missing registered name %q", catalog, name)
		}
	}
	for _, deferred := range []string{"adx", "stochastic", "supertrend"} {
		if strings.Contains(catalog, deferred) {
			t.Errorf("catalog %q advertises deferred indicator %q", catalog, deferred)
		}
	}
	for _, spec := range []string{"sma:20", "ma_distance:20:50", "macd —"} {
		if !strings.Contains(catalog, spec) {
			t.Errorf("catalog %q is missing %q", catalog, spec)
		}
	}
}
