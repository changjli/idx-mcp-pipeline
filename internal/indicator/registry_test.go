package indicator

import (
	"errors"
	"math"
	"strings"
	"testing"
)

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
			got, ok := Value(req, tc.closes)
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := Parse(tc.spec)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.spec, err)
			}
			if _, ok := Value(req, tc.closes); ok != tc.wantOK {
				t.Errorf("Value(%q) ok = %v, want %v (%d rows)", tc.spec, ok, tc.wantOK, len(tc.closes))
			}
		})
	}
}

func TestParse(t *testing.T) {
	cases := []struct {
		spec       string
		wantName   string
		wantPeriod int
		wantErr    bool
	}{
		{spec: "sma", wantName: "sma", wantPeriod: 20},
		{spec: "sma:50", wantName: "sma", wantPeriod: 50},
		{spec: "  EMA:200  ", wantName: "ema", wantPeriod: 200},
		{spec: "rsi:14", wantName: "rsi", wantPeriod: 14},
		{spec: "rsi", wantName: "rsi", wantPeriod: 14},
		{spec: "sma:1", wantErr: true},
		{spec: "sma:0", wantErr: true},
		{spec: "sma:401", wantErr: true},
		{spec: "sma:x", wantErr: true},
		{spec: "sma:", wantErr: true},
		{spec: "", wantErr: true},
		{spec: "macd", wantErr: true}, // ticket 02 grows the registry
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
			if req.Name != tc.wantName || req.Period != tc.wantPeriod {
				t.Errorf("Parse(%q) = %s:%d, want %s:%d",
					tc.spec, req.Name, req.Period, tc.wantName, tc.wantPeriod)
			}
		})
	}
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

func TestRequestKey_AlwaysCarriesPeriod(t *testing.T) {
	req, err := Parse("sma")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if got, want := req.Key(), "sma:20"; got != want {
		t.Errorf("Key() = %q, want %q", got, want)
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

func TestCatalog_ListsEveryRegisteredName(t *testing.T) {
	catalog := Catalog()
	for _, name := range Names() {
		if !strings.Contains(catalog, name) {
			t.Errorf("catalog %q is missing registered name %q", catalog, name)
		}
	}
}
