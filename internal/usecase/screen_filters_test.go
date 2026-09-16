package usecase

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// filter builds a filter the way a caller writes one.
func filter(indicator, op string, bounds ...float64) ScreenStocksFilter {
	return ScreenStocksFilter{Indicator: indicator, Op: op, Value: FilterValue(bounds)}
}

// noFilters is the caller's explicit "no structural filters" — an empty list,
// which is a different request from saying nothing at all.
func noFilters() *[]ScreenStocksFilter {
	empty := []ScreenStocksFilter{}
	return &empty
}

// TestFilterValue_Unmarshal — the DSL's value payload carries one number for a
// scalar comparison and two for a band, and anything else is rejected here
// rather than reaching a comparison as a zero.
func TestFilterValue_Unmarshal(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    FilterValue
		wantErr bool
	}{
		{name: "scalar", raw: `0`, want: FilterValue{0}},
		{name: "scalar negative", raw: `-1.5`, want: FilterValue{-1.5}},
		{name: "band", raw: `[0.7, 0.95]`, want: FilterValue{0.7, 0.95}},
		{name: "one-element array is a one-bound payload", raw: `[3]`, want: FilterValue{3}},
		{name: "three-element array is a three-bound payload", raw: `[1, 2, 3]`, want: FilterValue{1, 2, 3}},
		{name: "empty array is an empty payload", raw: `[]`, want: FilterValue{}},
		{name: "string", raw: `"0.7"`, wantErr: true},
		{name: "bool", raw: `true`, wantErr: true},
		{name: "null", raw: `null`, wantErr: true},
		{name: "object", raw: `{"low": 0.7}`, wantErr: true},
		{name: "string in a band", raw: `[0.7, "0.95"]`, wantErr: true},
		{name: "null in a band", raw: `[0.7, null]`, wantErr: true},
		{name: "nested array", raw: `[[0.7], [0.95]]`, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got FilterValue
			err := json.Unmarshal([]byte(tc.raw), &got)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("unmarshal %s = %v, want an error", tc.raw, got)
				}
				if !errors.Is(err, ErrInvalidArgument) {
					t.Fatalf("err = %v, want it to wrap ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("bounds = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("bounds = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestFilterValue_Marshal — a response's filters read like the request's: a bare
// number for a scalar comparison, an array for a band.
func TestFilterValue_Marshal(t *testing.T) {
	cases := []struct {
		value FilterValue
		want  string
	}{
		{value: FilterValue{0}, want: `0`},
		{value: FilterValue{0.7, 0.95}, want: `[0.7,0.95]`},
	}

	for _, tc := range cases {
		got, err := json.Marshal(tc.value)
		if err != nil {
			t.Fatalf("marshal %v: %v", tc.value, err)
		}
		if string(got) != tc.want {
			t.Fatalf("marshal %v = %s, want %s", tc.value, got, tc.want)
		}
	}
}

// TestScreenStocksFilter_RoundTrip — the filter object the tool documents is the
// same object the response echoes, so a shortlist can be replayed from its own
// response.
func TestScreenStocksFilter_RoundTrip(t *testing.T) {
	raw := `[{"indicator":"ma_distance:20:50","op":"gt","value":0},
	         {"indicator":"range_position:60","op":"between","value":[0.7,0.95]}]`

	var parsed []ScreenStocksFilter
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(parsed) != 2 {
		t.Fatalf("filters = %d, want 2", len(parsed))
	}
	if parsed[0].Indicator != "ma_distance:20:50" || parsed[0].Op != "gt" || len(parsed[0].Value) != 1 {
		t.Fatalf("filter 0 = %+v, want a one-bound comparison", parsed[0])
	}
	if parsed[1].Op != "between" || len(parsed[1].Value) != 2 || parsed[1].Value[0] != 0.7 {
		t.Fatalf("filter 1 = %+v, want a two-bound band", parsed[1])
	}

	echoed, err := json.Marshal(parsed)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var reparsed []ScreenStocksFilter
	if err := json.Unmarshal(echoed, &reparsed); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if len(reparsed) != 2 || reparsed[0].Value[0] != parsed[0].Value[0] ||
		reparsed[1].Value[1] != parsed[1].Value[1] {
		t.Fatalf("round trip lost the payload: %s", echoed)
	}
}

// TestCompileScreenerFilters_AcceptsEveryOperator — all five threshold ops
// compile, and each keeps the indicator spec it was written against so a row's
// column key and the filter's key are the same string.
func TestCompileScreenerFilters_AcceptsEveryOperator(t *testing.T) {
	cases := []struct {
		name    string
		filter  ScreenStocksFilter
		wantKey string
	}{
		{name: "lt", filter: filter("rsi:14", filterOpLt, 40), wantKey: "rsi:14"},
		{name: "gt", filter: filter("ma_distance:20:50", filterOpGt, 0), wantKey: "ma_distance:20:50"},
		{name: "lte", filter: filter("volume_ratio:20", filterOpLte, 1), wantKey: "volume_ratio:20"},
		{name: "gte", filter: filter("rsi:14", filterOpGte, 60), wantKey: "rsi:14"},
		{name: "between", filter: filter("range_position:60", filterOpBetween, 0.7, 0.95), wantKey: "range_position:60"},
		{name: "bare name takes the registry default period", filter: filter("rsi", filterOpLt, 40), wantKey: "rsi:14"},
		{name: "fixed-parameter entry carries no period", filter: filter("macd", filterOpGt, 0), wantKey: "macd"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := compileScreenerFilters([]ScreenStocksFilter{tc.filter})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(compiled) != 1 || compiled[0].key() != tc.wantKey {
				t.Fatalf("compiled = %+v, want key %s", compiled, tc.wantKey)
			}
		})
	}
}

// TestCompileScreenerFilters_Errors — a malformed filter is rejected before any
// read, with a message that names what the caller can write instead.
func TestCompileScreenerFilters_Errors(t *testing.T) {
	cases := []struct {
		name     string
		filters  []ScreenStocksFilter
		wantText []string
	}{
		{
			name:     "unknown indicator",
			filters:  []ScreenStocksFilter{filter("smma:20", filterOpGt, 0)},
			wantText: []string{"unknown indicator", "smma", "ma_distance"},
		},
		{
			name:     "unknown operator",
			filters:  []ScreenStocksFilter{filter("rsi:14", "above", 40)},
			wantText: []string{"operator", "above", "between", "lt"},
		},
		{
			name:     "empty operator",
			filters:  []ScreenStocksFilter{filter("rsi:14", "", 40)},
			wantText: []string{"operator", "between", "lte"},
		},
		{
			name:     "scalar operator given a band",
			filters:  []ScreenStocksFilter{filter("rsi:14", filterOpGt, 40, 60)},
			wantText: []string{"gt", "one", "[40, 60]"},
		},
		{
			name:     "scalar operator given no bound",
			filters:  []ScreenStocksFilter{filter("rsi:14", filterOpGt)},
			wantText: []string{"gt", "one"},
		},
		{
			name:     "between given one bound",
			filters:  []ScreenStocksFilter{filter("range_position:60", filterOpBetween, 0.7)},
			wantText: []string{"between", "low, high"},
		},
		{
			name:     "between given three bounds",
			filters:  []ScreenStocksFilter{filter("range_position:60", filterOpBetween, 0.5, 0.7, 0.95)},
			wantText: []string{"between", "low, high"},
		},
		{
			name:     "between reversed",
			filters:  []ScreenStocksFilter{filter("range_position:60", filterOpBetween, 0.95, 0.7)},
			wantText: []string{"between", "low", "high"},
		},
		{
			name:     "bad period",
			filters:  []ScreenStocksFilter{filter("rsi:0", filterOpLt, 40)},
			wantText: []string{"period"},
		},
		{
			name:     "the offending filter is named by index",
			filters:  []ScreenStocksFilter{filter("rsi:14", filterOpLt, 40), filter("rsi:14", "near", 40)},
			wantText: []string{"filters[1]"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileScreenerFilters(tc.filters)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
			for _, want := range tc.wantText {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("message %q does not carry %q", err.Error(), want)
				}
			}
		})
	}
}

// TestCompileScreenerFilters_RejectsOverCap — a filter list is bounded, and an
// over-cap call is rejected rather than truncated (the house rule).
func TestCompileScreenerFilters_RejectsOverCap(t *testing.T) {
	filters := make([]ScreenStocksFilter, 0, maxScreenerFilters+1)
	for i := 0; i <= maxScreenerFilters; i++ {
		filters = append(filters, filter("rsi:14", filterOpGt, float64(i)))
	}
	_, err := compileScreenerFilters(filters)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
	if !strings.Contains(err.Error(), "at most") {
		t.Fatalf("message %q, want the cap stated", err.Error())
	}
}

// TestCompiledFilter_Matches — the comparison boundaries are inclusive where the
// operator says so, so a value exactly on a threshold is a pass for gte/lte and
// a band, and a miss for gt/lt.
func TestCompiledFilter_Matches(t *testing.T) {
	cases := []struct {
		name   string
		filter ScreenStocksFilter
		value  *float64
		want   bool
	}{
		{name: "gt above", filter: filter("rsi:14", filterOpGt, 0), value: f64p(0.1), want: true},
		{name: "gt on the line", filter: filter("rsi:14", filterOpGt, 0), value: f64p(0), want: false},
		{name: "gt below", filter: filter("rsi:14", filterOpGt, 0), value: f64p(-0.1), want: false},
		{name: "gte on the line", filter: filter("rsi:14", filterOpGte, 0), value: f64p(0), want: true},
		{name: "lt below", filter: filter("rsi:14", filterOpLt, 0), value: f64p(-0.1), want: true},
		{name: "lt on the line", filter: filter("rsi:14", filterOpLt, 0), value: f64p(0), want: false},
		{name: "lte on the line", filter: filter("rsi:14", filterOpLte, 1), value: f64p(1), want: true},
		{name: "lte above", filter: filter("rsi:14", filterOpLte, 1), value: f64p(1.01), want: false},
		{name: "between at the low edge", filter: filter("range_position:60", filterOpBetween, 0.7, 0.95), value: f64p(0.7), want: true},
		{name: "between at the high edge", filter: filter("range_position:60", filterOpBetween, 0.7, 0.95), value: f64p(0.95), want: true},
		{name: "between inside", filter: filter("range_position:60", filterOpBetween, 0.7, 0.95), value: f64p(0.8), want: true},
		{name: "between below", filter: filter("range_position:60", filterOpBetween, 0.7, 0.95), value: f64p(0.699), want: false},
		{name: "between above", filter: filter("range_position:60", filterOpBetween, 0.7, 0.95), value: f64p(0.951), want: false},
		{name: "degenerate band admits its own value", filter: filter("range_position:60", filterOpBetween, 0.5, 0.5), value: f64p(0.5), want: true},
		{name: "degenerate band rejects a neighbour", filter: filter("range_position:60", filterOpBetween, 0.5, 0.5), value: f64p(0.6), want: false},
		// An unobserved value never clears a threshold: a row whose filter column
		// is below its warm-up cannot be said to satisfy the comparison, and
		// treating a missing number as a pass would readmit exactly the rows the
		// filter exists to remove.
		{name: "null never matches a gt", filter: filter("rsi:14", filterOpGt, -100), value: nil, want: false},
		{name: "null never matches an lt", filter: filter("rsi:14", filterOpLt, 100), value: nil, want: false},
		{name: "null never matches a band", filter: filter("rsi:14", filterOpBetween, -100, 100), value: nil, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := compileScreenerFilters([]ScreenStocksFilter{tc.filter})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := compiled[0].matches(tc.value); got != tc.want {
				t.Fatalf("matches(%v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

// TestResolveScreenerFilters — the three states are distinct: saying nothing
// runs the shipped default set, an empty list runs no structural filters, and a
// populated list replaces the defaults rather than adding to them.
func TestResolveScreenerFilters(t *testing.T) {
	def := resolveScreenerFilters(nil)
	if len(def) != len(ScreenerDefaultFilters) {
		t.Fatalf("absent = %v, want the shipped default set %v", def, ScreenerDefaultFilters)
	}
	for i, f := range def {
		if f.Indicator != ScreenerDefaultFilters[i].Indicator || f.Op != ScreenerDefaultFilters[i].Op {
			t.Fatalf("default filter %d = %+v, want %+v", i, f, ScreenerDefaultFilters[i])
		}
	}

	if got := resolveScreenerFilters(noFilters()); len(got) != 0 {
		t.Fatalf("empty list = %v, want no structural filters", got)
	}

	custom := []ScreenStocksFilter{filter("rsi:14", filterOpLt, 40)}
	got := resolveScreenerFilters(&custom)
	if len(got) != 1 || got[0].Indicator != "rsi:14" {
		t.Fatalf("custom = %v, want exactly the caller's filter", got)
	}
}

// TestScreenerDefaultFilters_Shape — the shipped set is the spec's
// sideways-breakout-zone shape, and it stays written against the default column
// set so the funnel and its columns describe the same picture.
func TestScreenerDefaultFilters_Shape(t *testing.T) {
	want := []struct {
		indicator string
		op        string
		bounds    []float64
	}{
		{"ma_distance:20:50", filterOpGt, []float64{0}},
		{"range_position:60", filterOpBetween, []float64{0.70, 0.95}},
		{"volume_ratio:20", filterOpLte, []float64{1}},
	}

	if len(ScreenerDefaultFilters) != len(want) {
		t.Fatalf("default filters = %d, want %d", len(ScreenerDefaultFilters), len(want))
	}
	for i, w := range want {
		got := ScreenerDefaultFilters[i]
		if got.Indicator != w.indicator || got.Op != w.op || len(got.Value) != len(w.bounds) {
			t.Fatalf("filter %d = %+v, want %s %s %v", i, got, w.indicator, w.op, w.bounds)
		}
		for j, bound := range w.bounds {
			if got.Value[j] != bound {
				t.Fatalf("filter %d bound %d = %v, want %v", i, j, got.Value[j], bound)
			}
		}
	}

	// A default filter that is not also a default column would be computed but
	// never shown, which would make the funnel's cut inexplicable from its rows.
	columns := make(map[string]bool, len(ScreenerDefaultIndicators))
	for _, key := range ScreenerDefaultIndicators {
		columns[key] = true
	}
	for _, f := range ScreenerDefaultFilters {
		if !columns[f.Indicator] {
			t.Fatalf("default filter %s is not one of the default columns %v", f.Indicator, ScreenerDefaultIndicators)
		}
	}
}
