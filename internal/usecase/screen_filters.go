package usecase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/indicator"
)

// The structural filter DSL — funnel stage 3 (screener spec, ticket 05). Each
// filter is a threshold comparison on ONE registered indicator's latest value:
// {indicator, op, value}, AND-combined. Direct indicator-to-indicator
// comparison is out of scope by design — "price above MA20" is the derived
// indicator ma_distance (one number), so it stays a threshold and not an
// expression language. Temporal logic (crossovers, streaks, state changes) is
// likewise out of scope: trajectory judgment belongs to compute_indicators
// series mode, which hands the caller full arrays.
const (
	filterOpLt      = "lt"
	filterOpGt      = "gt"
	filterOpLte     = "lte"
	filterOpGte     = "gte"
	filterOpBetween = "between"

	// maxScreenerFilters bounds the list. Every filter is one more comparison
	// per survivor, so an unbounded list is a way to make a screen arbitrarily
	// expensive by accident.
	maxScreenerFilters = 32
)

// filterOps is the enumeration every unknown-operator error carries. Sorted so
// the message is stable across calls.
var filterOps = []string{filterOpBetween, filterOpGt, filterOpGte, filterOpLt, filterOpLte}

// ScreenerDefaultFilters is the shipped structural filter set — the
// sideways-breakout-zone shape of spec story 4: price above its fast moving
// average, sitting high in its range but not yet broken out, on volume that is
// not above its own average. Every number here is a caller parameter in
// practice: passing `filters` replaces this set entirely, which is why no
// threshold gets a parameter of its own. Exported so the MCP tool description
// names exactly what runs rather than paraphrasing it.
var ScreenerDefaultFilters = []ScreenStocksFilter{
	{Indicator: "ma_distance:20:50", Op: filterOpGt, Value: FilterValue{0}},
	{Indicator: "range_position:60", Op: filterOpBetween, Value: FilterValue{0.70, 0.95}},
	{Indicator: "volume_ratio:20", Op: filterOpLte, Value: FilterValue{1}},
}

// ScreenerDefaultFiltersDoc renders the shipped filter set the way a caller
// writes it — "ma_distance:20:50 gt [0]; ..." — so the MCP tool description
// names exactly what runs instead of paraphrasing it, the same reason
// ScreenerDefaultIndicators is exported.
func ScreenerDefaultFiltersDoc() string {
	parts := make([]string, 0, len(ScreenerDefaultFilters))
	for _, filter := range ScreenerDefaultFilters {
		parts = append(parts, fmt.Sprintf("%s %s %s", filter.Indicator, filter.Op, renderFilterValue(filter.Value)))
	}
	return strings.Join(parts, "; ")
}

// ScreenStocksFilter is one structural filter as a caller writes it: a
// threshold comparison against a single registered indicator's latest value.
// Filters are AND-combined, so adding one can only ever narrow a shortlist.
type ScreenStocksFilter struct {
	// Indicator is a full registry spec, e.g. "ma_distance:20:50" or a bare
	// "rsi" for the entry's default periods.
	Indicator string `json:"indicator"`
	// Op is one of lt, gt, lte, gte, between.
	Op string `json:"op"`
	// Value carries the bounds the operator reads: one number for the four
	// scalar comparisons, [low, high] for between.
	Value FilterValue `json:"value"`
}

// FilterValue is a filter's numeric payload as it crosses the wire: a bare
// number for a scalar comparison, a two-element array for a band. It normalizes
// both to a bounds slice so the DSL has one numeric shape internally whatever
// the caller wrote, and MarshalJSON restores the caller-facing shape so a
// response's filters read like the request's.
//
// It decodes numbers only. A string, a bool, a null, or a nested array is
// rejected here rather than arriving at a comparison as a zero — "value was
// unreadable" must never be indistinguishable from "value was 0".
type FilterValue []float64

// UnmarshalJSON accepts a JSON number or a JSON array of numbers.
func (v *FilterValue) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return fmt.Errorf("%w: filter value is missing; write a number, or [low, high] for between", ErrInvalidArgument)
	}

	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return fmt.Errorf("%w: filter value must be a number or [low, high], got %s",
			ErrInvalidArgument, string(trimmed))
	}

	switch typed := payload.(type) {
	case json.Number:
		bound, err := typed.Float64()
		if err != nil {
			return fmt.Errorf("%w: filter value %s is not a number", ErrInvalidArgument, typed.String())
		}
		*v = FilterValue{bound}
		return nil
	case []any:
		bounds := make(FilterValue, 0, len(typed))
		for _, item := range typed {
			number, ok := item.(json.Number)
			if !ok {
				return fmt.Errorf("%w: filter value must be a number or [low, high], got %s",
					ErrInvalidArgument, string(trimmed))
			}
			bound, err := number.Float64()
			if err != nil {
				return fmt.Errorf("%w: filter value %s is not a number", ErrInvalidArgument, number.String())
			}
			bounds = append(bounds, bound)
		}
		*v = bounds
		return nil
	default:
		return fmt.Errorf("%w: filter value must be a number or [low, high], got %s",
			ErrInvalidArgument, string(trimmed))
	}
}

// MarshalJSON emits a bare number for a one-bound payload and an array
// otherwise, mirroring the shape a caller writes.
func (v FilterValue) MarshalJSON() ([]byte, error) {
	if len(v) == 1 {
		return json.Marshal(v[0])
	}
	return json.Marshal([]float64(v))
}

// compiledFilter is one validated filter bound to its parsed registry request,
// so the key a filter compares against is the same string the row's column map
// is keyed by.
type compiledFilter struct {
	request indicator.Request
	op      string
	bounds  FilterValue
}

// key is the column key the filter reads, e.g. "ma_distance:20:50".
func (f compiledFilter) key() string { return f.request.Key() }

// matches reports whether a row's value for this filter's indicator satisfies
// the comparison. A missing value — the indicator below its warm-up, or a
// column the stored rows never carried — never matches: an unknown number
// cannot be said to clear a threshold, and reading it as a pass would silently
// readmit exactly the rows the filter exists to remove.
func (f compiledFilter) matches(value *float64) bool {
	if value == nil {
		return false
	}
	observed := *value
	switch f.op {
	case filterOpLt:
		return observed < f.bounds[0]
	case filterOpGt:
		return observed > f.bounds[0]
	case filterOpLte:
		return observed <= f.bounds[0]
	case filterOpGte:
		return observed >= f.bounds[0]
	default: // between, inclusive at both ends
		return observed >= f.bounds[0] && observed <= f.bounds[1]
	}
}

// resolveScreenerFilters resolves the structural filter set. Three states are
// deliberately distinct: an absent list (nil) runs the shipped default set, an
// explicitly empty list runs no structural filters at all — the DSL is the only
// way structure is expressed, so "none" has to be sayable — and a populated
// list replaces the defaults entirely rather than adding to them.
//
// Note this differs from `indicators`, where an empty list means the defaults:
// a column set has no "off" state worth naming, a filter set does.
func resolveScreenerFilters(filters *[]ScreenStocksFilter) []ScreenStocksFilter {
	if filters == nil {
		return ScreenerDefaultFilters
	}
	if *filters == nil {
		// A caller who asked for no structural filters gets an echo that marshals
		// as [], not null: "no filters" is a request, not a missing answer.
		return []ScreenStocksFilter{}
	}
	return *filters
}

// compileScreenerFilters validates the filter set and binds each filter to its
// parsed registry request. Every rule runs before any read, so a typo costs one
// round trip and returns the same structured error whatever called the usecase:
// the indicator must parse through the registry (an unknown name reports the
// valid names), the operator must be one of the five, and the payload must
// carry exactly the bounds that operator reads.
func compileScreenerFilters(filters []ScreenStocksFilter) ([]compiledFilter, error) {
	if len(filters) > maxScreenerFilters {
		return nil, fmt.Errorf("%w: at most %d filters per screen, got %d",
			ErrInvalidArgument, maxScreenerFilters, len(filters))
	}

	compiled := make([]compiledFilter, 0, len(filters))
	for i, filter := range filters {
		request, err := indicator.Parse(filter.Indicator)
		if err != nil {
			return nil, fmt.Errorf("%w: filters[%d]: %s", ErrInvalidArgument, i, err)
		}
		op, err := resolveFilterOp(filter.Op, i)
		if err != nil {
			return nil, err
		}
		if err := validateFilterBounds(op, filter.Value, i); err != nil {
			return nil, err
		}
		compiled = append(compiled, compiledFilter{request: request, op: op, bounds: filter.Value})
	}
	return compiled, nil
}

// resolveFilterOp normalizes and validates the operator. An empty operator is
// rejected rather than defaulted: a filter with no comparison is not a filter,
// and guessing one would apply a threshold the caller never wrote.
func resolveFilterOp(op string, index int) (string, error) {
	norm := strings.ToLower(strings.TrimSpace(op))
	for _, valid := range filterOps {
		if norm == valid {
			return valid, nil
		}
	}
	return "", fmt.Errorf("%w: filters[%d]: op %q is not a threshold operator; valid: %s",
		ErrInvalidArgument, index, op, strings.Join(filterOps, ", "))
}

// validateFilterBounds checks the payload against the operator's arity: one
// bound for the scalar comparisons, a low/high pair for between, all finite. A
// reversed band is rejected rather than swapped, so a screen that returns
// nothing says so through its funnel instead of hiding a transposed pair.
func validateFilterBounds(op string, bounds FilterValue, index int) error {
	want := 1
	if op == filterOpBetween {
		want = 2
	}
	if len(bounds) != want {
		if op == filterOpBetween {
			return fmt.Errorf("%w: filters[%d]: between compares against a band — write value as [low, high], got %s",
				ErrInvalidArgument, index, renderFilterValue(bounds))
		}
		return fmt.Errorf("%w: filters[%d]: %s compares against one value — write value as a number, got %s",
			ErrInvalidArgument, index, op, renderFilterValue(bounds))
	}

	for _, bound := range bounds {
		if math.IsNaN(bound) || math.IsInf(bound, 0) {
			return fmt.Errorf("%w: filters[%d]: value must be a finite number, got %s",
				ErrInvalidArgument, index, renderFilterValue(bounds))
		}
	}

	if op == filterOpBetween && bounds[0] > bounds[1] {
		return fmt.Errorf("%w: filters[%d]: between needs low <= high, got %s",
			ErrInvalidArgument, index, renderFilterValue(bounds))
	}
	return nil
}

// renderFilterValue prints a payload the way a caller would write it, so an
// arity error shows the offending value rather than only its length.
func renderFilterValue(bounds FilterValue) string {
	parts := make([]string, 0, len(bounds))
	for _, bound := range bounds {
		parts = append(parts, strconv.FormatFloat(bound, 'g', -1, 64))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// applyStructuralFilters keeps the rows that clear every filter — the stage's
// whole job, and a no-op when the caller asked for no structural filters.
func applyStructuralFilters(rows []ScreenStocksRow, filters []compiledFilter) []ScreenStocksRow {
	if len(filters) == 0 {
		return rows
	}
	kept := make([]ScreenStocksRow, 0, len(rows))
	for _, row := range rows {
		passes := true
		for _, filter := range filters {
			if !filter.matches(row.Values[filter.key()]) {
				passes = false
				break
			}
		}
		if passes {
			kept = append(kept, row)
		}
	}
	return kept
}

// unionRequests extends the column set with the filters' indicators. A filter
// the caller did not also ask for as a column still has to be computed — the
// stage cannot compare against a number nothing produced — and the response
// echoes the union, so `indicators` states exactly what was computed and `sort`
// can name any of it. Requested columns keep their order; a filter-only key is
// appended in first-appearance order.
func unionRequests(requested []indicator.Request, filters []compiledFilter) []indicator.Request {
	seen := make(map[string]bool, len(requested)+len(filters))
	union := make([]indicator.Request, 0, len(requested)+len(filters))
	for _, request := range requested {
		if seen[request.Key()] {
			continue
		}
		seen[request.Key()] = true
		union = append(union, request)
	}
	for _, filter := range filters {
		if seen[filter.key()] {
			continue
		}
		seen[filter.key()] = true
		union = append(union, filter.request)
	}
	return union
}
