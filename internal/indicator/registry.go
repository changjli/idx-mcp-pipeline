// Package indicator is the screener's indicator registry (spec
// .scratch/screener-tool/spec.md): one entry per indicator, so growing the set
// is one registry entry plus fixtures and the MCP tool definition never
// changes. Computation runs over stored Daily Price columns — nothing is
// persisted, no upstream call (Heroku H12 constraint, ADR-0009).
//
// Library-sourced formulas come from github.com/cinar/indicator, pinned to the
// v1.x line (MIT). The v2 line is AGPLv3 and must not be used. An entry
// computes in-package when the library has no such indicator (ROC) or hardcodes
// parameters the registry exposes (Bollinger width, MACD's fixed periods,
// volume ratio, range position, MA slope, and the derived MA indicators).
//
// Warm-up: the library emits a value for every input row, averaging a short
// window at the head of the series (SMA as a running average, EMA seeded at
// the first value, RMA seeded the same way). The registry instead declares
// RequiredRows per entry and the caller reports a short series as insufficient
// rather than a partial-window value — a value the trader would read as
// fully-warmed when it is not.
package indicator

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// ErrInvalid marks a bad indicator request: an unknown name, an unparsable
// spec, or a period outside the entry's bounds. Callers map it to the tool's
// INVALID_ARGUMENT envelope.
var ErrInvalid = errors.New("invalid indicator")

// v1 period bounds, shared by every entry: low enough for a short MA, high
// enough for the screener's longest MA, and cheap over a capped ticker list.
const (
	minPeriod = 2
	maxPeriod = 400
)

// Entry is one registered indicator. A v1 entry takes periods (one for a
// single-window indicator, two for a pair) and nothing else; an indicator
// needing more parameters gets its own fields here when it lands.
type Entry struct {
	// Name is the registry key the caller names, e.g. "sma".
	Name string
	// Summary is the one-line description used in error enumeration and the
	// tool description. It carries the units of the value, since the screener
	// compares the value against caller thresholds.
	Summary string
	// DefaultPeriods are the periods a bare spec gets: one value for a
	// single-window indicator (sma), fast-then-slow for a pair (ma_distance),
	// and empty for a fixed-parameter indicator (`macd`, `obv`), which accepts
	// no period at all.
	DefaultPeriods []int
	// Needs is the set of stored columns the formula reads beyond close.
	Needs Columns
	// RequiredRows is the number of usable rows a value needs before it is
	// reported at all. Below it the value is null plus an insufficient flag.
	RequiredRows func(periods []int) int
	// Compute returns the value at the end of the series, which is fully
	// warmed because the caller checked RequiredRows first.
	Compute func(periods []int, s Series) float64
}

// Request is a parsed, validated indicator request.
type Request struct {
	Name    string
	Periods []int
}

// Key is the canonical response key: "sma:20", "ma_distance:20:50", or a bare
// "macd" for a fixed-parameter entry. Periods are always present when the entry
// has them, so a row's value is self-describing without a schema lookup.
func (r Request) Key() string {
	if len(r.Periods) == 0 {
		return r.Name
	}
	parts := make([]string, 0, len(r.Periods)+1)
	parts = append(parts, r.Name)
	for _, period := range r.Periods {
		parts = append(parts, strconv.Itoa(period))
	}
	return strings.Join(parts, ":")
}

// last returns the final element, the latest value in an ascending series.
func last(values []float64) float64 {
	return values[len(values)-1]
}

// Names returns the registered names, sorted — the enumeration every
// unknown-name error carries. Sorted so the message is stable across calls.
func Names() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Catalog returns "name:defaults — summary" lines for the tool description, so
// registry growth never edits the tool definition.
func Catalog() string {
	parts := make([]string, 0, len(registry))
	for _, name := range Names() {
		entry := registry[name]
		parts = append(parts, fmt.Sprintf("%s — %s", entry.spec(), entry.Summary))
	}
	return strings.Join(parts, "; ")
}

// spec renders the entry's bare spec: "sma:20", "ma_distance:20:50", "macd".
func (e Entry) spec() string {
	if len(e.DefaultPeriods) == 0 {
		return e.Name
	}
	return Request{Name: e.Name, Periods: e.DefaultPeriods}.Key()
}

// Lookup resolves a name (case-insensitive, trimmed) or returns an ErrInvalid
// wrapping the valid names.
func Lookup(name string) (Entry, error) {
	norm := strings.ToLower(strings.TrimSpace(name))
	entry, ok := registry[norm]
	if !ok {
		return Entry{}, fmt.Errorf("%w: unknown indicator %q; valid: %s",
			ErrInvalid, name, strings.Join(Names(), ", "))
	}
	return entry, nil
}

// Parse parses one spec: "sma" (the entry's default periods), "sma:50", or
// "ma_distance:10:30" for a pair. A fixed-parameter entry takes no period.
func Parse(spec string) (Request, error) {
	raw := strings.TrimSpace(spec)
	if raw == "" {
		return Request{}, fmt.Errorf("%w: empty indicator spec; valid: %s",
			ErrInvalid, strings.Join(Names(), ", "))
	}

	name, rest, hasPeriods := strings.Cut(raw, ":")
	entry, err := Lookup(name)
	if err != nil {
		return Request{}, err
	}

	// Defaults are copied rather than aliased: the registry map is
	// process-global, and a caller that wrote into a request's periods would
	// otherwise rewrite every later request's defaults.
	periods := append([]int(nil), entry.DefaultPeriods...)
	if hasPeriods {
		periods, err = parsePeriods(entry, rest)
		if err != nil {
			return Request{}, err
		}
	}
	return Request{Name: entry.Name, Periods: periods}, nil
}

// parsePeriods validates the caller's periods against the entry's arity. An
// override names every period the entry has or none: a half-specified pair
// ("ma_distance:10") would silently keep a default the caller did not name and
// report a value under a key they did not ask for.
func parsePeriods(entry Entry, rest string) ([]int, error) {
	if len(entry.DefaultPeriods) == 0 {
		return nil, fmt.Errorf("%w: %s takes no period — write %q", ErrInvalid, entry.Name, entry.Name)
	}

	given := strings.Split(rest, ":")
	if len(given) != len(entry.DefaultPeriods) {
		return nil, fmt.Errorf("%w: %s takes %d period(s) — write %q",
			ErrInvalid, entry.Name, len(entry.DefaultPeriods), entry.spec())
	}

	periods := make([]int, 0, len(given))
	for _, raw := range given {
		period, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("%w: %s period %q is not an integer",
				ErrInvalid, entry.Name, strings.TrimSpace(raw))
		}
		if period < minPeriod || period > maxPeriod {
			return nil, fmt.Errorf("%w: %s period must be %d..%d, got %d",
				ErrInvalid, entry.Name, minPeriod, maxPeriod, period)
		}
		periods = append(periods, period)
	}

	// A pair is fast-then-slow: the reverse order would flip the sign of every
	// spread or distance the screener filters on.
	if len(periods) == 2 && periods[0] >= periods[1] {
		return nil, fmt.Errorf("%w: %s needs the fast period shorter than the slow one, got %d:%d",
			ErrInvalid, entry.Name, periods[0], periods[1])
	}
	return periods, nil
}

// Value computes a request's latest value over an ascending series. ok is false
// when the series is shorter than the entry's warm-up requirement (insufficient
// history — the caller flags it rather than reporting a short-window value),
// when the needed columns are missing on too many rows, or when the computation
// yields a non-finite value (a zero denominator, say).
func Value(req Request, s Series) (float64, bool) {
	entry, ok := registry[req.Name]
	if !ok {
		return 0, false
	}
	usable := s.Rows(entry.Needs)
	if usable.Len() < entry.RequiredRows(req.Periods) {
		return 0, false
	}
	value := entry.Compute(req.Periods, usable)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

// RequiredRows is the warm-up bar for a request, so the caller can report the
// binding constraint on a row.
func RequiredRows(req Request) int {
	entry, ok := registry[req.Name]
	if !ok {
		return 0
	}
	return entry.RequiredRows(req.Periods)
}
