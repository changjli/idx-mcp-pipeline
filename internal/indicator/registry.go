// Package indicator is the screener's indicator registry (spec
// .scratch/screener-tool/spec.md): one entry per indicator, so growing the set
// is one registry entry plus fixtures and the MCP tool definition never
// changes. Computation runs over stored Daily Price closes — nothing is
// persisted, no upstream call (Heroku H12 constraint, ADR-0009).
//
// Library-sourced formulas come from github.com/cinar/indicator, pinned to the
// v1.x line (MIT). The v2 line is AGPLv3 and must not be used.
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

	cinar "github.com/cinar/indicator"
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

// Entry is one registered indicator. Every v1 entry takes a single optional
// `period` parameter; an indicator needing more (or no) parameters gets its
// own fields here when it lands.
type Entry struct {
	// Name is the registry key the caller names, e.g. "sma".
	Name string
	// Summary is the one-line description used in error enumeration and the
	// tool description.
	Summary string
	// DefaultPeriod applies when the spec carries no ":period" suffix.
	DefaultPeriod int
	// RequiredRows is the number of usable price rows a value needs before it
	// is reported at all. Below it the value is null plus an insufficient flag.
	RequiredRows func(period int) int
	// Compute returns the value at the end of the series, which is fully
	// warmed because the caller checked RequiredRows first.
	Compute func(period int, closes []float64) float64
}

// Request is a parsed, validated indicator request.
type Request struct {
	Name   string
	Period int
}

// Key is the canonical response key, e.g. "sma:20". The period is always
// present so a row's value is self-describing without a schema lookup.
func (r Request) Key() string {
	return r.Name + ":" + strconv.Itoa(r.Period)
}

var registry = map[string]Entry{
	"sma": {
		Name:          "sma",
		Summary:       "simple moving average of close",
		DefaultPeriod: 20,
		RequiredRows:  func(period int) int { return period },
		Compute: func(period int, closes []float64) float64 {
			return last(cinar.Sma(period, closes))
		},
	},
	"ema": {
		Name:          "ema",
		Summary:       "exponential moving average of close",
		DefaultPeriod: 20,
		// The library seeds the EMA at the first close, so shorter windows
		// exist numerically but are still mostly the seed. One period is the
		// conventional "settled" bar.
		RequiredRows: func(period int) int { return period },
		Compute: func(period int, closes []float64) float64 {
			return last(cinar.Ema(period, closes))
		},
	},
	"rsi": {
		Name:          "rsi",
		Summary:       "relative strength index (Wilder smoothing)",
		DefaultPeriod: 14,
		// Period price changes need period+1 closes; the library's RMA seed
		// window is the first period of those changes.
		RequiredRows: func(period int) int { return period + 1 },
		Compute: func(period int, closes []float64) float64 {
			_, rsi := cinar.RsiPeriod(period, closes)
			return last(rsi)
		},
	},
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

// Catalog returns "name (param summary)" lines for the tool description.
func Catalog() string {
	parts := make([]string, 0, len(registry))
	for _, name := range Names() {
		entry := registry[name]
		parts = append(parts, fmt.Sprintf("%s:%d — %s", name, entry.DefaultPeriod, entry.Summary))
	}
	return strings.Join(parts, "; ")
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

// Parse parses one spec: "sma" (the entry's default period) or "sma:50".
func Parse(spec string) (Request, error) {
	raw := strings.TrimSpace(spec)
	if raw == "" {
		return Request{}, fmt.Errorf("%w: empty indicator spec; valid: %s",
			ErrInvalid, strings.Join(Names(), ", "))
	}

	name, periodStr, hasPeriod := strings.Cut(raw, ":")
	entry, err := Lookup(name)
	if err != nil {
		return Request{}, err
	}

	period := entry.DefaultPeriod
	if hasPeriod {
		period, err = strconv.Atoi(strings.TrimSpace(periodStr))
		if err != nil {
			return Request{}, fmt.Errorf("%w: %s period %q is not an integer",
				ErrInvalid, entry.Name, strings.TrimSpace(periodStr))
		}
	}
	if period < minPeriod || period > maxPeriod {
		return Request{}, fmt.Errorf("%w: %s period must be %d..%d, got %d",
			ErrInvalid, entry.Name, minPeriod, maxPeriod, period)
	}
	return Request{Name: entry.Name, Period: period}, nil
}

// Value computes a request's latest value over an ascending close series.
// ok is false when the series is shorter than the entry's warm-up requirement
// (insufficient history — the caller flags it rather than reporting a
// short-window value) or when the library returns a non-finite value.
func Value(req Request, closes []float64) (float64, bool) {
	entry, ok := registry[req.Name]
	if !ok {
		return 0, false
	}
	if len(closes) < entry.RequiredRows(req.Period) {
		return 0, false
	}
	value := entry.Compute(req.Period, closes)
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
	return entry.RequiredRows(req.Period)
}
