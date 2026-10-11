package usecase

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// MeasurementFilterSetDir is the pre-R2 location of the named filter sets. A
// Grade never reads it — the Active Filter Set comes from the Config Document on
// R2 (ADR-0016) — but `measure migrate` reads it once to seed the document, and
// the file shape below is exactly the shape a document's sets entry uses.
const MeasurementFilterSetDir = "measurement/filtersets"

// defaultMeasurementFilterSet is the built-in name: the shipped screener
// defaults. Its zero-value request means "the shipped screener defaults", so it
// can never drift from the screen_stocks tool the way a copied file could. It is
// also the fallback active set when no Config Document is published.
const defaultMeasurementFilterSet = "default"

// MeasurementFilterSet is one named Replay configuration: the ScreenStocks
// request a Replay runs, minus the anchor and the row cap the engine supplies.
// A name is how a tuning decision is versioned — reports snapshot the resolved
// params and the Config Hash, so a later edit shows up as a diff against the
// report that graded the previous version.
type MeasurementFilterSet struct {
	// Name is the registry key — a Config Document's sets key, or "default"
	// for the built-in set.
	Name string
	// Description is a one-line human label rendered in the report.
	Description string
	// Params is the ScreenStocks request the Replay runs. The zero value runs
	// the shipped screener defaults; the engine overrides AsOf and Limit.
	Params ScreenStocksRequest
}

// MeasurementFilterRegistry is the set of named filter sets a screen run can
// grade: the built-in "default" plus every set the Active Filter Set's Config
// Document carries, with `active` naming the one a bare run grades.
type MeasurementFilterRegistry struct {
	sets   map[string]MeasurementFilterSet
	active string
}

// NewMeasurementFilterRegistry returns the built-in registry: the shipped
// screener defaults under the name "default", active. It is what a run uses when
// no Config Document is published — a fresh bucket is a valid fresh start, not
// an error.
func NewMeasurementFilterRegistry() *MeasurementFilterRegistry {
	return newBuiltinMeasurementFilterRegistry()
}

// newBuiltinMeasurementFilterRegistry is the registry with only the shipped
// default set.
func newBuiltinMeasurementFilterRegistry() *MeasurementFilterRegistry {
	return &MeasurementFilterRegistry{
		active: defaultMeasurementFilterSet,
		sets: map[string]MeasurementFilterSet{
			defaultMeasurementFilterSet: {
				Name:        defaultMeasurementFilterSet,
				Description: "the shipped screener default set (ma_distance:20:50 > 0; range_position:60 in [0.70, 0.95]; volume_ratio:20 <= 1)",
				Params:      ScreenStocksRequest{},
			},
		},
	}
}

// Resolve looks a named set up case-insensitively. An empty name resolves to the
// Active Filter Set (or the built-in default when none is set), so a bare run
// grades what is in force rather than a hard-coded name. An unknown name is a
// structured INVALID_ARGUMENT naming the known sets, never a silent fallback —
// grading the wrong filter set while reporting the asked-for name would corrupt
// the loop's evidence.
func (r *MeasurementFilterRegistry) Resolve(name string) (MeasurementFilterSet, error) {
	norm := strings.ToLower(strings.TrimSpace(name))
	if norm == "" {
		norm = r.active
		if norm == "" {
			norm = defaultMeasurementFilterSet
		}
	}
	if set, ok := r.sets[norm]; ok {
		return set, nil
	}
	return MeasurementFilterSet{}, fmt.Errorf("%w: unknown filter set %q; known: %s",
		ErrInvalidArgument, name, strings.Join(r.Names(), ", "))
}

// Active returns the name a bare Resolve grades.
func (r *MeasurementFilterRegistry) Active() string {
	if r.active == "" {
		return defaultMeasurementFilterSet
	}
	return r.active
}

// Names returns the registry keys sorted, for stable "known sets" error text
// and discovery.
func (r *MeasurementFilterRegistry) Names() []string {
	names := make([]string, 0, len(r.sets))
	for name := range r.sets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// MeasurementFilterSetFile is the on-disk / on-R2 shape of a named filter set.
// Every field is optional: an omitted knob means the shipped screener default
// applies. The two pointer fields preserve the DSL's three-state semantics
// crossing JSON — `"filters": []` is an explicit "no structural filters" while an
// absent `filters` key runs the defaults, and `"min_value": 0` is a legitimate
// "no floor" while an absent key uses the shipped floor.
type MeasurementFilterSetFile struct {
	Description          string                `json:"description"`
	Window               int                   `json:"window"`
	MinValue             *int64                `json:"min_value"`
	SuspensionWindowDays int                   `json:"suspension_window_days"`
	FlowWindowDays       int                   `json:"flow_window_days"`
	Indicators           []string              `json:"indicators"`
	Filters              *[]ScreenStocksFilter `json:"filters"`
	Sort                 string                `json:"sort"`
	Order                string                `json:"order"`
}

// params maps the file shape onto the ScreenStocks request.
func (f MeasurementFilterSetFile) params() ScreenStocksRequest {
	return ScreenStocksRequest{
		Window:               f.Window,
		MinValue:             f.MinValue,
		SuspensionWindowDays: f.SuspensionWindowDays,
		FlowWindowDays:       f.FlowWindowDays,
		Indicators:           f.Indicators,
		Filters:              f.Filters,
		Sort:                 f.Sort,
		Order:                f.Order,
	}
}

// readMeasurementFilterSetFiles reads every <name>.json under dir into the file
// shape. A missing directory is not an error — it is how a checkout that never
// tuned anything looks. A malformed file is a structured error naming the file,
// so a typo is fixed rather than silently skipped. Only `migrate` calls this.
func readMeasurementFilterSetFiles(dir string) (map[string]MeasurementFilterSetFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]MeasurementFilterSetFile{}, nil
		}
		return nil, fmt.Errorf("read filter sets directory %s: %w", dir, err)
	}

	sets := make(map[string]MeasurementFilterSetFile)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		name := strings.ToLower(strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name())))
		if name == "" {
			return nil, fmt.Errorf("%w: filter set file %s has no name before .json", ErrInvalidArgument, path)
		}
		if _, duplicate := sets[name]; duplicate {
			// Two filenames that differ only by case are one name; the registry
			// cannot decide between them, so it says so.
			return nil, fmt.Errorf("%w: duplicate filter set %q in %s", ErrInvalidArgument, name, path)
		}

		file, err := readMeasurementFilterSetFile(path)
		if err != nil {
			return nil, err
		}
		sets[name] = file
	}
	return sets, nil
}

// readMeasurementFilterSetFile decodes one file strictly.
func readMeasurementFilterSetFile(path string) (MeasurementFilterSetFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return MeasurementFilterSetFile{}, fmt.Errorf("read filter set %s: %w", path, err)
	}
	file, err := decodeMeasurementFilterSet(raw)
	if err != nil {
		return MeasurementFilterSetFile{}, fmt.Errorf("filter set %s: %w", path, err)
	}
	return file, nil
}

// decodeMeasurementFilterSet decodes one filter-set document strictly: unknown
// fields are rejected (a mistyped knob is an error, never silently ignored) and
// trailing content after the object is rejected.
func decodeMeasurementFilterSet(raw []byte) (MeasurementFilterSetFile, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	var file MeasurementFilterSetFile
	if err := decoder.Decode(&file); err != nil {
		return MeasurementFilterSetFile{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return MeasurementFilterSetFile{}, fmt.Errorf("%w: unexpected trailing content", ErrInvalidArgument)
	}
	return file, nil
}
