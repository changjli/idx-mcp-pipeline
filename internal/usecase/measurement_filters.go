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

// MeasurementFilterSetDir is where named filter sets live, relative to the repo
// root. Filter sets are data, not code: the AI and the user tune by adding or
// editing a <name>.json file, so a filter change is a small data edit rather
// than a Go edit that can break a build.
const MeasurementFilterSetDir = "measurement/filtersets"

// defaultMeasurementFilterSet is the built-in name a bare `cmd/measure` grades.
// Its zero-value request means "the shipped screener defaults", so it can never
// drift from the screen_stocks tool the way a copied file could.
const defaultMeasurementFilterSet = "default"

// MeasurementFilterSet is one named Replay configuration: the ScreenStocks
// request a Replay runs, minus the anchor and the row cap the engine supplies.
// A name is how a tuning decision is versioned — reports snapshot the resolved
// params, so a later edit shows up as a diff against the report that graded the
// previous version.
type MeasurementFilterSet struct {
	// Name is the registry key — the filename stem for a loaded file, or
	// "default" for the built-in set.
	Name string
	// Description is a one-line human label rendered in the report.
	Description string
	// Params is the ScreenStocks request the Replay runs. The zero value runs
	// the shipped screener defaults; the engine overrides AsOf and Limit.
	Params ScreenStocksRequest
}

// MeasurementFilterRegistry is the set of named filter sets the engine can
// grade: the built-in "default" plus one <name>.json per file in the filter-set
// directory. Files are the tuning surface; the built-in default is the fallback
// that keeps `cmd/measure` working on a checkout with no files.
type MeasurementFilterRegistry struct {
	sets map[string]MeasurementFilterSet
}

// NewMeasurementFilterRegistry builds the registry from the built-in default and
// every <name>.json under dir. An empty dir (or one that does not exist) yields
// just the built-in default — a fresh checkout is never broken by the absence of
// the tuning files. Any other read or parse failure is an error naming the file.
func NewMeasurementFilterRegistry(dir string) (*MeasurementFilterRegistry, error) {
	registry := newBuiltinMeasurementFilterRegistry()
	if strings.TrimSpace(dir) == "" {
		return registry, nil
	}
	files, err := loadMeasurementFilterSetFiles(dir)
	if err != nil {
		return nil, err
	}
	for name, set := range files {
		// A file may override the built-in default; the file is the more
		// specific statement and the report snapshots whatever ran.
		registry.sets[name] = set
	}
	return registry, nil
}

// newBuiltinMeasurementFilterRegistry is the registry with only the shipped
// default set.
func newBuiltinMeasurementFilterRegistry() *MeasurementFilterRegistry {
	return &MeasurementFilterRegistry{sets: map[string]MeasurementFilterSet{
		defaultMeasurementFilterSet: {
			Name:        defaultMeasurementFilterSet,
			Description: "the shipped screener default set (ma_distance:20:50 > 0; range_position:60 in [0.70, 0.95]; volume_ratio:20 <= 1)",
			Params:      ScreenStocksRequest{},
		},
	}}
}

// Resolve looks a named set up case-insensitively. An unknown name is a
// structured INVALID_ARGUMENT naming the known sets, never a silent fallback to
// the default — grading the wrong filter set while reporting the asked-for name
// would corrupt the loop's evidence.
func (r *MeasurementFilterRegistry) Resolve(name string) (MeasurementFilterSet, error) {
	norm := strings.ToLower(strings.TrimSpace(name))
	if norm == "" {
		norm = defaultMeasurementFilterSet
	}
	if set, ok := r.sets[norm]; ok {
		return set, nil
	}
	return MeasurementFilterSet{}, fmt.Errorf("%w: unknown filter set %q; known: %s",
		ErrInvalidArgument, name, strings.Join(r.Names(), ", "))
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

// measurementFilterSetFile is the on-disk shape of a named filter set. Every
// field is optional: an omitted knob means the shipped screener default applies.
// The two pointer fields preserve the DSL's three-state semantics crossing JSON —
// `"filters": []` is an explicit "no structural filters" while an absent
// `filters` key runs the defaults, and `"min_value": 0` is a legitimate "no
// floor" while an absent key uses the shipped floor.
type measurementFilterSetFile struct {
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
func (f measurementFilterSetFile) params() ScreenStocksRequest {
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

// loadMeasurementFilterSetFiles reads every <name>.json under dir. A missing
// directory is not an error — it is how a checkout that never tuned anything
// looks. A malformed file is a structured error naming the file, so a typo is
// fixed rather than silently skipped.
func loadMeasurementFilterSetFiles(dir string) (map[string]MeasurementFilterSet, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]MeasurementFilterSet{}, nil
		}
		return nil, fmt.Errorf("read filter sets directory %s: %w", dir, err)
	}

	sets := make(map[string]MeasurementFilterSet)
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
		sets[name] = MeasurementFilterSet{
			Name:        name,
			Description: file.Description,
			Params:      file.params(),
		}
	}
	return sets, nil
}

// readMeasurementFilterSetFile decodes one file strictly: unknown fields are
// rejected (a mistyped knob is an error, never silently ignored) and trailing
// content after the object is rejected.
func readMeasurementFilterSetFile(path string) (measurementFilterSetFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return measurementFilterSetFile{}, fmt.Errorf("read filter set %s: %w", path, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	var file measurementFilterSetFile
	if err := decoder.Decode(&file); err != nil {
		return measurementFilterSetFile{}, fmt.Errorf("%w: filter set %s: %v", ErrInvalidArgument, path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return measurementFilterSetFile{}, fmt.Errorf("%w: filter set %s: unexpected trailing content", ErrInvalidArgument, path)
	}
	return file, nil
}
