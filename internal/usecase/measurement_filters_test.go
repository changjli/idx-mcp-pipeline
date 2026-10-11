package usecase

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeFilterSet writes a filter-set file into a fresh temp directory and
// returns the directory.
func writeFilterSet(t *testing.T, filename, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, filename), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", filename, err)
	}
	return dir
}

// registryFromDir builds the registry a Config Document would from the
// filter-set files in dir, active "default" — mirroring SeedFromDir, the path
// `measure migrate` seeds and a Grade resolves through.
func registryFromDir(t *testing.T, dir string) (*MeasurementFilterRegistry, error) {
	t.Helper()
	files, err := readMeasurementFilterSetFiles(dir)
	if err != nil {
		return nil, err
	}
	sets := map[string]MeasurementFilterSetFile{defaultMeasurementFilterSet: {}}
	for name, file := range files {
		sets[name] = file
	}
	doc := MeasurementConfigDocument{Active: defaultMeasurementFilterSet, Sets: sets}
	return doc.registry()
}

// TestMeasurementFilterRegistry_BuiltinDefault — with no Config Document the
// registry still resolves the built-in default for empty and explicit names, and
// refuses anything else with a structured error naming the known sets.
func TestMeasurementFilterRegistry_BuiltinDefault(t *testing.T) {
	registry := NewMeasurementFilterRegistry()

	for _, name := range []string{"", "default", "DEFAULT", "  Default "} {
		set, err := registry.Resolve(name)
		if err != nil {
			t.Fatalf("resolve(%q): %v", name, err)
		}
		if set.Name != defaultMeasurementFilterSet || set.Params.Filters != nil {
			t.Fatalf("resolve(%q) = %+v, want the built-in default (zero-value request)", name, set)
		}
	}

	if names := strings.Join(registry.Names(), ","); names != "default" {
		t.Fatalf("names = %q, want only default", names)
	}
	if _, err := registry.Resolve("momentum"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unknown set err = %v, want ErrInvalidArgument", err)
	}
}

// TestMeasurementFilterRegistry_ActiveResolves — an empty name resolves to the
// document's active set, not a hard-coded "default".
func TestMeasurementFilterRegistry_ActiveResolves(t *testing.T) {
	doc := MeasurementConfigDocument{
		Active: "momentum",
		Sets: map[string]MeasurementFilterSetFile{
			"momentum": {Description: "rsi >= 60"},
			"value":    {Description: "low pe"},
		},
	}
	registry, err := doc.registry()
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	set, err := registry.Resolve("")
	if err != nil {
		t.Fatalf("resolve active: %v", err)
	}
	if set.Name != "momentum" || set.Description != "rsi >= 60" {
		t.Fatalf("active set = %+v, want momentum", set)
	}
	if names := strings.Join(registry.Names(), ","); names != "default,momentum,value" {
		t.Fatalf("names = %q, want default,momentum,value", names)
	}
	if _, err := registry.Resolve("default"); err != nil {
		t.Fatalf("built-in default should still resolve: %v", err)
	}
}

// TestMeasurementConfigDocument_ActiveNotInSets — an active naming a set the
// document does not carry is refused rather than silently falling back.
func TestMeasurementConfigDocument_ActiveNotInSets(t *testing.T) {
	doc := MeasurementConfigDocument{
		Active: "ghost",
		Sets:   map[string]MeasurementFilterSetFile{"momentum": {}},
	}
	if _, err := doc.registry(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

// TestMeasurementFilterRegistry_LoadsFiles — a <name>.json file becomes a named
// set whose params map onto the ScreenStocks request, with the filename as the
// name.
func TestMeasurementFilterRegistry_LoadsFiles(t *testing.T) {
	dir := writeFilterSet(t, "momentum.json", `{
		"description": "rsi >= 60 and above the 20/50 MA",
		"window": 120,
		"min_value": 7000000000,
		"suspension_window_days": 3,
		"flow_window_days": 10,
		"indicators": ["rsi:14", "ma_distance:20:50"],
		"filters": [
			{"indicator": "rsi:14", "op": "gte", "value": 60},
			{"indicator": "range_position:60", "op": "between", "value": [0.5, 0.95]}
		],
		"sort": "rsi:14",
		"order": "desc"
	}`)

	registry, err := registryFromDir(t, dir)
	if err != nil {
		t.Fatalf("build registry: %v", err)
	}

	set, err := registry.Resolve("MOMENTUM")
	if err != nil {
		t.Fatalf("resolve momentum: %v", err)
	}
	if set.Name != "momentum" || set.Description != "rsi >= 60 and above the 20/50 MA" {
		t.Fatalf("set = %+v, want the loaded name/description", set)
	}
	p := set.Params
	if p.Window != 120 || p.MinValue == nil || *p.MinValue != 7_000_000_000 ||
		p.SuspensionWindowDays != 3 || p.FlowWindowDays != 10 || p.Sort != "rsi:14" || p.Order != "desc" {
		t.Fatalf("params = %+v, want the file's values", p)
	}
	if strings.Join(p.Indicators, ",") != "rsi:14,ma_distance:20:50" {
		t.Fatalf("indicators = %v", p.Indicators)
	}
	if p.Filters == nil || len(*p.Filters) != 2 {
		t.Fatalf("filters = %v, want two structural filters", p.Filters)
	}
	if (*p.Filters)[1].Op != filterOpBetween || len((*p.Filters)[1].Value) != 2 {
		t.Fatalf("band filter = %+v, want a between [0.5, 0.95]", (*p.Filters)[1])
	}

	if names := strings.Join(registry.Names(), ","); names != "default,momentum" {
		t.Fatalf("names = %q, want default,momentum", names)
	}
	if _, err := registry.Resolve("value"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unknown err = %v, want ErrInvalidArgument", err)
	} else if !strings.Contains(err.Error(), "momentum") {
		t.Fatalf("unknown-name error %q should list the known sets", err)
	}
}

// TestMeasurementFilterRegistry_MissingDirIsFine — a checkout with no filtersets
// directory is not an error; the built-in default still resolves.
func TestMeasurementFilterRegistry_MissingDirIsFine(t *testing.T) {
	registry, err := registryFromDir(t, filepath.Join(t.TempDir(), "nope"))
	if err != nil {
		t.Fatalf("missing dir: %v", err)
	}
	if _, err := registry.Resolve("default"); err != nil {
		t.Fatalf("default after missing dir: %v", err)
	}
}

// TestMeasurementFilterRegistry_FiltersThreeStates — omitting `filters` runs the
// shipped defaults (nil), while an explicit `[]` asks for no structural filters.
// The distinction is the DSL's, preserved across JSON.
func TestMeasurementFilterRegistry_FiltersThreeStates(t *testing.T) {
	omitted, err := registryFromDir(t, writeFilterSet(t, "omitted.json", `{"description":"x"}`))
	if err != nil {
		t.Fatalf("omitted: %v", err)
	}
	set, _ := omitted.Resolve("omitted")
	if set.Params.Filters != nil {
		t.Fatalf("omitted filters = %v, want nil (ship the defaults)", set.Params.Filters)
	}

	empty, err := registryFromDir(t, writeFilterSet(t, "empty.json", `{"description":"x","filters":[]}`))
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	set, _ = empty.Resolve("empty")
	if set.Params.Filters == nil || len(*set.Params.Filters) != 0 {
		t.Fatalf("empty filters = %v, want an explicit empty list", set.Params.Filters)
	}
}

// TestMeasurementFilterRegistry_MinValueZero — `min_value: 0` is a legitimate
// "no floor" and stays distinct from an omitted key (the shipped floor).
func TestMeasurementFilterRegistry_MinValueZero(t *testing.T) {
	registry, err := registryFromDir(t, writeFilterSet(t, "zero.json", `{"description":"x","min_value":0}`))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	set, _ := registry.Resolve("zero")
	if set.Params.MinValue == nil || *set.Params.MinValue != 0 {
		t.Fatalf("min_value = %v, want a present 0", set.Params.MinValue)
	}
}

// TestMeasurementFilterRegistry_IgnoresNonJSON — READMEs and other files in the
// directory are ignored; only <name>.json becomes a set.
func TestMeasurementFilterRegistry_IgnoresNonJSON(t *testing.T) {
	dir := writeFilterSet(t, "momentum.json", `{"description":"x"}`)
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# hi"), 0o644); err != nil {
		t.Fatalf("write readme: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub.json"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	registry, err := registryFromDir(t, dir)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if names := strings.Join(registry.Names(), ","); names != "default,momentum" {
		t.Fatalf("names = %q, want README and directories ignored", names)
	}
}

// TestMeasurementFilterRegistry_MalformedFile — an unreadable file is a
// structured error naming the file, never a silently skipped set.
func TestMeasurementFilterRegistry_MalformedFile(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{name: "unknown field (typo)", content: `{"descriptoin":"x"}`},
		{name: "invalid json", content: `{"window": `},
		{name: "wrong value type", content: `{"window":"many"}`},
		{name: "trailing content", content: `{"window":250} {"window":10}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := registryFromDir(t, writeFilterSet(t, "broken.json", tc.content))
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
			if !strings.Contains(err.Error(), "broken.json") {
				t.Fatalf("err = %v, want the offending filename", err)
			}
		})
	}
}

// TestMeasurementFilterRegistry_DuplicateCaseNames — two filenames that differ
// only by case resolve to one name, which is ambiguous and refused.
func TestMeasurementFilterRegistry_DuplicateCaseNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"Momentum.json", "momentum.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(`{"description":"x"}`), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if _, err := registryFromDir(t, dir); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument for duplicate names", err)
	}
}
