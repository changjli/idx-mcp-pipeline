package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func configDocForTest(active string, sets map[string]MeasurementFilterSetFile) MeasurementConfigDocument {
	return MeasurementConfigDocument{Active: active, Sets: sets}
}

// newFakeObjectStore builds the shared in-memory ObjectStore used by the config
// and report store tests (fetch_disclosure_test.go defines the type).
func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{objects: map[string][]byte{}}
}

// TestMeasurementConfigStore_PutGetRoundTrip — Put publishes an immutable
// version and moves the active pointer at it; Get returns the same document and
// the Config Hash addresses the stored version.
func TestMeasurementConfigStore_PutGetRoundTrip(t *testing.T) {
	objects := newFakeObjectStore()
	store := NewMeasurementConfigStore(objects)
	ctx := context.Background()

	doc := configDocForTest("volume-tolerant", map[string]MeasurementFilterSetFile{
		"default":         {},
		"volume-tolerant": {Description: "volume_ratio:20 lte 1.5"},
	})
	hash, err := store.Put(ctx, doc)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if hash == "" {
		t.Fatal("put returned an empty hash")
	}

	pointerKey := MeasurementConfigPrefix + "/active.json"
	versionKey := fmt.Sprintf(measurementConfigVersionKey, hash)
	if _, ok := objects.objects[pointerKey]; !ok {
		t.Fatalf("pointer %s not written", pointerKey)
	}
	versionBytes, ok := objects.objects[versionKey]
	if !ok {
		t.Fatalf("version %s not written", versionKey)
	}
	if contentHash(versionBytes) != hash {
		t.Fatalf("version bytes do not hash to the returned hash")
	}

	got, gotHash, err := store.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gotHash != hash || got.Active != "volume-tolerant" {
		t.Fatalf("get = %+v hash %s, want active volume-tolerant @ %s", got, gotHash, hash)
	}
	if got.Sets["volume-tolerant"].Description != "volume_ratio:20 lte 1.5" {
		t.Fatalf("set lost in round trip: %+v", got.Sets)
	}

	registry, active, pinned, err := store.Registry(ctx)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	if active != "volume-tolerant" || pinned != hash {
		t.Fatalf("registry active/hash = %s/%s, want volume-tolerant/%s", active, pinned, hash)
	}
	set, err := registry.Resolve("")
	if err != nil || set.Name != "volume-tolerant" {
		t.Fatalf("bare resolve = %+v (%v), want the active set", set, err)
	}
}

// TestMeasurementConfigStore_VersionsKept — a second Put leaves the first
// version fetchable, so an old report's Config Hash still resolves.
func TestMeasurementConfigStore_VersionsKept(t *testing.T) {
	objects := newFakeObjectStore()
	store := NewMeasurementConfigStore(objects)
	ctx := context.Background()

	first, err := store.Put(ctx, configDocForTest("default", map[string]MeasurementFilterSetFile{"default": {Description: "v1"}}))
	if err != nil {
		t.Fatalf("put v1: %v", err)
	}
	second, err := store.Put(ctx, configDocForTest("momentum", map[string]MeasurementFilterSetFile{
		"default":  {},
		"momentum": {Description: "v2"},
	}))
	if err != nil {
		t.Fatalf("put v2: %v", err)
	}
	if first == second {
		t.Fatal("distinct documents share a version hash")
	}
	for _, hash := range []string{first, second} {
		key := fmt.Sprintf(measurementConfigVersionKey, hash)
		if _, ok := objects.objects[key]; !ok {
			t.Fatalf("version %s was evicted by a later put", key)
		}
	}

	_, current, err := store.Get(ctx)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if current != second {
		t.Fatalf("pointer = %s, want the latest %s", current, second)
	}
}

// TestMeasurementConfigStore_MissingIsFreshStart — an empty bucket is not an
// error: the registry is the built-in default and no hash is pinned.
func TestMeasurementConfigStore_MissingIsFreshStart(t *testing.T) {
	store := NewMeasurementConfigStore(newFakeObjectStore())
	ctx := context.Background()

	doc, hash, err := store.Get(ctx)
	if err != nil || doc != nil || hash != "" {
		t.Fatalf("get = (%+v, %q, %v), want (nil, \"\", nil)", doc, hash, err)
	}
	registry, active, pinned, err := store.Registry(ctx)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	if active != defaultMeasurementFilterSet || pinned != "" {
		t.Fatalf("active/hash = %s/%s, want default/empty", active, pinned)
	}
	if _, err := registry.Resolve("default"); err != nil {
		t.Fatalf("built-in default should resolve: %v", err)
	}
}

// TestMeasurementConfigStore_CorruptConfig — a malformed pointer, a dangling
// hash, or a tampered version is a structured error, never a silent wrong set.
func TestMeasurementConfigStore_CorruptConfig(t *testing.T) {
	ctx := context.Background()
	pointerKey := MeasurementConfigPrefix + "/active.json"

	t.Run("malformed pointer", func(t *testing.T) {
		objects := newFakeObjectStore()
		objects.objects[pointerKey] = []byte("{not json")
		if _, _, err := NewMeasurementConfigStore(objects).Get(ctx); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("dangling hash", func(t *testing.T) {
		objects := newFakeObjectStore()
		objects.objects[pointerKey] = []byte(`{"hash":"deadbeef","active":"default"}`)
		if _, _, err := NewMeasurementConfigStore(objects).Get(ctx); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("tampered version", func(t *testing.T) {
		objects := newFakeObjectStore()
		store := NewMeasurementConfigStore(objects)
		hash, err := store.Put(ctx, configDocForTest("default", map[string]MeasurementFilterSetFile{"default": {}}))
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		objects.objects[fmt.Sprintf(measurementConfigVersionKey, hash)] = []byte(`{"active":"default","sets":{}}`)
		if _, _, err := store.Get(ctx); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
	})
}

// TestMeasurementConfigStore_SeedFromDir — migrate seeds the built-in default
// plus the local filter sets, with the chosen set active.
func TestMeasurementConfigStore_SeedFromDir(t *testing.T) {
	dir := writeFilterSet(t, "volume-tolerant.json", `{"description":"volume_ratio:20 lte 1.5","window":60}`)
	objects := newFakeObjectStore()
	store := NewMeasurementConfigStore(objects)

	hash, err := store.SeedFromDir(context.Background(), dir, "volume-tolerant")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	doc, gotHash, err := store.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gotHash != hash || doc.Active != "volume-tolerant" {
		t.Fatalf("seeded doc = %+v @ %s", doc, gotHash)
	}
	if _, ok := doc.Sets["default"]; !ok {
		t.Fatalf("seeded doc has no default entry: %+v", doc.Sets)
	}
	if doc.Sets["volume-tolerant"].Window != 60 {
		t.Fatalf("volume-tolerant not seeded: %+v", doc.Sets["volume-tolerant"])
	}
}

// TestGradeScreen_PinsConfig — the screen Grade stamps the Config Hash and the
// active set into the report and its nested per-set reports.
func TestGradeScreen_PinsConfig(t *testing.T) {
	days, screener, prices := measurementGradeScenario()
	grade, err := newGradeEngine(screener, prices, nil).GradeScreen(context.Background(), ScreenGradeOptions{
		Anchors:         []time.Time{days[0]},
		FilterSet:       "default",
		ConfigHash:      "abc123",
		ActiveFilterSet: "volume-tolerant",
	})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}
	if grade.ConfigHash != "abc123" || grade.ActiveFilterSet != "volume-tolerant" {
		t.Fatalf("grade pins = %s/%s", grade.ConfigHash, grade.ActiveFilterSet)
	}
	for _, set := range grade.FilterSets {
		if set.ConfigHash != "abc123" || set.ActiveFilterSet != "volume-tolerant" {
			t.Fatalf("nested set pins = %s/%s", set.ConfigHash, set.ActiveFilterSet)
		}
	}
}
