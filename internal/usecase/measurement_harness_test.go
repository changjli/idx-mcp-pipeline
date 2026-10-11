package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

func newHarnessForTest(objects storage.ObjectStore) *MeasurementHarness {
	return NewMeasurementHarness(nil, logrus.New(), objects, "testrev")
}

func TestMeasurementHarness_ListReportsLatestOnly(t *testing.T) {
	objects := newFakeObjectStore()
	h := newHarnessForTest(objects)
	ctx := context.Background()

	for _, base := range []string{"2026-07-01_default", "2026-08-01_volume-tolerant"} {
		if _, _, err := h.Reports.Sealed(ctx, artifactForTest(base, base)); err != nil {
			t.Fatalf("seal: %v", err)
		}
	}

	bases, err := h.ListReports(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(bases) != 2 || bases[0] != "2026-08-01_volume-tolerant" || bases[1] != "2026-07-01_default" {
		t.Fatalf("bases = %v, want the two latest bases newest-first", bases)
	}
	for _, base := range bases {
		if strings.Contains(base, "versions") {
			t.Fatalf("base %q leaks a sealed version", base)
		}
	}
}

func TestMeasurementHarness_GetReport(t *testing.T) {
	objects := newFakeObjectStore()
	h := newHarnessForTest(objects)
	ctx := context.Background()
	if _, _, err := h.Reports.Sealed(ctx, artifactForTest("2026-07-01_default", "body")); err != nil {
		t.Fatalf("seal: %v", err)
	}

	doc, err := h.GetReport(ctx, "2026-07-01_default")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if doc.Base != "2026-07-01_default" || string(doc.Report) != "{\"body\":\"body\"}\n" || doc.Markdown != "# body\n" {
		t.Fatalf("doc = %+v", doc)
	}

	if _, err := h.GetReport(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing report err = %v, want ErrNotFound", err)
	}
}

func TestMeasurementHarness_PutConfigCAS(t *testing.T) {
	objects := newFakeObjectStore()
	h := newHarnessForTest(objects)
	ctx := context.Background()

	doc := MeasurementConfigDocument{Active: "default", Sets: map[string]MeasurementFilterSetFile{"default": {}}}
	first, err := h.PutConfig(ctx, doc, "")
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	if _, err := h.PutConfig(ctx, doc, "stale-hash"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("stale expected_hash err = %v, want ErrInvalidArgument", err)
	}
	second, err := h.PutConfig(ctx, doc, first)
	if err != nil {
		t.Fatalf("matching expected_hash: %v", err)
	}
	if second != first {
		t.Fatalf("re-putting identical content changed the hash: %s -> %s", first, second)
	}
}

func TestMeasurementHarness_GetConfigEmptyIsBuiltinDefault(t *testing.T) {
	h := newHarnessForTest(newFakeObjectStore())
	doc, hash, err := h.GetConfig(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if hash != "" || doc.Active != "default" || len(doc.Sets) != 0 {
		t.Fatalf("empty config = %+v hash %q", doc, hash)
	}
}

func TestMeasurementHarness_NoObjectStorageErrors(t *testing.T) {
	h := newHarnessForTest(nil)
	ctx := context.Background()
	if _, _, err := h.GetConfig(ctx); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("GetConfig err = %v, want ErrInvalidArgument", err)
	}
	if _, err := h.PutConfig(ctx, MeasurementConfigDocument{}, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("PutConfig err = %v, want ErrInvalidArgument", err)
	}
	if _, err := h.ListReports(ctx); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ListReports err = %v, want ErrInvalidArgument", err)
	}
	if _, err := h.GetReport(ctx, "x"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("GetReport err = %v, want ErrInvalidArgument", err)
	}
}

func TestMeasurementHarness_RegistryFallsBackToBuiltin(t *testing.T) {
	h := newHarnessForTest(nil)
	registry, active, hash, err := h.registry(context.Background())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	if active != "default" || hash != "" {
		t.Fatalf("active=%q hash=%q, want the built-in default", active, hash)
	}
	if _, err := registry.Resolve(""); err != nil {
		t.Fatalf("resolve active: %v", err)
	}
}
