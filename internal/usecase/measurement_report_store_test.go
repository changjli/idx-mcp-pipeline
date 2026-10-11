package usecase

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
)

func artifactForTest(base, body string) ReportArtifact {
	return ReportArtifact{
		Base:     base,
		JSON:     []byte("{\"body\":\"" + body + "\"}\n"),
		Markdown: []byte("# " + body + "\n"),
	}
}

// TestMeasurementReportStore_SealedWritesVersionAndLatest — a sealed report
// lands under both the immutable version key and the bare latest key, and Get
// reads the latest.
func TestMeasurementReportStore_SealedWritesVersionAndLatest(t *testing.T) {
	objects := newFakeObjectStore()
	store := NewMeasurementReportStore(objects)
	ctx := context.Background()

	artifact := artifactForTest("2026-06-01_default", "v1")
	jsonKey, mdKey, err := store.Sealed(ctx, artifact)
	if err != nil {
		t.Fatalf("sealed: %v", err)
	}
	if jsonKey != MeasurementReportPrefix+"/2026-06-01_default.json" {
		t.Fatalf("json key = %s", jsonKey)
	}
	if mdKey != MeasurementReportPrefix+"/2026-06-01_default.md" {
		t.Fatalf("md key = %s", mdKey)
	}

	version := reportVersionID(artifact.JSON)
	versionJSON := fmt.Sprintf("%s/versions/%s/%s.json", MeasurementReportPrefix, artifact.Base, version)
	if _, ok := objects.objects[versionJSON]; !ok {
		t.Fatalf("sealed version %s not written", versionJSON)
	}

	gotJSON, gotMD, err := store.Get(ctx, artifact.Base)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(gotJSON, artifact.JSON) || !bytes.Equal(gotMD, artifact.Markdown) {
		t.Fatalf("get returned different content")
	}
}

// TestMeasurementReportStore_SealedIsIdempotent — sealing identical content
// twice is a no-op (what makes `migrate` re-runnable).
func TestMeasurementReportStore_SealedIsIdempotent(t *testing.T) {
	objects := newFakeObjectStore()
	store := NewMeasurementReportStore(objects)
	ctx := context.Background()
	artifact := artifactForTest("2026-06-01_default", "v1")

	if _, _, err := store.Sealed(ctx, artifact); err != nil {
		t.Fatalf("first: %v", err)
	}
	afterFirst := len(objects.objects)
	if _, _, err := store.Sealed(ctx, artifact); err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(objects.objects) != afterFirst {
		t.Fatalf("second seal added objects: %d -> %d", afterFirst, len(objects.objects))
	}
}

// TestMeasurementReportStore_LatestMovesVersionsRemain — re-sealing the same
// base with new content moves the latest key but keeps the old version, so a
// journal Grader's "grade again later" never destroys earlier evidence.
func TestMeasurementReportStore_LatestMovesVersionsRemain(t *testing.T) {
	objects := newFakeObjectStore()
	store := NewMeasurementReportStore(objects)
	ctx := context.Background()

	first := artifactForTest("all_journal", "unresolved")
	if _, _, err := store.Sealed(ctx, first); err != nil {
		t.Fatalf("first: %v", err)
	}
	second := artifactForTest("all_journal", "resolved")
	if _, _, err := store.Sealed(ctx, second); err != nil {
		t.Fatalf("second: %v", err)
	}

	for _, artifact := range []ReportArtifact{first, second} {
		key := fmt.Sprintf("%s/versions/%s/%s.json", MeasurementReportPrefix, artifact.Base, reportVersionID(artifact.JSON))
		if _, ok := objects.objects[key]; !ok {
			t.Fatalf("version %s missing", key)
		}
	}
	latest, _, err := store.Get(ctx, "all_journal")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(latest, second.JSON) {
		t.Fatalf("latest = %s, want the second report", latest)
	}
}

// TestMeasurementReportStore_List — the store lists object keys under a prefix
// through the storage's lister.
func TestMeasurementReportStore_List(t *testing.T) {
	objects := newFakeObjectStore()
	store := NewMeasurementReportStore(objects)
	if _, _, err := store.Sealed(context.Background(), artifactForTest("2026-06-01_default", "v1")); err != nil {
		t.Fatalf("sealed: %v", err)
	}
	keys, err := store.List(context.Background(), MeasurementReportPrefix)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	joined := strings.Join(keys, ",")
	if !strings.Contains(joined, "/2026-06-01_default.json") || !strings.Contains(joined, "/versions/2026-06-01_default/") {
		t.Fatalf("keys = %v, want the latest and the version", keys)
	}
}
