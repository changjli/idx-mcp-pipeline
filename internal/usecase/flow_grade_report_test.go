package usecase

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFlowGradeReport_WriteFiles — a single-anchor Flow Grade writes <as-of>_flow,
// the JSON round-trips with its sign and magnitude blocks, and the markdown
// carries the counts, the bucket tables, the coverage caveat, and the
// flow-specific success criteria.
func TestFlowGradeReport_WriteFiles(t *testing.T) {
	anchor, prices, flow := flowGradeScenario()
	grade, err := newFlowGradeEngine(prices, flow).
		GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	dir := t.TempDir()
	jsonPath, mdPath, err := grade.WriteFiles(dir)
	if err != nil {
		t.Fatalf("WriteFiles: %v", err)
	}
	if filepath.Base(jsonPath) != "2026-06-01_flow.json" {
		t.Fatalf("json path = %s, want 2026-06-01_flow.json", filepath.Base(jsonPath))
	}
	if filepath.Base(mdPath) != "2026-06-01_flow.md" {
		t.Fatalf("md path = %s, want 2026-06-01_flow.md", filepath.Base(mdPath))
	}

	payload, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var decoded FlowGradeReport
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("reparse json: %v", err)
	}
	if decoded.Kind != flowGradeKind || decoded.GitRevision != "testrev" {
		t.Fatalf("round-tripped grade = %+v", decoded)
	}
	if len(decoded.SignBuckets) != 4 || len(decoded.Magnitude) != 3 {
		t.Fatalf("blocks = %d sign / %d magnitude, want 4/3", len(decoded.SignBuckets), len(decoded.Magnitude))
	}
	if !strings.Contains(string(payload), `"state": "unobserved"`) {
		t.Fatalf("JSON missing the unobserved bucket:\n%s", payload)
	}

	md := RenderFlowGradeMarkdown(grade)
	for _, want := range []string{
		"# Flow grade @ 2026-06-01",
		"## Counts",
		"## Sign buckets",
		"## Magnitude terciles (signed, pooled)",
		"## Per-date breakdown",
		"## Success criteria",
		"## Notes",
		"Covered days med (min–max)",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q\n---\n%s", want, md)
		}
	}
}

// TestFlowGradeReport_RangeFilename — a multi-anchor Grade's filename spans the
// first and last Replay Date, and the anchor label renders as a range.
func TestFlowGradeReport_RangeFilename(t *testing.T) {
	anchor, prices, flow := flowGradeScenario()

	grade, err := newFlowGradeEngine(prices, flow).
		GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor, prices.calendar[30]}})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	jsonPath, _, err := grade.WriteFiles(t.TempDir())
	if err != nil {
		t.Fatalf("WriteFiles: %v", err)
	}
	want := "2026-06-01_to_2026-07-01_flow.json"
	if filepath.Base(jsonPath) != want {
		t.Fatalf("json path = %s, want %s", filepath.Base(jsonPath), want)
	}
	if !strings.Contains(RenderFlowGradeMarkdown(grade), "2026-06-01 → 2026-07-01") {
		t.Fatalf("range label missing:\n%s", RenderFlowGradeMarkdown(grade))
	}
}
