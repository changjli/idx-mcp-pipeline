package usecase

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestFlowGradeReport_Artifact — a single-anchor Flow Grade renders <as-of>_flow,
// the JSON round-trips with its sign and magnitude blocks, and the markdown
// carries the counts, the bucket tables, the coverage caveat, and the
// flow-specific success criteria.
func TestFlowGradeReport_Artifact(t *testing.T) {
	anchor, prices, flow := flowGradeScenario()
	grade, err := newFlowGradeEngine(prices, flow).
		GradeFlow(context.Background(), FlowGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	if artifact.Base != "2026-06-01_flow" {
		t.Fatalf("base = %s, want 2026-06-01_flow", artifact.Base)
	}

	payload := artifact.JSON
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

	md := string(artifact.Markdown)
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

	artifact, err := grade.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	want := "2026-06-01_to_2026-07-01_flow"
	if artifact.Base != want {
		t.Fatalf("base = %s, want %s", artifact.Base, want)
	}
	if !strings.Contains(RenderFlowGradeMarkdown(grade), "2026-06-01 → 2026-07-01") {
		t.Fatalf("range label missing:\n%s", RenderFlowGradeMarkdown(grade))
	}
}
