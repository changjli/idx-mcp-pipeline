package usecase

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// TestDisclosureGradeReport_Artifact — a single-filing-day Disclosure Grade
// renders <as-of>_disclosures, the JSON round-trips with its rows and reject
// sample, and the markdown carries every section a reader follows.
func TestDisclosureGradeReport_Artifact(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	if artifact.Base != "2026-06-01_disclosures" {
		t.Fatalf("base = %s, want 2026-06-01_disclosures", artifact.Base)
	}

	payload := artifact.JSON
	var decoded DisclosureGradeReport
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("reparse json: %v", err)
	}
	if decoded.Kind != disclosureGradeKind || decoded.GitRevision != "testrev" {
		t.Fatalf("round-tripped grade = %+v", decoded)
	}
	if decoded.DisclosureLookbackDays != 7 || len(decoded.Rows) != 3 {
		t.Fatalf("lookback/rows = %d/%d, want 7/3", decoded.DisclosureLookbackDays, len(decoded.Rows))
	}
	if len(decoded.RejectSampling.Rows) != 3 {
		t.Fatalf("reject sample rows = %d, want 3", len(decoded.RejectSampling.Rows))
	}
	if !strings.Contains(string(payload), `"material": null`) {
		t.Fatalf("JSON missing the unfilled material slots:\n%s", payload)
	}

	md := string(artifact.Markdown)
	for _, want := range []string{
		"# Disclosure grade @ 2026-06-01",
		"## Counts",
		"## Reaction vs Liquidity Baseline",
		"## Category split (keyword whitelist)",
		"## Per-date breakdown",
		"## Passed disclosures",
		"## Reject sampling (manual, monthly)",
		"Material?",
		"## Discipline flags",
		"## Success criteria",
		"## Notes",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q\n---\n%s", want, md)
		}
	}
}

// TestDisclosureGradeReport_RangeFilename — a multi-day Grade's filename spans
// the first and last Filing Day, and the anchor label renders as a range.
func TestDisclosureGradeReport_RangeFilename(t *testing.T) {
	anchor, days, prices, source := disclosureGradeScenario()
	second := days[15]
	source.passedByDay[second.Format("2006-01-02")] = []entity.Disclosure{
		disclosure(10, "AAA", second, "Dividen", "pdf-a3"),
	}

	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor, second}})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	want := "2026-06-01_to_2026-06-16_disclosures"
	if artifact.Base != want {
		t.Fatalf("base = %s, want %s", artifact.Base, want)
	}
	if !strings.Contains(RenderDisclosureGradeMarkdown(grade), "2026-06-01 → 2026-06-16") {
		t.Fatalf("range label missing:\n%s", RenderDisclosureGradeMarkdown(grade))
	}
}
