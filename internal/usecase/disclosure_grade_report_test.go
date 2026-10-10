package usecase

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// TestDisclosureGradeReport_WriteFiles — a single-filing-day Disclosure Grade
// writes <as-of>_disclosures, the JSON round-trips with its rows and reject
// sample, and the markdown carries every section a reader follows.
func TestDisclosureGradeReport_WriteFiles(t *testing.T) {
	anchor, _, prices, source := disclosureGradeScenario()
	grade, err := newDisclosureGradeEngine(prices, source).
		GradeDisclosures(context.Background(), DisclosureGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	dir := t.TempDir()
	jsonPath, mdPath, err := grade.WriteFiles(dir)
	if err != nil {
		t.Fatalf("WriteFiles: %v", err)
	}
	if filepath.Base(jsonPath) != "2026-06-01_disclosures.json" {
		t.Fatalf("json path = %s, want 2026-06-01_disclosures.json", filepath.Base(jsonPath))
	}
	if filepath.Base(mdPath) != "2026-06-01_disclosures.md" {
		t.Fatalf("md path = %s, want 2026-06-01_disclosures.md", filepath.Base(mdPath))
	}

	payload, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
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

	md := RenderDisclosureGradeMarkdown(grade)
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

	jsonPath, _, err := grade.WriteFiles(t.TempDir())
	if err != nil {
		t.Fatalf("WriteFiles: %v", err)
	}
	want := "2026-06-01_to_2026-06-16_disclosures.json"
	if filepath.Base(jsonPath) != want {
		t.Fatalf("json path = %s, want %s", filepath.Base(jsonPath), want)
	}
	if !strings.Contains(RenderDisclosureGradeMarkdown(grade), "2026-06-01 → 2026-06-16") {
		t.Fatalf("range label missing:\n%s", RenderDisclosureGradeMarkdown(grade))
	}
}
