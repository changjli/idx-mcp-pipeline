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

// TestJournalGradeReport_WriteFiles — a windowed Journal Grade writes
// <from>_to_<until>_journal, the JSON round-trips, and the markdown carries the
// counts, outcomes, unfilled shadow, return distribution, per-verdict zones,
// malformed section, and the journal's standing questions.
func TestJournalGradeReport_WriteFiles(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{
		verdicts: []JournalVerdict{
			journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125),
			{Date: date, Ticker: "BBB", Verdict: verdictWatch, Rationale: "wait"},
		},
		issues: []JournalIssue{{File: "measurement/journal/2026-06.jsonl", Line: 9, Reason: "invalid JSON"}},
	}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 106, 101), journalBar("AAA", days[2], 121, 102)},
	}}
	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)

	grade, err := newJournalGradeEngine(source, prices).
		GradeJournal(context.Background(), JournalGradeOptions{From: &from, Until: &until})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	dir := t.TempDir()
	jsonPath, mdPath, err := grade.WriteFiles(dir)
	if err != nil {
		t.Fatalf("WriteFiles: %v", err)
	}
	if filepath.Base(jsonPath) != "2026-06-01_to_2026-06-30_journal.json" {
		t.Fatalf("json path = %s", filepath.Base(jsonPath))
	}
	if filepath.Base(mdPath) != "2026-06-01_to_2026-06-30_journal.md" {
		t.Fatalf("md path = %s", filepath.Base(mdPath))
	}

	payload, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("read json: %v", err)
	}
	var decoded JournalGradeReport
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("reparse json: %v", err)
	}
	if decoded.Kind != journalGradeKind || decoded.GitRevision != "testrev" {
		t.Fatalf("round-tripped grade = %+v", decoded)
	}
	if decoded.EntryWindowDays != journalEntryWindowDays || decoded.ResolutionDays != journalResolutionDays {
		t.Fatalf("windows = %d/%d", decoded.EntryWindowDays, decoded.ResolutionDays)
	}
	if decoded.Counts.Tradable != 1 || decoded.Counts.Watch != 1 || decoded.Counts.Malformed != 1 {
		t.Fatalf("counts = %+v", decoded.Counts)
	}
	if decoded.Summary.Wins != 1 {
		t.Fatalf("summary = %+v", decoded.Summary)
	}

	md := RenderJournalGradeMarkdown(grade)
	for _, want := range []string{
		"# Journal grade — 2026-06-01 → 2026-06-30",
		"## Counts",
		"## Outcomes (tradable verdicts)",
		"## Unfilled shadow (never in the win rate)",
		"## Return distribution (resolved verdicts)",
		"## Verdicts",
		"## Malformed lines (skipped, never graded)",
		"## Discipline flags",
		"## Success criteria",
		"## Notes",
		"| 100-105 |", // the entry zone renders
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q\n---\n%s", want, md)
		}
	}
}

// TestJournalGradeReport_OpenWindowFilename — with no window the report is
// all_journal.
func TestJournalGradeReport_OpenWindowFilename(t *testing.T) {
	grade, err := newJournalGradeEngine(&fakeJournalSource{}, &fakeJournalPrices{}).
		GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}
	jsonPath, _, err := grade.WriteFiles(t.TempDir())
	if err != nil {
		t.Fatalf("WriteFiles: %v", err)
	}
	if filepath.Base(jsonPath) != "all_journal.json" {
		t.Fatalf("json path = %s, want all_journal.json", filepath.Base(jsonPath))
	}
	if !strings.Contains(RenderJournalGradeMarkdown(grade), "# Journal grade — all") {
		t.Fatalf("open-window label missing:\n%s", RenderJournalGradeMarkdown(grade))
	}
}
