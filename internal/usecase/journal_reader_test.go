package usecase

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeJournalFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write journal: %v", err)
	}
}

// TestJournalFileReader_ParsesAndNormalizes — valid lines are parsed, the ticker
// and verdict are canonicalized, and a no-trade line never carries zones.
func TestJournalFileReader_ParsesAndNormalizes(t *testing.T) {
	dir := t.TempDir()
	writeJournalFile(t, dir, "2026-06.jsonl",
		`{"date":"2026-06-01","ticker":"aa","verdict":"setup","entryLow":100,"entryHigh":105,"clLow":90,"clHigh":95,"tpLow":120,"tpHigh":125,"rationale":"breakout"}`+"\n"+
			`{"date":"2026-06-02","ticker":"BB","verdict":"watch","entryLow":100,"entryHigh":105,"clLow":90,"clHigh":95,"tpLow":120,"tpHigh":125,"rationale":"wait"}`+"\n")

	verdicts, issues, err := NewJournalFileReader(dir).ReadVerdicts(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(issues) != 0 {
		t.Fatalf("issues = %+v, want none", issues)
	}
	if len(verdicts) != 2 {
		t.Fatalf("verdicts = %d, want 2", len(verdicts))
	}
	if verdicts[0].Ticker != "AA" || verdicts[0].Verdict != verdictSetup {
		t.Fatalf("normalized verdict = %+v", verdicts[0])
	}
	if verdicts[0].EntryHigh == nil || *verdicts[0].EntryHigh != 105 || verdicts[0].TPHigh == nil || *verdicts[0].TPHigh != 125 {
		t.Fatalf("zones not parsed: %+v", verdicts[0])
	}
	if verdicts[1].Verdict != verdictWatch || verdicts[1].EntryLow != nil || verdicts[1].TPHigh != nil || verdicts[1].CLHigh != nil {
		t.Fatalf("no-trade verdict kept zones: %+v", verdicts[1])
	}
	if verdicts[0].SourceLine != 1 || verdicts[1].SourceLine != 2 {
		t.Fatalf("source lines = %d/%d, want 1/2", verdicts[0].SourceLine, verdicts[1].SourceLine)
	}
}

// TestJournalFileReader_PointsAllowed — a zone with low == high is a point and
// must validate.
func TestJournalFileReader_PointsAllowed(t *testing.T) {
	dir := t.TempDir()
	writeJournalFile(t, dir, "2026-06.jsonl",
		`{"date":"2026-06-01","ticker":"AA","verdict":"SETUP","entryLow":100,"entryHigh":100,"clLow":90,"clHigh":90,"tpLow":120,"tpHigh":120,"rationale":"point"}`+"\n")

	verdicts, issues, err := NewJournalFileReader(dir).ReadVerdicts(nil, nil)
	if err != nil || len(issues) != 0 || len(verdicts) != 1 {
		t.Fatalf("point zones: verdicts=%d issues=%+v err=%v", len(verdicts), issues, err)
	}
}

// TestJournalFileReader_ReportsMalformed — unparseable JSON and schema-invalid
// lines are reported with their line numbers and skipped, never silently
// dropped.
func TestJournalFileReader_ReportsMalformed(t *testing.T) {
	dir := t.TempDir()
	writeJournalFile(t, dir, "2026-06.jsonl",
		`{"date":"2026-06-01","ticker":"AA","verdict":"SETUP","entryLow":100,"entryHigh":105,"clLow":90,"clHigh":95,"tpLow":120,"tpHigh":125,"rationale":"ok"}`+"\n"+ // 1 valid
			`{"date":"2026-06-01","ticker":"AA","verdict":"SETUP","rationale":"no zones"}`+"\n"+ // 2 missing zones
			`this is not json`+"\n"+ // 3 bad JSON
			`{"date":"2026-06-01","ticker":"AA","verdict":"HOLD","rationale":"x"}`+"\n"+ // 4 unknown verdict
			`{"date":"2026-06-01","ticker":"AA","verdict":"SETUP","entryLow":100,"entryHigh":105,"clLow":90,"clHigh":110,"tpLow":120,"tpHigh":125,"rationale":"x"}`+"\n"+ // 5 clHigh > entryLow
			`{"date":"2026-06-01","ticker":"AA","verdict":"SETUP","entryLow":100,"entryHigh":130,"clLow":90,"clHigh":95,"tpLow":120,"tpHigh":125,"rationale":"x"}`+"\n"+ // 6 entryHigh > tpLow
			`{"date":"2026-06-02","ticker":"BB","verdict":"PASS","rationale":"no"}`+"\n") // 7 valid

	verdicts, issues, err := NewJournalFileReader(dir).ReadVerdicts(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(verdicts) != 2 {
		t.Fatalf("valid verdicts = %d, want 2", len(verdicts))
	}
	wantLines := []int{2, 3, 4, 5, 6}
	if len(issues) != len(wantLines) {
		t.Fatalf("issues = %+v, want %d", issues, len(wantLines))
	}
	for i, want := range wantLines {
		if issues[i].Line != want {
			t.Fatalf("issue %d line = %d, want %d (%+v)", i, issues[i].Line, want, issues[i])
		}
		if issues[i].File == "" || issues[i].Reason == "" {
			t.Fatalf("issue %d missing file/reason: %+v", i, issues[i])
		}
	}
}

// TestJournalFileReader_WindowScoping — only the month files covering the
// window are opened, and a line outside the window is not returned.
func TestJournalFileReader_WindowScoping(t *testing.T) {
	dir := t.TempDir()
	writeJournalFile(t, dir, "2026-05.jsonl", `{"date":"2026-05-15","ticker":"MAY","verdict":"SETUP","entryLow":100,"entryHigh":105,"clLow":90,"clHigh":95,"tpLow":120,"tpHigh":125,"rationale":"may"}`+"\n")
	writeJournalFile(t, dir, "2026-06.jsonl", `{"date":"2026-06-15","ticker":"JUN","verdict":"SETUP","entryLow":100,"entryHigh":105,"clLow":90,"clHigh":95,"tpLow":120,"tpHigh":125,"rationale":"jun"}`+"\n")

	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	verdicts, _, err := NewJournalFileReader(dir).ReadVerdicts(&from, &until)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(verdicts) != 1 || verdicts[0].Ticker != "JUN" {
		t.Fatalf("window verdicts = %+v, want only JUN", verdicts)
	}
}

// TestJournalFileReader_MissingDirectory — no journal directory means no
// verdicts, not an error.
func TestJournalFileReader_MissingDirectory(t *testing.T) {
	verdicts, issues, err := NewJournalFileReader(filepath.Join(t.TempDir(), "absent")).ReadVerdicts(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(verdicts) != 0 || len(issues) != 0 {
		t.Fatalf("got %d verdicts / %d issues, want none", len(verdicts), len(issues))
	}
}
