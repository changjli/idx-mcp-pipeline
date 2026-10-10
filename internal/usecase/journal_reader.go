package usecase

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// MeasurementJournalDir is where the AI appends its Verdicts, relative to the
// repo root: one JSONL file per decision month, `YYYY-MM.jsonl`, append-only.
// It is git-tracked primary evidence (ADR-0014), the same deliberate exception
// the reports directory makes.
const MeasurementJournalDir = "measurement/journal"

// JournalFileReader reads the Decision Journal from disk. It is the concrete
// MeasurementJournalSource: it discovers the month files covering a window,
// parses each line, validates it, and returns the malformed lines it skipped so
// they are reported, never silently dropped.
type JournalFileReader struct {
	Dir string
}

func NewJournalFileReader(dir string) *JournalFileReader {
	return &JournalFileReader{Dir: dir}
}

// ReadVerdicts parses every journal line in the decision-date window. A nil
// bound is open at that end; with no bounds the whole journal is read. Only the
// month files covering the window are opened, so an out-of-scope malformed line
// never pollutes the report. A missing directory (or a missing month file in the
// window) means "no verdicts", not an error; a genuine read failure propagates.
func (r *JournalFileReader) ReadVerdicts(from, to *time.Time) ([]JournalVerdict, []JournalIssue, error) {
	files, err := journalFiles(r.Dir, from, to)
	if err != nil {
		return nil, nil, err
	}
	var verdicts []JournalVerdict
	var issues []JournalIssue
	for _, file := range files {
		fileVerdicts, fileIssues, err := readJournalFile(file, from, to)
		if err != nil {
			return nil, nil, err
		}
		verdicts = append(verdicts, fileVerdicts...)
		issues = append(issues, fileIssues...)
	}
	return verdicts, issues, nil
}

// journalFiles lists the journal files in scope: the glob of every month file
// when the window is open at either end, else exactly the month files covering
// [from, to]. Files that do not exist are skipped (that month had no verdicts).
func journalFiles(dir string, from, to *time.Time) ([]string, error) {
	if from == nil || to == nil {
		files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
		if err != nil {
			return nil, fmt.Errorf("list journal files in %s: %w", dir, err)
		}
		sort.Strings(files)
		return files, nil
	}

	var files []string
	month := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(to.Year(), to.Month(), 1, 0, 0, 0, 0, time.UTC)
	for !month.After(end) {
		path := filepath.Join(dir, month.Format("2006-01")+".jsonl")
		if _, err := os.Stat(path); err == nil {
			files = append(files, path)
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat journal %s: %w", path, err)
		}
		month = month.AddDate(0, 1, 0)
	}
	return files, nil
}

// readJournalFile parses one month file, returning the valid in-window verdicts
// and an issue for every line that could not be parsed or validated.
func readJournalFile(path string, from, to *time.Time) ([]JournalVerdict, []JournalIssue, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read journal %s: %w", path, err)
	}

	var verdicts []JournalVerdict
	var issues []JournalIssue
	for i, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		var verdict JournalVerdict
		if err := json.Unmarshal([]byte(line), &verdict); err != nil {
			issues = append(issues, JournalIssue{File: path, Line: i + 1, Reason: "invalid JSON: " + err.Error()})
			continue
		}
		verdict.SourceFile = path
		verdict.SourceLine = i + 1
		if err := normalizeJournalVerdict(&verdict); err != nil {
			issues = append(issues, JournalIssue{File: path, Line: i + 1, Reason: err.Error()})
			continue
		}
		if !journalDateInRange(verdict.Date, from, to) {
			continue
		}
		verdicts = append(verdicts, verdict)
	}
	return verdicts, issues, nil
}

// normalizeJournalVerdict canonicalizes and validates one line. A SETUP must
// carry three positive, ordered zones — clLow <= clHigh <= entryLow <= entryHigh
// <= tpLow <= tpHigh — where a point is a zone with low == high. A WATCH or PASS
// line carries no zones (any present are ignored). Every verdict needs a date, a
// ticker, and a one-line rationale.
func normalizeJournalVerdict(v *JournalVerdict) error {
	day, err := time.Parse("2006-01-02", strings.TrimSpace(v.Date))
	if err != nil {
		return fmt.Errorf("date must be YYYY-MM-DD, got %q", v.Date)
	}
	v.Date = day.Format("2006-01-02")

	v.Ticker = strings.ToUpper(strings.TrimSpace(v.Ticker))
	if v.Ticker == "" {
		return fmt.Errorf("ticker is required")
	}

	v.Verdict = strings.ToUpper(strings.TrimSpace(v.Verdict))
	switch v.Verdict {
	case verdictSetup, verdictWatch, verdictPass:
	default:
		return fmt.Errorf("unknown verdict %q; expected SETUP, WATCH or PASS", v.Verdict)
	}

	if strings.TrimSpace(v.Rationale) == "" {
		return fmt.Errorf("rationale is required")
	}

	if v.Verdict != verdictSetup {
		v.EntryLow, v.EntryHigh, v.CLLow, v.CLHigh, v.TPLow, v.TPHigh = nil, nil, nil, nil, nil, nil
		return nil
	}

	if v.EntryLow == nil || v.EntryHigh == nil || v.CLLow == nil || v.CLHigh == nil || v.TPLow == nil || v.TPHigh == nil {
		return fmt.Errorf("SETUP requires entryLow, entryHigh, clLow, clHigh, tpLow and tpHigh")
	}
	if *v.EntryLow <= 0 || *v.EntryHigh <= 0 || *v.CLLow <= 0 || *v.CLHigh <= 0 || *v.TPLow <= 0 || *v.TPHigh <= 0 {
		return fmt.Errorf("SETUP zones must be positive")
	}
	if *v.CLLow > *v.CLHigh {
		return fmt.Errorf("SETUP clLow must not exceed clHigh")
	}
	if *v.EntryLow > *v.EntryHigh {
		return fmt.Errorf("SETUP entryLow must not exceed entryHigh")
	}
	if *v.TPLow > *v.TPHigh {
		return fmt.Errorf("SETUP tpLow must not exceed tpHigh")
	}
	if *v.CLHigh > *v.EntryLow {
		return fmt.Errorf("SETUP clHigh must not exceed entryLow (the cut-loss zone must sit below the entry zone)")
	}
	if *v.EntryHigh > *v.TPLow {
		return fmt.Errorf("SETUP entryHigh must not exceed tpLow (the entry zone must sit below the target zone)")
	}
	return nil
}

// journalDateInRange reports whether a canonical date falls in the inclusive
// window; an open bound is unbounded.
func journalDateInRange(date string, from, to *time.Time) bool {
	day, err := time.Parse("2006-01-02", date)
	if err != nil {
		return false
	}
	if from != nil && day.Before(*from) {
		return false
	}
	if to != nil && day.After(*to) {
		return false
	}
	return true
}
