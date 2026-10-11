package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// journalConflictPrices fills AAA on day 1 (range 101-106 overlaps the 100-105
// entry zone) and touches 121 on day 2, so a target at 120 wins and a target at
// 130 expires.
func journalConflictPrices(days []time.Time) *fakeJournalPrices {
	return &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 106, 101), journalBar("AAA", days[2], 121, 102)},
	}}
}

// TestGradeJournal_CrossSessionConflictExcluded — two sessions that disagree on
// (verdict, zones) with no Committed Verdict are a conflict: the key is excluded
// from grading, counted once as logged, listed, and flagged — never picked by
// timestamp.
func TestGradeJournal_CrossSessionConflictExcluded(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")

	early := journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125) // morning plan: wins
	early.ID, early.Session = 1, "0700"
	late := journalSetup(date, "AAA", 100, 105, 90, 95, 130, 135) // 08:00 re-run: would expire
	late.ID, late.Session = 2, "0800"
	// CreatedAt keeps the re-run later, so a timestamp rule would pick it.
	late.CreatedAt = early.CreatedAt.Add(1)

	report, err := newJournalGradeEngine(
		&fakeJournalSource{verdicts: []JournalVerdict{early, late}},
		journalConflictPrices(days),
	).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("grade: %v", err)
	}

	if report.Counts.Conflicts != 1 || len(report.Conflicts) != 1 {
		t.Fatalf("conflicts = %d/%d, want 1/1", report.Counts.Conflicts, len(report.Conflicts))
	}
	if report.Counts.Logged != 1 || report.Counts.Tradable != 0 {
		t.Fatalf("counts = %+v, want logged 1 (the key) and nothing tradable", report.Counts)
	}
	if len(report.Verdicts) != 0 {
		t.Fatalf("verdicts = %+v, want the conflicted key excluded", report.Verdicts)
	}
	if report.Summary.Wins != 0 || report.Summary.Losses != 0 {
		t.Fatalf("summary = %+v, want no resolved outcome", report.Summary)
	}
	if !hasFlagPrefix(report.Flags, "conflict:") {
		t.Fatalf("flags = %v, want a conflict flag", report.Flags)
	}

	conflict := report.Conflicts[0]
	if conflict.DecisionDay != date || conflict.Ticker != "AAA" || len(conflict.Rows) != 2 {
		t.Fatalf("conflict = %+v, want both sessions listed", conflict)
	}
	if conflict.Rows[0].Session != "0700" || conflict.Rows[1].Session != "0800" {
		t.Fatalf("conflict rows = %+v, want one row per session", conflict.Rows)
	}
}

// TestGradeJournal_CommittedResolvesConflict — a Committed Verdict on a
// conflicted key is the one graded, and the conflict is no longer reported.
func TestGradeJournal_CommittedResolvesConflict(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")

	early := journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125)
	early.ID, early.Session = 1, "0700"
	late := journalSetup(date, "AAA", 100, 105, 90, 95, 130, 135)
	late.ID, late.Session = 2, "0800"
	late.Committed = true

	report, err := newJournalGradeEngine(
		&fakeJournalSource{verdicts: []JournalVerdict{early, late}},
		journalConflictPrices(days),
	).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("grade: %v", err)
	}

	if report.Counts.Conflicts != 0 || len(report.Conflicts) != 0 {
		t.Fatalf("conflicts = %d/%d, want none once committed", report.Counts.Conflicts, len(report.Conflicts))
	}
	row := findJournalVerdict(t, report, "AAA")
	if row.TPLow == nil || !almostEqual(*row.TPLow, 130) {
		t.Fatalf("graded row = %+v, want the committed target 130", row)
	}
	if row.Outcome != outcomeExpired {
		t.Fatalf("outcome = %s, want expired (day2 High 121 < 130)", row.Outcome)
	}
}

// TestGradeJournal_CrossSessionAgreementIsDuplicate — two sessions that agree on
// (verdict, zones) are a benign duplicate: one graded, none reported as a
// conflict, rationale prose never a disagreement.
func TestGradeJournal_CrossSessionAgreementIsDuplicate(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")

	first := journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125)
	first.ID, first.Session, first.Rationale = 1, "0700", "first wording"
	second := journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125)
	second.ID, second.Session, second.Rationale = 2, "0800", "different wording, same plan"

	report, err := newJournalGradeEngine(
		&fakeJournalSource{verdicts: []JournalVerdict{first, second}},
		journalConflictPrices(days),
	).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("grade: %v", err)
	}

	if report.Counts.Conflicts != 0 || report.Counts.Duplicates != 1 {
		t.Fatalf("counts = %+v, want 0 conflicts and 1 duplicate", report.Counts)
	}
	if len(report.Duplicates) != 1 || report.Duplicates[0].Ref == "" {
		t.Fatalf("duplicates = %+v, want the second session reported", report.Duplicates)
	}
	if row := findJournalVerdict(t, report, "AAA"); row.Outcome != outcomeWin {
		t.Fatalf("graded row = %+v, want a win on the shared plan", row)
	}
}

// TestGradeJournal_SupersedesWithoutCommitStaysConflict — supersedes is
// provenance, not a resolution signal: a divergent row that supersedes another
// but is not itself committed leaves the key a conflict for a human to resolve.
func TestGradeJournal_SupersedesWithoutCommitStaysConflict(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")

	early := journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125)
	early.ID, early.Session = 1, "0700"
	late := journalSetup(date, "AAA", 100, 105, 90, 95, 130, 135)
	late.ID, late.Session = 2, "0800"
	late.Supersedes = &early.ID
	// Committed deliberately left false.

	report, err := newJournalGradeEngine(
		&fakeJournalSource{verdicts: []JournalVerdict{early, late}},
		journalConflictPrices(days),
	).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("grade: %v", err)
	}
	if report.Counts.Conflicts != 1 || len(report.Verdicts) != 0 {
		t.Fatalf("conflicts = %d, verdicts = %d; want the key held out", report.Counts.Conflicts, len(report.Verdicts))
	}
	if report.Conflicts[0].Rows[1].Supersedes == nil || *report.Conflicts[0].Rows[1].Supersedes != 1 {
		t.Fatalf("conflict row = %+v, want the supersedes edge recorded", report.Conflicts[0].Rows[1])
	}
}
