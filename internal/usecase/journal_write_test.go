package usecase

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeJournalWriter is an in-memory MeasurementJournalWriter: it records the
// commit flag and applies the same commit move the SQL writer does, so the
// run-verdict logic is testable above the repository seam.
type fakeJournalWriter struct {
	rows     []JournalVerdict
	nextID   int64
	commits  []bool
	writeErr error
}

func (f *fakeJournalWriter) Write(_ context.Context, v JournalVerdict, commit bool) (int64, time.Time, error) {
	if f.writeErr != nil {
		return 0, time.Time{}, f.writeErr
	}
	f.nextID++
	v.ID = f.nextID
	v.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(f.nextID) * time.Minute)
	v.Committed = commit
	if commit {
		for i := range f.rows {
			f.rows[i].Committed = false
		}
	}
	f.rows = append(f.rows, v)
	f.commits = append(f.commits, commit)
	return v.ID, v.CreatedAt, nil
}

func (f *fakeJournalWriter) ReadKey(_ context.Context, day time.Time, ticker string) ([]JournalVerdict, error) {
	key := day.Format("2006-01-02")
	out := make([]JournalVerdict, 0)
	for _, row := range f.rows {
		if row.Date == key && row.Ticker == ticker {
			out = append(out, row)
		}
	}
	return out, nil
}

func f64(v float64) *float64 { return &v }

func setupVerdict(session, date, ticker string) JournalVerdict {
	return JournalVerdict{
		Session: session, Date: date, Ticker: ticker, Verdict: verdictSetup,
		EntryLow: f64(100), EntryHigh: f64(105), CLLow: f64(90), CLHigh: f64(95),
		TPLow: f64(130), TPHigh: f64(135), Rationale: "breakout",
	}
}

func TestLogVerdict_AppendsSetup(t *testing.T) {
	w := &fakeJournalWriter{}
	res, err := LogVerdict(context.Background(), w, LogVerdictInput{
		Session: "s1", Date: "2026-05-01", Ticker: "bbri", Verdict: "setup",
		EntryLow: f64(100), EntryHigh: f64(105), CLLow: f64(90), CLHigh: f64(95),
		TPLow: f64(130), TPHigh: f64(135), Rationale: "breakout",
	})
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if res.ID != 1 || res.Ticker != "BBRI" || res.Committed || res.Conflict != nil {
		t.Fatalf("result = %+v", res)
	}
	if len(w.commits) != 1 || w.commits[0] {
		t.Fatalf("commits = %v, want [false]", w.commits)
	}
}

func TestLogVerdict_NonSetupStripsZones(t *testing.T) {
	w := &fakeJournalWriter{}
	_, err := LogVerdict(context.Background(), w, LogVerdictInput{
		Session: "s1", Date: "2026-05-01", Ticker: "BBRI", Verdict: "PASS",
		EntryLow: f64(100), Rationale: "no setup",
	})
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if w.rows[0].EntryLow != nil {
		t.Fatalf("PASS kept zones: %+v", w.rows[0])
	}
}

func TestLogVerdict_RejectsMalformed(t *testing.T) {
	cases := []struct {
		name string
		in   LogVerdictInput
	}{
		{"missing session", LogVerdictInput{Date: "2026-05-01", Ticker: "BBRI", Verdict: "PASS", Rationale: "x"}},
		{"bad date", LogVerdictInput{Session: "s", Date: "05/01/2026", Ticker: "BBRI", Verdict: "PASS", Rationale: "x"}},
		{"unknown verdict", LogVerdictInput{Session: "s", Date: "2026-05-01", Ticker: "BBRI", Verdict: "MAYBE", Rationale: "x"}},
		{"missing rationale", LogVerdictInput{Session: "s", Date: "2026-05-01", Ticker: "BBRI", Verdict: "PASS"}},
		{"setup unordered zones", LogVerdictInput{Session: "s", Date: "2026-05-01", Ticker: "BBRI", Verdict: "SETUP",
			EntryLow: f64(100), EntryHigh: f64(105), CLLow: f64(90), CLHigh: f64(95), TPLow: f64(80), TPHigh: f64(85), Rationale: "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LogVerdict(context.Background(), &fakeJournalWriter{}, tc.in)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestLogVerdict_SupersedesImpliesCommit(t *testing.T) {
	w := &fakeJournalWriter{}
	supersedes := int64(7)
	res, err := LogVerdict(context.Background(), w, LogVerdictInput{
		Session: "s2", Date: "2026-05-01", Ticker: "BBRI", Verdict: "SETUP",
		EntryLow: f64(100), EntryHigh: f64(105), CLLow: f64(90), CLHigh: f64(95),
		TPLow: f64(130), TPHigh: f64(135), Rationale: "re-planned", Supersedes: &supersedes,
	})
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if !res.Committed || !w.commits[0] {
		t.Fatalf("supersedes did not commit: result=%+v commits=%v", res, w.commits)
	}
}

func TestLogVerdict_CommitMovesTheCommittedRow(t *testing.T) {
	w := &fakeJournalWriter{}
	// A prior committed row on the key.
	w.rows = append(w.rows, func() JournalVerdict {
		v := setupVerdict("s1", "2026-05-01", "BBRI")
		v.ID = 1
		v.Committed = true
		return v
	}())
	w.nextID = 1

	_, err := LogVerdict(context.Background(), w, LogVerdictInput{
		Session: "s2", Date: "2026-05-01", Ticker: "BBRI", Verdict: "SETUP", Committed: true,
		EntryLow: f64(101), EntryHigh: f64(106), CLLow: f64(91), CLHigh: f64(96),
		TPLow: f64(131), TPHigh: f64(136), Rationale: "correction",
	})
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	committed := 0
	for _, row := range w.rows {
		if row.Committed {
			committed++
		}
	}
	if committed != 1 || !w.rows[len(w.rows)-1].Committed {
		t.Fatalf("committed rows = %d, want exactly the new one: %+v", committed, w.rows)
	}
}

func TestLogVerdict_CrossSessionConflictSurfaced(t *testing.T) {
	w := &fakeJournalWriter{}
	w.rows = append(w.rows, setupVerdict("s1", "2026-05-01", "BBRI"))
	w.rows[0].ID = 1
	w.nextID = 1

	res, err := LogVerdict(context.Background(), w, LogVerdictInput{
		Session: "s2", Date: "2026-05-01", Ticker: "BBRI", Verdict: "PASS", Rationale: "disagree",
	})
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if res.Conflict == nil || res.Conflict.Ticker != "BBRI" || len(res.Conflict.Rows) != 2 {
		t.Fatalf("conflict = %+v, want a two-row VerdictConflict", res.Conflict)
	}
}

func TestLogVerdict_CommitResolvesConflict(t *testing.T) {
	w := &fakeJournalWriter{}
	pass := JournalVerdict{Session: "s2", Date: "2026-05-01", Ticker: "BBRI", Verdict: verdictPass, Rationale: "no"}
	pass.ID = 2
	w.rows = append(w.rows, setupVerdict("s1", "2026-05-01", "BBRI"), pass)
	w.rows[0].ID = 1
	w.nextID = 2

	res, err := LogVerdict(context.Background(), w, LogVerdictInput{
		Session: "s3", Date: "2026-05-01", Ticker: "BBRI", Verdict: "SETUP", Committed: true,
		EntryLow: f64(100), EntryHigh: f64(105), CLLow: f64(90), CLHigh: f64(95),
		TPLow: f64(130), TPHigh: f64(135), Rationale: "committed",
	})
	if err != nil {
		t.Fatalf("log: %v", err)
	}
	if res.Conflict != nil {
		t.Fatalf("committed write left a conflict: %+v", res.Conflict)
	}
}

func TestLogVerdict_SameSessionDuplicateShadowed(t *testing.T) {
	w := &fakeJournalWriter{}
	in := LogVerdictInput{
		Session: "s1", Date: "2026-05-01", Ticker: "BBRI", Verdict: "SETUP",
		EntryLow: f64(100), EntryHigh: f64(105), CLLow: f64(90), CLHigh: f64(95),
		TPLow: f64(130), TPHigh: f64(135), Rationale: "first",
	}
	if _, err := LogVerdict(context.Background(), w, in); err != nil {
		t.Fatalf("first: %v", err)
	}
	in.Rationale = "second"
	res, err := LogVerdict(context.Background(), w, in)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(res.Shadowed) != 1 {
		t.Fatalf("shadowed = %+v, want the first row reported", res.Shadowed)
	}
	if res.Conflict != nil {
		t.Fatalf("same-session duplicate is not a conflict: %+v", res.Conflict)
	}
}

func TestLogVerdict_PropagatesWriteError(t *testing.T) {
	sentinel := errors.New("db down")
	_, err := LogVerdict(context.Background(), &fakeJournalWriter{writeErr: sentinel}, LogVerdictInput{
		Session: "s", Date: "2026-05-01", Ticker: "BBRI", Verdict: "PASS", Rationale: "x",
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the write error", err)
	}
}
