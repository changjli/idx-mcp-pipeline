package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// LogVerdictInput is one `log_verdict` request: a Verdict to append, plus the
// provenance the writer needs to uphold ADR-0016's conflict rule. The camel-case
// zone names mirror a journal row's JSON shape.
type LogVerdictInput struct {
	// Session identifies the agent run writing the Verdict. Required: the table
	// is NOT NULL and the same-session/cross-session rule keys on it.
	Session string `json:"session"`
	// Date is the decision day: the last stored Trading Day whose close the plan
	// reads (ADR-0014), YYYY-MM-DD.
	Date string `json:"date"`
	// Ticker is the IDX code; normalized to uppercase.
	Ticker  string `json:"ticker"`
	Verdict string `json:"verdict"`
	// The six Tradable Zones, present only on a SETUP (a point is low == high).
	EntryLow  *float64 `json:"entryLow,omitempty"`
	EntryHigh *float64 `json:"entryHigh,omitempty"`
	CLLow     *float64 `json:"clLow,omitempty"`
	CLHigh    *float64 `json:"clHigh,omitempty"`
	TPLow     *float64 `json:"tpLow,omitempty"`
	TPHigh    *float64 `json:"tpHigh,omitempty"`
	Rationale string   `json:"rationale"`
	// Supersedes names the row a deliberate correction replaces. It is
	// provenance; the writer also sets Committed when it is present (ADR-0016).
	Supersedes *int64 `json:"supersedes,omitempty"`
	// Committed marks this the Verdict the Grader scores when the key holds more
	// than one. Setting it moves the single Committed Verdict for the key.
	Committed bool `json:"committed,omitempty"`
}

// LogVerdictResult reports what an append did: the new row, whether it is now
// the Committed Verdict, and any Verdict Conflict the write left on the key.
type LogVerdictResult struct {
	ID         int64          `json:"id"`
	Session    string         `json:"session"`
	Ticker     string         `json:"ticker"`
	Verdict    string         `json:"verdict"`
	Committed  bool           `json:"committed"`
	Superseded *int64         `json:"superseded,omitempty"`
	Shadowed   []JournalIssue `json:"shadowed,omitempty"`
	// Conflict is non-nil when the key now holds divergent cross-session Verdicts
	// with no Committed Verdict — surfaced at write time, never auto-resolved.
	Conflict *VerdictConflict `json:"conflict,omitempty"`
}

// MeasurementJournalWriter is the Decision Journal's write seam. Its SQL
// implementation (JournalSQLWriter) runs the append and the commit move in one
// transaction; the run-verdict logic above it is tested with a fake.
type MeasurementJournalWriter interface {
	// Write appends v. When commit is true it first clears any committed row on
	// the key, so at most one row is committed (the partial unique index). It
	// returns the new row's id and created_at.
	Write(ctx context.Context, v JournalVerdict, commit bool) (int64, time.Time, error)
	// ReadKey returns every row on (day, ticker), oldest-first (created_at, id).
	ReadKey(ctx context.Context, day time.Time, ticker string) ([]JournalVerdict, error)
}

// LogVerdict appends one Verdict under ADR-0016's conflict rule. It validates
// through the Grader's own normalizeJournalVerdict, so the tool, the table
// CHECKs, and the Grader cannot disagree. A cross-session disagreement is
// appended, never overwritten; supersedes implies committed; committing moves
// the single Committed Verdict for the key. The result surfaces any Verdict
// Conflict the write left unresolved.
func LogVerdict(ctx context.Context, w MeasurementJournalWriter, in LogVerdictInput) (*LogVerdictResult, error) {
	if w == nil {
		return nil, fmt.Errorf("%w: journal writer is not configured", ErrInvalidArgument)
	}

	v := JournalVerdict{
		Session:    strings.TrimSpace(in.Session),
		Date:       in.Date,
		Ticker:     in.Ticker,
		Verdict:    in.Verdict,
		EntryLow:   in.EntryLow,
		EntryHigh:  in.EntryHigh,
		CLLow:      in.CLLow,
		CLHigh:     in.CLHigh,
		TPLow:      in.TPLow,
		TPHigh:     in.TPHigh,
		Rationale:  in.Rationale,
		Supersedes: in.Supersedes,
	}
	if v.Session == "" {
		return nil, fmt.Errorf("%w: session is required (the agent run writing the Verdict)", ErrInvalidArgument)
	}
	if err := normalizeJournalVerdict(&v); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}

	// A deliberate correction is the acted-on Verdict, so supersedes commits
	// (ADR-0016 amendment).
	commit := in.Committed || in.Supersedes != nil
	id, createdAt, err := w.Write(ctx, v, commit)
	if err != nil {
		return nil, err
	}
	v.ID = id
	v.CreatedAt = createdAt
	v.Committed = commit

	day, err := time.Parse("2006-01-02", v.Date)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	rows, err := w.ReadKey(ctx, day, v.Ticker)
	if err != nil {
		return nil, err
	}

	current, shadowed := collapseSessionDuplicates(rows)
	result := &LogVerdictResult{
		ID:         id,
		Session:    v.Session,
		Ticker:     v.Ticker,
		Verdict:    v.Verdict,
		Committed:  commit,
		Superseded: in.Supersedes,
		Shadowed:   shadowed,
	}
	if len(current) > 1 && !journalVerdictsAgree(current) {
		if _, ok := committedVerdict(current); !ok {
			conflict := buildVerdictConflict(current)
			result.Conflict = &conflict
		}
	}
	return result, nil
}
