package usecase

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

// JournalSQLSource reads the Decision Journal from the measurement_journal
// table. It is the concrete MeasurementJournalSource: it scans every row in a
// decision-date window, oldest-first, and returns the raw Verdicts — the
// session/conflict/committed rule is the Grader's, not the source's. A row it
// cannot scan is reported as an issue, never silently dropped.
type JournalSQLSource struct {
	DB *sqlx.DB
}

func NewJournalSQLSource(db *sqlx.DB) *JournalSQLSource {
	return &JournalSQLSource{DB: db}
}

// journalRow is the raw table row before it becomes a JournalVerdict.
type journalRow struct {
	ID          int64     `db:"id"`
	DecisionDay time.Time `db:"decision_day"`
	Ticker      string    `db:"ticker"`
	Verdict     string    `db:"verdict"`
	EntryLow    *float64  `db:"entry_low"`
	EntryHigh   *float64  `db:"entry_high"`
	CLLow       *float64  `db:"cl_low"`
	CLHigh      *float64  `db:"cl_high"`
	TPLow       *float64  `db:"tp_low"`
	TPHigh      *float64  `db:"tp_high"`
	Rationale   string    `db:"rationale"`
	Session     string    `db:"session"`
	Supersedes  *int64    `db:"supersedes"`
	Committed   bool      `db:"committed"`
	CreatedAt   time.Time `db:"created_at"`
}

// journalSelect reads the whole window. A nil bound is open at that end; the
// cast makes an untyped NULL comparable to the DATE column. Created_at then id
// is insert order, which is what same-session last-wins reads.
const journalSelect = `
	SELECT id, decision_day, ticker, verdict,
	       entry_low, entry_high, cl_low, cl_high, tp_low, tp_high,
	       rationale, session, supersedes, committed, created_at
	FROM measurement_journal
	WHERE ($1::date IS NULL OR decision_day >= $1::date)
	  AND ($2::date IS NULL OR decision_day <= $2::date)
	ORDER BY created_at, id`

// ReadVerdicts returns every Verdict row in the inclusive decision-date window,
// oldest-first. A nil bound is open at that end; with no bounds the whole
// journal is read. A read failure propagates (a database error must never look
// like an empty journal); a row that cannot be scanned becomes an issue.
func (s *JournalSQLSource) ReadVerdicts(from, to *time.Time) ([]JournalVerdict, []JournalIssue, error) {
	if s.DB == nil {
		return nil, nil, fmt.Errorf("journal source: no database")
	}

	rows, err := s.DB.Queryx(journalSelect, from, to)
	if err != nil {
		return nil, nil, fmt.Errorf("read measurement_journal: %w", err)
	}
	defer rows.Close()

	var verdicts []JournalVerdict
	var issues []JournalIssue
	for rows.Next() {
		var row journalRow
		if err := rows.StructScan(&row); err != nil {
			issues = append(issues, JournalIssue{
				Ref:    "measurement_journal",
				Reason: "unreadable row: " + err.Error(),
			})
			continue
		}
		verdicts = append(verdicts, row.verdict())
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read measurement_journal: %w", err)
	}
	return verdicts, issues, nil
}

// verdict maps the raw row onto the Grader's Verdict shape, canonicalizing the
// decision day to YYYY-MM-DD.
func (r journalRow) verdict() JournalVerdict {
	return JournalVerdict{
		Date:       r.DecisionDay.Format("2006-01-02"),
		Ticker:     r.Ticker,
		Verdict:    r.Verdict,
		EntryLow:   r.EntryLow,
		EntryHigh:  r.EntryHigh,
		CLLow:      r.CLLow,
		CLHigh:     r.CLHigh,
		TPLow:      r.TPLow,
		TPHigh:     r.TPHigh,
		Rationale:  r.Rationale,
		ID:         r.ID,
		Session:    r.Session,
		Supersedes: r.Supersedes,
		Committed:  r.Committed,
		CreatedAt:  r.CreatedAt,
	}
}
