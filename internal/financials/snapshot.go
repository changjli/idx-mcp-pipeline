package financials

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// SuspectQuarterEndShareFloor is the snapshot-level guard: a month whose share
// of rows with a calendar quarter-end fsDate falls below 90% is recorded as
// suspect and derives no statements. Normal months are >= 98.8%; the corrupt
// 2025-12 snapshot is 0%. The floor exists because that snapshot stamps filing
// dates instead of period ends, so a naive ingest would create phantom periods.
const SuspectQuarterEndShareFloor = 0.90

// SnapshotRow is one company's row from a monthly ratio snapshot, after the
// wire layer converted billions to exact rupiah. FsDate and FiscalYearEnd stay
// as wire strings because the period guard owns their interpretation and its
// rejection reasons are part of the ingest log.
type SnapshotRow struct {
	Ticker        string
	FsDate        string
	FiscalYearEnd string

	Assets          *int64
	Liabilities     *int64
	Equity          *int64
	Revenue         *int64
	EBT             *int64
	NetProfit       *int64
	NetProfitParent *int64

	EPS       *float64
	BookValue *float64
	ROA       *float64
	ROE       *float64
	NPM       *float64
	DER       *float64

	Audit   string
	Opinion string
}

// Rejection is one row the period guard refused. Rejected rows are counted and
// logged, never written.
type Rejection struct {
	Ticker string
	Reason string
}

// Snapshot is one month's payload after both guards. Statements is empty for a
// suspect or empty month (nothing is derived from those). QuarterEndShare is
// nil for an empty month, where the share is undefined.
type Snapshot struct {
	Month                  time.Time // first day of the snapshot month
	Status                 string
	RowCount               int
	DistinctPeriods        int
	DefaultedFiscalYearEnd int // rows whose fiscalYearEnd was missing and defaulted to December
	QuarterEndShare        *float64
	ContentHash            string
	Statements             []Statement
	Rejected               []Rejection
}

// RupiahFromBillions converts one snapshot money value (billions of IDR) to
// exact rupiah. The wire value carries 2 decimals, so rounding the scaled
// float to the nearest rupiah recovers the intended integer exactly. A nil
// input stays nil: absent is not zero.
func RupiahFromBillions(v *float64) *int64 {
	if v == nil {
		return nil
	}
	n := int64(math.Round(*v * 1e9))
	return &n
}

// ValidateSnapshot applies the snapshot-level then the row-level guard to one
// month's rows. payloadHash is stored on the run ledger; it is provenance, not
// part of a statement's content hash.
func ValidateSnapshot(rows []SnapshotRow, month time.Time, payloadHash string) *Snapshot {
	month = FirstOfMonth(month)
	snap := &Snapshot{
		Month:       month,
		RowCount:    len(rows),
		ContentHash: payloadHash,
		Statements:  []Statement{},
		Rejected:    []Rejection{},
	}
	if len(rows) == 0 {
		snap.Status = StatusEmpty
		return snap
	}

	quarterEnds := 0
	for _, r := range rows {
		if d, err := parseRowDate(r.FsDate); err == nil && IsQuarterEnd(d) {
			quarterEnds++
		}
	}
	share := float64(quarterEnds) / float64(len(rows))
	snap.QuarterEndShare = &share
	if share < SuspectQuarterEndShareFloor {
		snap.Status = StatusSuspect
		return snap
	}
	snap.Status = StatusOK

	snapshotEnd := MonthEnd(month.Year(), month.Month())
	periods := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		if _, defaulted, err := ParseFiscalYearEnd(r.FiscalYearEnd); err == nil && defaulted {
			snap.DefaultedFiscalYearEnd++
		}
		st, reason := validateRow(r, snapshotEnd)
		if reason != "" {
			snap.Rejected = append(snap.Rejected, Rejection{Ticker: r.Ticker, Reason: reason})
			continue
		}
		snap.Statements = append(snap.Statements, st)
		periods[st.PeriodEnd.Format("2006-01-02")] = struct{}{}
	}
	snap.DistinctPeriods = len(periods)
	return snap
}

// validateRow applies the row-level guard and derives the fiscal period. It
// returns a reason on rejection, "" on acceptance. The guard is per-issuer:
// fiscalYearEnd decides which month ends are real quarter ends, so a Jan-FY
// issuer's 2026-07-31 is a legitimate 6M period while a Dec-FY issuer's
// 2025-07-31 (7M) is rejected.
func validateRow(r SnapshotRow, snapshotEnd time.Time) (Statement, string) {
	if strings.TrimSpace(r.Ticker) == "" {
		return Statement{}, "empty ticker"
	}
	periodEnd, err := parseRowDate(r.FsDate)
	if err != nil {
		return Statement{}, fmt.Sprintf("invalid fsDate %q", r.FsDate)
	}
	if !IsMonthEnd(periodEnd) {
		return Statement{}, fmt.Sprintf("fsDate %s is not a month end", periodEnd.Format("2006-01-02"))
	}
	fyMonth, _, err := ParseFiscalYearEnd(r.FiscalYearEnd)
	if err != nil {
		return Statement{}, fmt.Sprintf("unrecognised fiscalYearEnd %q", r.FiscalYearEnd)
	}
	fyEnd, duration := fiscalPeriod(periodEnd, fyMonth)
	if periodEnd.After(snapshotEnd) {
		return Statement{}, fmt.Sprintf("period end %s is after the snapshot month end %s",
			periodEnd.Format("2006-01-02"), snapshotEnd.Format("2006-01-02"))
	}
	if duration != 3 && duration != 6 && duration != 9 && duration != 12 {
		return Statement{}, fmt.Sprintf("duration %d months is not one of 3/6/9/12", duration)
	}

	return Statement{
		Ticker:          strings.ToUpper(strings.TrimSpace(r.Ticker)),
		PeriodEnd:       periodEnd,
		FiscalYearEnd:   fyEnd,
		DurationMonths:  duration,
		Assets:          r.Assets,
		Liabilities:     r.Liabilities,
		Equity:          r.Equity,
		Revenue:         r.Revenue,
		EBT:             r.EBT,
		NetProfit:       r.NetProfit,
		NetProfitParent: r.NetProfitParent,
		EPS:             r.EPS,
		BookValue:       r.BookValue,
		ROA:             r.ROA,
		ROE:             r.ROE,
		NPM:             r.NPM,
		DER:             r.DER,
		AuditStatus:     NormalizeAudit(r.Audit),
		Opinion:         NormalizeOpinion(r.Opinion),
		Source:          SourceRatioSnapshot,
		ValueBasis:      ValueBasisIDX,
		SourceFSDate:    periodEnd,
	}, ""
}

// parseRowDate parses a wire YYYY-MM-DD date. Times are UTC midnight, matching
// the DATE columns.
func parseRowDate(s string) (time.Time, error) {
	return time.Parse("2006-01-02", strings.TrimSpace(s))
}

// ParseFiscalYearEnd maps IDX's fiscal-year-end month abbreviation ("Dec",
// "Jun", "Mar", "Jan") to a month. "-" and an empty value default to December
// (13 such rows across all snapshots); defaulted reports that fallback so the
// ingest can flag it. Any other value is an error rather than a silent guess.
func ParseFiscalYearEnd(s string) (month time.Month, defaulted bool, err error) {
	v := strings.TrimSpace(s)
	if v == "" || v == "-" {
		return time.December, true, nil
	}
	if len(v) < 3 {
		return 0, false, fmt.Errorf("invalid fiscal year end %q", s)
	}
	switch strings.ToLower(v[:3]) {
	case "jan":
		return time.January, false, nil
	case "feb":
		return time.February, false, nil
	case "mar":
		return time.March, false, nil
	case "apr":
		return time.April, false, nil
	case "may":
		return time.May, false, nil
	case "jun":
		return time.June, false, nil
	case "jul":
		return time.July, false, nil
	case "aug":
		return time.August, false, nil
	case "sep":
		return time.September, false, nil
	case "oct":
		return time.October, false, nil
	case "nov":
		return time.November, false, nil
	case "dec":
		return time.December, false, nil
	}
	return 0, false, fmt.Errorf("invalid fiscal year end %q", s)
}

// fiscalPeriod returns the fiscal year end the period accumulates to and its
// cumulative duration in months. Both values are derived, never trusted from
// the payload.
func fiscalPeriod(periodEnd time.Time, fyMonth time.Month) (time.Time, int) {
	fyEnd := MonthEnd(periodEnd.Year(), fyMonth)
	if fyEnd.Before(periodEnd) {
		fyEnd = MonthEnd(periodEnd.Year()+1, fyMonth)
	}
	start := fyEnd.AddDate(-1, 0, 1) // day after the prior fiscal year end
	months := (periodEnd.Year()-start.Year())*12 + int(periodEnd.Month()-start.Month()) + 1
	return fyEnd, months
}

// FirstOfMonth truncates a date to the first day of its month (UTC midnight).
func FirstOfMonth(d time.Time) time.Time {
	return time.Date(d.Year(), d.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// MonthEnd returns the last day of the month at UTC midnight.
func MonthEnd(year int, month time.Month) time.Time {
	return time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
}

// IsMonthEnd reports whether d is the last calendar day of its month.
func IsMonthEnd(d time.Time) bool {
	return d.Day() == MonthEnd(d.Year(), d.Month()).Day()
}

// IsQuarterEnd reports whether d is the last day of a calendar quarter. This
// is the snapshot-level guard's test; the row-level guard uses the issuer's
// fiscal calendar instead.
func IsQuarterEnd(d time.Time) bool {
	return IsMonthEnd(d) && d.Month()%3 == 0
}

// NormalizeAudit maps IDX's dirty audit encoding to U, A or LR. Anything else
// (null, "0", "-", unknown) is nil: the schema's CHECK is U | A | LR | NULL.
func NormalizeAudit(s string) *string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "U":
		return strPtr("U")
	case "A":
		return strPtr("A")
	case "LR":
		return strPtr("LR")
	}
	return nil
}

// NormalizeOpinion maps IDX's dirty auditor-opinion encodings to the four
// canonical codes. The source mixes abbreviations, English text, Indonesian
// text, "-", "(nul)", "0.0" and "A"; anything unrecognised is nil (none).
// WDP is checked before WTM because the qualified phrase contains the
// unqualified phrase as a prefix.
func NormalizeOpinion(s string) *string {
	v := strings.ToLower(strings.TrimSpace(s))
	switch v {
	case "wtm", "wtp": // WTP is the old wording of unqualified
		return strPtr("WTM")
	case "wdp":
		return strPtr("WDP")
	case "tw":
		return strPtr("TW")
	case "tmp":
		return strPtr("TMP")
	}
	switch {
	case strings.Contains(v, "wajar dengan pengecualian"):
		return strPtr("WDP")
	case strings.Contains(v, "tanpa modifikasian"), strings.Contains(v, "unqualified"):
		return strPtr("WTM")
	case strings.Contains(v, "tidak wajar"), strings.Contains(v, "adverse"):
		return strPtr("TW")
	case strings.Contains(v, "tidak memberikan pendapat"), strings.Contains(v, "disclaimer"):
		return strPtr("TMP")
	case strings.Contains(v, "qualified"):
		return strPtr("WDP")
	}
	return nil
}

func strPtr(s string) *string { return &s }
