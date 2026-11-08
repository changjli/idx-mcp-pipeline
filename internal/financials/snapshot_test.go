package financials

import (
	"testing"
	"time"
)

func date(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func i64(v int64) *int64     { return &v }
func f64(v float64) *float64 { return &v }

// TestValidateRow_PeriodGuard walks the worked examples from the endpoint
// findings: the guard is per-issuer, so the fiscalYearEnd decides which month
// ends are real periods.
func TestValidateRow_PeriodGuard(t *testing.T) {
	snapshotEnd := MonthEnd(2026, time.October)

	tests := []struct {
		name    string
		row     SnapshotRow
		wantOK  bool
		wantDur int
		wantFY  time.Time
		wantWhy string
	}{
		{
			name:    "Dec-FY Q3 is 9M",
			row:     SnapshotRow{Ticker: "AADI", FsDate: "2026-09-30", FiscalYearEnd: "Dec"},
			wantOK:  true,
			wantDur: 9,
			wantFY:  date(2026, time.December, 31),
		},
		{
			name:    "Jan-FY July is a legitimate 6M period",
			row:     SnapshotRow{Ticker: "AMIN", FsDate: "2026-07-31", FiscalYearEnd: "Jan"},
			wantOK:  true,
			wantDur: 6,
			wantFY:  date(2027, time.January, 31),
		},
		{
			name:    "Dec-FY October is 10M and rejected",
			row:     SnapshotRow{Ticker: "ASII", FsDate: "2025-10-31", FiscalYearEnd: "Dec"},
			wantWhy: "duration 10 months is not one of 3/6/9/12",
		},
		{
			name:    "Dec-FY July is 7M and rejected",
			row:     SnapshotRow{Ticker: "INKP", FsDate: "2025-07-31", FiscalYearEnd: "Dec"},
			wantWhy: "duration 7 months is not one of 3/6/9/12",
		},
		{
			name:    "not a month end",
			row:     SnapshotRow{Ticker: "MDKA", FsDate: "2025-09-25", FiscalYearEnd: "Dec"},
			wantWhy: "fsDate 2025-09-25 is not a month end",
		},
		{
			name:    "period after the snapshot month",
			row:     SnapshotRow{Ticker: "TEST", FsDate: "2026-11-30", FiscalYearEnd: "Dec"},
			wantWhy: "period end 2026-11-30 is after the snapshot month end 2026-10-31",
		},
		{
			name:    "Mar-FY fiscal year end is 12M",
			row:     SnapshotRow{Ticker: "HEXA", FsDate: "2026-03-31", FiscalYearEnd: "Mar"},
			wantOK:  true,
			wantDur: 12,
			wantFY:  date(2026, time.March, 31),
		},
		{
			name:    "Jun-FY fiscal year end is 12M",
			row:     SnapshotRow{Ticker: "CANI", FsDate: "2026-06-30", FiscalYearEnd: "Jun"},
			wantOK:  true,
			wantDur: 12,
			wantFY:  date(2026, time.June, 30),
		},
		{
			name:    "missing fiscalYearEnd defaults to December",
			row:     SnapshotRow{Ticker: "TEST", FsDate: "2026-09-30", FiscalYearEnd: "-"},
			wantOK:  true,
			wantDur: 9,
			wantFY:  date(2026, time.December, 31),
		},
		{
			name:    "unrecognised fiscalYearEnd is rejected",
			row:     SnapshotRow{Ticker: "TEST", FsDate: "2026-09-30", FiscalYearEnd: "Xyz"},
			wantWhy: `unrecognised fiscalYearEnd "Xyz"`,
		},
		{
			name:    "empty fsDate is rejected",
			row:     SnapshotRow{Ticker: "TEST", FsDate: "", FiscalYearEnd: "Dec"},
			wantWhy: `invalid fsDate ""`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, reason := validateRow(tc.row, snapshotEnd)
			if tc.wantOK {
				if reason != "" {
					t.Fatalf("rejected: %s", reason)
				}
				if st.DurationMonths != tc.wantDur {
					t.Errorf("duration = %d, want %d", st.DurationMonths, tc.wantDur)
				}
				if !st.FiscalYearEnd.Equal(tc.wantFY) {
					t.Errorf("fiscal year end = %s, want %s", st.FiscalYearEnd.Format("2006-01-02"), tc.wantFY.Format("2006-01-02"))
				}
				if st.Source != SourceRatioSnapshot || st.ValueBasis != ValueBasisIDX {
					t.Errorf("provenance = %q/%q, want the Tier-1 constants", st.Source, st.ValueBasis)
				}
				if !st.SourceFSDate.Equal(st.PeriodEnd) {
					t.Errorf("source_fs_date = %s, want %s", st.SourceFSDate, st.PeriodEnd)
				}
				return
			}
			if reason != tc.wantWhy {
				t.Errorf("reason = %q, want %q", reason, tc.wantWhy)
			}
		})
	}
}

func TestValidateSnapshot_SnapshotGuard(t *testing.T) {
	month := date(2026, time.October, 1)

	valid := func(code string) SnapshotRow {
		return SnapshotRow{Ticker: code, FsDate: "2026-09-30", FiscalYearEnd: "Dec"}
	}

	t.Run("empty is not an error", func(t *testing.T) {
		snap := ValidateSnapshot(nil, month, "hash")
		if snap.Status != StatusEmpty {
			t.Errorf("status = %q, want empty", snap.Status)
		}
		if snap.QuarterEndShare != nil {
			t.Errorf("quarter_end_share = %v, want nil for an empty month", *snap.QuarterEndShare)
		}
		if len(snap.Statements) != 0 {
			t.Errorf("statements = %d, want 0", len(snap.Statements))
		}
	})

	t.Run("90 percent is not suspect and the bad row is rejected", func(t *testing.T) {
		rows := make([]SnapshotRow, 0, 10)
		for i := 0; i < 9; i++ {
			rows = append(rows, valid(string(rune('A'+i))+"AAA"))
		}
		rows = append(rows, SnapshotRow{Ticker: "BADD", FsDate: "2025-09-25", FiscalYearEnd: "Dec"})
		snap := ValidateSnapshot(rows, month, "hash")
		if snap.Status != StatusOK {
			t.Fatalf("status = %q, want ok (share %.2f)", snap.Status, *snap.QuarterEndShare)
		}
		if snap.QuarterEndShare == nil || *snap.QuarterEndShare < 0.9 {
			t.Fatalf("quarter_end_share = %v, want >= 0.9", snap.QuarterEndShare)
		}
		if len(snap.Statements) != 9 {
			t.Errorf("statements = %d, want 9", len(snap.Statements))
		}
		if len(snap.Rejected) != 1 || snap.Rejected[0].Ticker != "BADD" {
			t.Errorf("rejected = %+v, want the not-month-end row", snap.Rejected)
		}
	})

	t.Run("below 90 percent is suspect and derives nothing", func(t *testing.T) {
		rows := make([]SnapshotRow, 0, 10)
		for i := 0; i < 8; i++ {
			rows = append(rows, valid(string(rune('A'+i))+"AAA"))
		}
		rows = append(rows, SnapshotRow{Ticker: "X1", FsDate: "2025-10-31", FiscalYearEnd: "Dec"})
		rows = append(rows, SnapshotRow{Ticker: "X2", FsDate: "2025-09-25", FiscalYearEnd: "Dec"})
		snap := ValidateSnapshot(rows, month, "hash")
		if snap.Status != StatusSuspect {
			t.Fatalf("status = %q, want suspect", snap.Status)
		}
		if len(snap.Statements) != 0 {
			t.Errorf("statements = %d, want 0 for a suspect snapshot", len(snap.Statements))
		}
	})

	t.Run("2025-12 filing dates are 0 percent and quarantine the month", func(t *testing.T) {
		rows := []SnapshotRow{
			{Ticker: "ASII", FsDate: "2025-10-31", FiscalYearEnd: "Dec"},
			{Ticker: "INKP", FsDate: "2025-07-31", FiscalYearEnd: "Dec"},
			{Ticker: "MDKA", FsDate: "2025-09-25", FiscalYearEnd: "Dec"},
		}
		snap := ValidateSnapshot(rows, date(2025, time.December, 1), "hash")
		if snap.Status != StatusSuspect {
			t.Fatalf("status = %q, want suspect", snap.Status)
		}
		if snap.QuarterEndShare == nil || *snap.QuarterEndShare != 0 {
			t.Errorf("quarter-end share = %v, want 0", snap.QuarterEndShare)
		}
		if len(snap.Statements) != 0 {
			t.Errorf("statements = %d, want 0", len(snap.Statements))
		}
	})

	t.Run("distinct periods and defaults are counted", func(t *testing.T) {
		rows := []SnapshotRow{
			valid("AAAA"),
			valid("BBBB"),
			{Ticker: "CCCC", FsDate: "2026-03-31", FiscalYearEnd: "-"}, // same 3M period, defaulted
		}
		snap := ValidateSnapshot(rows, month, "hash")
		if snap.DistinctPeriods != 2 {
			t.Errorf("distinct periods = %d, want 2", snap.DistinctPeriods)
		}
		if snap.DefaultedFiscalYearEnd != 1 {
			t.Errorf("defaulted = %d, want 1", snap.DefaultedFiscalYearEnd)
		}
	})
}

func TestRupiahFromBillions(t *testing.T) {
	tests := []struct {
		in   *float64
		want *int64
	}{
		{nil, nil},
		{f64(2352380.59), i64(2352380590000000)},
		{f64(27893.36), i64(27893360000000)},
		{f64(-33.41), i64(-33410000000)},
		{f64(0), i64(0)},
	}
	for _, tc := range tests {
		got := RupiahFromBillions(tc.in)
		if tc.want == nil {
			if got != nil {
				t.Errorf("got %d, want nil", *got)
			}
			continue
		}
		if got == nil || *got != *tc.want {
			t.Errorf("got %v, want %d", got, *tc.want)
		}
	}
}

func TestNormalizeAudit(t *testing.T) {
	cases := []struct {
		in   string
		want *string
	}{
		{"U", strPtr("U")},
		{"u", strPtr("U")},
		{" A ", strPtr("A")},
		{"LR", strPtr("LR")},
		{"lr", strPtr("LR")},
		{"", nil},
		{"-", nil},
		{"0", nil},
		{"(nul)", nil},
	}
	for _, tc := range cases {
		got := NormalizeAudit(tc.in)
		if !samePtr(got, tc.want) {
			t.Errorf("NormalizeAudit(%q) = %v, want %v", tc.in, deref(got), deref(tc.want))
		}
	}
}

func TestNormalizeOpinion(t *testing.T) {
	cases := []struct {
		in   string
		want *string
	}{
		{"WTM", strPtr("WTM")},
		{"wtp", strPtr("WTM")}, // old wording of unqualified
		{"WDP", strPtr("WDP")},
		{"TW", strPtr("TW")},
		{"TMP", strPtr("TMP")},
		{"Wajar Dengan Pengecualian / Qualified", strPtr("WDP")},
		{"Wajar Tanpa Modifikasian", strPtr("WTM")},
		{"Unqualified", strPtr("WTM")},
		{"Qualified", strPtr("WDP")},
		{"Tidak Wajar", strPtr("TW")},
		{"Tidak Memberikan Pendapat", strPtr("TMP")},
		{"-", nil},
		{"", nil},
		{"0.0", nil},
		{"(nul)", nil},
		{"A", nil},
	}
	for _, tc := range cases {
		got := NormalizeOpinion(tc.in)
		if !samePtr(got, tc.want) {
			t.Errorf("NormalizeOpinion(%q) = %v, want %v", tc.in, deref(got), deref(tc.want))
		}
	}
}

func TestStatementContentHash(t *testing.T) {
	base := Statement{
		Ticker:         "AADI",
		PeriodEnd:      date(2026, time.June, 30),
		FiscalYearEnd:  date(2026, time.December, 31),
		DurationMonths: 6,
		Assets:         i64(113148130000000),
		Equity:         i64(70419660000000),
		EPS:            f64(1833.76),
		AuditStatus:    strPtr("LR"),
	}

	// Identical values hash identically.
	if base.ContentHash() != base.ContentHash() {
		t.Fatal("content hash is not deterministic")
	}

	changedValue := base
	changedValue.Assets = i64(1)
	if changedValue.ContentHash() == base.ContentHash() {
		t.Error("a fundamental change must change the hash")
	}

	changedAssurance := base
	changedAssurance.AuditStatus = strPtr("A")
	if changedAssurance.ContentHash() == base.ContentHash() {
		t.Error("an assurance change must change the hash")
	}

	// nil and zero are distinct, never conflated.
	zero := base
	zero.Assets = i64(0)
	if zero.ContentHash() == base.ContentHash() {
		t.Error("nil and zero must hash differently")
	}
}

func TestPeriodLabel(t *testing.T) {
	st := Statement{PeriodEnd: date(2026, time.September, 30), DurationMonths: 9}
	if got := st.PeriodLabel(); got != "9M 2026" {
		t.Errorf("label = %q, want 9M 2026", got)
	}
}

func samePtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
