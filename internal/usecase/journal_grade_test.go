package usecase

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// ─── Journal Grade fakes ─────────────────────────────────────────────────────

type fakeJournalSource struct {
	verdicts []JournalVerdict
	issues   []JournalIssue
	err      error
	gotFrom  *time.Time
	gotTo    *time.Time
}

func (f *fakeJournalSource) ReadVerdicts(from, to *time.Time) ([]JournalVerdict, []JournalIssue, error) {
	f.gotFrom, f.gotTo = from, to
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.verdicts, f.issues, nil
}

type fakeJournalPrices struct {
	calendar []time.Time
	series   map[string][]entity.DailyPrice
}

func (f *fakeJournalPrices) TradingDaysInRangeAll(_ *sqlx.DB, _, _ time.Time) ([]time.Time, error) {
	return f.calendar, nil
}

func (f *fakeJournalPrices) FindByTickerAndDateRange(_ *sqlx.DB, ticker string, _, _ time.Time) ([]entity.DailyPrice, error) {
	return f.series[ticker], nil
}

func newJournalGradeEngine(journal MeasurementJournalSource, prices JournalPriceSource) *MeasurementEngine {
	engine := NewMeasurementEngine(nil, logrus.New(), nil, nil, nil, "testrev")
	engine.Journal = journal
	engine.JournalPrices = prices
	return engine
}

// ─── Fixtures ────────────────────────────────────────────────────────────────

func journalAnchorAndCalendar() (time.Time, []time.Time) {
	anchor := measurementAnchor()
	return anchor, measurementCalendar(anchor, 55)
}

// journalBar is one OHLC row with a High/Low an intraday touch reads; the close
// is incidental.
func journalBar(ticker string, day time.Time, high, low float64) entity.DailyPrice {
	return entity.DailyPrice{
		Ticker:     ticker,
		TradingDay: day,
		High:       f64p(high),
		Low:        f64p(low),
		Close:      f64p((high + low) / 2),
	}
}

// journalSetup builds a SETUP with the standard zones (entry 100-105, cut-loss
// 90-95, target 120-125) unless the test passes its own.
func journalSetup(date, ticker string, entryLow, entryHigh, clLow, clHigh, tpLow, tpHigh float64) JournalVerdict {
	return JournalVerdict{
		Date: date, Ticker: ticker, Verdict: verdictSetup,
		EntryLow: f64p(entryLow), EntryHigh: f64p(entryHigh),
		CLLow: f64p(clLow), CLHigh: f64p(clHigh),
		TPLow: f64p(tpLow), TPHigh: f64p(tpHigh), Rationale: "test setup",
	}
}

func findJournalVerdict(t *testing.T, report *JournalGradeReport, ticker string) TickerVerdict {
	t.Helper()
	for _, row := range report.Verdicts {
		if row.Ticker == ticker {
			return row
		}
	}
	t.Fatalf("ticker %s not in report: %+v", ticker, report.Verdicts)
	return TickerVerdict{}
}

func almostEqual(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// ─── Resolution tests ────────────────────────────────────────────────────────

// TestGradeJournal_ResolutionOrder — entry-first, then the TP-vs-CL race: a fill
// the entry-first way (day range overlaps the entry zone), TP first is a win, CL
// first is a loss, both the same day is a loss (the conservative tie), a full
// resolution window with neither is expired, and a verdict whose entry window has
// not elapsed is unresolved.
func TestGradeJournal_ResolutionOrder(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")

	expBars := []entity.DailyPrice{journalBar("EXP", days[1], 106, 101)}
	for i := 2; i <= 20; i++ {
		expBars = append(expBars, journalBar("EXP", days[i], 110, 102))
	}

	source := &fakeJournalSource{verdicts: []JournalVerdict{
		journalSetup(date, "WIN", 100, 105, 90, 95, 120, 125),                          // day1 fill, day2 High 121 -> win
		journalSetup(date, "LOSS", 100, 105, 90, 95, 120, 125),                         // day1 fill, day2 Low 89 -> loss
		journalSetup(date, "TIE", 100, 105, 90, 95, 120, 125),                          // day1 fill, day2 High 121 & Low 89 -> tie loss
		journalSetup(date, "EXP", 100, 105, 90, 95, 120, 125),                          // day1 fill, neither ever -> expired
		journalSetup(days[50].Format("2006-01-02"), "UNR", 100, 105, 90, 95, 120, 125), // never fills, window short
	}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"WIN":  {journalBar("WIN", days[1], 106, 101), journalBar("WIN", days[2], 121, 102)},
		"LOSS": {journalBar("LOSS", days[1], 106, 101), journalBar("LOSS", days[2], 108, 89)},
		"TIE":  {journalBar("TIE", days[1], 106, 101), journalBar("TIE", days[2], 121, 89)},
		"EXP":  expBars,
		"UNR":  {journalBar("UNR", days[51], 115, 110)},
	}}

	report, err := newJournalGradeEngine(source, prices).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertOutcome := func(ticker, outcome string, wantReturn *float64) {
		t.Helper()
		row := findJournalVerdict(t, report, ticker)
		if row.Outcome != outcome {
			t.Fatalf("%s outcome = %q, want %q", ticker, row.Outcome, outcome)
		}
		switch {
		case wantReturn == nil && row.Return != nil:
			t.Fatalf("%s return = %v, want nil", ticker, *row.Return)
		case wantReturn != nil && row.Return == nil:
			t.Fatalf("%s return = nil, want %v", ticker, *wantReturn)
		case wantReturn != nil && !almostEqual(*row.Return, *wantReturn):
			t.Fatalf("%s return = %v, want %v", ticker, *row.Return, *wantReturn)
		}
	}
	// Booking is conservative: fill at entryHigh (105), win at tpLow (120), loss at clHigh (95).
	assertOutcome("WIN", outcomeWin, f64p(120.0/105.0-1))
	assertOutcome("LOSS", outcomeLoss, f64p(95.0/105.0-1))
	assertOutcome("TIE", outcomeLoss, f64p(95.0/105.0-1)) // tie resolves at CL, not TP
	assertOutcome("EXP", outcomeExpired, nil)
	assertOutcome("UNR", outcomeUnresolved, nil)

	if report.Counts.Tradable != 5 || report.Counts.Logged != 5 {
		t.Fatalf("tradable/logged = %d/%d, want 5/5", report.Counts.Tradable, report.Counts.Logged)
	}
	if report.Summary.Wins != 1 || report.Summary.Losses != 2 ||
		report.Summary.Expired != 1 || report.Summary.Unresolved != 1 || report.Summary.Unfilled != 0 {
		t.Fatalf("summary split = %+v", report.Summary)
	}
	if report.Summary.WinRate == nil || !almostEqual(*report.Summary.WinRate, 1.0/3.0) {
		t.Fatalf("win rate = %v, want 1/3", report.Summary.WinRate)
	}
	// Planned R:R = (tpLow - entryHigh) / (entryHigh - clHigh) = 15/10 = 1.5.
	if report.Summary.MedianPlannedRR == nil || !almostEqual(*report.Summary.MedianPlannedRR, 1.5) {
		t.Fatalf("median planned R:R = %v, want 1.5", report.Summary.MedianPlannedRR)
	}
	if report.Summary.BreakevenWinRate == nil || !almostEqual(*report.Summary.BreakevenWinRate, 1.0/2.5) {
		t.Fatalf("breakeven = %v, want 1/2.5", report.Summary.BreakevenWinRate)
	}
	if report.Kind != journalGradeKind || report.GitRevision != "testrev" ||
		report.EntryWindowDays != journalEntryWindowDays || report.ResolutionDays != journalResolutionDays {
		t.Fatalf("report meta = %+v", report)
	}
	if !hasFlagPrefix(report.Flags, "discipline:") {
		t.Fatalf("expected a discipline flag (all R:R 1.5 < 2), got %v", report.Flags)
	}
}

// TestGradeJournal_UnfilledShadows — a verdict whose entry zone is never tapped
// is unfilled, never a win or loss, and carries a shadow read of what the thesis
// would have done over the entry window.
func TestGradeJournal_UnfilledShadows(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{verdicts: []JournalVerdict{
		journalSetup(date, "MISS", 100, 105, 90, 95, 120, 125),  // gaps above the entry zone, High 130 -> missed_tp
		journalSetup(date, "DODGE", 100, 105, 90, 95, 120, 125), // stays below the entry zone, Low 85 -> dodged_cl
		journalSetup(date, "FLAT", 100, 105, 90, 95, 120, 125),  // above the entry zone, never target/stop -> flat
	}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"MISS":  {journalBar("MISS", days[1], 130, 126)},
		"DODGE": {journalBar("DODGE", days[1], 99, 85)},
		"FLAT":  {journalBar("FLAT", days[1], 115, 110)},
	}}

	report, err := newJournalGradeEngine(source, prices).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Summary.Unfilled != 3 || report.Summary.UnfilledMissedTP != 1 ||
		report.Summary.UnfilledDodgedCL != 1 || report.Summary.UnfilledFlat != 1 {
		t.Fatalf("unfilled summary = %+v", report.Summary)
	}
	if report.Summary.Wins != 0 || report.Summary.Losses != 0 || report.Summary.WinRate != nil {
		t.Fatalf("unfilled must not touch the win rate: %+v", report.Summary)
	}
	for ticker, wantShadow := range map[string]string{
		"MISS": shadowMissedTP, "DODGE": shadowDodgedCL, "FLAT": shadowFlat,
	} {
		row := findJournalVerdict(t, report, ticker)
		if row.Outcome != outcomeUnfilled || row.Shadow != wantShadow || row.Return != nil {
			t.Fatalf("%s row = %+v, want unfilled/%s with no return", ticker, row, wantShadow)
		}
	}
}

// TestGradeJournal_TwoWindows — the resolution clock starts at the fill, not the
// decision day: a verdict that fills on day 18 and wins on day 24 is still a win
// even though day 24 is past the decision-day + 20 boundary.
func TestGradeJournal_TwoWindows(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")

	bars := make([]entity.DailyPrice, 0, 20)
	for i := 1; i <= 17; i++ { // above the entry zone: no fill
		bars = append(bars, journalBar("LATE", days[i], 115, 110))
	}
	bars = append(bars, journalBar("LATE", days[18], 106, 101)) // fill day
	bars = append(bars, journalBar("LATE", days[24], 121, 102)) // win, 6 trading days after the fill

	source := &fakeJournalSource{verdicts: []JournalVerdict{
		journalSetup(date, "LATE", 100, 105, 90, 95, 120, 125),
	}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{"LATE": bars}}

	report, err := newJournalGradeEngine(source, prices).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	row := findJournalVerdict(t, report, "LATE")
	if row.Outcome != outcomeWin {
		t.Fatalf("outcome = %q, want win (resolution clock starts at the fill)", row.Outcome)
	}
	if want := days[24].Format("2006-01-02"); row.ResolvedOn != want {
		t.Fatalf("resolved on = %q, want %q", row.ResolvedOn, want)
	}
}

// TestGradeJournal_PointsAreDegenerateZones — a point is a zone with low == high,
// so the old single-price plans still work.
func TestGradeJournal_PointsAreDegenerateZones(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{verdicts: []JournalVerdict{
		journalSetup(date, "PTP", 100, 100, 90, 90, 120, 120),
	}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"PTP": {journalBar("PTP", days[1], 100, 99), journalBar("PTP", days[2], 120, 100)},
	}}

	report, err := newJournalGradeEngine(source, prices).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	row := findJournalVerdict(t, report, "PTP")
	if row.Outcome != outcomeWin || row.Return == nil || !almostEqual(*row.Return, 120.0/100.0-1) {
		t.Fatalf("point-zone row = %+v, want win at entry 100 / tp 120", row)
	}
}

// TestGradeJournal_NoTradeCalibration — WATCH and PASS lines are no-trade: no
// outcome, never resolved, and they feed Calibration.
func TestGradeJournal_NoTradeCalibration(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{verdicts: []JournalVerdict{
		journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125),
		{Date: date, Ticker: "BBB", Verdict: verdictWatch, Rationale: "wait"},
		{Date: date, Ticker: "CCC", Verdict: verdictPass, Rationale: "no setup"},
	}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 106, 101), journalBar("AAA", days[2], 121, 102)},
	}}

	report, err := newJournalGradeEngine(source, prices).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Counts.Watch != 1 || report.Counts.Pass != 1 || report.Counts.NoTrade != 2 || report.Counts.Tradable != 1 {
		t.Fatalf("counts = %+v", report.Counts)
	}
	if report.Summary.Calibration == nil || !almostEqual(*report.Summary.Calibration, 2.0/3.0) {
		t.Fatalf("calibration = %v, want 2/3", report.Summary.Calibration)
	}
	for _, ticker := range []string{"BBB", "CCC"} {
		row := findJournalVerdict(t, report, ticker)
		if row.Outcome != "" || row.Return != nil {
			t.Fatalf("%s no-trade row = %+v, want empty outcome and nil return", ticker, row)
		}
	}
}

// TestGradeJournal_DedupeLastWins — a repeated (date, ticker) keeps the later
// line; the shadowed line is reported, not graded.
func TestGradeJournal_DedupeLastWins(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	first := journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125) // would win
	first.SourceFile = "measurement/journal/2026-06.jsonl"
	first.SourceLine = 1
	second := journalSetup(date, "AAA", 100, 105, 90, 95, 130, 135) // supersedes: day2 High 121 < 130
	second.SourceFile = "measurement/journal/2026-06.jsonl"
	second.SourceLine = 2
	source := &fakeJournalSource{verdicts: []JournalVerdict{first, second}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 106, 101), journalBar("AAA", days[2], 121, 102)},
	}}

	report, err := newJournalGradeEngine(source, prices).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Counts.Logged != 1 || report.Counts.Duplicates != 1 {
		t.Fatalf("logged/duplicates = %d/%d, want 1/1", report.Counts.Logged, report.Counts.Duplicates)
	}
	if len(report.Duplicates) != 1 || report.Duplicates[0].Line != 1 {
		t.Fatalf("duplicates = %+v, want the first line shadowed", report.Duplicates)
	}
	row := findJournalVerdict(t, report, "AAA")
	if row.Outcome != outcomeExpired || row.TPLow == nil || !almostEqual(*row.TPLow, 130) {
		t.Fatalf("last-wins row = %+v, want the superseding tpLow=130 to win (expired)", row)
	}
}

// TestGradeJournal_MalformedReportedNotGraded — issues from the reader surface
// in the report and are counted, never graded.
func TestGradeJournal_MalformedReportedNotGraded(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{
		verdicts: []JournalVerdict{journalSetup(date, "AAA", 100, 105, 90, 95, 120, 125)},
		issues:   []JournalIssue{{File: "measurement/journal/2026-06.jsonl", Line: 2, Reason: "invalid JSON"}},
	}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 106, 101), journalBar("AAA", days[2], 121, 102)},
	}}

	report, err := newJournalGradeEngine(source, prices).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Counts.Malformed != 1 || len(report.Malformed) != 1 {
		t.Fatalf("malformed = %d/%d, want 1/1", report.Counts.Malformed, len(report.Malformed))
	}
	if report.Counts.Logged != 1 || report.Summary.Wins != 1 {
		t.Fatalf("graded counts = %+v / %+v", report.Counts, report.Summary)
	}
	if !hasFlagPrefix(report.Flags, "malformed:") {
		t.Fatalf("expected a malformed flag, got %v", report.Flags)
	}
}

// TestGradeJournal_Guards — the grader requires its sources and rejects an
// inverted window with the house invalid-argument sentinel.
func TestGradeJournal_Guards(t *testing.T) {
	engine := NewMeasurementEngine(nil, logrus.New(), nil, nil, nil, "testrev")
	if _, err := engine.GradeJournal(context.Background(), JournalGradeOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing sources err = %v, want ErrInvalidArgument", err)
	}

	from := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	_, days := journalAnchorAndCalendar()
	ready := newJournalGradeEngine(&fakeJournalSource{}, &fakeJournalPrices{calendar: days})
	if _, err := ready.GradeJournal(context.Background(), JournalGradeOptions{From: &from, Until: &until}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("inverted window err = %v, want ErrInvalidArgument", err)
	}
}

func hasFlagPrefix(flags []string, prefix string) bool {
	for _, flag := range flags {
		if len(flag) >= len(prefix) && flag[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}
