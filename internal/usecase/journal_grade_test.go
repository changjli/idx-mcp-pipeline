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
	return anchor, measurementCalendar(anchor, 25)
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

func journalSetup(date, ticker string, entry, tp, sl float64) JournalVerdict {
	return JournalVerdict{
		Date: date, Ticker: ticker, Verdict: verdictSetup,
		Entry: f64p(entry), TP: f64p(tp), SL: f64p(sl), Rationale: "test setup",
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

// TestGradeJournal_ResolutionOrder — the four outcomes of a Tradable Verdict:
// TP touched first is a win, SL touched first is a loss, both touched the same
// day is a loss (the conservative tie), and neither touched within the window is
// expired. A verdict without a full 20 post-decision trading days is unresolved.
func TestGradeJournal_ResolutionOrder(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{verdicts: []JournalVerdict{
		journalSetup(date, "AAA", 100, 110, 95),                          // day1 High 111 -> TP first: win
		journalSetup(date, "BBB", 100, 120, 90),                          // day1 Low 89   -> SL first: loss
		journalSetup(date, "CCC", 100, 110, 95),                          // day1 High 111 & Low 94 -> tie: loss
		journalSetup(date, "DDD", 100, 130, 80),                          // never touched: expired
		journalSetup(days[10].Format("2006-01-02"), "EEE", 100, 130, 80), // 15 post days: unresolved
	}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 111, 99)},
		"BBB": {journalBar("BBB", days[1], 105, 89)},
		"CCC": {journalBar("CCC", days[1], 111, 94)},
		"DDD": {journalBar("DDD", days[1], 105, 96)},
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
	assertOutcome("AAA", outcomeWin, f64p(0.10))
	assertOutcome("BBB", outcomeLoss, f64p(-0.10))
	assertOutcome("CCC", outcomeLoss, f64p(-0.05)) // tie resolves at SL, not TP
	assertOutcome("DDD", outcomeExpired, nil)
	assertOutcome("EEE", outcomeUnresolved, nil)

	if report.Counts.Tradable != 5 || report.Counts.Logged != 5 {
		t.Fatalf("tradable/logged = %d/%d, want 5/5", report.Counts.Tradable, report.Counts.Logged)
	}
	if report.Summary.Wins != 1 || report.Summary.Losses != 2 ||
		report.Summary.Expired != 1 || report.Summary.Unresolved != 1 {
		t.Fatalf("summary split = %+v", report.Summary)
	}
	if report.Summary.WinRate == nil || !almostEqual(*report.Summary.WinRate, 1.0/3.0) {
		t.Fatalf("win rate = %v, want 1/3", report.Summary.WinRate)
	}
	if report.Summary.MeanReturn == nil || !almostEqual(*report.Summary.MeanReturn, (0.10-0.10-0.05)/3) {
		t.Fatalf("mean return = %v", report.Summary.MeanReturn)
	}
	if report.Summary.MedianReturn == nil || !almostEqual(*report.Summary.MedianReturn, -0.05) {
		t.Fatalf("median return = %v, want -0.05", report.Summary.MedianReturn)
	}
	if report.Kind != journalGradeKind || report.GitRevision != "testrev" {
		t.Fatalf("kind/revision = %q/%q", report.Kind, report.GitRevision)
	}
	if report.ResolutionDays != journalResolutionDays {
		t.Fatalf("resolution days = %d, want %d", report.ResolutionDays, journalResolutionDays)
	}
}

// TestGradeJournal_NoTradeCalibration — WATCH and PASS lines are no-trade: they
// carry no outcome, are never resolved, and feed Calibration.
func TestGradeJournal_NoTradeCalibration(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{verdicts: []JournalVerdict{
		journalSetup(date, "AAA", 100, 110, 95),
		{Date: date, Ticker: "BBB", Verdict: verdictWatch, Rationale: "wait"},
		{Date: date, Ticker: "CCC", Verdict: verdictPass, Rationale: "no setup"},
	}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 111, 99)},
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

// TestGradeJournal_PlannedRRAndBreakeven — the planned reward-to-risk is read
// per verdict, the median drives the Breakeven Win Rate, and a plan below the
// 1:2 gate is flagged, never skipped.
func TestGradeJournal_PlannedRRAndBreakeven(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{verdicts: []JournalVerdict{
		journalSetup(date, "AAA", 100, 120, 90), // R:R 2.0
		journalSetup(date, "BBB", 100, 110, 80), // R:R 0.5, below the gate
	}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 125, 99)},
		"BBB": {journalBar("BBB", days[1], 115, 79)},
	}}

	report, err := newJournalGradeEngine(source, prices).GradeJournal(context.Background(), JournalGradeOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Summary.MedianPlannedRR == nil || !almostEqual(*report.Summary.MedianPlannedRR, 1.25) {
		t.Fatalf("median planned R:R = %v, want 1.25", report.Summary.MedianPlannedRR)
	}
	if report.Summary.BreakevenWinRate == nil || !almostEqual(*report.Summary.BreakevenWinRate, 1.0/2.25) {
		t.Fatalf("breakeven = %v, want 1/2.25", report.Summary.BreakevenWinRate)
	}
	if !hasFlagPrefix(report.Flags, "discipline:") {
		t.Fatalf("expected a discipline flag for the sub-2 R:R plan, got %v", report.Flags)
	}
}

// TestGradeJournal_DedupeLastWins — a repeated (date, ticker) keeps the later
// line; the shadowed line is reported, not graded.
func TestGradeJournal_DedupeLastWins(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	first := journalSetup(date, "AAA", 100, 110, 95)
	first.SourceFile = "measurement/journal/2026-06.jsonl"
	first.SourceLine = 1
	second := journalSetup(date, "AAA", 100, 105, 95) // tighter TP supersedes
	second.SourceFile = "measurement/journal/2026-06.jsonl"
	second.SourceLine = 2
	source := &fakeJournalSource{verdicts: []JournalVerdict{first, second}}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 107, 99)},
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
	if row.Outcome != outcomeWin || row.TP == nil || !almostEqual(*row.TP, 105) {
		t.Fatalf("last-wins row = %+v, want the superseding TP=105 to win", row)
	}
}

// TestGradeJournal_MalformedReportedNotGraded — issues from the reader surface
// in the report and are counted, never graded.
func TestGradeJournal_MalformedReportedNotGraded(t *testing.T) {
	anchor, days := journalAnchorAndCalendar()
	date := anchor.Format("2006-01-02")
	source := &fakeJournalSource{
		verdicts: []JournalVerdict{journalSetup(date, "AAA", 100, 110, 95)},
		issues:   []JournalIssue{{File: "measurement/journal/2026-06.jsonl", Line: 2, Reason: "invalid JSON"}},
	}
	prices := &fakeJournalPrices{calendar: days, series: map[string][]entity.DailyPrice{
		"AAA": {journalBar("AAA", days[1], 111, 99)},
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
