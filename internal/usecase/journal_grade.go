package usecase

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// The Decision Journal Grader's fixed vocabulary. Every string here is a report
// key or a journal-line value, so changing one changes what a reader (or the
// agent writing the journal) parses; they are listed in CONTEXT.md (Verdict,
// Verdict Outcome, Resolution Window) and fixed by ADR-0014.
const (
	journalGradeKind = "journal_grade"

	// Verdict values (CONTEXT: Verdict). SETUP is the only Tradable Verdict.
	verdictSetup = "SETUP"
	verdictWatch = "WATCH"
	verdictPass  = "PASS"

	// Verdict Outcome values (CONTEXT: Verdict Outcome).
	outcomeWin        = "win"
	outcomeLoss       = "loss"
	outcomeExpired    = "expired"
	outcomeUnresolved = "unresolved"

	// journalResolutionDays is the Resolution Window: the tradable verdict
	// grader's fixed 20-trading-day clock, the harness's longest Forward Return
	// horizon (ADR-0014). Deliberately not a flag.
	journalResolutionDays = maxMeasurementHorizon

	// plannedRRGate is the skill's minimum planned reward-to-risk ratio (1:2).
	// A tradable verdict below it is flagged, never skipped.
	plannedRRGate = 2.0
)

// MeasurementJournalSource is the Journal Grader's read seam: the parsed
// Verdicts in a decision-date window plus the malformed lines found while
// reading. The concrete *JournalFileReader satisfies it.
type MeasurementJournalSource interface {
	ReadVerdicts(from, to *time.Time) ([]JournalVerdict, []JournalIssue, error)
}

// JournalPriceSource is the Journal Grader's price seam: the market-wide trading
// day calendar and one ticker's OHLC rows over a window, enough to resolve a
// verdict's TP/SL on intraday touch. The concrete *repository.DailyPriceRepository
// satisfies it. It is deliberately narrower than MeasurementPriceSource — the
// journal grader replays nothing and reads no Liquidity Baseline (ADR-0014).
type JournalPriceSource interface {
	TradingDaysInRangeAll(db *sqlx.DB, from, to time.Time) ([]time.Time, error)
	FindByTickerAndDateRange(db *sqlx.DB, ticker string, from, to time.Time) ([]entity.DailyPrice, error)
}

// JournalVerdict is one parsed Decision Journal line: a Verdict the agent
// logged at decision time. Entry/TP/SL are present only on a SETUP; the source
// fields are provenance for the malformed/duplicate report, never journal data.
type JournalVerdict struct {
	Date      string   `json:"date"`
	Ticker    string   `json:"ticker"`
	Verdict   string   `json:"verdict"`
	Entry     *float64 `json:"entry,omitempty"`
	TP        *float64 `json:"tp,omitempty"`
	SL        *float64 `json:"sl,omitempty"`
	Rationale string   `json:"rationale"`

	SourceFile string `json:"-"`
	SourceLine int    `json:"-"`
}

// JournalIssue is one malformed or shadowed journal line, reported so a reader
// can fix it and so the grader never silently drops evidence.
type JournalIssue struct {
	File   string `json:"file"`
	Line   int    `json:"line"`
	Reason string `json:"reason"`
}

// JournalGradeOptions is a parsed `grade journal` request: the inclusive
// decision-date window to grade. A nil bound is open at that end.
type JournalGradeOptions struct {
	From  *time.Time
	Until *time.Time
}

// JournalCounts is the run-level journal population: how many lines the source
// yielded, the verdict split, and how many lines were malformed or shadowed.
type JournalCounts struct {
	Logged     int `json:"logged"`
	Tradable   int `json:"tradable"`
	Watch      int `json:"watch"`
	Pass       int `json:"pass"`
	NoTrade    int `json:"no_trade"`
	Malformed  int `json:"malformed"`
	Duplicates int `json:"duplicates"`
}

// TickerVerdict is one logged verdict's drill-down row: the line as written,
// its planned reward-to-risk, and — for a Tradable Verdict — how it resolved.
type TickerVerdict struct {
	Date       string   `json:"date"`
	Ticker     string   `json:"ticker"`
	Verdict    string   `json:"verdict"`
	Entry      *float64 `json:"entry,omitempty"`
	TP         *float64 `json:"tp,omitempty"`
	SL         *float64 `json:"sl,omitempty"`
	Rationale  string   `json:"rationale"`
	PlannedRR  *float64 `json:"planned_rr,omitempty"`
	Outcome    string   `json:"outcome,omitempty"`
	Return     *float64 `json:"return,omitempty"`
	ResolvedOn string   `json:"resolved_on,omitempty"`
}

// JournalSummary is the graded headline: the resolved-verdict split, the win
// rate and return distribution over resolved verdicts, the Expectancy, and the
// calibration figures for the no-trade verdicts (CONTEXT: Expectancy,
// Calibration, Breakeven Win Rate).
type JournalSummary struct {
	Wins             int      `json:"wins"`
	Losses           int      `json:"losses"`
	Expired          int      `json:"expired"`
	Unresolved       int      `json:"unresolved"`
	WinRate          *float64 `json:"win_rate"`
	MedianReturn     *float64 `json:"median_return"`
	MeanReturn       *float64 `json:"mean_return"`
	MeanWin          *float64 `json:"mean_win"`
	MeanLoss         *float64 `json:"mean_loss"`
	MedianPlannedRR  *float64 `json:"median_planned_rr"`
	BreakevenWinRate *float64 `json:"breakeven_win_rate"`
	Calibration      *float64 `json:"calibration"`
}

// JournalGradeReport is the artifact one `grade journal` run writes.
type JournalGradeReport struct {
	Kind            string             `json:"kind"`
	From            string             `json:"from,omitempty"`
	Until           string             `json:"until,omitempty"`
	GitRevision     string             `json:"git_revision"`
	GeneratedAt     string             `json:"generated_at"`
	ResolutionDays  int                `json:"resolution_days"`
	Counts          JournalCounts      `json:"counts"`
	Summary         JournalSummary     `json:"summary"`
	Verdicts        []TickerVerdict    `json:"verdicts"`
	Malformed       []JournalIssue     `json:"malformed"`
	Duplicates      []JournalIssue     `json:"duplicates"`
	Flags           []string           `json:"flags"`
	SuccessCriteria []SuccessCriterion `json:"success_criteria"`
	Notes           []string           `json:"notes"`
}

// GradeJournal is the Decision Journal Grader: it reads the agent-written
// verdicts in a decision-date window, dedupes them to one observation per
// (date, ticker) with last-wins, and resolves each Tradable Verdict's TP/SL
// against stored prices over the Resolution Window. Malformed and shadowed
// lines are reported, never silently dropped. There is no Replay, no Screen, and
// no Baseline: a plan's own TP and SL are its counterfactual (ADR-0014).
func (e *MeasurementEngine) GradeJournal(_ context.Context, opts JournalGradeOptions) (*JournalGradeReport, error) {
	if e.Journal == nil || e.JournalPrices == nil {
		return nil, fmt.Errorf("%w: journal grade requires a journal source and a price source", ErrInvalidArgument)
	}
	if opts.From != nil && opts.Until != nil && opts.From.After(*opts.Until) {
		return nil, fmt.Errorf("%w: --from %s is after --until %s", ErrInvalidArgument,
			opts.From.Format("2006-01-02"), opts.Until.Format("2006-01-02"))
	}

	parsed, issues, err := e.Journal.ReadVerdicts(opts.From, opts.Until)
	if err != nil {
		return nil, err
	}

	verdicts, shadowed := dedupeJournalVerdicts(parsed)

	rows := make([]TickerVerdict, 0, len(verdicts))
	counts := JournalCounts{Logged: len(verdicts), Malformed: len(issues), Duplicates: len(shadowed)}
	var resolvedReturns []float64
	var winReturns []float64
	var lossReturns []float64
	var plannedRRs []float64
	summary := JournalSummary{}

	for _, verdict := range verdicts {
		row := TickerVerdict{
			Date:      verdict.Date,
			Ticker:    verdict.Ticker,
			Verdict:   verdict.Verdict,
			Entry:     verdict.Entry,
			TP:        verdict.TP,
			SL:        verdict.SL,
			Rationale: verdict.Rationale,
		}

		if verdict.Verdict != verdictSetup {
			counts.NoTrade++
			if verdict.Verdict == verdictWatch {
				counts.Watch++
			} else {
				counts.Pass++
			}
			rows = append(rows, row)
			continue
		}

		counts.Tradable++
		rr := plannedRewardRisk(verdict)
		row.PlannedRR = rr
		if rr != nil {
			plannedRRs = append(plannedRRs, *rr)
		}

		outcome, realized, resolvedOn, err := e.resolveJournalVerdict(verdict)
		if err != nil {
			return nil, err
		}
		row.Outcome = outcome
		row.Return = realized
		row.ResolvedOn = resolvedOn
		switch outcome {
		case outcomeWin:
			summary.Wins++
			if realized != nil {
				resolvedReturns = append(resolvedReturns, *realized)
				winReturns = append(winReturns, *realized)
			}
		case outcomeLoss:
			summary.Losses++
			if realized != nil {
				resolvedReturns = append(resolvedReturns, *realized)
				lossReturns = append(lossReturns, *realized)
			}
		case outcomeExpired:
			summary.Expired++
		case outcomeUnresolved:
			summary.Unresolved++
		}
		rows = append(rows, row)
	}

	summary.WinRate = ratio(summary.Wins, summary.Wins+summary.Losses)
	if len(resolvedReturns) > 0 {
		median := median(resolvedReturns)
		mean := mean(resolvedReturns)
		summary.MedianReturn = &median
		summary.MeanReturn = &mean
	}
	if len(winReturns) > 0 {
		mean := mean(winReturns)
		summary.MeanWin = &mean
	}
	if len(lossReturns) > 0 {
		mean := mean(lossReturns)
		summary.MeanLoss = &mean
	}
	if len(plannedRRs) > 0 {
		median := median(plannedRRs)
		summary.MedianPlannedRR = &median
		if median > 0 {
			breakeven := 1 / (1 + median)
			summary.BreakevenWinRate = &breakeven
		}
	}
	summary.Calibration = ratio(counts.NoTrade, counts.Logged)

	report := &JournalGradeReport{
		Kind:            journalGradeKind,
		From:            formatOptionalDay(opts.From),
		Until:           formatOptionalDay(opts.Until),
		GitRevision:     e.Revision,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
		ResolutionDays:  journalResolutionDays,
		Counts:          counts,
		Summary:         summary,
		Verdicts:        rows,
		Malformed:       nonNilIssues(issues),
		Duplicates:      nonNilIssues(shadowed),
		Flags:           journalGradeFlags(counts, summary, rows),
		SuccessCriteria: journalSuccessCriteria(summary, counts),
		Notes:           journalGradeNotes(),
	}
	return report, nil
}

// resolveJournalVerdict resolves one Tradable Verdict against stored prices over
// the Resolution Window. The window is the first 20 market Trading Days after
// the decision day; fewer stored market days leaves the verdict unresolved. The
// decision day itself never resolves the trade. A level is touched on intraday
// High/Low; the earliest touch wins, and both on the same day is a loss (the
// conservative tie). A window that elapses with neither level touched is
// expired (ADR-0014).
func (e *MeasurementEngine) resolveJournalVerdict(verdict JournalVerdict) (string, *float64, string, error) {
	date, err := time.Parse("2006-01-02", verdict.Date)
	if err != nil {
		return "", nil, "", fmt.Errorf("resolve verdict %s %s: %w", verdict.Date, verdict.Ticker, err)
	}

	until := date.AddDate(0, 0, measurementLookaheadDays)
	calendar, err := e.JournalPrices.TradingDaysInRangeAll(e.DB, date, until)
	if err != nil {
		return "", nil, "", fmt.Errorf("read market trading days from %s: %w", verdict.Date, err)
	}
	post := make([]time.Time, 0, journalResolutionDays)
	for _, day := range calendar {
		if day.After(date) {
			post = append(post, day)
		}
	}
	if len(post) < journalResolutionDays {
		return outcomeUnresolved, nil, "", nil
	}
	post = post[:journalResolutionDays]

	series, err := e.JournalPrices.FindByTickerAndDateRange(e.DB, verdict.Ticker, date, until)
	if err != nil {
		return "", nil, "", fmt.Errorf("read prices for %s from %s: %w", verdict.Ticker, verdict.Date, err)
	}
	byDay := make(map[string]entity.DailyPrice, len(series))
	for _, price := range series {
		byDay[price.TradingDay.Format("2006-01-02")] = price
	}

	entry, tp, sl := *verdict.Entry, *verdict.TP, *verdict.SL
	for _, day := range post {
		price, ok := byDay[day.Format("2006-01-02")]
		if !ok || price.High == nil || price.Low == nil {
			continue
		}
		tpTouched := *price.High >= tp
		slTouched := *price.Low <= sl
		switch {
		case tpTouched && slTouched:
			ret := sl/entry - 1
			return outcomeLoss, &ret, day.Format("2006-01-02"), nil
		case tpTouched:
			ret := tp/entry - 1
			return outcomeWin, &ret, day.Format("2006-01-02"), nil
		case slTouched:
			ret := sl/entry - 1
			return outcomeLoss, &ret, day.Format("2006-01-02"), nil
		}
	}
	return outcomeExpired, nil, "", nil
}

// plannedRewardRisk is a Tradable Verdict's planned reward-to-risk ratio,
// (TP − entry) / (entry − SL). Both distances are positive for a validated
// SETUP, so the ratio is always positive.
func plannedRewardRisk(verdict JournalVerdict) *float64 {
	if verdict.Entry == nil || verdict.TP == nil || verdict.SL == nil {
		return nil
	}
	risk := *verdict.Entry - *verdict.SL
	if risk <= 0 {
		return nil
	}
	reward := *verdict.TP - *verdict.Entry
	rr := reward / risk
	return &rr
}

// dedupeJournalVerdicts collapses the parsed lines to one observation per
// (date, ticker). The last written line wins — the append-only file's way of
// correcting itself — and each shadowed line is reported. The result is sorted
// by (date, ticker) for a deterministic report.
func dedupeJournalVerdicts(in []JournalVerdict) ([]JournalVerdict, []JournalIssue) {
	index := make(map[string]int, len(in))
	out := make([]JournalVerdict, 0, len(in))
	shadowed := make([]JournalIssue, 0)
	for _, verdict := range in {
		key := verdict.Date + "|" + verdict.Ticker
		if at, ok := index[key]; ok {
			shadowed = append(shadowed, JournalIssue{
				File: out[at].SourceFile,
				Line: out[at].SourceLine,
				Reason: fmt.Sprintf("duplicate %s %s; shadowed by line %d in %s",
					verdict.Date, verdict.Ticker, verdict.SourceLine, verdict.SourceFile),
			})
			out[at] = verdict
			continue
		}
		index[key] = len(out)
		out = append(out, verdict)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Date != out[j].Date {
			return out[i].Date < out[j].Date
		}
		return out[i].Ticker < out[j].Ticker
	})
	return out, shadowed
}

// ratio is n/d, or nil when d is zero so an empty population never reads as 0%.
func ratio(n, d int) *float64 {
	if d == 0 {
		return nil
	}
	value := float64(n) / float64(d)
	return &value
}

// mean returns the arithmetic mean of the values.
func mean(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

// nonNilIssues keeps a report's issue lists as empty JSON arrays, not null.
func nonNilIssues(issues []JournalIssue) []JournalIssue {
	if issues == nil {
		return []JournalIssue{}
	}
	return issues
}

// journalGradeFlags renders the discipline warnings: malformed and shadowed
// lines, unresolved verdicts awaiting their window, and SETUP plans below the
// skill's 1:2 reward-to-risk gate. Flags, never refusals.
func journalGradeFlags(counts JournalCounts, summary JournalSummary, rows []TickerVerdict) []string {
	flags := []string{}
	if counts.Malformed > 0 {
		flags = append(flags, fmt.Sprintf(
			"malformed: %d journal line(s) could not be parsed or validated; reported and skipped, never graded", counts.Malformed))
	}
	if counts.Duplicates > 0 {
		flags = append(flags, fmt.Sprintf(
			"duplicate: %d journal line(s) shadowed by a later line for the same (date, ticker); the later line was graded", counts.Duplicates))
	}
	if counts.Logged == 0 {
		flags = append(flags, "no data: no journal verdicts found in the requested window")
	}
	if summary.Unresolved > 0 {
		flags = append(flags, fmt.Sprintf(
			"unresolved: %d tradable verdict(s) lack %d post-decision trading days; grade again once the window closes",
			summary.Unresolved, journalResolutionDays))
	}
	lowRR := 0
	for _, row := range rows {
		if row.PlannedRR != nil && *row.PlannedRR < plannedRRGate {
			lowRR++
		}
	}
	if lowRR > 0 {
		flags = append(flags, fmt.Sprintf(
			"discipline: %d tradable verdict(s) planned a reward-to-risk below the 1:%.0f gate", lowRR, plannedRRGate))
	}
	return flags
}

// journalSuccessCriteria answers the journal's standing questions with this
// run's numbers, so the report restates why the loop exists.
func journalSuccessCriteria(summary JournalSummary, counts JournalCounts) []SuccessCriterion {
	return []SuccessCriterion{
		{Question: "Do the AI's Tradable Verdicts have positive Expectancy?", Answer: expectancyAnswer(summary)},
		{Question: "Is the observed win rate above the R:R-implied Breakeven Win Rate?", Answer: breakevenAnswer(summary)},
		{Question: "How often did the deep-dive gate decline to trade (Calibration)?", Answer: calibrationAnswer(summary, counts)},
	}
}

func expectancyAnswer(summary JournalSummary) string {
	if summary.MeanReturn == nil {
		return "Unresolved — no Tradable Verdict has resolved yet."
	}
	if *summary.MeanReturn > 0 {
		return fmt.Sprintf("Yes — mean realized return (Expectancy) is %+.2f%% per resolved verdict.", *summary.MeanReturn*100)
	}
	return fmt.Sprintf("No — mean realized return (Expectancy) is %+.2f%% per resolved verdict.", *summary.MeanReturn*100)
}

func breakevenAnswer(summary JournalSummary) string {
	if summary.WinRate == nil || summary.BreakevenWinRate == nil {
		return "Unresolved — need both a resolved win rate and a planned reward-to-risk."
	}
	verdict := "above"
	if *summary.WinRate < *summary.BreakevenWinRate {
		verdict = "below"
	}
	return fmt.Sprintf("%s — win rate %.1f%% vs a Breakeven Win Rate of %.1f%% at the median planned R:R (%.2f).",
		verdict, *summary.WinRate*100, *summary.BreakevenWinRate*100, *summary.MedianPlannedRR)
}

func calibrationAnswer(summary JournalSummary, counts JournalCounts) string {
	if counts.Logged == 0 {
		return "Unresolved — no verdicts logged."
	}
	return fmt.Sprintf("The gate declined %d of %d verdict(s) (%.1f%%) — %d WATCH, %d PASS.",
		counts.NoTrade, counts.Logged, *summary.Calibration*100, counts.Watch, counts.Pass)
}

// journalGradeNotes are the standing caveats every Journal Grade carries.
func journalGradeNotes() []string {
	return []string{
		fmt.Sprintf("Resolution Window: %d market trading days after the decision day; the decision day itself never resolves the trade.", journalResolutionDays),
		"A TP/SL is resolved on intraday High/Low touch; the earliest touch wins and both touched the same day is a loss (the conservative tie). Gap-through counts as a touch; fill/slippage realism is not modeled.",
		"Entry is the logged price, taken as the assumed fill at decision time; the grader does not model waiting for a limit order.",
		"Win rate is wins / (wins + losses): expired (neither level touched) and unresolved (window not yet elapsed) verdicts are reported separately and excluded.",
		"Journal grading is absolute — a plan's own TP and SL are its counterfactual, so no Liquidity Baseline is read.",
		"One line per deep-dive is logged for every verdict, including WATCH/PASS, so Calibration is meaningful.",
	}
}

// compile-time checks: the concrete repository and reader satisfy the Journal
// Grader's seams.
var (
	_ MeasurementJournalSource = (*JournalFileReader)(nil)
	_ JournalPriceSource       = (*repository.DailyPriceRepository)(nil)
)
