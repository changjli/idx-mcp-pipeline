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
	outcomeUnfilled   = "unfilled"

	// Unfilled shadow labels: what the thesis would have done over the entry
	// window had the entry zone been tapped. Never part of the win rate.
	shadowMissedTP = "missed_tp"
	shadowDodgedCL = "dodged_cl"
	shadowFlat     = "flat"

	// The two windows (ADR-0014): a Tradable Verdict has journalEntryWindowDays
	// trading days after the decision day to fill, then journalResolutionDays
	// trading days from the fill day (inclusive) to touch TP or CL. Both are the
	// harness's longest Forward Return horizon, so the journal's clock matches
	// the Graders'.
	journalEntryWindowDays = maxMeasurementHorizon
	journalResolutionDays  = maxMeasurementHorizon
	// journalLookaheadDays bounds the calendar span read for one verdict: entry
	// window + resolution window is ~40 trading days, which holidays can push
	// past 60 calendar days.
	journalLookaheadDays = 120

	// plannedRRGate is the skill's minimum planned reward-to-risk ratio (1:2),
	// read on the worst-case edges. A Tradable Verdict below it is flagged,
	// never skipped.
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
// verdict's zones on intraday touch. The concrete *repository.DailyPriceRepository
// satisfies it. It is deliberately narrower than MeasurementPriceSource — the
// journal grader replays nothing and reads no Liquidity Baseline (ADR-0014).
type JournalPriceSource interface {
	TradingDaysInRangeAll(db *sqlx.DB, from, to time.Time) ([]time.Time, error)
	FindByTickerAndDateRange(db *sqlx.DB, ticker string, from, to time.Time) ([]entity.DailyPrice, error)
}

// JournalVerdict is one parsed Decision Journal line: a Verdict the agent logged
// at decision time. A SETUP carries three zones (entry, cut-loss, target);
// WATCH/PASS carry none. The source fields are provenance for the
// malformed/duplicate report, never journal data.
type JournalVerdict struct {
	Date      string   `json:"date"`
	Ticker    string   `json:"ticker"`
	Verdict   string   `json:"verdict"`
	EntryLow  *float64 `json:"entryLow,omitempty"`
	EntryHigh *float64 `json:"entryHigh,omitempty"`
	CLLow     *float64 `json:"clLow,omitempty"`
	CLHigh    *float64 `json:"clHigh,omitempty"`
	TPLow     *float64 `json:"tpLow,omitempty"`
	TPHigh    *float64 `json:"tpHigh,omitempty"`
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

// TickerVerdict is one logged verdict's drill-down row: the zones as written,
// the worst-case planned reward-to-risk, and — for a Tradable Verdict — how it
// resolved. Shadow is set only on an unfilled verdict.
type TickerVerdict struct {
	Date       string   `json:"date"`
	Ticker     string   `json:"ticker"`
	Verdict    string   `json:"verdict"`
	EntryLow   *float64 `json:"entry_low,omitempty"`
	EntryHigh  *float64 `json:"entry_high,omitempty"`
	CLLow      *float64 `json:"cl_low,omitempty"`
	CLHigh     *float64 `json:"cl_high,omitempty"`
	TPLow      *float64 `json:"tp_low,omitempty"`
	TPHigh     *float64 `json:"tp_high,omitempty"`
	Rationale  string   `json:"rationale"`
	PlannedRR  *float64 `json:"planned_rr,omitempty"`
	Outcome    string   `json:"outcome,omitempty"`
	Shadow     string   `json:"shadow,omitempty"`
	Return     *float64 `json:"return,omitempty"`
	ResolvedOn string   `json:"resolved_on,omitempty"`
}

// JournalSummary is the graded headline: the resolved-verdict split, the win
// rate and return distribution over resolved verdicts, the Expectancy, the
// unfilled breakdown, and the calibration figures for the no-trade verdicts
// (CONTEXT: Expectancy, Calibration, Breakeven Win Rate).
type JournalSummary struct {
	Wins             int      `json:"wins"`
	Losses           int      `json:"losses"`
	Expired          int      `json:"expired"`
	Unresolved       int      `json:"unresolved"`
	Unfilled         int      `json:"unfilled"`
	UnfilledMissedTP int      `json:"unfilled_missed_tp"`
	UnfilledDodgedCL int      `json:"unfilled_dodged_cl"`
	UnfilledFlat     int      `json:"unfilled_flat"`
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
	EntryWindowDays int                `json:"entry_window_days"`
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

// journalResolution is one Tradable Verdict's outcome: the resolved state plus,
// for an unfilled verdict, the shadow read of what the thesis would have done.
type journalResolution struct {
	Outcome    string
	Shadow     string
	Return     *float64
	ResolvedOn string
}

// GradeJournal is the Decision Journal Grader: it reads the agent-written
// verdicts in a decision-date window, dedupes them to one observation per
// (date, ticker) with last-wins, and resolves each Tradable Verdict's entry/TP/CL
// zones against stored prices — entry-first, then TP-vs-CL from the fill. A
// verdict whose entry zone is never tapped is unfilled, with a shadow read of
// what the thesis would have done. Malformed and shadowed lines are reported,
// never silently dropped. There is no Replay, no Screen, and no Baseline: a
// plan's own zones are its counterfactual (ADR-0014).
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
	var resolvedReturns, winReturns, lossReturns, plannedRRs []float64
	summary := JournalSummary{}

	for _, verdict := range verdicts {
		row := TickerVerdict{
			Date:      verdict.Date,
			Ticker:    verdict.Ticker,
			Verdict:   verdict.Verdict,
			EntryLow:  verdict.EntryLow,
			EntryHigh: verdict.EntryHigh,
			CLLow:     verdict.CLLow,
			CLHigh:    verdict.CLHigh,
			TPLow:     verdict.TPLow,
			TPHigh:    verdict.TPHigh,
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

		resolved, err := e.resolveJournalVerdict(verdict)
		if err != nil {
			return nil, err
		}
		row.Outcome = resolved.Outcome
		row.Shadow = resolved.Shadow
		row.Return = resolved.Return
		row.ResolvedOn = resolved.ResolvedOn
		switch resolved.Outcome {
		case outcomeWin:
			summary.Wins++
			if resolved.Return != nil {
				resolvedReturns = append(resolvedReturns, *resolved.Return)
				winReturns = append(winReturns, *resolved.Return)
			}
		case outcomeLoss:
			summary.Losses++
			if resolved.Return != nil {
				resolvedReturns = append(resolvedReturns, *resolved.Return)
				lossReturns = append(lossReturns, *resolved.Return)
			}
		case outcomeExpired:
			summary.Expired++
		case outcomeUnresolved:
			summary.Unresolved++
		case outcomeUnfilled:
			summary.Unfilled++
			switch resolved.Shadow {
			case shadowMissedTP:
				summary.UnfilledMissedTP++
			case shadowDodgedCL:
				summary.UnfilledDodgedCL++
			default:
				summary.UnfilledFlat++
			}
		}
		rows = append(rows, row)
	}

	summary.WinRate = ratio(summary.Wins, summary.Wins+summary.Losses)
	if len(resolvedReturns) > 0 {
		medianReturn := median(resolvedReturns)
		meanReturn := mean(resolvedReturns)
		summary.MedianReturn = &medianReturn
		summary.MeanReturn = &meanReturn
	}
	if len(winReturns) > 0 {
		meanWin := mean(winReturns)
		summary.MeanWin = &meanWin
	}
	if len(lossReturns) > 0 {
		meanLoss := mean(lossReturns)
		summary.MeanLoss = &meanLoss
	}
	if len(plannedRRs) > 0 {
		medianRR := median(plannedRRs)
		summary.MedianPlannedRR = &medianRR
		if medianRR > 0 {
			breakeven := 1 / (1 + medianRR)
			summary.BreakevenWinRate = &breakeven
		}
	}
	summary.Calibration = ratio(counts.NoTrade, counts.Logged)

	return &JournalGradeReport{
		Kind:            journalGradeKind,
		From:            formatOptionalDay(opts.From),
		Until:           formatOptionalDay(opts.Until),
		GitRevision:     e.Revision,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
		EntryWindowDays: journalEntryWindowDays,
		ResolutionDays:  journalResolutionDays,
		Counts:          counts,
		Summary:         summary,
		Verdicts:        rows,
		Malformed:       nonNilIssues(issues),
		Duplicates:      nonNilIssues(shadowed),
		Flags:           journalGradeFlags(counts, summary, rows),
		SuccessCriteria: journalSuccessCriteria(summary, counts),
		Notes:           journalGradeNotes(),
	}, nil
}

// resolveJournalVerdict resolves one Tradable Verdict's zones against stored
// prices. Entry-first: the first day in the entry window whose [Low, High]
// overlaps the entry zone is the fill (at entryHigh, the worst fill for a long).
// If no day overlaps and the entry window has fully elapsed, the verdict is
// unfilled (with a shadow read); if the window has not elapsed, unresolved. Once
// filled, the journalResolutionDays trading days starting at the fill day are
// walked: TP touched first is a win at tpLow, CL first a loss at clHigh, both the
// same day a loss, and a full window with neither is expired (ADR-0014).
func (e *MeasurementEngine) resolveJournalVerdict(verdict JournalVerdict) (journalResolution, error) {
	date, err := time.Parse("2006-01-02", verdict.Date)
	if err != nil {
		return journalResolution{}, fmt.Errorf("resolve verdict %s %s: %w", verdict.Date, verdict.Ticker, err)
	}

	until := date.AddDate(0, 0, journalLookaheadDays)
	calendar, err := e.JournalPrices.TradingDaysInRangeAll(e.DB, date, until)
	if err != nil {
		return journalResolution{}, fmt.Errorf("read market trading days from %s: %w", verdict.Date, err)
	}
	post := make([]time.Time, 0, len(calendar))
	for _, day := range calendar {
		if day.After(date) {
			post = append(post, day)
		}
	}

	series, err := e.JournalPrices.FindByTickerAndDateRange(e.DB, verdict.Ticker, date, until)
	if err != nil {
		return journalResolution{}, fmt.Errorf("read prices for %s from %s: %w", verdict.Ticker, verdict.Date, err)
	}
	byDay := make(map[string]entity.DailyPrice, len(series))
	for _, price := range series {
		byDay[price.TradingDay.Format("2006-01-02")] = price
	}

	fillIdx := journalFillIndex(post, byDay, verdict)
	if fillIdx < 0 {
		if len(post) < journalEntryWindowDays {
			return journalResolution{Outcome: outcomeUnresolved}, nil
		}
		return journalResolution{
			Outcome: outcomeUnfilled,
			Shadow:  journalShadow(post[:journalEntryWindowDays], byDay, verdict),
		}, nil
	}

	entry := *verdict.EntryHigh
	end := fillIdx + journalResolutionDays
	if end > len(post) {
		end = len(post)
	}
	for j := fillIdx; j < end; j++ {
		price, ok := byDay[post[j].Format("2006-01-02")]
		if !ok || price.High == nil || price.Low == nil {
			continue
		}
		tpTouched := *price.High >= *verdict.TPLow
		clTouched := *price.Low <= *verdict.CLHigh
		switch {
		case tpTouched && clTouched:
			ret := *verdict.CLHigh/entry - 1
			return journalResolution{Outcome: outcomeLoss, Return: &ret, ResolvedOn: post[j].Format("2006-01-02")}, nil
		case tpTouched:
			ret := *verdict.TPLow/entry - 1
			return journalResolution{Outcome: outcomeWin, Return: &ret, ResolvedOn: post[j].Format("2006-01-02")}, nil
		case clTouched:
			ret := *verdict.CLHigh/entry - 1
			return journalResolution{Outcome: outcomeLoss, Return: &ret, ResolvedOn: post[j].Format("2006-01-02")}, nil
		}
	}
	if len(post) < fillIdx+journalResolutionDays {
		return journalResolution{Outcome: outcomeUnresolved}, nil
	}
	return journalResolution{Outcome: outcomeExpired}, nil
}

// journalFillIndex returns the index into post of the first day whose [Low,
// High] overlaps the entry zone, within the entry window, or -1 for no fill. A
// day with no stored row never fills.
func journalFillIndex(post []time.Time, byDay map[string]entity.DailyPrice, verdict JournalVerdict) int {
	limit := journalEntryWindowDays
	if len(post) < limit {
		limit = len(post)
	}
	for i := 0; i < limit; i++ {
		price, ok := byDay[post[i].Format("2006-01-02")]
		if !ok || price.High == nil || price.Low == nil {
			continue
		}
		if *price.High >= *verdict.EntryLow && *price.Low <= *verdict.EntryHigh {
			return i
		}
	}
	return -1
}

// journalShadow reads what an unfilled verdict's thesis would have done over the
// entry window: the first day to reach the TP zone is missed_tp, the CL zone is
// dodged_cl (both the same day, or neither, resolve conservatively), and neither
// is flat. It is a plan-placement signal, never part of the win rate.
func journalShadow(window []time.Time, byDay map[string]entity.DailyPrice, verdict JournalVerdict) string {
	for _, day := range window {
		price, ok := byDay[day.Format("2006-01-02")]
		if !ok || price.High == nil || price.Low == nil {
			continue
		}
		tpTouched := *price.High >= *verdict.TPLow
		clTouched := *price.Low <= *verdict.CLHigh
		switch {
		case tpTouched && clTouched:
			return shadowDodgedCL
		case tpTouched:
			return shadowMissedTP
		case clTouched:
			return shadowDodgedCL
		}
	}
	return shadowFlat
}

// plannedRewardRisk is a Tradable Verdict's worst-case planned reward-to-risk
// ratio: (tpLow − entryHigh) / (entryHigh − clHigh). Read on the edges, so a
// verdict whose zone is only 1:2 when measured mid-to-mid can still fail the
// gate.
func plannedRewardRisk(verdict JournalVerdict) *float64 {
	if verdict.EntryHigh == nil || verdict.TPLow == nil || verdict.CLHigh == nil {
		return nil
	}
	risk := *verdict.EntryHigh - *verdict.CLHigh
	if risk <= 0 {
		return nil
	}
	rr := (*verdict.TPLow - *verdict.EntryHigh) / risk
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
// skill's 1:2 worst-case reward-to-risk gate. Flags, never refusals.
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
			"unresolved: %d tradable verdict(s) have not finished their entry (%d) + resolution (%d) windows; grade again later",
			summary.Unresolved, journalEntryWindowDays, journalResolutionDays))
	}
	lowRR := 0
	for _, row := range rows {
		if row.PlannedRR != nil && *row.PlannedRR < plannedRRGate {
			lowRR++
		}
	}
	if lowRR > 0 {
		flags = append(flags, fmt.Sprintf(
			"discipline: %d tradable verdict(s) planned a worst-case reward-to-risk below the 1:%.0f gate", lowRR, plannedRRGate))
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
	return fmt.Sprintf("Mean realized return (Expectancy) is %+.2f%% per resolved verdict.", *summary.MeanReturn*100)
}

func breakevenAnswer(summary JournalSummary) string {
	if summary.WinRate == nil || summary.BreakevenWinRate == nil {
		return "Unresolved — need both a resolved win rate and a planned reward-to-risk."
	}
	position := "above"
	if *summary.WinRate < *summary.BreakevenWinRate {
		position = "below"
	}
	return fmt.Sprintf("%s — win rate %.1f%% vs a Breakeven Win Rate of %.1f%% at the median planned R:R (%.2f).",
		position, *summary.WinRate*100, *summary.BreakevenWinRate*100, *summary.MedianPlannedRR)
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
		fmt.Sprintf("Entry-first: a verdict has %d trading days after the decision day to tap its entry zone, then %d trading days from the fill to touch TP or CL. The decision day itself never fills the trade.", journalEntryWindowDays, journalResolutionDays),
		"A fill needs the day's [Low, High] to overlap the entry zone; TP triggers on High >= tpLow and CL on Low <= clHigh. The earliest touch wins, both the same day is a loss (the conservative tie), and a gap through a level counts as a touch. Fill/slippage realism is not modeled.",
		"Booking is conservative: a fill is taken at entryHigh, a win at tpLow, a loss at clHigh (the near edge of each zone).",
		"An unfilled verdict (the entry zone is never tapped in the entry window) is neither win nor loss; its shadow read (missed_tp / dodged_cl / flat) shows what the thesis would have done, and is excluded from the win rate.",
		"Win rate is wins / (wins + losses): expired, unresolved and unfilled verdicts are reported separately and excluded.",
		"Journal grading is absolute — a plan's own zones are its counterfactual, so no Liquidity Baseline is read.",
		"One line per deep-dive is logged for every verdict, including WATCH/PASS, so Calibration is meaningful.",
	}
}

// compile-time checks: the concrete repository and reader satisfy the Journal
// Grader's seams.
var (
	_ MeasurementJournalSource = (*JournalFileReader)(nil)
	_ JournalPriceSource       = (*repository.DailyPriceRepository)(nil)
)
