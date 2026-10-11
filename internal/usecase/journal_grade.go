package usecase

import (
	"context"
	"fmt"
	"sort"
	"strings"
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

// MeasurementJournalSource is the Journal Grader's read seam: every raw
// Verdict row in a decision-date window, ordered oldest-first, plus the rows it
// could not read. It deliberately returns raw rows, not deduped ones — the
// session/conflict/committed rule is the Grader's, so a source cannot decide
// what a Verdict Conflict is. The concrete *JournalSQLSource satisfies it.
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

// JournalVerdict is one Decision Journal row: a Verdict the agent logged at
// decision time. A SETUP carries three zones (entry, cut-loss, target);
// WATCH/PASS carry none. The provenance fields below are for the
// duplicate/conflict report, never journal data.
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

	// ID is the row's surrogate key; Session identifies the agent run that wrote
	// it; Supersedes is the row a deliberate correction replaces; Committed marks
	// the Verdict the Grader scores on a key that holds more than one; CreatedAt
	// orders same-session last-wins. Provenance, so never surfaced as journal
	// data — the report carries what it needs under Duplicates/Conflicts.
	ID         int64     `json:"-"`
	Session    string    `json:"-"`
	Supersedes *int64    `json:"-"`
	Committed  bool      `json:"-"`
	CreatedAt  time.Time `json:"-"`
}

// JournalIssue is one malformed or shadowed journal row, reported so a reader
// can fix it and so the Grader never silently drops evidence. Ref is a
// source-agnostic pointer to the row (for the SQL table, `measurement_journal#<id>`),
// not a file and line: the journal is a table.
type JournalIssue struct {
	Ref    string `json:"ref"`
	Reason string `json:"reason"`
}

// VerdictConflict is a (decision_day, ticker) key whose Verdicts disagree
// across sessions and no row is Committed: the Grader excludes the key from
// grading and reports it for a human to resolve — never picked by timestamp.
// Rows is one entry per session, so the disagreement is legible.
type VerdictConflict struct {
	DecisionDay string        `json:"decision_day"`
	Ticker      string        `json:"ticker"`
	Rows        []ConflictRow `json:"rows"`
}

// ConflictRow is one session's Verdict within a Verdict Conflict.
type ConflictRow struct {
	ID         int64    `json:"id"`
	Session    string   `json:"session"`
	Verdict    string   `json:"verdict"`
	Supersedes *int64   `json:"supersedes,omitempty"`
	EntryLow   *float64 `json:"entry_low,omitempty"`
	EntryHigh  *float64 `json:"entry_high,omitempty"`
	CLLow      *float64 `json:"cl_low,omitempty"`
	CLHigh     *float64 `json:"cl_high,omitempty"`
	TPLow      *float64 `json:"tp_low,omitempty"`
	TPHigh     *float64 `json:"tp_high,omitempty"`
	Rationale  string   `json:"rationale,omitempty"`
}

// JournalGradeOptions is a parsed `grade journal` request: the inclusive
// decision-date window to grade. A nil bound is open at that end.
type JournalGradeOptions struct {
	From  *time.Time
	Until *time.Time
}

// JournalCounts is the run-level journal population: how many keys the source
// yielded, the verdict split, and how many rows were malformed, shadowed, or in
// an unresolved Verdict Conflict. Logged counts every distinct (date, ticker)
// key, including conflicted ones; Conflicts counts the keys excluded from
// grading for want of a Committed Verdict.
type JournalCounts struct {
	Logged     int `json:"logged"`
	Tradable   int `json:"tradable"`
	Watch      int `json:"watch"`
	Pass       int `json:"pass"`
	NoTrade    int `json:"no_trade"`
	Malformed  int `json:"malformed"`
	Duplicates int `json:"duplicates"`
	Conflicts  int `json:"conflicts"`
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
	Conflicts       []VerdictConflict  `json:"conflicts"`
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

	parsed, sourceIssues, err := e.Journal.ReadVerdicts(opts.From, opts.Until)
	if err != nil {
		return nil, err
	}

	valid, normalizeIssues := normalizeJournalVerdicts(parsed)
	issues := append(sourceIssues, normalizeIssues...)

	verdicts, shadowed, conflicts := dedupeJournalVerdicts(valid)

	rows := make([]TickerVerdict, 0, len(verdicts))
	counts := JournalCounts{
		Logged:     len(verdicts) + len(conflicts),
		Conflicts:  len(conflicts),
		Malformed:  len(issues),
		Duplicates: len(shadowed),
	}
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
	// Calibration is the no-trade share of *classified* Verdicts: a conflicted
	// key has no single Verdict, so it is counted in Logged (a Verdict exists)
	// but can be neither no-trade nor tradable, and must not dilute the share.
	summary.Calibration = ratio(counts.NoTrade, counts.Logged-counts.Conflicts)

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
		Conflicts:       nonNilConflicts(conflicts),
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

// normalizeJournalVerdict canonicalizes and validates one row. A SETUP must
// carry three positive, ordered zones — clLow <= clHigh <= entryLow <= entryHigh
// <= tpLow <= tpHigh — where a point is a zone with low == high. A WATCH or PASS
// row carries no zones (any present are ignored). Every verdict needs a date, a
// ticker, and a one-line rationale. The measurement_journal CHECK constraints
// enforce the same shape; this is the net for a row written out of band.
func normalizeJournalVerdict(v *JournalVerdict) error {
	day, err := time.Parse("2006-01-02", strings.TrimSpace(v.Date))
	if err != nil {
		return fmt.Errorf("date must be YYYY-MM-DD, got %q", v.Date)
	}
	v.Date = day.Format("2006-01-02")

	v.Ticker = strings.ToUpper(strings.TrimSpace(v.Ticker))
	if v.Ticker == "" {
		return fmt.Errorf("ticker is required")
	}

	v.Verdict = strings.ToUpper(strings.TrimSpace(v.Verdict))
	switch v.Verdict {
	case verdictSetup, verdictWatch, verdictPass:
	default:
		return fmt.Errorf("unknown verdict %q; expected SETUP, WATCH or PASS", v.Verdict)
	}

	if strings.TrimSpace(v.Rationale) == "" {
		return fmt.Errorf("rationale is required")
	}

	if v.Verdict != verdictSetup {
		v.EntryLow, v.EntryHigh, v.CLLow, v.CLHigh, v.TPLow, v.TPHigh = nil, nil, nil, nil, nil, nil
		return nil
	}

	if v.EntryLow == nil || v.EntryHigh == nil || v.CLLow == nil || v.CLHigh == nil || v.TPLow == nil || v.TPHigh == nil {
		return fmt.Errorf("SETUP requires entryLow, entryHigh, clLow, clHigh, tpLow and tpHigh")
	}
	if *v.EntryLow <= 0 || *v.EntryHigh <= 0 || *v.CLLow <= 0 || *v.CLHigh <= 0 || *v.TPLow <= 0 || *v.TPHigh <= 0 {
		return fmt.Errorf("SETUP zones must be positive")
	}
	if *v.CLLow > *v.CLHigh {
		return fmt.Errorf("SETUP clLow must not exceed clHigh")
	}
	if *v.EntryLow > *v.EntryHigh {
		return fmt.Errorf("SETUP entryLow must not exceed entryHigh")
	}
	if *v.TPLow > *v.TPHigh {
		return fmt.Errorf("SETUP tpLow must not exceed tpHigh")
	}
	if *v.CLHigh > *v.EntryLow {
		return fmt.Errorf("SETUP clHigh must not exceed entryLow (the cut-loss zone must sit below the entry zone)")
	}
	if *v.EntryHigh > *v.TPLow {
		return fmt.Errorf("SETUP entryHigh must not exceed tpLow (the entry zone must sit below the target zone)")
	}
	return nil
}

// normalizeJournalVerdicts canonicalizes and validates every raw row, returning
// the valid rows and one issue per row that could not be normalized. The table's
// CHECK constraints already enforce this shape, so on a clean journal it is a
// no-op; it is the net for a row written out of band.
func normalizeJournalVerdicts(in []JournalVerdict) ([]JournalVerdict, []JournalIssue) {
	valid := make([]JournalVerdict, 0, len(in))
	issues := make([]JournalIssue, 0)
	for _, verdict := range in {
		if err := normalizeJournalVerdict(&verdict); err != nil {
			issues = append(issues, JournalIssue{Ref: verdictRef(verdict), Reason: err.Error()})
			continue
		}
		valid = append(valid, verdict)
	}
	return valid, issues
}

// verdictRef names a journal row in a report without assuming a file: the
// table row's id and the session that wrote it.
func verdictRef(verdict JournalVerdict) string {
	ref := fmt.Sprintf("measurement_journal#%d", verdict.ID)
	if verdict.Session != "" {
		ref += " session=" + verdict.Session
	}
	return ref
}

// dedupeJournalVerdicts collapses the raw rows to one observation per
// (decision_day, ticker) under ADR-0016's rule:
//
//   - rows of one session are an accidental double-write: last-wins by
//     (created_at, id), each shadowed row reported;
//   - rows of different sessions that agree on (verdict, zones) are a benign
//     duplicate: one graded, the rest reported — rationale alone is never a
//     disagreement, it is not graded;
//   - rows of different sessions that disagree are a Verdict Conflict. Exactly
//     one committed row is scored, the rest retained as audit; with none
//     committed the key is excluded from grading and returned as a conflict for
//     a human to resolve — never picked by timestamp.
//
// Observations and conflicts are each sorted by (date, ticker) for a
// deterministic report.
func dedupeJournalVerdicts(in []JournalVerdict) ([]JournalVerdict, []JournalIssue, []VerdictConflict) {
	groups := groupJournalVerdicts(in)
	out := make([]JournalVerdict, 0, len(groups))
	shadowed := make([]JournalIssue, 0)
	conflicts := make([]VerdictConflict, 0)

	for _, group := range groups {
		current, dupIssues := collapseSessionDuplicates(group.rows)
		shadowed = append(shadowed, dupIssues...)

		switch {
		case len(current) == 1:
			out = append(out, current[0])
		case journalVerdictsAgree(current):
			kept, rest := agreeingVerdict(current)
			shadowed = append(shadowed, rest...)
			out = append(out, kept)
		default:
			if committed, ok := committedVerdict(current); ok {
				out = append(out, committed)
				continue
			}
			conflicts = append(conflicts, buildVerdictConflict(current))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Date != out[j].Date {
			return out[i].Date < out[j].Date
		}
		return out[i].Ticker < out[j].Ticker
	})
	sort.Slice(conflicts, func(i, j int) bool {
		if conflicts[i].DecisionDay != conflicts[j].DecisionDay {
			return conflicts[i].DecisionDay < conflicts[j].DecisionDay
		}
		return conflicts[i].Ticker < conflicts[j].Ticker
	})
	return out, shadowed, conflicts
}

// journalGroup is the raw rows sharing one (decision_day, ticker) key.
type journalGroup struct {
	rows []JournalVerdict
}

// groupJournalVerdicts buckets rows by key, preserving first-seen order.
func groupJournalVerdicts(in []JournalVerdict) []journalGroup {
	index := make(map[string]int, len(in))
	groups := make([]journalGroup, 0, len(in))
	for _, verdict := range in {
		key := verdict.Date + "|" + verdict.Ticker
		if at, ok := index[key]; ok {
			groups[at].rows = append(groups[at].rows, verdict)
			continue
		}
		index[key] = len(groups)
		groups = append(groups, journalGroup{rows: []JournalVerdict{verdict}})
	}
	return groups
}

// collapseSessionDuplicates keeps the last row of each session (by created_at,
// id) and reports every shadowed row. The result keeps session first-seen order.
func collapseSessionDuplicates(rows []JournalVerdict) ([]JournalVerdict, []JournalIssue) {
	order := make([]string, 0, len(rows))
	latest := make(map[string]JournalVerdict, len(rows))
	issues := make([]JournalIssue, 0)

	record := func(loser, winner JournalVerdict) {
		issues = append(issues, JournalIssue{
			Ref: verdictRef(loser),
			Reason: fmt.Sprintf("duplicate %s %s in session %s; shadowed by %s",
				loser.Date, loser.Ticker, loser.Session, verdictRef(winner)),
		})
	}

	for _, verdict := range rows {
		prev, ok := latest[verdict.Session]
		if !ok {
			order = append(order, verdict.Session)
			latest[verdict.Session] = verdict
			continue
		}
		if journalVerdictNewer(verdict, prev) {
			record(prev, verdict)
			latest[verdict.Session] = verdict
		} else {
			record(verdict, prev)
		}
	}

	current := make([]JournalVerdict, 0, len(order))
	for _, session := range order {
		current = append(current, latest[session])
	}
	return current, issues
}

// journalVerdictNewer reports whether a is the later write than b: by
// created_at, then by id (the row's insert order).
func journalVerdictNewer(a, b JournalVerdict) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	return a.ID > b.ID
}

// journalVerdictsAgree reports whether every row shares the first row's graded
// content (verdict and the six zones); rationale is not graded, so a re-worded
// rationale is not a disagreement.
func journalVerdictsAgree(rows []JournalVerdict) bool {
	for _, verdict := range rows[1:] {
		if !sameVerdictContent(rows[0], verdict) {
			return false
		}
	}
	return true
}

// sameVerdictContent compares the graded content of two Verdicts.
func sameVerdictContent(a, b JournalVerdict) bool {
	return a.Verdict == b.Verdict &&
		sameFloat(a.EntryLow, b.EntryLow) && sameFloat(a.EntryHigh, b.EntryHigh) &&
		sameFloat(a.CLLow, b.CLLow) && sameFloat(a.CLHigh, b.CLHigh) &&
		sameFloat(a.TPLow, b.TPLow) && sameFloat(a.TPHigh, b.TPHigh)
}

// sameFloat compares two optional levels.
func sameFloat(a, b *float64) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// committedVerdict returns the single committed row among session-current rows,
// or false when none is committed (a Committed Verdict is the only resolution
// signal the Grader reads; supersedes is provenance the writer sets alongside
// it). More than one committed row is prevented by the table's partial unique
// index; if it happens anyway the conflict stays unresolved rather than guess.
func committedVerdict(rows []JournalVerdict) (JournalVerdict, bool) {
	var found JournalVerdict
	ok := false
	for _, verdict := range rows {
		if !verdict.Committed {
			continue
		}
		if ok {
			return JournalVerdict{}, false
		}
		found, ok = verdict, true
	}
	return found, ok
}

// agreeingVerdict picks the observation among session-current rows that agree on
// graded content: the committed row if one is marked, else the newest write.
func agreeingVerdict(rows []JournalVerdict) (JournalVerdict, []JournalIssue) {
	kept := rows[0]
	for _, verdict := range rows[1:] {
		if journalVerdictNewer(verdict, kept) {
			kept = verdict
		}
	}
	if committed, ok := committedVerdict(rows); ok {
		kept = committed
	}

	rest := make([]JournalIssue, 0, len(rows)-1)
	for _, verdict := range rows {
		if verdict.Session == kept.Session {
			continue
		}
		rest = append(rest, JournalIssue{
			Ref: verdictRef(verdict),
			Reason: fmt.Sprintf("identical %s %s from another session; %s graded",
				verdict.Date, verdict.Ticker, verdictRef(kept)),
		})
	}
	return kept, rest
}

// buildVerdictConflict renders one unresolved conflict: the competing
// session-current rows, so a reader can see the disagreement and commit one.
func buildVerdictConflict(rows []JournalVerdict) VerdictConflict {
	conflict := VerdictConflict{
		DecisionDay: rows[0].Date,
		Ticker:      rows[0].Ticker,
		Rows:        make([]ConflictRow, 0, len(rows)),
	}
	for _, verdict := range rows {
		conflict.Rows = append(conflict.Rows, ConflictRow{
			ID:         verdict.ID,
			Session:    verdict.Session,
			Verdict:    verdict.Verdict,
			Supersedes: verdict.Supersedes,
			EntryLow:   verdict.EntryLow,
			EntryHigh:  verdict.EntryHigh,
			CLLow:      verdict.CLLow,
			CLHigh:     verdict.CLHigh,
			TPLow:      verdict.TPLow,
			TPHigh:     verdict.TPHigh,
			Rationale:  verdict.Rationale,
		})
	}
	return conflict
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

// nonNilConflicts keeps the conflict list an empty JSON array, not null.
func nonNilConflicts(conflicts []VerdictConflict) []VerdictConflict {
	if conflicts == nil {
		return []VerdictConflict{}
	}
	return conflicts
}

// journalGradeFlags renders the discipline warnings: malformed and shadowed
// rows, unresolved Verdict Conflicts, unresolved verdicts awaiting their window,
// and SETUP plans below the skill's 1:2 worst-case reward-to-risk gate. Flags,
// never refusals.
func journalGradeFlags(counts JournalCounts, summary JournalSummary, rows []TickerVerdict) []string {
	flags := []string{}
	if counts.Malformed > 0 {
		flags = append(flags, fmt.Sprintf(
			"malformed: %d journal row(s) could not be validated; reported and skipped, never graded", counts.Malformed))
	}
	if counts.Duplicates > 0 {
		flags = append(flags, fmt.Sprintf(
			"duplicate: %d journal row(s) shadowed by another for the same (date, ticker); one was graded", counts.Duplicates))
	}
	if counts.Conflicts > 0 {
		flags = append(flags, fmt.Sprintf(
			"conflict: %d (date, ticker) key(s) hold Verdicts from different sessions that disagree; each is excluded from grading until a Committed Verdict is marked", counts.Conflicts))
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
	classified := counts.Logged - counts.Conflicts
	if classified == 0 {
		return "Unresolved — no classified verdicts logged."
	}
	return fmt.Sprintf("The gate declined %d of %d classified verdict(s) (%.1f%%) — %d WATCH, %d PASS.",
		counts.NoTrade, classified, *summary.Calibration*100, counts.Watch, counts.Pass)
}

// journalGradeNotes are the standing caveats every Journal Grade carries.
func journalGradeNotes() []string {
	return []string{
		fmt.Sprintf("Entry-first: a verdict has %d trading days after the decision day to tap its entry zone, then %d trading days from the fill to touch TP or CL. The decision day itself never fills the trade.", journalEntryWindowDays, journalResolutionDays),
		"A fill needs the day's [Low, High] to overlap the entry zone; TP triggers on High >= tpLow and CL on Low <= clHigh. The earliest touch wins, both the same day is a loss (the conservative tie), and a gap through a level counts as a touch. Fill/slippage realism is not modeled.",
		"Booking is conservative: a fill is taken at entryHigh, a win at tpLow, a loss at clHigh (the near edge of each zone).",
		"An unfilled verdict (the entry zone is never tapped in the entry window) is neither win nor loss; its shadow read (missed_tp / dodged_cl / flat) shows what the thesis would have done, and is excluded from the win rate.",
		"Win rate is wins / (wins + losses): expired, unresolved and unfilled verdicts are reported separately and excluded.",
		"Verdicts sharing a (decision day, ticker) from one session are last-wins; from different sessions that disagree they are a Verdict Conflict, excluded from grading until a Committed Verdict is marked — the Grader never picks by timestamp.",
		"Calibration is the no-trade share of classified verdicts: a conflicted key counts as logged but is neither no-trade nor tradable, so it is excluded from the share's denominator.",
		"Journal grading is absolute — a plan's own zones are its counterfactual, so no Liquidity Baseline is read.",
		"One Verdict per deep-dive is logged for every verdict, including WATCH/PASS, so Calibration is meaningful.",
	}
}

// compile-time checks: the concrete repository and source satisfy the Journal
// Grader's seams.
var (
	_ MeasurementJournalSource = (*JournalSQLSource)(nil)
	_ JournalPriceSource       = (*repository.DailyPriceRepository)(nil)
)
