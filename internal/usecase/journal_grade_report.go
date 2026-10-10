package usecase

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// WriteFiles renders a Journal Grade as JSON and markdown under dir, creating
// the directory if needed, and returns the two paths. The base name is
// <from>_journal, <from>_to_<until>_journal, or all_journal when the window is
// open at both ends.
func (g *JournalGradeReport) WriteFiles(dir string) (string, string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("create report directory %s: %w", dir, err)
	}

	base := g.reportBaseName()
	jsonPath := filepath.Join(dir, base+".json")
	mdPath := filepath.Join(dir, base+".md")

	payload, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("marshal journal grade report: %w", err)
	}
	payload = append(payload, '\n')

	if err := os.WriteFile(jsonPath, payload, 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", jsonPath, err)
	}
	if err := os.WriteFile(mdPath, []byte(RenderJournalGradeMarkdown(g)), 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", mdPath, err)
	}
	return jsonPath, mdPath, nil
}

// reportBaseName derives the filename stem from the decision-date window.
func (g *JournalGradeReport) reportBaseName() string {
	name := "all"
	switch {
	case g.From != "" && g.Until != "" && g.From != g.Until:
		name = g.From + "_to_" + g.Until
	case g.From != "":
		name = g.From
	case g.Until != "":
		name = "until_" + g.Until
	}
	return name + "_journal"
}

// RenderJournalGradeMarkdown renders the human-readable Journal Grade: the
// window and revision, the verdict/outcome counts, the return distribution and
// Expectancy, the per-verdict drill-down, the unfilled shadow breakdown, the
// malformed and shadowed lines, the discipline flags, and the journal's
// standing questions.
func RenderJournalGradeMarkdown(g *JournalGradeReport) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Journal grade — %s\n\n", journalWindowLabel(g))
	fmt.Fprintf(&b, "- **Window:** %s\n", journalWindowLabel(g))
	fmt.Fprintf(&b, "- **Git revision:** `%s`\n", g.GitRevision)
	fmt.Fprintf(&b, "- **Generated:** %s\n", g.GeneratedAt)
	fmt.Fprintf(&b, "- **Entry window:** %d trading days; **resolution window:** %d trading days from the fill\n\n",
		g.EntryWindowDays, g.ResolutionDays)

	b.WriteString("## Counts\n\n")
	b.WriteString("| Logged | Tradable (SETUP) | WATCH | PASS | Malformed | Duplicates |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d | %d | %d |\n\n",
		g.Counts.Logged, g.Counts.Tradable, g.Counts.Watch, g.Counts.Pass, g.Counts.Malformed, g.Counts.Duplicates)

	b.WriteString("## Outcomes (tradable verdicts)\n\n")
	b.WriteString("| Wins | Losses | Expired | Unresolved | Unfilled | Win rate | Calibration (no-trade share) |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d | %d | %s | %s |\n\n",
		g.Summary.Wins, g.Summary.Losses, g.Summary.Expired, g.Summary.Unresolved, g.Summary.Unfilled,
		formatPercent(g.Summary.WinRate), formatPercent(g.Summary.Calibration))

	b.WriteString("## Unfilled shadow (never in the win rate)\n\n")
	b.WriteString("| missed_tp (ran anyway) | dodged_cl (fell anyway) | flat |\n")
	b.WriteString("|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| %d | %d | %d |\n\n",
		g.Summary.UnfilledMissedTP, g.Summary.UnfilledDodgedCL, g.Summary.UnfilledFlat)

	b.WriteString("## Return distribution (resolved verdicts)\n\n")
	b.WriteString("| Median return | Mean return (Expectancy) | Mean win | Mean loss | Median planned R:R | Breakeven win rate |\n")
	b.WriteString("|---:|---:|---:|---:|---:|---:|\n")
	fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n\n",
		formatPercentSigned(g.Summary.MedianReturn), formatPercentSigned(g.Summary.MeanReturn),
		formatPercentSigned(g.Summary.MeanWin), formatPercentSigned(g.Summary.MeanLoss),
		formatRatio(g.Summary.MedianPlannedRR), formatPercent(g.Summary.BreakevenWinRate))

	b.WriteString("## Verdicts\n\n")
	b.WriteString("| Date | Ticker | Verdict | Entry | TP | CL | R:R | Outcome | Shadow | Return | Resolved on |\n")
	b.WriteString("|---|---|---|---|---|---|---:|---|---|---:|---|\n")
	for _, row := range g.Verdicts {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			row.Date, row.Ticker, row.Verdict,
			formatZone(row.EntryLow, row.EntryHigh), formatZone(row.TPLow, row.TPHigh), formatZone(row.CLLow, row.CLHigh),
			formatRatio(row.PlannedRR), verdictOutcomeLabel(row.Outcome), row.Shadow,
			formatPercentSigned(row.Return), row.ResolvedOn)
	}
	b.WriteString("\n")

	if len(g.Malformed) > 0 {
		b.WriteString("## Malformed lines (skipped, never graded)\n\n")
		for _, issue := range g.Malformed {
			fmt.Fprintf(&b, "- `%s:%d` — %s\n", issue.File, issue.Line, issue.Reason)
		}
		b.WriteString("\n")
	}
	if len(g.Duplicates) > 0 {
		b.WriteString("## Shadowed duplicates (later line graded)\n\n")
		for _, issue := range g.Duplicates {
			fmt.Fprintf(&b, "- `%s:%d` — %s\n", issue.File, issue.Line, issue.Reason)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Discipline flags\n\n")
	if len(g.Flags) == 0 {
		b.WriteString("None.\n\n")
	} else {
		for _, flag := range g.Flags {
			fmt.Fprintf(&b, "- %s\n", flag)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Success criteria\n\n")
	for i, criterion := range g.SuccessCriteria {
		fmt.Fprintf(&b, "%d. **%s** %s\n", i+1, criterion.Question, criterion.Answer)
	}
	b.WriteString("\n")

	if len(g.Notes) > 0 {
		b.WriteString("## Notes\n\n")
		for _, note := range g.Notes {
			fmt.Fprintf(&b, "- %s\n", note)
		}
		b.WriteString("\n")
	}

	return b.String()
}

// journalWindowLabel renders the decision-date window for a title or header.
func journalWindowLabel(g *JournalGradeReport) string {
	switch {
	case g.From != "" && g.Until != "" && g.From == g.Until:
		return g.From
	case g.From != "" && g.Until != "":
		return g.From + " → " + g.Until
	case g.From != "":
		return "from " + g.From
	case g.Until != "":
		return "until " + g.Until
	default:
		return "all"
	}
}

// verdictOutcomeLabel renders a per-verdict outcome, using an em dash for a
// no-trade verdict that is never resolved.
func verdictOutcomeLabel(outcome string) string {
	if outcome == "" {
		return "—"
	}
	return outcome
}

// formatZone renders a zone as `low-high`, a point as its single value (low ==
// high), or an em dash when absent.
func formatZone(low, high *float64) string {
	if low == nil || high == nil {
		return "—"
	}
	if *low == *high {
		return formatLevel(low)
	}
	return formatLevel(low) + "-" + formatLevel(high)
}

// formatLevel renders a price level, or an em dash when absent.
func formatLevel(value *float64) string {
	if value == nil {
		return "—"
	}
	return strconv.FormatFloat(*value, 'f', -1, 64)
}

// formatRatio renders a reward-to-risk ratio as N:1, or an em dash when absent.
func formatRatio(value *float64) string {
	if value == nil {
		return "—"
	}
	return fmt.Sprintf("%.2f:1", *value)
}
