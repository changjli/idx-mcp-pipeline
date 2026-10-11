package usecase

import (
	"fmt"
	"strings"
)

// Artifact renders a Disclosure Grade as a ReportArtifact for the report store.
// The base name is <as-of>_disclosures for one filing day and
// <first>_to_<last>_disclosures for a range.
func (g *DisclosureGradeReport) Artifact() (ReportArtifact, error) {
	return newReportArtifact(g.reportBaseName(), g, RenderDisclosureGradeMarkdown(g))
}

// reportBaseName derives the filename stem from the Grade's Filing Day range.
func (g *DisclosureGradeReport) reportBaseName() string {
	name := g.AsOfStart
	if g.AsOfEnd != "" && g.AsOfEnd != g.AsOfStart {
		name += "_to_" + g.AsOfEnd
	}
	return name + "_disclosures"
}

// RenderDisclosureGradeMarkdown renders the human-readable summary: metadata,
// the population counts, the passed-vs-baseline comparison, the category split,
// the per-date breakdown, the per-filing drill-down, the manual Reject Sampling
// section, the discipline flags, and the standing questions.
func RenderDisclosureGradeMarkdown(g *DisclosureGradeReport) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Disclosure grade @ %s\n\n", disclosureAnchorLabel(g))
	fmt.Fprintf(&b, "- **Filing days:** %s (%d)\n", strings.Join(g.ReplayDates, ", "), len(g.ReplayDates))
	fmt.Fprintf(&b, "- **Git revision:** `%s`\n", g.GitRevision)
	fmt.Fprintf(&b, "- **Generated:** %s\n", g.GeneratedAt)
	fmt.Fprintf(&b, "- **Disclosure lookback:** %d days (the 3-layer filter's anomaly gate)\n", g.DisclosureLookbackDays)
	if g.HoldoutStart != "" {
		fmt.Fprintf(&b, "- **Holdout start:** %s (sealed window: %t)\n", g.HoldoutStart, g.SealedWindow)
	}
	b.WriteString("\n")

	b.WriteString("## Counts\n\n")
	b.WriteString("| Population | Count |\n|---|---:|\n")
	fmt.Fprintf(&b, "| Liquidity Baseline | %d |\n", g.Counts.Baseline)
	fmt.Fprintf(&b, "| Passed ticker-days (observations) | %d |\n", g.Counts.Observations)
	fmt.Fprintf(&b, "| Passed disclosures (rows) | %d |\n", g.Counts.Disclosures)
	fmt.Fprintf(&b, "| Distinct tickers | %d |\n", g.Counts.DistinctTickers)
	fmt.Fprintf(&b, "| Fully unresolved observations | %d |\n", g.Counts.Unresolved)
	fmt.Fprintf(&b, "| Rejected pool (Reject Sample seed) | %d |\n\n", g.Counts.RejectedPool)

	b.WriteString("## Reaction vs Liquidity Baseline\n\n")
	b.WriteString(disclosureHorizonTable(g.Horizons))
	b.WriteString("\n")

	b.WriteString("## Category split (keyword whitelist)\n\n")
	if len(g.Categories) == 0 {
		b.WriteString("None.\n\n")
	} else {
		b.WriteString("| Category | n | 5d median | 5d Excess | 10d median | 10d Excess | 20d median | 20d Excess |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
		for _, bucket := range g.Categories {
			fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %s | %s |\n",
				bucket.Category, bucket.N,
				bucketStat(bucket.Horizons, 5, false), bucketStat(bucket.Horizons, 5, true),
				bucketStat(bucket.Horizons, 10, false), bucketStat(bucket.Horizons, 10, true),
				bucketStat(bucket.Horizons, 20, false), bucketStat(bucket.Horizons, 20, true))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Per-date breakdown\n\n")
	b.WriteString("| Filing date | Baseline n | Passed | Rejected | 5d Excess | 10d Excess | 20d Excess |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|\n")
	for _, date := range g.PerDate {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %s | %s | %s |\n",
			date.AsOf, date.Baseline, date.Passed, date.Rejected,
			perDateExcess(date.Horizons, 5), perDateExcess(date.Horizons, 10), perDateExcess(date.Horizons, 20))
	}
	b.WriteString("\n")

	b.WriteString("## Passed disclosures\n\n")
	if len(g.Rows) == 0 {
		b.WriteString("None.\n\n")
	} else {
		b.WriteString("| Ticker | Filing date | Reaction anchor | Categories | 5d | 10d | 20d | Unresolved | PDF |\n")
		b.WriteString("|---|---|---|---|---:|---:|---:|---|---|\n")
		for _, row := range g.Rows {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				row.Ticker, row.FilingDate, row.ReactionAnchor, categoryList(row.Categories),
				formatPercent(row.Returns[5]), formatPercent(row.Returns[10]), formatPercent(row.Returns[20]),
				unresolvedLabel(row.Unresolved), row.PDFURL)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Reject sampling (manual, monthly)\n\n")
	fmt.Fprintf(&b, "%s\n\n", g.RejectSampling.Procedure)
	fmt.Fprintf(&b, "- Sample size: **%d** of a %d-filing reject pool (%d drawn).\n\n",
		g.RejectSampling.N, g.RejectSampling.Pool, g.RejectSampling.Sampled)
	if len(g.RejectSampling.Rows) == 0 {
		b.WriteString("No rejected filings to sample.\n\n")
	} else {
		b.WriteString("Copy this table into the monthly note and fill the **Material?** column by hand (material / not).\n\n")
		b.WriteString("| Ticker | Filing date | Title | 5d | 10d | 20d | Material? | Note | PDF |\n")
		b.WriteString("|---|---|---|---:|---:|---:|---|---|---|\n")
		for _, row := range g.RejectSampling.Rows {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				row.Ticker, row.FilingDate, escapeCell(row.Title),
				formatPercent(row.Returns[5]), formatPercent(row.Returns[10]), formatPercent(row.Returns[20]),
				materialLabel(row.Material), row.Note, row.PDFURL)
		}
		b.WriteString("\n")
		b.WriteString("False-negative rate = material / N; record it here: **____**\n\n")
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

// disclosureAnchorLabel is the Grade's anchor display: the single date, or the
// range.
func disclosureAnchorLabel(g *DisclosureGradeReport) string {
	if g.AsOfEnd == "" || g.AsOfEnd == g.AsOfStart {
		return g.AsOfStart
	}
	return g.AsOfStart + " → " + g.AsOfEnd
}

// disclosureHorizonTable renders the passed-vs-baseline per-horizon stats,
// including each population's unresolved count so a horizon with no data is
// visible rather than a zero.
func disclosureHorizonTable(horizons []HorizonStats) string {
	var b strings.Builder
	b.WriteString("| Horizon | Passed median | Passed hit rate | Passed n | Passed unresolved | Baseline median | Baseline n | Excess | Small-n |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|:---:|\n")
	for _, stats := range horizons {
		fmt.Fprintf(&b, "| %dd | %s | %s | %d | %d | %s | %d | %s | %s |\n",
			stats.Horizon,
			formatPercent(stats.Survivor.Median), formatPercent(stats.Survivor.HitRate),
			stats.Survivor.N, stats.Survivor.Unresolved,
			formatPercent(stats.Baseline.Median), stats.Baseline.N,
			formatPercentSigned(stats.Excess), formatFlag(stats.SmallN))
	}
	return b.String()
}

// categoryList renders a row's categories, or an em dash when it carries none.
func categoryList(categories []string) string {
	if len(categories) == 0 {
		return "—"
	}
	return strings.Join(categories, ", ")
}

// unresolvedLabel renders a row's unresolved horizons, or an em dash when none.
func unresolvedLabel(horizons []int) string {
	if len(horizons) == 0 {
		return "—"
	}
	parts := make([]string, 0, len(horizons))
	for _, horizon := range horizons {
		parts = append(parts, fmt.Sprintf("%dd", horizon))
	}
	return strings.Join(parts, ", ")
}

// materialLabel renders a Reject Sample verdict slot: the agent fills it in by
// hand, so it stays an em dash until then.
func materialLabel(material *bool) string {
	if material == nil {
		return "—"
	}
	if *material {
		return "material"
	}
	return "not"
}

// escapeCell keeps a disclosure title from breaking the markdown table.
func escapeCell(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "|", "\\|")
}
