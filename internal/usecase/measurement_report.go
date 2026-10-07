package usecase

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MeasurementReportDir is where reports land, relative to the repo root. Reports
// are git-tracked primary evidence (spec: a deliberate exception to the repo's
// no-artifacts-in-git pattern) because they snapshot filter sets that will since
// have changed and cannot be regenerated.
const MeasurementReportDir = "measurement/reports"

// WriteFiles renders the report as JSON and markdown under dir, creating the
// directory if needed, and returns the two paths. Filenames are
// <as-of>_<filter-set>.{json,md} so a report is addressable from its own name.
func (r *MeasurementReport) WriteFiles(dir string) (string, string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("create report directory %s: %w", dir, err)
	}

	base := fmt.Sprintf("%s_%s", r.AsOf, sanitizeReportName(r.FilterSet))
	jsonPath := filepath.Join(dir, base+".json")
	mdPath := filepath.Join(dir, base+".md")

	payload, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("marshal measurement report: %w", err)
	}
	payload = append(payload, '\n')

	if err := os.WriteFile(jsonPath, payload, 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", jsonPath, err)
	}
	if err := os.WriteFile(mdPath, []byte(RenderMeasurementMarkdown(r)), 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", mdPath, err)
	}
	return jsonPath, mdPath, nil
}

// sanitizeReportName keeps a filter-set name safe as a filename segment. Names
// are registry keys, but a stray path separator must never escape the reports
// directory.
func sanitizeReportName(name string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", " ", "_", "..", "_")
	cleaned := replacer.Replace(strings.TrimSpace(name))
	if cleaned == "" {
		return "unnamed"
	}
	return cleaned
}

// RenderMeasurementMarkdown renders the human-readable summary. It restates the
// anchor, params, revision, and funnel, prints the per-horizon comparison, the
// discipline flags, and the spec's three success-criteria questions, so a reader
// never has to reconstruct the loop's purpose from the numbers.
func RenderMeasurementMarkdown(r *MeasurementReport) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Measurement report — %s @ %s\n\n", r.FilterSet, r.AsOf)

	fmt.Fprintf(&b, "- **As of:** %s\n", r.AsOf)
	fmt.Fprintf(&b, "- **Filter set:** `%s`", r.FilterSet)
	if r.FilterSetDescription != "" {
		fmt.Fprintf(&b, " — %s", r.FilterSetDescription)
	}
	b.WriteString("\n")
	if r.HoldoutStart != "" {
		fmt.Fprintf(&b, "- **Holdout start:** %s (sealed window: %t)\n", r.HoldoutStart, r.SealedWindow)
	}
	fmt.Fprintf(&b, "- **Replay dates:** %s (%d)\n", strings.Join(r.ReplayDates, ", "), len(r.ReplayDates))
	fmt.Fprintf(&b, "- **Git revision:** `%s`\n", r.GitRevision)
	fmt.Fprintf(&b, "- **Generated:** %s\n\n", r.GeneratedAt)

	b.WriteString("## Funnel\n\n")
	b.WriteString("| Stage | Count |\n|---|---:|\n")
	fmt.Fprintf(&b, "| Universe | %d |\n", r.Funnel.Universe)
	fmt.Fprintf(&b, "| After hard filters (Liquidity Baseline) | %d |\n", r.Funnel.AfterValue)
	fmt.Fprintf(&b, "| After structural filters (survivors) | %d |\n\n", r.Funnel.AfterStructure)

	b.WriteString("## Forward returns (close-to-close)\n\n")
	b.WriteString("| Horizon | Survivor median | Survivor hit rate | Survivor n | Baseline median | Baseline n | Excess | Small-n |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|:---:|\n")
	for _, stats := range r.Horizons {
		fmt.Fprintf(&b, "| %dd | %s | %s | %d | %s | %d | %s | %s |\n",
			stats.Horizon,
			formatPercent(stats.Survivor.Median),
			formatPercent(stats.Survivor.HitRate),
			stats.Survivor.N,
			formatPercent(stats.Baseline.Median),
			stats.Baseline.N,
			formatPercentSigned(stats.Excess),
			formatFlag(stats.SmallN),
		)
	}
	b.WriteString("\n")

	b.WriteString("## Big-Cap Basket (secondary reference)\n\n")
	b.WriteString("| Horizon | Median | Hit rate | n |\n|---|---:|---:|---:|\n")
	for _, stats := range r.Horizons {
		fmt.Fprintf(&b, "| %dd | %s | %s | %d |\n",
			stats.Horizon, formatPercent(stats.BigCap.Median), formatPercent(stats.BigCap.HitRate), stats.BigCap.N)
	}
	b.WriteString("\n")

	b.WriteString("## Discipline flags\n\n")
	if len(r.Flags) == 0 {
		b.WriteString("None.\n\n")
	} else {
		for _, flag := range r.Flags {
			fmt.Fprintf(&b, "- %s\n", flag)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Success criteria\n\n")
	for i, criterion := range r.SuccessCriteria {
		fmt.Fprintf(&b, "%d. **%s** %s\n", i+1, criterion.Question, criterion.Answer)
	}
	b.WriteString("\n")

	if len(r.Notes) > 0 {
		b.WriteString("## Notes\n\n")
		for _, note := range r.Notes {
			fmt.Fprintf(&b, "- %s\n", note)
		}
		b.WriteString("\n")
	}

	return b.String()
}

// formatPercent renders a return/hit-rate fraction as a percentage, or an em
// dash when nil — a missing number never reads as 0%.
func formatPercent(value *float64) string {
	if value == nil {
		return "—"
	}
	return fmt.Sprintf("%.2f%%", *value*100)
}

// formatPercentSigned renders a signed percentage, or an em dash when nil.
func formatPercentSigned(value *float64) string {
	if value == nil {
		return "—"
	}
	return fmt.Sprintf("%+.2f%%", *value*100)
}

func formatFlag(flag bool) string {
	if flag {
		return "yes"
	}
	return "no"
}

// WriteFiles renders a Grade as JSON and markdown under dir, creating the
// directory if needed, and returns the two paths. The base name is
// <as-of>_<set> for one anchor and <first>_to_<last>_<set> for a range, with
// `_vs_<other>` appended when the run compares two sets — so a tuning decision
// is a readable diff between two reports.
func (g *ScreenGradeReport) WriteFiles(dir string) (string, string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("create report directory %s: %w", dir, err)
	}

	base := gradeReportBaseName(g)
	jsonPath := filepath.Join(dir, base+".json")
	mdPath := filepath.Join(dir, base+".md")

	payload, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("marshal screen grade report: %w", err)
	}
	payload = append(payload, '\n')

	if err := os.WriteFile(jsonPath, payload, 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", jsonPath, err)
	}
	if err := os.WriteFile(mdPath, []byte(RenderScreenGradeMarkdown(g)), 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", mdPath, err)
	}
	return jsonPath, mdPath, nil
}

// gradeReportBaseName derives the report filename stem from the Grade's dates,
// primary set, and comparison set.
func gradeReportBaseName(g *ScreenGradeReport) string {
	name := g.AsOfStart
	if g.AsOfEnd != "" && g.AsOfEnd != g.AsOfStart {
		name += "_to_" + g.AsOfEnd
	}
	set := "unnamed"
	if len(g.FilterSets) > 0 {
		set = sanitizeReportName(g.FilterSets[0].FilterSet)
	}
	base := name + "_" + set
	if g.Comparison != nil {
		base += "_vs_" + sanitizeReportName(g.Comparison.B)
	}
	return base
}

// RenderScreenGradeMarkdown renders the human-readable Grade: the run metadata,
// a per-date breakdown per filter set, the comparison table when two sets were
// asked for, and then each set's full report verbatim.
func RenderScreenGradeMarkdown(g *ScreenGradeReport) string {
	var b strings.Builder

	title := "unnamed"
	if len(g.FilterSets) > 0 {
		title = g.FilterSets[0].FilterSet
	}
	if g.Comparison != nil {
		title += " vs " + g.Comparison.B
	}

	fmt.Fprintf(&b, "# Screen grade — %s @ %s\n\n", title, gradeAnchorLabel(g))
	fmt.Fprintf(&b, "- **Replay dates:** %s (%d)\n", strings.Join(g.ReplayDates, ", "), len(g.ReplayDates))
	fmt.Fprintf(&b, "- **Git revision:** `%s`\n", g.GitRevision)
	fmt.Fprintf(&b, "- **Generated:** %s\n", g.GeneratedAt)
	if g.HoldoutStart != "" {
		fmt.Fprintf(&b, "- **Holdout start:** %s (sealed window: %t)\n", g.HoldoutStart, g.SealedWindow)
	}
	if len(g.SkippedAnchors) == 0 {
		b.WriteString("- **Skipped anchors:** none\n")
	} else {
		b.WriteString("- **Skipped anchors:**\n")
		for _, skipped := range g.SkippedAnchors {
			fmt.Fprintf(&b, "  - %s — %s\n", skipped.AsOf, skipped.Reason)
		}
	}
	b.WriteString("\n")

	b.WriteString("## Per-date breakdown\n\n")
	for _, set := range g.FilterSets {
		fmt.Fprintf(&b, "### %s\n\n", set.FilterSet)
		b.WriteString("| Replay date | Universe | Baseline n | Survivors n | 5d Excess | 10d Excess | 20d Excess |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|\n")
		for _, date := range set.PerDate {
			fmt.Fprintf(&b, "| %s | %d | %d | %d | %s | %s | %s |\n",
				date.AsOf, date.Counts.Universe, date.Counts.Baseline, date.Counts.Survivors,
				perDateExcess(date.Horizons, 5), perDateExcess(date.Horizons, 10), perDateExcess(date.Horizons, 20))
		}
		b.WriteString("\n")
	}

	if g.Comparison != nil {
		b.WriteString("## Comparison (Excess Delta)\n\n")
		fmt.Fprintf(&b, "A = `%s`, B = `%s`; like-for-like: %t\n\n", g.Comparison.A, g.Comparison.B, g.Comparison.LikeForLike)
		for _, note := range g.Comparison.Notes {
			fmt.Fprintf(&b, "- %s\n", note)
		}
		if len(g.Comparison.Notes) > 0 {
			b.WriteString("\n")
		}
		b.WriteString("| Horizon | A Excess | B Excess | Excess Delta | Small-n |\n")
		b.WriteString("|---|---:|---:|---:|:---:|\n")
		for _, delta := range g.Comparison.Horizons {
			fmt.Fprintf(&b, "| %dd | %s | %s | %s | %s |\n",
				delta.Horizon,
				formatPercentSigned(delta.ExcessA), formatPercentSigned(delta.ExcessB),
				formatPercentSigned(delta.ExcessDelta), formatFlag(delta.SmallN))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Filter sets\n\n")
	for i := range g.FilterSets {
		b.WriteString(RenderMeasurementMarkdown(&g.FilterSets[i]))
		b.WriteString("\n")
	}
	return b.String()
}

// gradeAnchorLabel is the Grade's anchor display: the single date, or the range.
func gradeAnchorLabel(g *ScreenGradeReport) string {
	if g.AsOfEnd == "" || g.AsOfEnd == g.AsOfStart {
		return g.AsOfStart
	}
	return g.AsOfStart + " → " + g.AsOfEnd
}

// perDateExcess finds one horizon's Excess in a per-date block.
func perDateExcess(horizons []HorizonStats, horizon int) string {
	for _, stats := range horizons {
		if stats.Horizon == horizon {
			return formatPercentSigned(stats.Excess)
		}
	}
	return "—"
}
