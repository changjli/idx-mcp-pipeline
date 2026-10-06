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
