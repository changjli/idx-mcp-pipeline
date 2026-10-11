package usecase

import (
	"fmt"
	"strings"
)

// Artifact renders a Flow Grade as a ReportArtifact for the report store. The
// base name is <as-of>_flow for one anchor and <first>_to_<last>_flow for a
// range.
func (g *FlowGradeReport) Artifact() (ReportArtifact, error) {
	return newReportArtifact(g.reportBaseName(), g, RenderFlowGradeMarkdown(g))
}

// reportBaseName derives the filename stem from the Grade's date range.
func (g *FlowGradeReport) reportBaseName() string {
	name := g.AsOfStart
	if g.AsOfEnd != "" && g.AsOfEnd != g.AsOfStart {
		name += "_to_" + g.AsOfEnd
	}
	return name + "_flow"
}

// RenderFlowGradeMarkdown renders the human-readable summary: metadata, the
// population counts, the Sign Buckets, the signed magnitude terciles, the
// per-date breakdown, the discipline flags, and the flow success criteria.
func RenderFlowGradeMarkdown(g *FlowGradeReport) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Flow grade @ %s\n\n", flowAnchorLabel(g))
	fmt.Fprintf(&b, "- **Replay dates:** %s (%d)\n", strings.Join(g.ReplayDates, ", "), len(g.ReplayDates))
	fmt.Fprintf(&b, "- **Git revision:** `%s`\n", g.GitRevision)
	fmt.Fprintf(&b, "- **Generated:** %s\n", g.GeneratedAt)
	fmt.Fprintf(&b, "- **Flow window:** %d trading days ending on the anchor (%d covered in the shortest anchor windows)\n", g.FlowWindowDays, g.FlowDaysInWindow)
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

	b.WriteString("## Counts\n\n")
	b.WriteString("| Population | Count |\n|---|---:|\n")
	fmt.Fprintf(&b, "| Liquidity Baseline | %d |\n", g.Counts.Baseline)
	fmt.Fprintf(&b, "| Observed foreign net | %d |\n", g.Counts.Observed)
	fmt.Fprintf(&b, "| Unobserved | %d |\n", g.Counts.Unobserved)
	fmt.Fprintf(&b, "| Distinct observed tickers | %d |\n\n", g.Counts.DistinctObserved)

	b.WriteString("## Sign buckets\n\n")
	b.WriteString(flowBucketTable(g.SignBuckets))
	b.WriteString("\n")

	b.WriteString("## Magnitude terciles (signed, pooled)\n\n")
	b.WriteString(flowTercileTable(g.Magnitude))
	b.WriteString("\n")

	b.WriteString("## Per-date breakdown\n\n")
	b.WriteString("| Replay date | Baseline n | Observed | Flow days in window | positive 20d Excess | negative 20d Excess | unobserved 20d Excess |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|\n")
	for _, date := range g.PerDate {
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %s | %s | %s |\n",
			date.AsOf, date.Baseline, date.Observed, date.FlowDaysInWindow,
			bucketExcess(date.Buckets, flowSignPositive, 20),
			bucketExcess(date.Buckets, flowSignNegative, 20),
			bucketExcess(date.Buckets, flowSignUnobserved, 20))
	}
	b.WriteString("\n")

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

// flowAnchorLabel is the Grade's anchor display: the single date, or the range.
func flowAnchorLabel(g *FlowGradeReport) string {
	if g.AsOfEnd == "" || g.AsOfEnd == g.AsOfStart {
		return g.AsOfStart
	}
	return g.AsOfStart + " → " + g.AsOfEnd
}

// flowBucketTable renders the Sign Buckets: one row per state with each
// horizon's median and Excess side by side, plus the covered-day spread.
func flowBucketTable(buckets []FlowBucket) string {
	var b strings.Builder
	b.WriteString("| Sign | n | Covered days med (min–max) | 5d median | 5d Excess | 10d median | 10d Excess | 20d median | 20d Excess |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, bucket := range buckets {
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %s | %s | %s |\n",
			bucket.State, bucket.N, formatCoverage(bucket.Coverage),
			bucketStat(bucket.Horizons, 5, false), bucketStat(bucket.Horizons, 5, true),
			bucketStat(bucket.Horizons, 10, false), bucketStat(bucket.Horizons, 10, true),
			bucketStat(bucket.Horizons, 20, false), bucketStat(bucket.Horizons, 20, true))
	}
	return b.String()
}

// flowTercileTable renders the signed magnitude terciles with their net ranges.
func flowTercileTable(terciles []FlowTercile) string {
	var b strings.Builder
	b.WriteString("| Tercile | n | Net range | Covered days med (min–max) | 5d median | 5d Excess | 10d median | 10d Excess | 20d median | 20d Excess |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, tercile := range terciles {
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			tercile.Tercile, tercile.N, formatNetRange(tercile.MinNet, tercile.MaxNet), formatCoverage(tercile.Coverage),
			bucketStat(tercile.Horizons, 5, false), bucketStat(tercile.Horizons, 5, true),
			bucketStat(tercile.Horizons, 10, false), bucketStat(tercile.Horizons, 10, true),
			bucketStat(tercile.Horizons, 20, false), bucketStat(tercile.Horizons, 20, true))
	}
	return b.String()
}

// formatCoverage renders a bucket's covered-day spread, or an em dash when the
// bucket has no observed row.
func formatCoverage(coverage FlowCoverageSummary) string {
	if coverage.DaysMedian == nil {
		return "—"
	}
	return fmt.Sprintf("%.0f (%d–%d)", *coverage.DaysMedian, coverage.DaysMin, coverage.DaysMax)
}

// formatNetRange renders a tercile's observed net range — the boundary a
// prospective filter would cut on — or an em dash for an empty tercile.
func formatNetRange(min, max *int64) string {
	if min == nil || max == nil {
		return "—"
	}
	return fmt.Sprintf("%d … %d", *min, *max)
}

// bucketExcess finds one per-date sign bucket's Excess at a horizon.
func bucketExcess(buckets []FlowBucket, state string, horizon int) string {
	bucket := findFlowBucket(buckets, state)
	if bucket == nil {
		return "—"
	}
	return bucketStat(bucket.Horizons, horizon, true)
}
