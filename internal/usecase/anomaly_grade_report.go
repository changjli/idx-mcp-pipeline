package usecase

import (
	"fmt"
	"strings"
)

// Artifact renders an Anomaly Grade as a ReportArtifact for the report store.
// The base name is <as-of>_anomalies for one anchor and
// <first>_to_<last>_anomalies for a range.
func (g *AnomalyGradeReport) Artifact() (ReportArtifact, error) {
	return newReportArtifact(g.reportBaseName(), g, RenderAnomalyGradeMarkdown(g))
}

// reportBaseName derives the filename stem from the Grade's date range.
func (g *AnomalyGradeReport) reportBaseName() string {
	name := g.AsOfStart
	if g.AsOfEnd != "" && g.AsOfEnd != g.AsOfStart {
		name += "_to_" + g.AsOfEnd
	}
	return name + "_anomalies"
}

// RenderAnomalyGradeMarkdown renders the human-readable summary: metadata, the
// population counts, each Direction Bucket with its nested confluence splits,
// the per-date breakdown, the discipline flags, and the spec's three questions.
func RenderAnomalyGradeMarkdown(g *AnomalyGradeReport) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Anomaly grade @ %s\n\n", anomalyAnchorLabel(g))
	fmt.Fprintf(&b, "- **Replay dates:** %s (%d)\n", strings.Join(g.ReplayDates, ", "), len(g.ReplayDates))
	fmt.Fprintf(&b, "- **Git revision:** `%s`\n", g.GitRevision)
	fmt.Fprintf(&b, "- **Generated:** %s\n", g.GeneratedAt)
	fmt.Fprintf(&b, "- **Flow lead-in window:** %d trading days ending %s\n", g.FlowWindowDays, g.BrokerWindowEnd)
	fmt.Fprintf(&b, "- **Disclosure lookback:** %d days\n", g.DisclosureLookbackDays)
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
	fmt.Fprintf(&b, "| Anomaly observations | %d |\n", g.Counts.Observations)
	fmt.Fprintf(&b, "| Distinct tickers | %d |\n", g.Counts.DistinctTickers)
	fmt.Fprintf(&b, "| Broker-unobserved | %d |\n\n", g.Counts.Unobserved)

	b.WriteString("## Direction buckets\n\n")
	for _, direction := range g.Directions {
		fmt.Fprintf(&b, "### %s (n=%d)\n\n", direction.Direction, direction.N)
		b.WriteString("Overall (survivor Forward Returns vs Liquidity Baseline):\n\n")
		b.WriteString(anomalyOverallTable(direction.Overall))
		b.WriteString("\n")
		if len(direction.Broker) > 0 {
			b.WriteString("Broker confirmation:\n\n")
			b.WriteString(anomalyBucketTable(direction.Broker))
			b.WriteString("\n")
		}
		if len(direction.Disclosure) > 0 {
			b.WriteString("Disclosure presence:\n\n")
			b.WriteString(anomalyBucketTable(direction.Disclosure))
			b.WriteString("\n")
		}
		if len(direction.Cross) > 0 {
			b.WriteString("Cross (observed-flow rows only):\n\n")
			b.WriteString(anomalyBucketTable(direction.Cross))
			b.WriteString("\n")
		}
	}

	b.WriteString("## Per-date breakdown\n\n")
	b.WriteString("| Replay date | Baseline n | Anomalies | up 20d Excess | down 20d Excess | volume_spike 20d Excess |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|\n")
	for _, date := range g.PerDate {
		fmt.Fprintf(&b, "| %s | %d | %d | %s | %s | %s |\n",
			date.AsOf, date.Baseline, date.Anomalies,
			directionExcess(date.Directions, directionUp, 20),
			directionExcess(date.Directions, directionDown, 20),
			directionExcess(date.Directions, directionVolumeSpike, 20))
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

// anomalyAnchorLabel is the Grade's anchor display: the single date, or the range.
func anomalyAnchorLabel(g *AnomalyGradeReport) string {
	if g.AsOfEnd == "" || g.AsOfEnd == g.AsOfStart {
		return g.AsOfStart
	}
	return g.AsOfStart + " → " + g.AsOfEnd
}

// anomalyOverallTable renders one direction's overall per-horizon stats.
func anomalyOverallTable(horizons []HorizonStats) string {
	var b strings.Builder
	b.WriteString("| Horizon | Median | Hit rate | n | Baseline median | Excess | Small-n |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|:---:|\n")
	for _, stats := range horizons {
		fmt.Fprintf(&b, "| %dd | %s | %s | %d | %s | %s | %s |\n",
			stats.Horizon,
			formatPercent(stats.Survivor.Median),
			formatPercent(stats.Survivor.HitRate),
			stats.Survivor.N,
			formatPercent(stats.Baseline.Median),
			formatPercentSigned(stats.Excess),
			formatFlag(stats.SmallN))
	}
	return b.String()
}

// anomalyBucketTable renders a split's buckets compactly: one row per state,
// with each horizon's median and Excess side by side.
func anomalyBucketTable(buckets []AnomalyBucket) string {
	var b strings.Builder
	b.WriteString("| State | n | 5d median | 5d Excess | 10d median | 10d Excess | 20d median | 20d Excess |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, bucket := range buckets {
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %s | %s |\n",
			bucket.State, bucket.N,
			bucketStat(bucket.Horizons, 5, false), bucketStat(bucket.Horizons, 5, true),
			bucketStat(bucket.Horizons, 10, false), bucketStat(bucket.Horizons, 10, true),
			bucketStat(bucket.Horizons, 20, false), bucketStat(bucket.Horizons, 20, true))
	}
	return b.String()
}

// bucketStat finds a horizon's median (excess=false) or Excess (excess=true).
func bucketStat(horizons []HorizonStats, horizon int, excess bool) string {
	for _, stats := range horizons {
		if stats.Horizon != horizon {
			continue
		}
		if excess {
			return formatPercentSigned(stats.Excess)
		}
		return formatPercent(stats.Survivor.Median)
	}
	return "—"
}

// directionExcess finds one per-date direction block's Excess at a horizon.
func directionExcess(directions []DirectionExcess, direction string, horizon int) string {
	for _, block := range directions {
		if block.Direction != direction {
			continue
		}
		return bucketStat(block.Horizons, horizon, true)
	}
	return "—"
}
