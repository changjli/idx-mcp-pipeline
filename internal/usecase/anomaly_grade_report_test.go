package usecase

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestAnomalyGradeReport_Artifact — a single-anchor Anomaly Grade renders
// <as-of>_anomalies, the JSON round-trips with its direction blocks, and the
// markdown carries the counts, all three direction sections, and the
// confluence splits.
func TestAnomalyGradeReport_Artifact(t *testing.T) {
	anchor, _, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	grade, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
		GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	if artifact.Base != "2026-06-01_anomalies" {
		t.Fatalf("base = %s, want 2026-06-01_anomalies", artifact.Base)
	}

	payload := artifact.JSON
	var decoded AnomalyGradeReport
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("reparse json: %v", err)
	}
	if decoded.Kind != anomalyGradeKind || decoded.GitRevision != "testrev" {
		t.Fatalf("round-tripped grade = %+v", decoded)
	}
	if decoded.BrokerWindowEnd != brokerWindowEndLabel || decoded.DisclosureLookbackDays != 7 {
		t.Fatalf("window labels = %s/%d, want %s/7", decoded.BrokerWindowEnd, decoded.DisclosureLookbackDays, brokerWindowEndLabel)
	}
	if len(decoded.Directions) != 3 || len(decoded.Directions[0].Broker) == 0 {
		t.Fatalf("direction blocks = %+v, want three with broker splits", decoded.Directions)
	}

	if !strings.Contains(string(payload), `"broker_unobserved": 1`) {
		t.Fatalf("JSON missing the unobserved count:\n%s", payload)
	}

	md := string(artifact.Markdown)
	for _, want := range []string{
		"# Anomaly grade @ 2026-06-01",
		"## Counts",
		"### up (n=2)",
		"Broker confirmation:",
		"Disclosure presence:",
		"Cross (observed-flow rows only):",
		"## Per-date breakdown",
		"## Success criteria",
		"## Notes",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q\n---\n%s", want, md)
		}
	}
}

// TestAnomalyGradeReport_RangeFilename — a multi-anchor Grade's filename spans
// the first and last Replay Date, and the anchor label renders as a range.
func TestAnomalyGradeReport_RangeFilename(t *testing.T) {
	anchor, days, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	second := days[30]
	anomalies.byDate[second.Format("2006-01-02")] = nil

	grade, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
		GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor, second}})
	if err != nil {
		t.Fatalf("build grade: %v", err)
	}

	artifact, err := grade.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	want := "2026-06-01_to_2026-07-01_anomalies"
	if artifact.Base != want {
		t.Fatalf("base = %s, want %s", artifact.Base, want)
	}
	if !strings.Contains(RenderAnomalyGradeMarkdown(grade), "2026-06-01 → 2026-07-01") {
		t.Fatalf("range label missing:\n%s", RenderAnomalyGradeMarkdown(grade))
	}
}
