package usecase

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// measurementReportForTest builds a real report through the engine, so the
// writer tests exercise the same shape the CLI writes.
func measurementReportForTest(t *testing.T) *MeasurementReport {
	t.Helper()
	anchor, _, screener, prices := measurementScenario()
	report, err := newMeasurementEngine(screener, prices).Measure(context.Background(), MeasurementOptions{AsOf: anchor})
	if err != nil {
		t.Fatalf("build report: %v", err)
	}
	return report
}

// TestMeasurementReport_JSONShape — the JSON artifact carries the fields the
// spec requires (as_of, params, revision, funnel) and the per-horizon stats,
// and round-trips back through encoding/json.
func TestMeasurementReport_JSONShape(t *testing.T) {
	report := measurementReportForTest(t)

	payload, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal object: %v", err)
	}
	for _, key := range []string{
		"as_of", "filter_set", "git_revision", "generated_at",
		"params", "funnel", "counts", "horizons", "survivors",
		"big_cap_basket", "flags", "success_criteria",
	} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("JSON missing required key %q; keys: %v", key, keysOf(decoded))
		}
	}

	// Forward returns are keyed by horizon-within-JSON, and a nil return stays
	// null rather than collapsing to 0.
	var survivor struct {
		Ticker      string              `json:"ticker"`
		Forward     map[string]*float64 `json:"forward_returns"`
		AnchorClose *float64            `json:"anchor_close"`
	}
	var survivors []json.RawMessage
	if err := json.Unmarshal(decoded["survivors"], &survivors); err != nil {
		t.Fatalf("unmarshal survivors: %v", err)
	}
	if len(survivors) != 2 {
		t.Fatalf("survivors = %d, want 2", len(survivors))
	}
	if err := json.Unmarshal(survivors[0], &survivor); err != nil {
		t.Fatalf("unmarshal survivor row: %v", err)
	}
	if survivor.Ticker != "AAA" || survivor.AnchorClose == nil || *survivor.AnchorClose != 1000 {
		t.Fatalf("survivor row = %+v, want AAA at 1000", survivor)
	}
	for _, key := range []string{"5", "10", "20"} {
		if _, ok := survivor.Forward[key]; !ok {
			t.Fatalf("forward_returns missing horizon %q; keys: %v", key, keysOfString(survivor.Forward))
		}
	}
	measurementApprox(t, "AAA 5d forward return", *survivor.Forward["5"], 0.10)
}

// TestMeasurementReport_MarkdownSummary — the markdown carries as_of, params,
// revision, funnel, discipline flags, and the spec's three success-criteria
// questions, so the loop's purpose is printed in every report.
func TestMeasurementReport_MarkdownSummary(t *testing.T) {
	report := measurementReportForTest(t)
	md := RenderMeasurementMarkdown(report)

	for _, want := range []string{
		"# Measurement report — default @ 2026-06-01",
		"Git revision:** `testrev`",
		"| Universe | 900 |",
		"| After hard filters (Liquidity Baseline) | 4 |",
		"| After structural filters (survivors) | 2 |",
		"## Forward returns (close-to-close)",
		"## Discipline flags",
		"1 replay date(s) < 3",
		"## Success criteria",
		"Do screener survivors beat the Liquidity Baseline at any horizon?",
		"Is edge front-loaded (<=2 days) or spread over 10-20d?",
		"Does broker/disclosure confirmation split Anomalies into different Forward Return populations?",
		"execution realism",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("markdown missing %q\n---\n%s", want, md)
		}
	}

	// Excess is rendered signed, and the 5d number is +5.00%.
	if !strings.Contains(md, "+5.00%") {
		t.Fatalf("markdown missing the signed 5d Excess:\n%s", md)
	}
}

// TestMeasurementReport_Artifact — the artifact's base name is
// <as-of>_<filter-set> and the JSON parses back into the report.
func TestMeasurementReport_Artifact(t *testing.T) {
	report := measurementReportForTest(t)

	artifact, err := report.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	if artifact.Base != "2026-06-01_default" {
		t.Fatalf("base = %s, want 2026-06-01_default", artifact.Base)
	}

	var decoded MeasurementReport
	if err := json.Unmarshal(artifact.JSON, &decoded); err != nil {
		t.Fatalf("reparse json: %v", err)
	}
	if decoded.AsOf != "2026-06-01" || decoded.FilterSet != "default" || decoded.GitRevision != "testrev" {
		t.Fatalf("round-tripped report = %+v", decoded)
	}

	if !strings.Contains(string(artifact.Markdown), "## Success criteria") {
		t.Fatalf("markdown summary is missing the success criteria:\n%s", artifact.Markdown)
	}
}

// TestMeasurementReport_FileNameSanitized — a filter-set name can never escape
// the reports prefix.
func TestMeasurementReport_FileNameSanitized(t *testing.T) {
	report := measurementReportForTest(t)
	report.FilterSet = "../escape"
	artifact, err := report.Artifact()
	if err != nil {
		t.Fatalf("Artifact: %v", err)
	}
	if strings.Contains(artifact.Base, "..") || strings.Contains(artifact.Base, "/") {
		t.Fatalf("base = %s, want a sanitized name", artifact.Base)
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func keysOfString(m map[string]*float64) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}
