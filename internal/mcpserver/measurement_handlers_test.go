package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeMeasurementHarness implements MeasurementHarness for handler tests: it
// records the inputs the handler built and returns canned outputs.
type fakeMeasurementHarness struct {
	screenOpts     usecase.ScreenGradeOptions
	anomalyOpts    usecase.AnomalyGradeOptions
	flowOpts       usecase.FlowGradeOptions
	disclosureOpts usecase.DisclosureGradeOptions
	journalOpts    usecase.JournalGradeOptions

	logVerdictIn    usecase.LogVerdictInput
	putDoc          usecase.MeasurementConfigDocument
	putExpectedHash string
	getReportBase   string

	gradeErr error

	configDoc  *usecase.MeasurementConfigDocument
	configHash string
	reports    []string
	reportDoc  *usecase.MeasurementReportDocument
}

func (f *fakeMeasurementHarness) GradeScreen(_ context.Context, opts usecase.ScreenGradeOptions) (*usecase.ScreenGradeReport, error) {
	f.screenOpts = opts
	if f.gradeErr != nil {
		return nil, f.gradeErr
	}
	return &usecase.ScreenGradeReport{Kind: "screen_grade"}, nil
}

func (f *fakeMeasurementHarness) GradeAnomalies(_ context.Context, opts usecase.AnomalyGradeOptions) (*usecase.AnomalyGradeReport, error) {
	f.anomalyOpts = opts
	if f.gradeErr != nil {
		return nil, f.gradeErr
	}
	return &usecase.AnomalyGradeReport{Kind: "anomaly_grade"}, nil
}

func (f *fakeMeasurementHarness) GradeFlow(_ context.Context, opts usecase.FlowGradeOptions) (*usecase.FlowGradeReport, error) {
	f.flowOpts = opts
	if f.gradeErr != nil {
		return nil, f.gradeErr
	}
	return &usecase.FlowGradeReport{Kind: "flow_grade"}, nil
}

func (f *fakeMeasurementHarness) GradeDisclosures(_ context.Context, opts usecase.DisclosureGradeOptions) (*usecase.DisclosureGradeReport, error) {
	f.disclosureOpts = opts
	if f.gradeErr != nil {
		return nil, f.gradeErr
	}
	return &usecase.DisclosureGradeReport{Kind: "disclosure_grade"}, nil
}

func (f *fakeMeasurementHarness) GradeJournal(_ context.Context, opts usecase.JournalGradeOptions) (*usecase.JournalGradeReport, error) {
	f.journalOpts = opts
	if f.gradeErr != nil {
		return nil, f.gradeErr
	}
	return &usecase.JournalGradeReport{Kind: "journal_grade"}, nil
}

func (f *fakeMeasurementHarness) LogVerdict(_ context.Context, in usecase.LogVerdictInput) (*usecase.LogVerdictResult, error) {
	f.logVerdictIn = in
	if f.gradeErr != nil {
		return nil, f.gradeErr
	}
	return &usecase.LogVerdictResult{ID: 42, Session: in.Session, Verdict: in.Verdict}, nil
}

func (f *fakeMeasurementHarness) GetConfig(_ context.Context) (*usecase.MeasurementConfigDocument, string, error) {
	if f.gradeErr != nil {
		return nil, "", f.gradeErr
	}
	return f.configDoc, f.configHash, nil
}

func (f *fakeMeasurementHarness) PutConfig(_ context.Context, doc usecase.MeasurementConfigDocument, expectedHash string) (string, error) {
	f.putDoc = doc
	f.putExpectedHash = expectedHash
	if f.gradeErr != nil {
		return "", f.gradeErr
	}
	return "newhash", nil
}

func (f *fakeMeasurementHarness) ListReports(_ context.Context) ([]string, error) {
	if f.gradeErr != nil {
		return nil, f.gradeErr
	}
	return f.reports, nil
}

func (f *fakeMeasurementHarness) GetReport(_ context.Context, base string) (*usecase.MeasurementReportDocument, error) {
	f.getReportBase = base
	if f.gradeErr != nil {
		return nil, f.gradeErr
	}
	return f.reportDoc, nil
}

func newMeasurementTestServer(h MeasurementHarness) *Server {
	return &Server{log: logrus.New(), measurementUC: h}
}

func measurementReq(name string, args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{Name: name, Arguments: args}}
}

// decodeResult unmarshals a successful text result body.
func decodeResult(t *testing.T, res *mcpgo.CallToolResult) map[string]any {
	t.Helper()
	if res.IsError {
		t.Fatalf("expected success, got error: %v", res.Content)
	}
	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want text", res.Content[0])
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return got
}

// decodeEnvelope asserts an error result and returns its envelope.
func decodeEnvelope(t *testing.T, res *mcpgo.CallToolResult) mcp.ErrorEnvelope {
	t.Helper()
	if !res.IsError {
		t.Fatal("expected an error result")
	}
	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want text", res.Content[0])
	}
	var got mcp.ErrorEnvelope
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	return got
}

func TestHandleMeasureScreen_ParsesArgs(t *testing.T) {
	fake := &fakeMeasurementHarness{}
	s := newMeasurementTestServer(fake)

	res, err := s.handleMeasureScreen(context.Background(), measurementReq("measure_screen", map[string]any{
		"as_of":         []any{"2026-05-01", "2026-06-01"},
		"filters":       "volume-tolerant",
		"compare":       "default",
		"holdout_start": "2026-04-01",
	}))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	decodeResult(t, res)

	if len(fake.screenOpts.Anchors) != 2 {
		t.Fatalf("anchors = %v", fake.screenOpts.Anchors)
	}
	if fake.screenOpts.FilterSet != "volume-tolerant" || fake.screenOpts.Compare != "default" {
		t.Fatalf("opts = %+v", fake.screenOpts)
	}
	if fake.screenOpts.HoldoutStart == nil || fake.screenOpts.HoldoutStart.Format("2006-01-02") != "2026-04-01" {
		t.Fatalf("holdout = %v", fake.screenOpts.HoldoutStart)
	}
}

func TestHandleMeasureScreen_RequiresAsOf(t *testing.T) {
	s := newMeasurementTestServer(&fakeMeasurementHarness{})
	res, err := s.handleMeasureScreen(context.Background(), measurementReq("measure_screen", map[string]any{}))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := decodeEnvelope(t, res); got.Error.Code != mcp.ErrorCodeInvalidArgument {
		t.Fatalf("code = %q, want INVALID_ARGUMENT", got.Error.Code)
	}
}

func TestHandleMeasureAnomalies_FlowAndDisclosures(t *testing.T) {
	fake := &fakeMeasurementHarness{}
	s := newMeasurementTestServer(fake)

	res, _ := s.handleMeasureAnomalies(context.Background(), measurementReq("measure_anomalies", map[string]any{
		"as_of": []any{"2026-05-01"}, "flow_window_days": float64(15),
	}))
	decodeResult(t, res)
	if fake.anomalyOpts.FlowWindowDays != 15 || len(fake.anomalyOpts.Anchors) != 1 {
		t.Fatalf("anomaly opts = %+v", fake.anomalyOpts)
	}

	res, _ = s.handleMeasureFlow(context.Background(), measurementReq("measure_flow", map[string]any{
		"as_of": []any{"2026-05-01"}, "flow_window_days": float64(10),
	}))
	decodeResult(t, res)
	if fake.flowOpts.FlowWindowDays != 10 {
		t.Fatalf("flow opts = %+v", fake.flowOpts)
	}

	res, _ = s.handleMeasureDisclosures(context.Background(), measurementReq("measure_disclosures", map[string]any{
		"as_of": []any{"2026-05-01"}, "reject_sample": float64(5),
	}))
	decodeResult(t, res)
	if fake.disclosureOpts.RejectSampleN != 5 {
		t.Fatalf("disclosure opts = %+v", fake.disclosureOpts)
	}
}

func TestHandleMeasureJournal_Window(t *testing.T) {
	fake := &fakeMeasurementHarness{}
	s := newMeasurementTestServer(fake)

	res, _ := s.handleMeasureJournal(context.Background(), measurementReq("measure_journal", map[string]any{
		"from": "2026-05-01", "until": "2026-06-01",
	}))
	decodeResult(t, res)
	if fake.journalOpts.From == nil || fake.journalOpts.Until == nil {
		t.Fatalf("journal opts = %+v", fake.journalOpts)
	}

	res, _ = s.handleMeasureJournal(context.Background(), measurementReq("measure_journal", map[string]any{
		"from": "2026-06-01", "until": "2026-05-01",
	}))
	if got := decodeEnvelope(t, res); got.Error.Code != mcp.ErrorCodeInvalidArgument {
		t.Fatalf("code = %q, want INVALID_ARGUMENT", got.Error.Code)
	}
}

func TestHandleLogVerdict_ParsesInput(t *testing.T) {
	fake := &fakeMeasurementHarness{}
	s := newMeasurementTestServer(fake)

	res, err := s.handleLogVerdict(context.Background(), measurementReq("log_verdict", map[string]any{
		"session": "run-1", "date": "2026-05-01", "ticker": "BBRI", "verdict": "SETUP",
		"entryLow": float64(100), "entryHigh": float64(105),
		"clLow": float64(90), "clHigh": float64(95),
		"tpLow": float64(130), "tpHigh": float64(135),
		"rationale": "breakout", "committed": true,
	}))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	got := decodeResult(t, res)
	if got["id"] != float64(42) {
		t.Fatalf("result = %v", got)
	}

	in := fake.logVerdictIn
	if in.Session != "run-1" || in.Ticker != "BBRI" || in.Verdict != "SETUP" || !in.Committed {
		t.Fatalf("input = %+v", in)
	}
	if in.EntryLow == nil || *in.EntryLow != 100 || in.TPHigh == nil || *in.TPHigh != 135 {
		t.Fatalf("zones = %+v", in)
	}
}

func TestHandleLogVerdict_ForwardsError(t *testing.T) {
	s := newMeasurementTestServer(&fakeMeasurementHarness{gradeErr: errors.New("boom")})
	res, _ := s.handleLogVerdict(context.Background(), measurementReq("log_verdict", map[string]any{"session": "s"}))
	if got := decodeEnvelope(t, res); got.Error.Code != mcp.ErrorCodeInternal {
		t.Fatalf("code = %q, want INTERNAL", got.Error.Code)
	}
}

func TestHandleGetScreenerConfig(t *testing.T) {
	fake := &fakeMeasurementHarness{
		configDoc: &usecase.MeasurementConfigDocument{Active: "volume-tolerant", Sets: map[string]usecase.MeasurementFilterSetFile{
			"volume-tolerant": {Description: "x"},
		}},
		configHash: "abc123",
	}
	s := newMeasurementTestServer(fake)
	res, err := s.handleGetScreenerConfig(context.Background(), measurementReq("get_screener_config", nil))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	got := decodeResult(t, res)
	if got["active"] != "volume-tolerant" || got["config_hash"] != "abc123" || got["published"] != true {
		t.Fatalf("result = %v", got)
	}
}

func TestHandlePutScreenerConfig(t *testing.T) {
	fake := &fakeMeasurementHarness{}
	s := newMeasurementTestServer(fake)
	res, err := s.handlePutScreenerConfig(context.Background(), measurementReq("put_screener_config", map[string]any{
		"active":        "default",
		"sets":          map[string]any{"default": map[string]any{"description": "v1"}},
		"expected_hash": "oldhash",
	}))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	got := decodeResult(t, res)
	if got["config_hash"] != "newhash" {
		t.Fatalf("result = %v", got)
	}
	if fake.putExpectedHash != "oldhash" || fake.putDoc.Active != "default" {
		t.Fatalf("captured doc=%+v expected=%q", fake.putDoc, fake.putExpectedHash)
	}
	if fake.putDoc.Sets["default"].Description != "v1" {
		t.Fatalf("sets = %+v", fake.putDoc.Sets)
	}
}

func TestHandleListMeasurementReports(t *testing.T) {
	s := newMeasurementTestServer(&fakeMeasurementHarness{reports: []string{"b", "a"}})
	res, _ := s.handleListMeasurementReports(context.Background(), measurementReq("list_measurement_reports", nil))
	got := decodeResult(t, res)
	reports, ok := got["reports"].([]any)
	if !ok || len(reports) != 2 || reports[0] != "b" {
		t.Fatalf("reports = %v", got["reports"])
	}
}

func TestHandleListMeasurementReports_EmptyIsArray(t *testing.T) {
	s := newMeasurementTestServer(&fakeMeasurementHarness{})
	res, _ := s.handleListMeasurementReports(context.Background(), measurementReq("list_measurement_reports", nil))
	text := res.Content[0].(mcpgo.TextContent).Text
	if text != `{"reports":[]}` {
		t.Fatalf("empty list = %s, want an empty array not null", text)
	}
}

func TestHandleGetMeasurementReport(t *testing.T) {
	fake := &fakeMeasurementHarness{reportDoc: &usecase.MeasurementReportDocument{
		Base:     "2026-05-01_default",
		Report:   json.RawMessage(`{"kind":"screen_grade"}`),
		Markdown: "# report",
	}}
	s := newMeasurementTestServer(fake)
	res, _ := s.handleGetMeasurementReport(context.Background(), measurementReq("get_measurement_report", map[string]any{"base": "2026-05-01_default"}))
	got := decodeResult(t, res)
	if fake.getReportBase != "2026-05-01_default" {
		t.Fatalf("base = %q", fake.getReportBase)
	}
	if got["base"] != "2026-05-01_default" || got["markdown"] != "# report" {
		t.Fatalf("result = %v", got)
	}
	report, ok := got["report"].(map[string]any)
	if !ok || report["kind"] != "screen_grade" {
		t.Fatalf("report = %v", got["report"])
	}
}

// TestMeasurementToolAnnotations — the graders and the read tools are read-only;
// the two writer tools are not (issue 12's rule: a write must not declare
// destructive=false via the read-only helper).
func TestMeasurementToolAnnotations(t *testing.T) {
	readOnly := func(tool mcpgo.Tool) bool {
		return tool.Annotations.ReadOnlyHint != nil && *tool.Annotations.ReadOnlyHint
	}
	for _, tool := range []mcpgo.Tool{
		toolMeasureScreen, toolMeasureAnomalies, toolMeasureFlow, toolMeasureDisclosures,
		toolMeasureJournal, toolGetScreenerConfig, toolListMeasurementReports, toolGetMeasurementReport,
	} {
		if !readOnly(tool) {
			t.Fatalf("%s should be read-only", tool.Name)
		}
	}
	for _, tool := range []mcpgo.Tool{toolLogVerdict, toolPutScreenerConfig} {
		if tool.Annotations.ReadOnlyHint == nil || *tool.Annotations.ReadOnlyHint {
			t.Fatalf("%s should be a write (readOnlyHint=false)", tool.Name)
		}
	}
}
