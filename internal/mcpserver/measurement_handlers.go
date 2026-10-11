package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// MeasurementHarness is the MCP-facing surface of the shared Measurement
// Harness (ADR-0017). Defined consumer-side so handler tests inject a fake; the
// concrete *usecase.MeasurementHarness satisfies it.
type MeasurementHarness interface {
	GradeScreen(ctx context.Context, opts usecase.ScreenGradeOptions) (*usecase.ScreenGradeReport, error)
	GradeAnomalies(ctx context.Context, opts usecase.AnomalyGradeOptions) (*usecase.AnomalyGradeReport, error)
	GradeFlow(ctx context.Context, opts usecase.FlowGradeOptions) (*usecase.FlowGradeReport, error)
	GradeDisclosures(ctx context.Context, opts usecase.DisclosureGradeOptions) (*usecase.DisclosureGradeReport, error)
	GradeJournal(ctx context.Context, opts usecase.JournalGradeOptions) (*usecase.JournalGradeReport, error)
	LogVerdict(ctx context.Context, in usecase.LogVerdictInput) (*usecase.LogVerdictResult, error)
	GetConfig(ctx context.Context) (*usecase.MeasurementConfigDocument, string, error)
	PutConfig(ctx context.Context, doc usecase.MeasurementConfigDocument, expectedHash string) (string, error)
	ListReports(ctx context.Context) ([]string, error)
	GetReport(ctx context.Context, base string) (*usecase.MeasurementReportDocument, error)
}

func (s *Server) handleMeasureScreen(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	anchors, err := measurementAnchors(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	holdout, err := measurementHoldout(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	filters, _ := args["filters"].(string)
	compare, _ := args["compare"].(string)

	report, err := s.measurementUC.GradeScreen(ctx, usecase.ScreenGradeOptions{
		Anchors:      anchors,
		FilterSet:    filters,
		Compare:      compare,
		HoldoutStart: holdout,
	})
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(report), nil
}

func (s *Server) handleMeasureAnomalies(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	anchors, err := measurementAnchors(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	holdout, err := measurementHoldout(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}

	report, err := s.measurementUC.GradeAnomalies(ctx, usecase.AnomalyGradeOptions{
		Anchors:        anchors,
		FlowWindowDays: argInt(args, "flow_window_days"),
		HoldoutStart:   holdout,
	})
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(report), nil
}

func (s *Server) handleMeasureFlow(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	anchors, err := measurementAnchors(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	holdout, err := measurementHoldout(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}

	report, err := s.measurementUC.GradeFlow(ctx, usecase.FlowGradeOptions{
		Anchors:        anchors,
		FlowWindowDays: argInt(args, "flow_window_days"),
		HoldoutStart:   holdout,
	})
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(report), nil
}

func (s *Server) handleMeasureDisclosures(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	anchors, err := measurementAnchors(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	holdout, err := measurementHoldout(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}

	report, err := s.measurementUC.GradeDisclosures(ctx, usecase.DisclosureGradeOptions{
		Anchors:       anchors,
		RejectSampleN: argInt(args, "reject_sample"),
		HoldoutStart:  holdout,
	})
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(report), nil
}

func (s *Server) handleMeasureJournal(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	opts, err := measurementJournalOptions(req.GetArguments())
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	report, err := s.measurementUC.GradeJournal(ctx, opts)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(report), nil
}

func (s *Server) handleLogVerdict(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	result, err := s.measurementUC.LogVerdict(ctx, usecase.LogVerdictInput{
		Session:    stringArg(args, "session"),
		Date:       stringArg(args, "date"),
		Ticker:     stringArg(args, "ticker"),
		Verdict:    stringArg(args, "verdict"),
		Rationale:  stringArg(args, "rationale"),
		EntryLow:   floatArg(args, "entryLow"),
		EntryHigh:  floatArg(args, "entryHigh"),
		CLLow:      floatArg(args, "clLow"),
		CLHigh:     floatArg(args, "clHigh"),
		TPLow:      floatArg(args, "tpLow"),
		TPHigh:     floatArg(args, "tpHigh"),
		Supersedes: int64Arg(args, "supersedes"),
		Committed:  boolArg(args, "committed"),
	})
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(result), nil
}

// screenerConfigResponse is the get_screener_config payload: the Config
// Document plus the hash that addresses its stored version.
type screenerConfigResponse struct {
	Active     string                                      `json:"active"`
	Sets       map[string]usecase.MeasurementFilterSetFile `json:"sets"`
	ConfigHash string                                      `json:"config_hash"`
	Published  bool                                        `json:"published"`
}

func (s *Server) handleGetScreenerConfig(ctx context.Context, _ mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	doc, hash, err := s.measurementUC.GetConfig(ctx)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(screenerConfigResponse{
		Active:     doc.Active,
		Sets:       doc.Sets,
		ConfigHash: hash,
		Published:  hash != "",
	}), nil
}

func (s *Server) handlePutScreenerConfig(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	doc, err := configDocumentFromArgs(args)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	hash, err := s.measurementUC.PutConfig(ctx, *doc, stringArg(args, "expected_hash"))
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(map[string]any{"config_hash": hash}), nil
}

type measurementReportListResponse struct {
	Reports []string `json:"reports"`
}

func (s *Server) handleListMeasurementReports(ctx context.Context, _ mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	bases, err := s.measurementUC.ListReports(ctx)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	if bases == nil {
		bases = []string{}
	}
	return textResult(measurementReportListResponse{Reports: bases}), nil
}

func (s *Server) handleGetMeasurementReport(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	doc, err := s.measurementUC.GetReport(ctx, stringArg(req.GetArguments(), "base"))
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(doc), nil
}

// ─── argument helpers ──────────────────────────────────────────────────────

// measurementAnchors parses the as_of argument, which may be a single date or a
// list of dates. An empty or missing value is the required-argument error the
// graders' own validation also raises.
func measurementAnchors(args map[string]any) ([]time.Time, error) {
	values := make([]string, 0)
	switch raw := args["as_of"].(type) {
	case string:
		if strings.TrimSpace(raw) != "" {
			values = append(values, raw)
		}
	case []any:
		for _, item := range raw {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				values = append(values, s)
			}
		}
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: as_of is required (YYYY-MM-DD)", usecase.ErrInvalidArgument)
	}
	anchors := make([]time.Time, 0, len(values))
	for _, raw := range values {
		day, err := parseMeasurementDay(raw, "as_of")
		if err != nil {
			return nil, err
		}
		anchors = append(anchors, day)
	}
	return anchors, nil
}

// measurementHoldout parses the optional holdout_start seal.
func measurementHoldout(args map[string]any) (*time.Time, error) {
	value := stringArg(args, "holdout_start")
	if value == "" {
		return nil, nil
	}
	day, err := parseMeasurementDay(value, "holdout_start")
	if err != nil {
		return nil, err
	}
	return &day, nil
}

// measurementJournalOptions parses the optional from/until decision-date window.
func measurementJournalOptions(args map[string]any) (usecase.JournalGradeOptions, error) {
	var opts usecase.JournalGradeOptions
	if value := stringArg(args, "from"); value != "" {
		day, err := parseMeasurementDay(value, "from")
		if err != nil {
			return opts, err
		}
		opts.From = &day
	}
	if value := stringArg(args, "until"); value != "" {
		day, err := parseMeasurementDay(value, "until")
		if err != nil {
			return opts, err
		}
		opts.Until = &day
	}
	if opts.From != nil && opts.Until != nil && opts.From.After(*opts.Until) {
		return opts, fmt.Errorf("%w: from %s is after until %s", usecase.ErrInvalidArgument,
			opts.From.Format("2006-01-02"), opts.Until.Format("2006-01-02"))
	}
	return opts, nil
}

// parseMeasurementDay parses a YYYY-MM-DD argument, wrapping the failure in the
// structured invalid-argument error the MCP envelope maps to INVALID_ARGUMENT.
func parseMeasurementDay(value, name string) (time.Time, error) {
	day, err := time.Parse("2006-01-02", strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s must be YYYY-MM-DD, got %q", usecase.ErrInvalidArgument, name, value)
	}
	return day, nil
}

// configDocumentFromArgs decodes the put_screener_config document from its two
// arguments, strictly (a mistyped knob is an error). expected_hash is a tool
// argument, not part of the document, so only active/sets are decoded.
func configDocumentFromArgs(args map[string]any) (*usecase.MeasurementConfigDocument, error) {
	docArgs := map[string]any{}
	for _, key := range []string{"active", "sets"} {
		if value, ok := args[key]; ok {
			docArgs[key] = value
		}
	}
	raw, err := json.Marshal(docArgs)
	if err != nil {
		return nil, fmt.Errorf("%w: encode config document: %v", usecase.ErrInvalidArgument, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var doc usecase.MeasurementConfigDocument
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("%w: %v", usecase.ErrInvalidArgument, err)
	}
	return &doc, nil
}

func stringArg(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return strings.TrimSpace(value)
}

func boolArg(args map[string]any, key string) bool {
	value, _ := args[key].(bool)
	return value
}

func argInt(args map[string]any, key string) int {
	if value, ok := args[key].(float64); ok {
		return int(value)
	}
	return 0
}

func floatArg(args map[string]any, key string) *float64 {
	value, ok := args[key].(float64)
	if !ok {
		return nil
	}
	return &value
}

func int64Arg(args map[string]any, key string) *int64 {
	value, ok := args[key].(float64)
	if !ok || value != float64(int64(value)) {
		return nil
	}
	n := int64(value)
	return &n
}
