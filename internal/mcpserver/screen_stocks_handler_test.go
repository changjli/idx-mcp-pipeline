package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeScreenStocksReader implements usecase.ScreenStocksReader for handler
// tests; it records the request the handler built.
type fakeScreenStocksReader struct {
	data *usecase.ScreenStocksResponse
	err  error
	got  usecase.ScreenStocksRequest
}

func (f *fakeScreenStocksReader) ScreenStocks(ctx context.Context, req usecase.ScreenStocksRequest) (*usecase.ScreenStocksResponse, error) {
	f.got = req
	return f.data, f.err
}

func newScreenStocksTestServer(reader usecase.ScreenStocksReader) *Server {
	return &Server{
		log:            logrus.New(),
		screenStocksUC: reader,
		tickers:        NewTickerValidator(nil, nil, logrus.New()),
	}
}

func screenStocksReq(args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "screen_stocks",
		Arguments: args,
	}}
}

// TestHandleScreenStocksSuccess — the happy path: every optional argument
// reaches the usecase as parsed, and the response carries the funnel, the rows,
// and the staleness envelope.
func TestHandleScreenStocksSuccess(t *testing.T) {
	value := int64(812_000_000_000)
	closePrice := 4850.0
	rsi := 61.2
	reader := &fakeScreenStocksReader{data: &usecase.ScreenStocksResponse{
		AsOf:                 "2026-09-11",
		Window:               120,
		MinValue:             2_000_000_000,
		SuspensionWindowDays: 3,
		Indicators:           []string{"rsi:14"},
		Filters: []usecase.ScreenStocksFilter{
			{Indicator: "rsi:14", Op: "between", Value: usecase.FilterValue{30, 70}},
		},
		Sort:         "rsi:14",
		Order:        "asc",
		Limit:        5,
		Funnel:       usecase.ScreenStocksFunnel{Universe: 900, AfterValue: 2, AfterStructure: 1},
		TotalMatches: 2,
		Count:        1,
		Rows: []usecase.ScreenStocksRow{{
			Ticker:       "BBRI",
			Value:        &value,
			Close:        &closePrice,
			Values:       map[string]*float64{"rsi:14": &rsi},
			Insufficient: []string{},
			HistoryRows:  120,
			RequiredRows: 15,
		}},
	}}
	s := newScreenStocksTestServer(reader)

	res, err := s.handleScreenStocks(context.Background(), screenStocksReq(map[string]any{
		"as_of":                  "2026-09-11",
		"window":                 float64(120),
		"min_value":              float64(2_000_000_000),
		"suspension_window_days": float64(3),
		"indicators":             []any{"rsi:14"},
		"sort":                   "rsi:14",
		"order":                  "asc",
		"limit":                  float64(5),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error result: %v", res.Content)
	}

	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T", res.Content[0])
	}
	var got screenStocksResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, text.Text)
	}

	if got.AsOf != "2026-09-11" || got.Window != 120 || got.Sort != "rsi:14" || got.Order != "asc" {
		t.Fatalf("envelope = %+v, want the request's parameters echoed", got)
	}
	if got.Funnel.Universe != 900 || got.Funnel.AfterValue != 2 || got.Funnel.AfterStructure != 1 {
		t.Fatalf("funnel = %+v, want universe 900 / after_value 2 / after_structure 1", got.Funnel)
	}
	// The filters echo back in the shape the caller wrote them, band and all.
	if len(got.Filters) != 1 || got.Filters[0].Op != "between" || len(got.Filters[0].Value) != 2 {
		t.Fatalf("filters = %+v, want the applied filter echoed", got.Filters)
	}
	if got.TotalMatches != 2 || got.Count != 1 || len(got.Rows) != 1 {
		t.Fatalf("counts = %d/%d/%d, want total 2 / count 1 / one row", got.TotalMatches, got.Count, len(got.Rows))
	}
	if got.Rows[0].Ticker != "BBRI" || got.Rows[0].Value == nil || *got.Rows[0].Value != value {
		t.Fatalf("row = %+v, want BBRI with its anchor-day value", got.Rows[0])
	}
	// The envelope is the tool's contract: no DB is wired here, so staleness
	// reports stale rather than omitting the fields.
	if !got.DataStale {
		t.Fatalf("data_stale = false, want true for an unwired DB")
	}

	// What the handler handed the usecase: parsed values, no silent defaults.
	if reader.got.Window != 120 || reader.got.Limit != 5 || reader.got.SuspensionWindowDays != 3 {
		t.Fatalf("request = %+v, want window 120 / limit 5 / suspension 3", reader.got)
	}
	if reader.got.MinValue == nil || *reader.got.MinValue != 2_000_000_000 {
		t.Fatalf("min_value = %v, want 2000000000", reader.got.MinValue)
	}
	if reader.got.AsOf == nil || reader.got.AsOf.Format("2006-01-02") != "2026-09-11" {
		t.Fatalf("as_of = %v, want 2026-09-11", reader.got.AsOf)
	}
	if len(reader.got.Indicators) != 1 || reader.got.Indicators[0] != "rsi:14" {
		t.Fatalf("indicators = %v, want [rsi:14]", reader.got.Indicators)
	}
}

// TestHandleScreenStocks_OptionalArgumentsAbsent — a bare call leaves every
// optional argument unset so the usecase applies its own defaults: nil
// min_value (the shipped floor), zero window/limit/suspension, empty indicators
// and sort (the stage-1 default column set), and no anchor.
func TestHandleScreenStocks_OptionalArgumentsAbsent(t *testing.T) {
	reader := &fakeScreenStocksReader{data: &usecase.ScreenStocksResponse{}}
	s := newScreenStocksTestServer(reader)

	res, err := s.handleScreenStocks(context.Background(), screenStocksReq(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error result: %v", res.Content)
	}

	if reader.got.MinValue != nil {
		t.Fatalf("min_value = %v, want nil so the usecase applies its default", *reader.got.MinValue)
	}
	if reader.got.AsOf != nil || reader.got.Window != 0 || reader.got.Limit != 0 || reader.got.SuspensionWindowDays != 0 {
		t.Fatalf("request = %+v, want every optional field unset", reader.got)
	}
	if reader.got.Indicators != nil || reader.got.Sort != "" || reader.got.Order != "" {
		t.Fatalf("request = %+v, want no column or sort arguments", reader.got)
	}
	// nil, not a pointer to an empty list: the usecase has to be able to tell
	// "said nothing" (default set) from "asked for none".
	if reader.got.Filters != nil {
		t.Fatalf("filters = %v, want nil so the usecase runs the shipped default set", reader.got.Filters)
	}
}

// TestHandleScreenStocks_FiltersArgument — the DSL survives the wire: a scalar
// comparison and a band both reach the usecase with their bounds intact.
func TestHandleScreenStocks_FiltersArgument(t *testing.T) {
	reader := &fakeScreenStocksReader{data: &usecase.ScreenStocksResponse{}}
	s := newScreenStocksTestServer(reader)

	_, err := s.handleScreenStocks(context.Background(), screenStocksReq(map[string]any{
		"filters": []any{
			map[string]any{"indicator": "ma_distance:20:50", "op": "gt", "value": float64(0)},
			map[string]any{"indicator": "range_position:60", "op": "between", "value": []any{0.70, 0.95}},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if reader.got.Filters == nil || len(*reader.got.Filters) != 2 {
		t.Fatalf("filters = %v, want the two parsed filters", reader.got.Filters)
	}
	filters := *reader.got.Filters
	if filters[0].Indicator != "ma_distance:20:50" || filters[0].Op != "gt" || len(filters[0].Value) != 1 || filters[0].Value[0] != 0 {
		t.Fatalf("filter 0 = %+v, want ma_distance:20:50 gt [0]", filters[0])
	}
	if filters[1].Op != "between" || len(filters[1].Value) != 2 || filters[1].Value[0] != 0.70 || filters[1].Value[1] != 0.95 {
		t.Fatalf("filter 1 = %+v, want range_position:60 between [0.7, 0.95]", filters[1])
	}
}

// TestHandleScreenStocks_FilterListStates — an absent argument and an explicit
// null both leave the usecase to its defaults, while an explicit empty array is
// a real request for no structural filters. The three must not collapse.
func TestHandleScreenStocks_FilterListStates(t *testing.T) {
	cases := []struct {
		name string
		args map[string]any
		want bool // want a non-nil pointer
	}{
		{name: "absent", args: map[string]any{}, want: false},
		{name: "null", args: map[string]any{"filters": nil}, want: false},
		{name: "empty array", args: map[string]any{"filters": []any{}}, want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeScreenStocksReader{data: &usecase.ScreenStocksResponse{}}
			s := newScreenStocksTestServer(reader)

			if _, err := s.handleScreenStocks(context.Background(), screenStocksReq(tc.args)); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := reader.got.Filters != nil; got != tc.want {
				t.Fatalf("filters present = %v, want %v", got, tc.want)
			}
			if tc.want && len(*reader.got.Filters) != 0 {
				t.Fatalf("filters = %v, want an empty list", *reader.got.Filters)
			}
		})
	}
}

// TestHandleScreenStocks_FilterArgumentErrors — a filter payload the DSL cannot
// read is an argument error before any read, never a zero reaching a comparison.
func TestHandleScreenStocks_FilterArgumentErrors(t *testing.T) {
	cases := []struct {
		name string
		args map[string]any
	}{
		{
			name: "filters is not an array",
			args: map[string]any{"filters": "ma_distance > 0"},
		},
		{
			name: "a filter is not an object",
			args: map[string]any{"filters": []any{"ma_distance:20:50 gt 0"}},
		},
		{
			name: "value is a string",
			args: map[string]any{"filters": []any{
				map[string]any{"indicator": "rsi:14", "op": "lt", "value": "40"},
			}},
		},
		{
			name: "value is a nested array",
			args: map[string]any{"filters": []any{
				map[string]any{"indicator": "range_position:60", "op": "between", "value": []any{[]any{0.7}, []any{0.95}}},
			}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newScreenStocksTestServer(&fakeScreenStocksReader{})
			res, err := s.handleScreenStocks(context.Background(), screenStocksReq(tc.args))
			if err != nil {
				t.Fatalf("unexpected protocol error: %v", err)
			}
			if !res.IsError {
				t.Fatal("expected an error result")
			}
			text, ok := res.Content[0].(mcpgo.TextContent)
			if !ok {
				t.Fatalf("content type = %T", res.Content[0])
			}
			var env mcp.ErrorEnvelope
			if err := json.Unmarshal([]byte(text.Text), &env); err != nil {
				t.Fatalf("unmarshal envelope: %v\n%s", err, text.Text)
			}
			if env.Error.Code != mcp.ErrorCodeInvalidArgument {
				t.Fatalf("code = %s, want %s (%s)", env.Error.Code, mcp.ErrorCodeInvalidArgument, env.Error.Message)
			}
			if !strings.Contains(env.Error.Message, "filters") {
				t.Fatalf("message = %q, want it to name the filters argument", env.Error.Message)
			}
		})
	}
}

// TestHandleScreenStocks_FilterSemanticsReachTheUsecase — the semantic rules
// stay in the usecase, so an unknown operator is rejected there (real usecase,
// no fake, no DB) with the INVALID_ARGUMENT envelope rather than being
// half-validated in the handler.
func TestHandleScreenStocks_FilterSemanticsReachTheUsecase(t *testing.T) {
	s := newScreenStocksTestServer(usecase.NewScreenStocksUseCase(nil, logrus.New(), nil))

	res, err := s.handleScreenStocks(context.Background(), screenStocksReq(map[string]any{
		"filters": []any{
			map[string]any{"indicator": "rsi:14", "op": "above", "value": float64(40)},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected protocol error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for an unknown operator")
	}
	text := res.Content[0].(mcpgo.TextContent)
	var env mcp.ErrorEnvelope
	if err := json.Unmarshal([]byte(text.Text), &env); err != nil {
		t.Fatalf("unmarshal envelope: %v\n%s", err, text.Text)
	}
	if env.Error.Code != mcp.ErrorCodeInvalidArgument {
		t.Fatalf("code = %s, want %s", env.Error.Code, mcp.ErrorCodeInvalidArgument)
	}
	if !strings.Contains(env.Error.Message, "between") {
		t.Fatalf("message = %q, want the valid operators enumerated", env.Error.Message)
	}
}

// TestHandleScreenStocks_MinValueIsCarriedVerbatim — an explicit 0 is a real
// request (no liquidity floor), not an absent argument, so it must survive as a
// pointer to zero rather than collapse into the default.
func TestHandleScreenStocks_MinValueIsCarriedVerbatim(t *testing.T) {
	reader := &fakeScreenStocksReader{data: &usecase.ScreenStocksResponse{}}
	s := newScreenStocksTestServer(reader)

	if _, err := s.handleScreenStocks(context.Background(), screenStocksReq(map[string]any{"min_value": float64(0)})); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reader.got.MinValue == nil || *reader.got.MinValue != 0 {
		t.Fatalf("min_value = %v, want a pointer to 0", reader.got.MinValue)
	}
}

// TestHandleScreenStocks_ArgumentErrors — unparsable arguments return the
// structured envelope before any read, with the code the caller can act on.
func TestHandleScreenStocks_ArgumentErrors(t *testing.T) {
	cases := []struct {
		name string
		args map[string]any
		code mcp.ErrorCode
	}{
		{
			name: "fractional min_value",
			args: map[string]any{"min_value": 1_500_000_000.5},
			code: mcp.ErrorCodeInvalidArgument,
		},
		{
			name: "negative min_value",
			args: map[string]any{"min_value": float64(-1)},
			code: mcp.ErrorCodeInvalidArgument,
		},
		{
			name: "min_value of the wrong type",
			args: map[string]any{"min_value": "5b"},
			code: mcp.ErrorCodeInvalidArgument,
		},
		{
			name: "unparsable as_of",
			args: map[string]any{"as_of": "11-09-2026"},
			code: mcp.ErrorCodeInvalidArgument,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newScreenStocksTestServer(&fakeScreenStocksReader{})
			res, err := s.handleScreenStocks(context.Background(), screenStocksReq(tc.args))
			if err != nil {
				t.Fatalf("unexpected protocol error: %v", err)
			}
			if !res.IsError {
				t.Fatal("expected an error result")
			}
			text, ok := res.Content[0].(mcpgo.TextContent)
			if !ok {
				t.Fatalf("content type = %T", res.Content[0])
			}
			var env mcp.ErrorEnvelope
			if err := json.Unmarshal([]byte(text.Text), &env); err != nil {
				t.Fatalf("unmarshal envelope: %v\n%s", err, text.Text)
			}
			if env.Error.Code != tc.code {
				t.Fatalf("code = %s, want %s (%s)", env.Error.Code, tc.code, env.Error.Message)
			}
		})
	}
}

// TestHandleScreenStocks_UsecaseErrorsAreEnveloped — a read failure or a
// validation failure from the usecase reaches the caller as the structured
// envelope, never as a bare protocol error.
func TestHandleScreenStocks_UsecaseErrorsAreEnveloped(t *testing.T) {
	cases := []struct {
		name    string
		ucErr   error
		wantMsg string
		wantCod mcp.ErrorCode
	}{
		{
			name:    "invalid argument",
			ucErr:   errors.New("invalid argument: limit must be 1..50, got 51"),
			wantMsg: "",
			wantCod: mcp.ErrorCodeInternal,
		},
		{
			name:    "wrapped sentinel",
			ucErr:   errInvalidArgumentForTest(),
			wantMsg: "limit must be 1..50",
			wantCod: mcp.ErrorCodeInvalidArgument,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newScreenStocksTestServer(&fakeScreenStocksReader{err: tc.ucErr})
			res, err := s.handleScreenStocks(context.Background(), screenStocksReq(map[string]any{}))
			if err != nil {
				t.Fatalf("unexpected protocol error: %v", err)
			}
			if !res.IsError {
				t.Fatal("expected an error result")
			}
			text, ok := res.Content[0].(mcpgo.TextContent)
			if !ok {
				t.Fatalf("content type = %T", res.Content[0])
			}
			var env mcp.ErrorEnvelope
			if err := json.Unmarshal([]byte(text.Text), &env); err != nil {
				t.Fatalf("unmarshal envelope: %v\n%s", err, text.Text)
			}
			if env.Error.Code != tc.wantCod {
				t.Fatalf("code = %s, want %s", env.Error.Code, tc.wantCod)
			}
			if tc.wantMsg != "" && !strings.Contains(env.Error.Message, tc.wantMsg) {
				t.Fatalf("message = %q, want it to carry %q", env.Error.Message, tc.wantMsg)
			}
		})
	}
}

// TestHandleScreenStocks_RealUsecaseValidation — the handler delegates every
// bound to the usecase, so an over-cap limit is rejected by the real usecase
// (no fake, no DB) with the INVALID_ARGUMENT envelope.
func TestHandleScreenStocks_RealUsecaseValidation(t *testing.T) {
	s := newScreenStocksTestServer(usecase.NewScreenStocksUseCase(nil, logrus.New(), nil))

	res, err := s.handleScreenStocks(context.Background(), screenStocksReq(map[string]any{"limit": float64(51)}))
	if err != nil {
		t.Fatalf("unexpected protocol error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for a limit above the cap")
	}
	text := res.Content[0].(mcpgo.TextContent)
	var env mcp.ErrorEnvelope
	if err := json.Unmarshal([]byte(text.Text), &env); err != nil {
		t.Fatalf("unmarshal envelope: %v\n%s", err, text.Text)
	}
	if env.Error.Code != mcp.ErrorCodeInvalidArgument {
		t.Fatalf("code = %s, want %s", env.Error.Code, mcp.ErrorCodeInvalidArgument)
	}
	if !strings.Contains(env.Error.Message, "limit must be 1..50") {
		t.Fatalf("message = %q, want the cap stated", env.Error.Message)
	}
}

// TestScreenStocksToolContract — the tool is read-only, takes no required
// arguments (the universe is the screen), and its description states the funnel
// contract rather than implying it.
func TestScreenStocksToolContract(t *testing.T) {
	if len(toolScreenStocks.InputSchema.Required) != 0 {
		t.Fatalf("required = %v, want none", toolScreenStocks.InputSchema.Required)
	}
	for _, prop := range []string{"min_value", "suspension_window_days", "window", "limit", "indicators", "filters", "sort", "order", "as_of"} {
		if _, ok := toolScreenStocks.InputSchema.Properties[prop]; !ok {
			t.Fatalf("property %q missing", prop)
		}
	}
	for _, want := range []string{"universe", "after_value", "after_structure", "total_matches", "insufficient"} {
		if !strings.Contains(toolScreenStocks.Description, want) {
			t.Fatalf("description does not mention %q", want)
		}
	}
	// The description is a format string over the exported defaults: a missing or
	// extra verb renders as %!s(MISSING) and would ship a tool that describes
	// itself in garbage.
	if strings.Contains(toolScreenStocks.Description, "%!") {
		t.Fatalf("description has an unexpanded format verb: %s", toolScreenStocks.Description)
	}
	for _, want := range []string{
		usecase.ScreenerDefaultFiltersDoc(),
		strings.Join(usecase.ScreenerDefaultIndicators, ", "),
	} {
		if !strings.Contains(toolScreenStocks.Description, want) {
			t.Fatalf("description does not carry the exported default %q", want)
		}
	}

	// The DSL's schema is what an MCP client reads to compose a filter: the
	// operator enumeration and the number-or-pair value must both be stated.
	filters := toolScreenStocks.InputSchema.Properties["filters"].(map[string]any)
	if filters["type"] != "array" {
		t.Fatalf("filters type = %v, want array", filters["type"])
	}
	items, ok := filters["items"].(map[string]any)
	if !ok {
		t.Fatalf("filters items = %T, want an object schema", filters["items"])
	}
	properties, ok := items["properties"].(map[string]any)
	if !ok {
		t.Fatalf("filters item properties = %T", items["properties"])
	}
	for _, field := range []string{"indicator", "op", "value"} {
		if _, ok := properties[field]; !ok {
			t.Fatalf("filter field %q missing from the schema", field)
		}
	}
	op := properties["op"].(map[string]any)
	values, ok := op["enum"].([]string)
	if !ok || len(values) != 5 {
		t.Fatalf("op enum = %v, want the five threshold operators", op["enum"])
	}
	if _, ok := properties["value"].(map[string]any)["anyOf"]; !ok {
		t.Fatalf("value schema = %v, want a number-or-pair union", properties["value"])
	}
}

// errInvalidArgumentForTest wraps the usecase sentinel the way the usecases do,
// so the envelope mapping is exercised end to end.
func errInvalidArgumentForTest() error {
	return fmt.Errorf("%w: limit must be 1..50, got 51", usecase.ErrInvalidArgument)
}
