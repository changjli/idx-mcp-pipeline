package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// fakeDisclosureReader implements usecase.DisclosureReader for handler tests.
// Only SearchDisclosures is exercised; the other methods are stubs.
type fakeDisclosureReader struct {
	data *usecase.DisclosureSearchData
	err  error
}

func (f *fakeDisclosureReader) ListIdxDisclosures(context.Context, string, *string, int) (*usecase.DisclosureListData, error) {
	return nil, nil
}

func (f *fakeDisclosureReader) ReadIdxDisclosure(context.Context, int64) (*usecase.ReadIdxDisclosureData, error) {
	return nil, nil
}

func (f *fakeDisclosureReader) SearchDisclosures(context.Context, string, *time.Time, *time.Time, int) (*usecase.DisclosureSearchData, error) {
	return f.data, f.err
}

// newSearchTestServer builds a Server with a fake disclosure reader and no DB
// (stalenessFor reports stale on nil wiring).
func newSearchTestServer(reader usecase.DisclosureReader) *Server {
	return &Server{
		log:          logrus.New(),
		disclosureUC: reader,
	}
}

// TestHandleSearchDisclosuresSuccess checks the happy path: category + range
// pass through to the usecase, the response carries the disclosure rows.
func TestHandleSearchDisclosuresSuccess(t *testing.T) {
	passed := true
	reader := &fakeDisclosureReader{data: &usecase.DisclosureSearchData{
		Query: "Dividen",
		Disclosures: []usecase.DisclosureSearchItem{
			{DisclosureID: 1, Ticker: "BBCA", Title: "Dividen Final", Date: "2026-08-12", Categories: []string{"Dividen"}, PassedFilter: &passed, ExtractionStatus: "ok"},
			{DisclosureID: 2, Ticker: "BBRI", Title: "Pembagian Dividen Interim", Date: "2026-08-10", Categories: []string{"Dividen"}, PassedFilter: &passed, ExtractionStatus: "pending"},
		},
	}}
	s := newSearchTestServer(reader)

	req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name: "search_disclosures",
		Arguments: map[string]any{
			"query":     "Dividen",
			"date_from": "2026-08-01",
			"date_to":   "2026-08-31",
			"limit":     float64(5),
		},
	}}
	res, err := s.handleSearchDisclosures(context.Background(), req)
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
	var got disclosureSearchResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, text.Text)
	}
	if got.Query != "Dividen" {
		t.Errorf("query = %q, want Dividen", got.Query)
	}
	if len(got.Disclosures) != 2 {
		t.Fatalf("disclosures = %d, want 2", len(got.Disclosures))
	}
	if got.Disclosures[0].Ticker != "BBCA" || got.Disclosures[0].Title != "Dividen Final" {
		t.Errorf("first row = %+v, want BBCA / Dividen Final", got.Disclosures[0])
	}
	if !got.DataStale {
		t.Error("data_stale must be true on nil wiring (no source_status)")
	}
}

// TestHandleSearchDisclosuresEmpty checks the empty result: a valid call with
// no matches returns an empty disclosures array, not an error.
func TestHandleSearchDisclosuresEmpty(t *testing.T) {
	reader := &fakeDisclosureReader{data: &usecase.DisclosureSearchData{
		Query:       "Right Issue",
		Disclosures: []usecase.DisclosureSearchItem{},
	}}
	s := newSearchTestServer(reader)

	req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "search_disclosures",
		Arguments: map[string]any{"query": "Right Issue"},
	}}
	res, err := s.handleSearchDisclosures(context.Background(), req)
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
	var got disclosureSearchResponse
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Disclosures == nil || len(got.Disclosures) != 0 {
		t.Fatalf("disclosures = %v, want empty non-nil array", got.Disclosures)
	}
}

// TestHandleSearchDisclosuresErrors checks the envelope paths: invalid dates
// (handler-level) and usecase errors mapped through exceptionToEnvelope.
func TestHandleSearchDisclosuresErrors(t *testing.T) {
	cases := []struct {
		name     string
		args     map[string]any
		reader   *fakeDisclosureReader
		wantCode mcp.ErrorCode
	}{
		{"invalid date_from", map[string]any{"query": "Dividen", "date_from": "abc"}, &fakeDisclosureReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid date_to", map[string]any{"query": "Dividen", "date_to": "2026-13-40"}, &fakeDisclosureReader{}, mcp.ErrorCodeInvalidArgument},
		{"invalid argument", map[string]any{"query": "Dividen"}, &fakeDisclosureReader{err: usecase.ErrInvalidArgument}, mcp.ErrorCodeInvalidArgument},
		{"invalid range", map[string]any{"query": "Dividen"}, &fakeDisclosureReader{err: usecase.ErrInvalidRange}, mcp.ErrorCodeInvalidArgument},
		{"internal", map[string]any{"query": "Dividen"}, &fakeDisclosureReader{err: errors.New("boom")}, mcp.ErrorCodeInternal},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSearchTestServer(tc.reader)
			req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
				Name:      "search_disclosures",
				Arguments: tc.args,
			}}
			res, err := s.handleSearchDisclosures(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !res.IsError {
				t.Fatal("result must be an error")
			}
			text, ok := res.Content[0].(mcpgo.TextContent)
			if !ok {
				t.Fatalf("content type = %T", res.Content[0])
			}
			var got mcp.ErrorEnvelope
			if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			if got.Error.Code != tc.wantCode {
				t.Errorf("code = %q, want %q", got.Error.Code, tc.wantCode)
			}
		})
	}
}
