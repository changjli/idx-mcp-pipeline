package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/tasks"
)

// fakeEnqueuer captures the task it was called with so the dispatch table can
// assert type + payload without Redis.
type fakeEnqueuer struct {
	task *asynq.Task
}

func (f *fakeEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	f.task = task
	return &asynq.TaskInfo{ID: "fake", Type: task.Type(), Queue: "ingest"}, nil
}

// TestDispatchTable locks the --task name → node + payload mapping: each
// registry node name resolves to the right asynq type and builds the right
// payload for a given date (and --arg extras where the node needs them).
func TestDispatchTable(t *testing.T) {
	day := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		wantType    string
		args        []string
		wantPayload any
	}{
		{"stock-summary", tasks.TypeStockSummary, nil, tasks.StockSummaryPayload{Date: "2026-08-30"}},
		{"announcements", tasks.TypeAnnouncements, nil, tasks.AnnouncementsPayload{Date: "2026-08-30"}},
		{"rss", tasks.TypeRSS, nil, tasks.RSSPayload{Date: "2026-08-30"}},
		{"cleanup", tasks.TypeCleanup, nil, tasks.CleanupPayload{Date: "2026-08-30"}},
		{"detect", tasks.TypeDetectAnomalies, nil, tasks.DetectAnomaliesPayload{Date: "2026-08-30"}},
		{"filter", tasks.TypeFilterDisclosures, nil, tasks.FilterDisclosuresPayload{Date: "2026-08-30"}},
		{"extract", tasks.TypeExtractDisclosure, []string{"id=42"}, tasks.ExtractDisclosurePayload{DisclosureID: 42}},
		{"broker-summary", tasks.TypeBrokerStockSummary, []string{"ticker=RAJA"}, tasks.BrokerStockSummaryPayload{Ticker: "RAJA", Date: "2026-08-30"}},
		{"financials", tasks.TypeFinancials, nil, tasks.FinancialsPayload{Date: "2026-08-30"}},
		{"financials-depth", tasks.TypeFinancialsDepth, []string{"ticker=RAJA", "month=2026-08"}, tasks.FinancialDepthPayload{Date: "2026-08-30", Ticker: "RAJA", Month: "2026-08"}},
		{"pipeline", tasks.TypePipelineDaily, nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := tasks.Graph.NodeByName(tt.name)
			if err != nil {
				t.Fatalf("NodeByName(%q): %v", tt.name, err)
			}
			enq := &fakeEnqueuer{}
			if _, err := node.Enqueue(enq, day, tt.args); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			if enq.task.Type() != tt.wantType {
				t.Errorf("type = %s, want %s", enq.task.Type(), tt.wantType)
			}
			if tt.wantPayload != nil {
				raw, _ := json.Marshal(tt.wantPayload)
				if string(enq.task.Payload()) != string(raw) {
					t.Errorf("payload = %s, want %s", enq.task.Payload(), raw)
				}
			}
		})
	}
}

func TestDispatchTableUnknownTask(t *testing.T) {
	if _, err := tasks.Graph.NodeByName("nope"); err == nil {
		t.Error("NodeByName(unknown) expected error, got nil")
	}
}

// TestParseMonthFlag pins the financials backfill's YYYY-MM bounds: an empty
// value is the zero time (the usecase applies its default), a valid month is
// parsed, and anything else is rejected rather than silently defaulted.
func TestParseMonthFlag(t *testing.T) {
	got, err := parseMonthFlag("--from", "")
	if err != nil || !got.IsZero() {
		t.Fatalf("empty = %v, %v; want zero time, nil", got, err)
	}
	got, err = parseMonthFlag("--from", "2024-03")
	if err != nil {
		t.Fatalf("valid month: %v", err)
	}
	if want := time.Date(2024, time.March, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("month = %s, want %s", got, want)
	}
	if _, err := parseMonthFlag("--from", "2024-3"); err == nil {
		t.Error("expected an error for a non-YYYY-MM value")
	}
}
