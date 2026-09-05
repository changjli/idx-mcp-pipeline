package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

// enqueueRecorder adapts a raw function into a pipeline.Enqueuer so tests can
// capture task types + options without Redis.
type enqueueRecorder func(task *asynq.Task, opts []asynq.Option) (*asynq.TaskInfo, error)

func (f enqueueRecorder) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	return f(task, opts)
}

func TestBrokerSummarySweepPayload_Marshal(t *testing.T) {
	p := BrokerSummarySweepPayload{Date: "2026-08-12"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got BrokerSummarySweepPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-08-12" {
		t.Errorf("payload = %+v, want date 2026-08-12", got)
	}
}

func TestBrokerSummarySweep_GraphNodeDateKeyed(t *testing.T) {
	node := Graph.Node(TypeBrokerStockSummarySweep)
	if node == nil {
		t.Fatalf("graph has no node for %s", TypeBrokerStockSummarySweep)
	}
	if node.Name != "broker-summary-sweep" {
		t.Errorf("node.Name = %q, want broker-summary-sweep", node.Name)
	}

	// Date-keyed TaskID so a re-enqueue for the same date dedups.
	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	key := node.Key(date.Format("2006-01-02"))
	want := "idx:broker_stock_summary_sweep:2026-08-12"
	if key != want {
		t.Errorf("node key = %q, want %q", key, want)
	}
	day, err := node.Day(key)
	if err != nil {
		t.Fatalf("Day(%q): %v", key, err)
	}
	if day.Format("2006-01-02") != date.Format("2006-01-02") {
		t.Errorf("Day round-trip = %s, want %s", day.Format("2006-01-02"), date.Format("2006-01-02"))
	}
}

func TestEnqueueBrokerSummarySweep_DedupKeyAndTimeout(t *testing.T) {
	var types []string
	var opts []asynq.Option
	enq := enqueueRecorder(func(task *asynq.Task, o []asynq.Option) (*asynq.TaskInfo, error) {
		types = append(types, task.Type())
		opts = o
		return &asynq.TaskInfo{ID: "fake", Type: task.Type(), Queue: "ingest"}, nil
	})
	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)

	info, err := EnqueueBrokerSummarySweep(enq, date)
	if err != nil {
		t.Fatalf("EnqueueBrokerSummarySweep: %v", err)
	}
	if info.Type != TypeBrokerStockSummarySweep {
		t.Errorf("enqueued type = %s, want %s", info.Type, TypeBrokerStockSummarySweep)
	}
	if len(types) != 1 || types[0] != TypeBrokerStockSummarySweep {
		t.Errorf("enqueued types = %v, want [%s]", types, TypeBrokerStockSummarySweep)
	}

	// Long task-level timeout: a full-market sweep exceeds asynq's 30m default
	// at IPOT's 2s pacing (~900 traded tickers ≈ 30 min + RTT).
	hasTimeout := false
	for _, o := range opts {
		if o.Type() == asynq.TimeoutOpt {
			hasTimeout = true
		}
	}
	if !hasTimeout {
		t.Error("sweep task missing asynq timeout option — would be killed at the 30m server default")
	}
}
