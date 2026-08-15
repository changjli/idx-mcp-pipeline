package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

func TestBrokerStockSummaryPayload_Marshal(t *testing.T) {
	p := BrokerStockSummaryPayload{Ticker: "RAJA", Date: "2026-08-12"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got BrokerStockSummaryPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Ticker != "RAJA" || got.Date != "2026-08-12" {
		t.Errorf("payload = %+v, want RAJA/2026-08-12", got)
	}
}

func TestEnqueueBrokerStockSummary_TaskKeyIncludesTickerAndDate(t *testing.T) {
	// Can't enqueue without Redis, but the TaskID dedup key must be
	// ticker+date so distinct tickers/days don't collide.
	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeBrokerStockSummary, "RAJA"+":"+dateKey)

	want := "idx:broker_stock_summary:RAJA:2026-08-12"
	if taskKey != want {
		t.Errorf("task key = %q, want %q", taskKey, want)
	}

	// Different ticker → different key.
	other := TaskKey(TypeBrokerStockSummary, "BBCA"+":"+dateKey)
	if other == taskKey {
		t.Error("expected distinct keys for distinct tickers")
	}
}

func TestBrokerStockSummaryTask_TypeAndPayload(t *testing.T) {
	payload := BrokerStockSummaryPayload{Ticker: "RAJA", Date: "2026-08-12"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeBrokerStockSummary, raw)

	if task.Type() != TypeBrokerStockSummary {
		t.Errorf("expected type %s, got %s", TypeBrokerStockSummary, task.Type())
	}
	var got BrokerStockSummaryPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Ticker != "RAJA" || got.Date != "2026-08-12" {
		t.Errorf("task payload = %+v, want RAJA/2026-08-12", got)
	}
}
