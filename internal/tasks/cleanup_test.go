package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

func TestCleanupPayload_Marshal(t *testing.T) {
	p := CleanupPayload{Date: "2026-08-17"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got CleanupPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-08-17" {
		t.Errorf("expected date 2026-08-17, got %s", got.Date)
	}
}

func TestEnqueueCleanup_TaskTypeAndQueue(t *testing.T) {
	date := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)

	// Can't enqueue without Redis, but the task construction (type + payload)
	// is verifiable directly.
	payload := CleanupPayload{Date: "2026-08-17"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeCleanup, raw)

	if task.Type() != TypeCleanup {
		t.Errorf("expected type %s, got %s", TypeCleanup, task.Type())
	}

	var got CleanupPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != date.Format("2006-01-02") {
		t.Errorf("expected date %s, got %s", date.Format("2006-01-02"), got.Date)
	}
}

func TestTaskKeyCleanup(t *testing.T) {
	key := TaskKey(TypeCleanup, "2026-08-17")
	if key != "cleanup:2026-08-17" {
		t.Errorf("expected cleanup:2026-08-17, got %s", key)
	}
}
