package tasks

import (
	"encoding/json"
	"testing"
)

func TestDetectAnomaliesPayload_Marshal(t *testing.T) {
	p := DetectAnomaliesPayload{Date: "2026-08-08", Attempt: 3}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got DetectAnomaliesPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-08-08" {
		t.Errorf("expected date 2026-08-08, got %s", got.Date)
	}
	if got.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", got.Attempt)
	}
}

func TestDetectAnomaliesTask_TypeAndPayload(t *testing.T) {
	task, err := detectAnomaliesTask("2026-08-08", 0)
	if err != nil {
		t.Fatalf("detectAnomaliesTask: %v", err)
	}

	if task.Type() != TypeDetectAnomalies {
		t.Errorf("expected type %s, got %s", TypeDetectAnomalies, task.Type())
	}

	var got DetectAnomaliesPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != "2026-08-08" {
		t.Errorf("expected date 2026-08-08, got %s", got.Date)
	}
	if got.Attempt != 0 {
		t.Errorf("expected attempt 0, got %d", got.Attempt)
	}
}

func TestTaskKeyDetectAnomalies(t *testing.T) {
	key := TaskKey(TypeDetectAnomalies, "2026-08-08")
	expected := "detect:anomalies:2026-08-08"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}
