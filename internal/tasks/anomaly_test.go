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

func TestTaskKeyDetectAnomalies(t *testing.T) {
	key := TaskKey(TypeDetectAnomalies, "2026-08-08")
	expected := "detect:anomalies:2026-08-08"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}
