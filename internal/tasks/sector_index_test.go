package tasks

import (
	"encoding/json"
	"testing"

	"github.com/hibiken/asynq"
)

func TestSectorIndexPayload_Marshal(t *testing.T) {
	p := SectorIndexPayload{Date: "2026-09-09"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got SectorIndexPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-09-09" {
		t.Errorf("payload = %+v, want 2026-09-09", got)
	}
}

func TestSectorIndexTask_TypeAndPayload(t *testing.T) {
	payload := SectorIndexPayload{Date: "2026-09-09"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeSectorIndex, raw)

	if task.Type() != TypeSectorIndex {
		t.Errorf("expected type %s, got %s", TypeSectorIndex, task.Type())
	}
	var got SectorIndexPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != "2026-09-09" {
		t.Errorf("task payload = %+v, want 2026-09-09", got)
	}
}

func TestSectorIndexTaskKey_DedupFormat(t *testing.T) {
	key := TaskKey(TypeSectorIndex, "2026-09-09")
	want := "idx:sector_index:2026-09-09"
	if key != want {
		t.Errorf("task key = %q, want %q", key, want)
	}
}
