package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
)

func TestSuspensionsPayload_Marshal(t *testing.T) {
	p := SuspensionsPayload{Date: "2026-09-06"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got SuspensionsPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-09-06" {
		t.Errorf("payload = %+v, want 2026-09-06", got)
	}
}

func TestSuspensionsTask_TypeAndPayload(t *testing.T) {
	payload := SuspensionsPayload{Date: "2026-09-06"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeSuspensions, raw)

	if task.Type() != TypeSuspensions {
		t.Errorf("expected type %s, got %s", TypeSuspensions, task.Type())
	}
	var got SuspensionsPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != "2026-09-06" {
		t.Errorf("task payload = %+v, want 2026-09-06", got)
	}
}

func TestSuspensionsTaskKey_DedupFormat(t *testing.T) {
	key := TaskKey(TypeSuspensions, "2026-09-06")
	want := "idx:suspensions:2026-09-06"
	if key != want {
		t.Errorf("task key = %q, want %q", key, want)
	}
}

// TestSuspensionRowsToEntities verifies the combined conversion: suspension
// rows keep their SPT/UPT type; UMA rows get type UMA.
func TestSuspensionRowsToEntities(t *testing.T) {
	suspensions := []client.Suspension{
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), Type: "SPT", Reason: "Penghentian Sementara TESTSA"},
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), Type: "UPT", Reason: "Pembukaan Kembali TESTSA"},
	}
	umas := []client.Uma{
		{Ticker: "TESTSB", EventDate: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), AnnouncementNo: "Peng-UMA-1", Reason: "UMA atas Saham TESTSB"},
	}
	rows := suspensionRowsToEntities(suspensions, umas)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0].Type != "SPT" || rows[1].Type != "UPT" {
		t.Errorf("suspension rows should keep SPT/UPT, got %s/%s", rows[0].Type, rows[1].Type)
	}
	if rows[2].Type != "UMA" {
		t.Errorf("UMA row type = %q, want UMA", rows[2].Type)
	}
}

// TestSuspensionsMaxEventDate verifies the watermark is the newest event date
// across both sets, and empty sets yield nil.
func TestSuspensionsMaxEventDate(t *testing.T) {
	suspensions := []client.Suspension{
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)},
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)},
	}
	umas := []client.Uma{
		{Ticker: "TESTSB", EventDate: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)},
	}
	wm := suspensionsMaxEventDate(suspensions, umas)
	if wm == nil || wm.Format("2006-01-02") != "2026-09-03" {
		t.Errorf("watermark = %v, want 2026-09-03", wm)
	}

	if wm := suspensionsMaxEventDate(nil, nil); wm != nil {
		t.Errorf("empty sets watermark = %v, want nil", wm)
	}
}
