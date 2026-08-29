package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

// fakeEnqueuer captures the task + options it was called with.
type fakeEnqueuer struct {
	tasks []fakeEnqueued
}

type fakeEnqueued struct {
	typ  string
	body []byte
	opts []asynq.Option
}

func (f *fakeEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	f.tasks = append(f.tasks, fakeEnqueued{typ: task.Type(), body: task.Payload(), opts: opts})
	return &asynq.TaskInfo{}, nil
}

type harnessPayload struct {
	Date string `json:"date"`
}

func optStrings(opts []asynq.Option) string {
	var sb strings.Builder
	for _, o := range opts {
		sb.WriteString(o.String())
		sb.WriteString("\n")
	}
	return sb.String()
}

func TestStage_EnqueueBundle(t *testing.T) {
	enq := &fakeEnqueuer{}
	stage := NewIngestStage("idx:test", nil, enq, 3)

	if _, err := stage.Enqueue("test:2026-08-29", harnessPayload{Date: "2026-08-29"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if len(enq.tasks) != 1 {
		t.Fatalf("expected 1 enqueue, got %d", len(enq.tasks))
	}
	got := enq.tasks[0]
	if got.typ != "idx:test" {
		t.Errorf("expected task type %q, got %q", "idx:test", got.typ)
	}

	opts := optStrings(got.opts)
	for _, want := range []string{"Queue(\"ingest\")", "MaxRetry(3)", "Retention", "TaskID(\"test:2026-08-29\")"} {
		if !strings.Contains(opts, want) {
			t.Errorf("expected option bundle to contain %q, got:\n%s", want, opts)
		}
	}

	var body harnessPayload
	if err := json.Unmarshal(got.body, &body); err != nil || body.Date != "2026-08-29" {
		t.Errorf("expected payload date 2026-08-29, got %q (err=%v)", body.Date, err)
	}
}

func TestStage_EnqueueWithoutDedupKey(t *testing.T) {
	enq := &fakeEnqueuer{}
	stage := NewIngestStage("idx:test", nil, enq, 3)
	if _, err := stage.Enqueue("", harnessPayload{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if strings.Contains(optStrings(enq.tasks[0].opts), "TaskID") {
		t.Error("empty dedupKey must omit the TaskID option")
	}
}

func TestStage_ReenqueueNoTaskID(t *testing.T) {
	enq := &fakeEnqueuer{}
	stage := NewIngestStage("idx:test", nil, enq, 3)

	if err := stage.Reenqueue(harnessPayload{Date: "2026-08-29"}, 30*time.Second); err != nil {
		t.Fatalf("Reenqueue: %v", err)
	}
	got := enq.tasks[0]
	opts := optStrings(got.opts)
	if strings.Contains(opts, "TaskID") {
		t.Errorf("re-enqueue must not carry a dedup TaskID, got:\n%s", opts)
	}
	if !strings.Contains(opts, "ProcessIn") {
		t.Errorf("expected delay option, got:\n%s", opts)
	}
}

func TestStage_DecodeAndParse(t *testing.T) {
	raw, err := json.Marshal(harnessPayload{Date: "2026-08-29"})
	if err != nil {
		t.Fatal(err)
	}
	task := asynq.NewTask("idx:test", raw)

	p, err := DecodeTask[harnessPayload](task)
	if err != nil || p.Date != "2026-08-29" {
		t.Errorf("DecodeTask: %+v (err=%v)", p, err)
	}
	if _, err := DecodeTask[harnessPayload](asynq.NewTask("idx:test", []byte("{bad"))); err == nil {
		t.Error("expected decode error for malformed payload")
	}

	day, err := ParseTaskDay("2026-08-29")
	if err != nil || day.Format("2006-01-02") != "2026-08-29" {
		t.Fatalf("ParseTaskDay: %v (%v)", day, err)
	}
	if _, err := ParseTaskDay("not-a-date"); err == nil {
		t.Error("expected error for invalid date")
	}
}

func TestStage_TaskIDEmptyContext(t *testing.T) {
	// asynq's task-id key is unexported (no public constructor), so only the
	// "no task id in context" path is assertable here; handler paths cover the
	// populated case end-to-end.
	if got := TaskID(context.Background()); got != "" {
		t.Errorf("expected empty task id, got %q", got)
	}
}
