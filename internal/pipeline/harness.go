package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

const (
	// IngestQueue is the asynq queue every ingest task runs on.
	IngestQueue = "ingest"
	// IngestRetention bounds a completed task's visibility window.
	IngestRetention = 24 * time.Hour
)

// Enqueuer is the minimal enqueue seam the harness needs. asynq v0.26 has no
// equivalent interface; *asynq.Client satisfies it structurally, and tests
// provide fakes.
type Enqueuer interface {
	Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
}

// Stage is one task type's boilerplate bundle: the enqueue options shared by
// every ingest task, typed payload decode, and the fetch
// start/success/failure log triplet. Handlers hold one Stage per task type;
// task chaining (the graph edges) stays outside — stages enqueue other task
// types through their Enqueuer directly.
type Stage struct {
	// Type is the asynq task type; also the "source" label in log events
	// and source_status rows.
	Type      string
	Queue     string
	MaxRetry  int
	Retention time.Duration
	Log       *logrus.Logger
	Enq       Enqueuer
	now       func() time.Time
}

// NewIngestStage builds the standard ingest-task stage. maxRetry is per task
// type (3 for most ingests, 2 for extract:disclosure). log may be nil for
// enqueue-only use (the Enqueue* helpers built on raw clients).
func NewIngestStage(taskType string, log *logrus.Logger, enq Enqueuer, maxRetry int) *Stage {
	return &Stage{
		Type:      taskType,
		Queue:     IngestQueue,
		MaxRetry:  maxRetry,
		Retention: IngestRetention,
		Log:       log,
		Enq:       enq,
		now:       time.Now,
	}
}

// DecodeTask unmarshals a task payload into P — the typed decode every
// handler performed inline.
func DecodeTask[P any](t *asynq.Task) (*P, error) {
	var p P
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}
	return &p, nil
}

// ParseTaskDay parses a payload's YYYY-MM-DD date field.
func ParseTaskDay(s string) (time.Time, error) {
	day, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid date %q: %w", s, err)
	}
	return day, nil
}

// TaskID extracts the asynq task id from the handler context.
func TaskID(ctx context.Context) string {
	id, _ := asynq.GetTaskID(ctx)
	return id
}

// Enqueue marshals payload and enqueues the stage's task type with the
// shared option bundle and a date-keyed dedup TaskID. asynq.ErrTaskIDConflict
// means already enqueued.
func (s *Stage) Enqueue(dedupKey string, payload any) (*asynq.TaskInfo, error) {
	return s.EnqueueWithOpts(dedupKey, payload)
}

// EnqueueWithOpts appends extra asynq options (e.g. asynq.ProcessIn for
// self-heal requeues) to the shared bundle. An empty dedupKey omits the
// TaskID option.
func (s *Stage) EnqueueWithOpts(dedupKey string, payload any, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal %s payload: %w", s.Type, err)
	}
	task := asynq.NewTask(s.Type, raw)
	options := []asynq.Option{
		asynq.Queue(s.Queue),
		asynq.MaxRetry(s.MaxRetry),
		asynq.Retention(s.Retention),
	}
	if dedupKey != "" {
		options = append(options, asynq.TaskID(dedupKey))
	}
	options = append(options, opts...)
	return s.Enq.Enqueue(task, options...)
}

// Reenqueue re-enqueues the stage's task type with a delay and a unique
// TaskID (no dedup key) — the re-enqueueing task still holds the keyed ID
// while active.
func (s *Stage) Reenqueue(payload any, delay time.Duration) error {
	_, err := s.EnqueueWithOpts("", payload, asynq.ProcessIn(delay))
	return err
}

// StartFetch emits the fetch_start event (msg "fetching") and returns a
// tracker stamping latency onto the matching success/failure event.
func (s *Stage) StartFetch(taskID, msg string, fields logrus.Fields) *Fetch {
	f := &Fetch{stage: s, taskID: taskID, start: s.now()}
	f.emit(logrus.InfoLevel, "fetch_start", msg, f.withIdentity(fields))
	return f
}

// Fetch tracks one upstream fetch window started by StartFetch.
type Fetch struct {
	stage  *Stage
	taskID string
	start  time.Time
}

// Ok emits fetch_success with the caller's event-specific fields (rows, ...)
// plus the measured latency.
func (f *Fetch) Ok(msg string, fields logrus.Fields) {
	f.emit(logrus.InfoLevel, "fetch_success", msg, f.withIdentity(f.withLatency(fields)))
}

// Fail emits fetch_failure with the error and measured latency.
func (f *Fetch) Fail(msg string, fetchErr error, fields logrus.Fields) {
	fields["error"] = fetchErr.Error()
	f.emit(logrus.ErrorLevel, "fetch_failure", msg, f.withIdentity(f.withLatency(fields)))
}

// withIdentity stamps the correlation keys every fetch event carries:
// task_id + source. Bulk backfill runs outside asynq — no task id exists, and
// the original triplet omitted the field rather than logging an empty value.
func (f *Fetch) withIdentity(fields logrus.Fields) logrus.Fields {
	out := make(logrus.Fields, len(fields)+2)
	for k, v := range fields {
		out[k] = v
	}
	if f.taskID != "" {
		out["task_id"] = f.taskID
	}
	out["source"] = f.stage.Type
	return out
}

func (f *Fetch) withLatency(fields logrus.Fields) logrus.Fields {
	// Latency is measured against the stage's own clock so an injected clock
	// (tests, benchmarking) keeps the measurement deterministic.
	fields["latency_ms"] = f.stage.now().Sub(f.start).Milliseconds()
	return fields
}

func (f *Fetch) emit(level logrus.Level, event, msg string, fields logrus.Fields) {
	if f.stage.Log == nil {
		return
	}
	out := make(logrus.Fields, len(fields)+1)
	for k, v := range fields {
		out[k] = v
	}
	out["event"] = event
	f.stage.Log.WithFields(out).Log(level, msg)
}
