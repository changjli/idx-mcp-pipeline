package tasks

import (
	"time"

	"github.com/hibiken/asynq"
)

// Task type constants for asynq task queue.
const (
	TypeNoop               = "noop"
	TypePipelineDaily      = "pipeline:daily"
	TypeStockSummary       = "idx:stock_summary"
	TypeAnnouncements      = "idx:announcements"
	TypeDetectAnomalies    = "detect:anomalies"
	TypeRSS                = "rss:ingest"
	TypeBrokerStockSummary = "idx:broker_stock_summary"
)

// TaskKey returns a dedup key for a task type and date.
// Format: "{type}:{date}" e.g. "noop:2026-08-06"
func TaskKey(typ, date string) string {
	return typ + ":" + date
}

// EnqueueNoop enqueues a noop task for the given date with a date-keyed TaskID.
// Returns ErrTaskIDConflict if a task for this date already exists.
func EnqueueNoop(client *asynq.Client, date time.Time) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeNoop, dateKey)
	payload := []byte(`{"date":"` + dateKey + `"}`)

	task := asynq.NewTask(TypeNoop, payload)
	return client.Enqueue(task, asynq.TaskID(taskKey), asynq.Queue("default"))
}
