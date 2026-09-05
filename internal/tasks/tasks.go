package tasks

import (
	"time"

	"github.com/hibiken/asynq"
)

// Task type constants for asynq task queue.
const (
	TypePipelineDaily      = "pipeline:daily"
	TypeStockSummary       = "idx:stock_summary"
	TypeAnnouncements      = "idx:announcements"
	TypeDetectAnomalies    = "detect:anomalies"
	TypeRSS                = "rss:ingest"
	TypeBrokerStockSummary = "idx:broker_stock_summary"
	// TypeBrokerStockSummaryRange is the on-demand range backfill task (issue
	// 12): one ticker over a date range, enqueued by the MCP backfill tool.
	TypeBrokerStockSummaryRange = "idx:broker_stock_summary_range"
	TypeFilterDisclosures       = "filter:disclosures"
	TypeExtractDisclosure       = "extract:disclosure"
	TypeCleanup                 = "cleanup"
)

// TaskKey returns a dedup key for a task type and date.
// Format: "{type}:{date}" e.g. "idx:stock_summary:2026-08-06"
func TaskKey(typ, date string) string {
	return typ + ":" + date
}

// EnqueuePipelineDaily enqueues the pipeline:daily fan-out task for the given
// date with a date-keyed TaskID. The handler ignores the payload and derives
// "today" from time.Now() server-side; the TaskID is for manual-trigger dedup
// only. Returns ErrTaskIDConflict if already enqueued for this date.
func EnqueuePipelineDaily(client *asynq.Client, date time.Time) (*asynq.TaskInfo, error) {
	return Graph.Node(TypePipelineDaily).Enqueue(client, date, nil)
}
