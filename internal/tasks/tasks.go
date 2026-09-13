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
	// TypeBrokerStockSummarySweep is the weekly ADTV-gated sweep (issue 14b):
	// one task per date that backfills broker summaries for liquid tickers
	// over the trailing window, skipping days already stored. Runs on top of
	// the anomaly-gated per-ticker flow.
	TypeBrokerStockSummarySweep = "idx:broker_stock_summary_sweep"
	TypeFilterDisclosures       = "filter:disclosures"
	TypeExtractDisclosure       = "extract:disclosure"
	TypeCleanup                 = "cleanup"
	// TypeCorporateActions is the daily corporate-actions calendar fetch (issue
	// 09): one request to GetIssuedHistory over a rolling window, persisted to
	// corporate_actions. The MCP tool is a pure DB read over the stored rows.
	TypeCorporateActions = "idx:corporate_actions"
	// TypeSuspensions is the daily BEI UMA/suspension list fetch (issue 10):
	// two requests (GetSuspension + GetUma) over a rolling window, persisted to
	// suspensions. The MCP tool is a pure DB read over the stored rows.
	TypeSuspensions = "idx:suspensions"
	// TypeKSEIBalancepos is the monthly KSEI balance-position ingestion (issue
	// 08): one zip download for the preceding month-end, persisted to
	// shareholder_composition. The task runs daily but no-ops once the latest
	// file is stored (watermark-gated). The MCP tool is a pure DB read.
	TypeKSEIBalancepos = "ksei:balancepos"
	// TypeSectorIndex is the 6-monthly sector/industry + index-membership
	// seeder (issue 15b): one stock-screener/get call, sector taxonomy upserted
	// into tickers, index membership replaced for the run date (point-in-time).
	// Scheduled Feb+Jul to match the LQ45/Kompas100/IDX80/IDX30 rebalance
	// cadence; the MCP tools read the stored rows.
	TypeSectorIndex = "idx:sector_index"
	// TypeIndexSummary is the daily index/sector summary ingestion (issue 18):
	// one GetIndexSummary call (all 45 indices incl. the 11 sector indices),
	// upserted to index_summaries keyed by (index_code, trading date). Fired in
	// the pipeline:daily Wave; feeds Stage-0 sector-rotation + market regime.
	TypeIndexSummary = "idx:index_summary"
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
