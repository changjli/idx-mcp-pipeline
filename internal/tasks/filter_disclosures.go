package tasks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// filterSelfRetryDelay is the delay between self-synchronizing retries while
// waiting for idx:announcements to have written disclosures fetched on the
// run date. Mirrors detect:anomalies' wait for daily_prices.
const filterSelfRetryDelay = 30 * time.Second

// filterMaxSelfRetry is the self-retry budget (~10 x 30s = 5 min wait). After
// exhausting it, the filter proceeds anyway — catch-up rows (passed_filter IS
// NULL, any date) still need processing, and today's disclosures (if they
// arrive late) are caught by the next day's run.
const filterMaxSelfRetry = 10

// FilterDisclosuresPayload is the payload for a filter:disclosures task.
type FilterDisclosuresPayload struct {
	Date    string `json:"date"`    // YYYY-MM-DD — the anomaly trading day that triggered the run
	Attempt int    `json:"attempt"` // self-synchronizing retry counter (waits for idx:announcements)
}

// EnqueueFilterDisclosures enqueues a filter:disclosures task for the given
// date. Uses a date-keyed TaskID for dedup. Returns ErrTaskIDConflict if
// already enqueued. Chained from detect:anomalies success (unconditionally,
// even when zero anomalies were flagged — non-anomaly disclosures still need
// marking passed_filter=false).
func EnqueueFilterDisclosures(enq pipeline.Enqueuer, date time.Time) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeFilterDisclosures).Enqueue(enq, date, nil)
}

// reenqueueFilterDisclosures re-enqueues a filter:disclosures task with a delay
// while self-syncing on idx:announcements. Uses a unique TaskID (no dedup key)
// because the current task still holds the date-keyed ID while active — mirrors
// reenqueueDetectAnomalies.
func reenqueueFilterDisclosures(enq pipeline.Enqueuer, date string, attempt int) error {
	stage := pipeline.NewIngestStage(TypeFilterDisclosures, nil, enq, 3)
	return stage.Reenqueue(FilterDisclosuresPayload{Date: date, Attempt: attempt}, filterSelfRetryDelay)
}

// NewFilterDisclosuresHandler returns an asynq handler for the
// filter:disclosures task type. The 3-layer filter itself lives in
// pipeline.DisclosureFilter (ADR-0006); the handler synchronizes with
// idx:announcements (self-retrying, then proceeding on catch-up rows), runs
// the filter, and enqueues one extract:disclosure task per passing row.
func NewFilterDisclosuresHandler(
	log *logrus.Logger,
	enq pipeline.Enqueuer,
	db *sqlx.DB,
	disclosureRepo *repository.DisclosureRepository,
	filter *pipeline.DisclosureFilter,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[FilterDisclosuresPayload](t)
		if err != nil {
			return err
		}
		today, err := pipeline.ParseTaskDay(p.Date)
		if err != nil {
			return err
		}

		// Self-synchronizing: wait for idx:announcements to have written
		// disclosures fetched on the run date. filter is chained from
		// detect:anomalies (anomalies already written), but announcements is
		// an independent fan-out with no sync to filter — without this gate,
		// a slow announcements endpoint makes filter win the race and today's
		// disclosures sit pending until the next day's catch-up run.
		announcementsDone, err := disclosureRepo.ExistsFetchedOnDate(db, p.Date)
		if err != nil {
			return fmt.Errorf("check announcements presence: %w", err)
		}
		if !announcementsDone {
			if p.Attempt >= filterMaxSelfRetry {
				log.Warnf("filter:disclosures: no disclosures fetched on %s after %d attempts — proceeding on catch-up rows", p.Date, p.Attempt)
				// Fall through to the filter run: catch-up rows (passed_filter
				// IS NULL, any date) still need processing today. Today's
				// disclosures, if they arrive after this point, are caught by
				// the next day's run.
			} else {
				log.Infof("filter:disclosures: announcements for %s not present (attempt %d), retrying in %s", p.Date, p.Attempt, filterSelfRetryDelay)
				if err := reenqueueFilterDisclosures(enq, p.Date, p.Attempt+1); err != nil {
					return fmt.Errorf("re-enqueue filter:disclosures: %w", err)
				}
				return nil // release current task; re-enqueued copy carries Attempt+1
			}
		}

		enqueue := func(id int64) {
			if _, err := EnqueueExtractDisclosure(enq, id); err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) {
				log.Warnf("filter:disclosures: enqueue extract for disclosure %d: %v", id, err)
			}
		}
		taskID := pipeline.TaskID(ctx)
		stats, err := filter.Filter(ctx, today, enqueue)
		if err != nil {
			return err
		}
		logEvent(log, logrus.InfoLevel, "disclosure_filtered", "disclosure filter run complete",
			logrus.Fields{
				"task_id":      taskID,
				"source":       TypeFilterDisclosures,
				"date":         today.Format("2006-01-02"),
				"total":        stats.Total,
				"passed":       stats.Passed,
				"rejected":     stats.Rejected,
				"re_extracted": stats.ReExtracted,
			})
		return nil
	}
}
