package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// NewPipelineDailyHandler returns an asynq handler for the pipeline:daily task type.
// It enqueues idx:stock_summary and idx:announcements tasks with date-keyed
// TaskIDs for dedup. Announcement ingestion is decoupled from trading data —
// a failure there is logged but doesn't fail the pipeline.
func NewPipelineDailyHandler(log *logrus.Logger, client *asynq.Client) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		now := time.Now()

		info, err := EnqueueStockSummary(client, now)
		if err == asynq.ErrTaskIDConflict {
			log.Infof("pipeline:daily: stock_summary task for %s already enqueued", now.Format("2006-01-02"))
		} else if err != nil {
			log.Errorf("pipeline:daily: failed to enqueue stock_summary: %v", err)
			return err
		} else {
			log.Infof("pipeline:daily: enqueued stock_summary task id=%s", info.ID)
		}

		info, err = EnqueueAnnouncements(client, now)
		if err == asynq.ErrTaskIDConflict {
			log.Infof("pipeline:daily: announcements task for %s already enqueued", now.Format("2006-01-02"))
		} else if err != nil {
			log.Errorf("pipeline:daily: failed to enqueue announcements: %v", err)
		} else {
			log.Infof("pipeline:daily: enqueued announcements task id=%s", info.ID)
		}

		info, err = EnqueueRSS(client, now)
		if err == asynq.ErrTaskIDConflict {
			log.Infof("pipeline:daily: rss task for %s already enqueued", now.Format("2006-01-02"))
		} else if err != nil {
			log.Errorf("pipeline:daily: failed to enqueue rss: %v", err)
		} else {
			log.Infof("pipeline:daily: enqueued rss task id=%s", info.ID)
		}

		// Retention cleanup runs after the ingestion wave: delayed so extraction
		// (chained off anomalies → filter) has time to finish before eviction.
		// Date-keyed TaskID dedups against a same-day re-run.
		info, err = EnqueueCleanup(client, now, asynq.ProcessIn(cleanupDelay))
		if err == asynq.ErrTaskIDConflict {
			log.Infof("pipeline:daily: cleanup task for %s already enqueued", now.Format("2006-01-02"))
		} else if err != nil {
			log.Errorf("pipeline:daily: failed to enqueue cleanup: %v", err)
		} else {
			log.Infof("pipeline:daily: enqueued cleanup task id=%s (in %s)", info.ID, cleanupDelay)
		}

		return nil
	}
}
