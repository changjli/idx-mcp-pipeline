package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// NewPipelineDailyHandler returns an asynq handler for the pipeline:daily task type.
// It enqueues an idx:stock_summary task with a date-keyed TaskID for dedup.
func NewPipelineDailyHandler(log *logrus.Logger, client *asynq.Client) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		now := time.Now()
		info, err := EnqueueStockSummary(client, now)
		if err == asynq.ErrTaskIDConflict {
			log.Infof("pipeline:daily: stock_summary task for %s already enqueued", now.Format("2006-01-02"))
			return nil
		}
		if err != nil {
			log.Errorf("pipeline:daily: failed to enqueue stock_summary: %v", err)
			return err
		}
		log.Infof("pipeline:daily: enqueued stock_summary task id=%s", info.ID)
		return nil
	}
}
