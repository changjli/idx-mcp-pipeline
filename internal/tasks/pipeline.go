package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// NewPipelineDailyHandler returns an asynq handler for the pipeline:daily task type.
// It enqueues a noop task with a date-keyed TaskID for dedup.
func NewPipelineDailyHandler(log *logrus.Logger, client *asynq.Client) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		now := time.Now()
		info, err := EnqueueNoop(client, now)
		if err == asynq.ErrTaskIDConflict {
			log.Infof("pipeline:daily: noop task for %s already enqueued", now.Format("2006-01-02"))
			return nil
		}
		if err != nil {
			log.Errorf("pipeline:daily: failed to enqueue noop: %v", err)
			return err
		}
		log.Infof("pipeline:daily: enqueued noop task id=%s", info.ID)
		return nil
	}
}
