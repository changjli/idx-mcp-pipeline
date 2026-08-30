package tasks

import (
	"context"
	"errors"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// NewPipelineDailyHandler returns an asynq handler for the pipeline:daily task type.
// It fires the pipeline:daily Wave — the fixed wave-1 fan-out declared in the
// graph registry (ADR-0008): stock_summary, announcements, rss, and cleanup.
// Date-keyed TaskIDs dedup against a same-day re-run. Announcement/rss/cleanup
// failures are logged but don't fail the pipeline; a stock_summary failure
// fails the fan-out so asynq retries the whole pipeline:daily task.
func NewPipelineDailyHandler(log *logrus.Logger, client *asynq.Client) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		now := time.Now()

		for _, name := range Graph.Node(TypePipelineDaily).Wave {
			node := Graph.Node(name)
			var opts []asynq.Option
			if node.Delay > 0 {
				opts = append(opts, asynq.ProcessIn(node.Delay))
			}
			info, err := node.Enqueue(client, now, nil, opts...)
			if errors.Is(err, asynq.ErrTaskIDConflict) {
				log.Infof("pipeline:daily: %s task for %s already enqueued", node.Type, now.Format("2006-01-02"))
			} else if err != nil {
				log.Errorf("pipeline:daily: failed to enqueue %s: %v", node.Type, err)
				if node.Type == TypeStockSummary {
					return err
				}
			} else {
				log.Infof("pipeline:daily: enqueued %s task id=%s", node.Type, info.ID)
			}
		}

		return nil
	}
}
