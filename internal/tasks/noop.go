package tasks

import (
	"context"
	"encoding/json"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// NoopPayload is the payload for a noop task.
type NoopPayload struct {
	Date string `json:"date"`
}

// NewNoopHandler returns an asynq handler for the noop task type.
// It logs the task execution and returns nil (success).
func NewNoopHandler(log *logrus.Logger) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var p NoopPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			log.Warnf("noop task: invalid payload: %v", err)
			// Still succeed — noop should never fail
			return nil
		}
		log.Infof("noop task executed: date=%s type=%s", p.Date, t.Type())
		return nil
	}
}
