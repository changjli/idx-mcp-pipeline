package tasks

import (
	"github.com/sirupsen/logrus"
)

// logEvent emits a structured pipeline event. logrus already provides ts
// (time) and level; the standard field set is:
//
//	event, task_id, source, ticker, msg, error, latency_ms, fetch_url, status
//
// msg is the logrus message (human-readable detail); event is a
// machine-readable event name (fetch_start, anomaly_detected, ...). Callers
// add task_id/source/ticker and any event-specific fields.
func logEvent(log *logrus.Logger, level logrus.Level, event, msg string, fields logrus.Fields) {
	// Copy so the caller's map is never mutated — a caller that reuses one
	// fields map across events would otherwise have the event field clobbered.
	out := make(logrus.Fields, len(fields)+1)
	for k, v := range fields {
		out[k] = v
	}
	out["event"] = event
	log.WithFields(out).Log(level, msg)
}
