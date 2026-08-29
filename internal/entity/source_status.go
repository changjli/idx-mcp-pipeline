package entity

import "time"

// defaultMaxAgeSeconds is the freshness window used when a source_status row
// has no max_age_seconds set (~1 trading day).
const defaultMaxAgeSeconds int32 = 24 * 60 * 60

type SourceStatus struct {
	Source              string     `db:"source"`
	LastSuccessAt       *time.Time `db:"last_success_at"`
	LastAttemptAt       *time.Time `db:"last_attempt_at"`
	LastError           *string    `db:"last_error"`
	ConsecutiveFailures int32      `db:"consecutive_failures"`
	Stale               bool       `db:"stale"`
	MaxAgeSeconds       int32      `db:"max_age_seconds"`
	HighWaterMark       *time.Time `db:"high_water_mark"`
}

// IsStale reports whether the source is stale at time now under the time-based
// definition (CONTEXT.md): stale when now - last_success_at > max_age,
// independent of the consecutive-failure count. A missing last_success_at (the
// source never succeeded) counts as stale. This is the single source of truth
// shared by the recorder and both read paths.
func (s SourceStatus) IsStale(now time.Time) bool {
	if s.LastSuccessAt == nil {
		return true
	}
	maxAge := time.Duration(s.MaxAgeSeconds) * time.Second
	if maxAge <= 0 {
		maxAge = time.Duration(defaultMaxAgeSeconds) * time.Second
	}
	return now.Sub(*s.LastSuccessAt) > maxAge
}
