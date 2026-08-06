package entity

import "time"

type SourceStatus struct {
	Source             string     `db:"source"`
	LastSuccessAt      *time.Time `db:"last_success_at"`
	LastAttemptAt      *time.Time `db:"last_attempt_at"`
	LastError          *string    `db:"last_error"`
	ConsecutiveFailures int32     `db:"consecutive_failures"`
	Stale              bool       `db:"stale"`
	MaxAgeSeconds      int32      `db:"max_age_seconds"`
	HighWaterMark      *time.Time `db:"high_water_mark"`
}
