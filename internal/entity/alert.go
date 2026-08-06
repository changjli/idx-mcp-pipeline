package entity

import "time"

type Alert struct {
	ID         int64      `db:"id"`
	Source     string     `db:"source"`
	AlertType  string     `db:"alert_type"`
	Message    string     `db:"message"`
	RaisedAt   time.Time  `db:"raised_at"`
	ResolvedAt *time.Time `db:"resolved_at"`
}

func (Alert) TableName() string { return "alerts" }
