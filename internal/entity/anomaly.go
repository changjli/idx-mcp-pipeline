package entity

import "time"

type Anomaly struct {
	ID            int64     `db:"id"`
	Ticker        string    `db:"ticker"`
	TradingDay    time.Time `db:"trading_day"`
	Type          string    `db:"type"`
	Direction     string    `db:"direction"`
	MagnitudePct  *float64  `db:"magnitude_pct"`
	BaselineRef   *float64  `db:"baseline_ref"`
	ObservedValue *float64  `db:"observed_value"`
	PriorValue    *float64  `db:"prior_value"`
}

func (Anomaly) TableName() string { return "anomalies" }
