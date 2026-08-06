package entity

import "time"

type DailyPrice struct {
	Ticker     string    `db:"ticker"`
	TradingDay time.Time `db:"trading_day"`
	Open       *float64  `db:"open"`
	High       *float64  `db:"high"`
	Low        *float64  `db:"low"`
	Close      *float64  `db:"close"`
	Volume     *int64    `db:"volume"`
	Value      *int64    `db:"value"`
	Frequency  *int32    `db:"frequency"`
	Source     string    `db:"source"`
	FetchedAt  time.Time `db:"fetched_at"`
}
