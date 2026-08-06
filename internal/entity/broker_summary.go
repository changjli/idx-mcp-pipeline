package entity

import "time"

type BrokerSummary struct {
	BrokerCode string    `db:"broker_code"`
	TradingDay time.Time `db:"trading_day"`
	FirmName   *string   `db:"firm_name"`
	Volume     *int64    `db:"volume"`
	Value      *int64    `db:"value"`
	Frequency  *int32    `db:"frequency"`
}
