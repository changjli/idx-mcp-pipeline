package entity

import "time"

// BrokerStockSummary is one broker's buy or sell activity for a ticker+day.
// Grain: (ticker, trading_day, broker_code, side) — the IPOT top-10 table.
type BrokerStockSummary struct {
	Ticker     string    `db:"ticker"`
	BrokerCode string    `db:"broker_code"`
	Side       string    `db:"side"` // 'buy' | 'sell'
	TradingDay time.Time `db:"trading_day"`
	Lot        *int64    `db:"lot"`
	Value      *int64    `db:"value"`
	AvgPrice   *int64    `db:"avg_price"`
	Rank       *int32    `db:"rank"`
}

// BrokerStockSummaryTotals is the footer summary line for a ticker+day.
type BrokerStockSummaryTotals struct {
	Ticker     string    `db:"ticker"`
	TradingDay time.Time `db:"trading_day"`
	TVal       *int64    `db:"t_val"`
	FNVal      *int64    `db:"f_nval"`
	TLot       *int64    `db:"t_lot"`
	Avg        *int64    `db:"avg"`
}
