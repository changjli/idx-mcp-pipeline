package entity

import "time"

// Broker summary provenance values. legacy_ipot is the scraped top-10 set the
// full-set change replaced; every new write states its writer explicitly so a
// sweep-era row is distinguishable from a backfilled one.
const (
	BrokerSourceLegacyIPOT = "legacy_ipot"
	BrokerSourceLive       = "live"
	BrokerSourceSweep      = "sweep"
	BrokerSourceBackfill   = "backfill"
)

// BrokerStockSummary is one broker's buy or sell activity for a ticker+day.
// Grain: (ticker, trading_day, broker_code, side). The full source payload is
// stored — rank is dense per side over every broker — and the tool's top-10
// view is a read-time projection. Source records which writer produced the row.
type BrokerStockSummary struct {
	Ticker     string    `db:"ticker"`
	BrokerCode string    `db:"broker_code"`
	Side       string    `db:"side"` // 'buy' | 'sell'
	TradingDay time.Time `db:"trading_day"`
	Lot        *int64    `db:"lot"`
	Value      *int64    `db:"value"`
	AvgPrice   *int64    `db:"avg_price"`
	Rank       *int32    `db:"rank"`
	Source     string    `db:"source"`
}

// BrokerStockSummaryTotals is the per-ticker-day summary line. FNVal nil means
// the foreign net is unobserved, never zero. Source matches the day's rows.
type BrokerStockSummaryTotals struct {
	Ticker     string    `db:"ticker"`
	TradingDay time.Time `db:"trading_day"`
	TVal       *int64    `db:"t_val"`
	FNVal      *int64    `db:"f_nval"`
	TLot       *int64    `db:"t_lot"`
	Avg        *int64    `db:"avg"`
	Source     string    `db:"source"`
}
