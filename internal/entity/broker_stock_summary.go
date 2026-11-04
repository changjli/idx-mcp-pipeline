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
	// Window-grain writers: archive is a composable month chunk, window is a
	// non-composable cached answer.
	BrokerSourceArchive = "archive"
	BrokerSourceWindow  = "window"
)

// BrokerStockSummary is one broker's buy or sell activity for a ticker+window.
// Grain: (ticker, trading_day, win_from, investor, broker_code, side).
// TradingDay is the window END; WinFrom is its start, and a single day is the
// degenerate win_from == trading_day window. The full source payload is stored
// — rank is dense per side over every broker — and the tool's top-10 view is a
// read-time projection. Source records which writer produced the row.
type BrokerStockSummary struct {
	Ticker     string    `db:"ticker"`
	BrokerCode string    `db:"broker_code"`
	Side       string    `db:"side"` // 'buy' | 'sell'
	TradingDay time.Time `db:"trading_day"`
	WinFrom    time.Time `db:"win_from"`
	Investor   string    `db:"investor"` // 'all' | 'f'
	Market     string    `db:"market"`   // 'RG'
	Lot        *int64    `db:"lot"`
	Value      *int64    `db:"value"`
	AvgPrice   *int64    `db:"avg_price"`
	Rank       *int32    `db:"rank"`
	Source     string    `db:"source"`
}

// BrokerStockSummaryTotals is the per-ticker-window summary line. TradingDay is
// the window end and WinFrom its start. FNVal nil means the foreign net is
// unobserved, never zero; in the foreign lane the day-total columns (TVal, TLot,
// Avg) are also nil because an f payload has no all-investor totals. Source
// matches the window's rows.
type BrokerStockSummaryTotals struct {
	Ticker     string    `db:"ticker"`
	TradingDay time.Time `db:"trading_day"`
	WinFrom    time.Time `db:"win_from"`
	Investor   string    `db:"investor"` // 'all' | 'f'
	Market     string    `db:"market"`   // 'RG'
	TVal       *int64    `db:"t_val"`
	FNVal      *int64    `db:"f_nval"`
	TLot       *int64    `db:"t_lot"`
	Avg        *int64    `db:"avg"`
	Source     string    `db:"source"`
}
