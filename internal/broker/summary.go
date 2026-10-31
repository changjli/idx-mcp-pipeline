// Package broker holds the source-neutral data shapes for broker activity.
//
// It is the seam between the broker-summary adapters (internal/indexalpha, and
// internal/ipot for the legacy financials path) and the usecases that consume
// them: adapters import this package, this package imports no adapter, so the
// dependency direction is adapter → broker ← usecase. Nothing here knows which
// vendor produced a row.
package broker

import "errors"

// ErrQuotaExhausted reports that the source refused a request because the
// account's monthly (or plan-daily) allowance is used up. It is shared by every
// quota-metered adapter so the MCP boundary can surface quota as its own
// structured error instead of a generic upstream failure. Retrying the same key
// will not help until the source's reset window passes.
var ErrQuotaExhausted = errors.New("broker: quota exhausted")

// ErrRateLimited reports a short-window rate limit (HTTP 429). Unlike
// ErrQuotaExhausted it clears on its own, so the caller may cool the key down
// and retry later.
var ErrRateLimited = errors.New("broker: rate limited")

// Row is one broker's buy or sell activity for a ticker+day.
type Row struct {
	BrokerCode string
	Lot        int64
	Value      int64
	AvgPrice   int64
	Rank       int
}

// Totals is a source's summary line for one ticker+day.
type Totals struct {
	TVal  int64  // total value (rupiah)
	FNVal *int64 // foreign net value (rupiah); nil = unobserved, never zero
	TLot  int64  // total lots
	Avg   int64  // average price (rupiah per share)
}

// Result is one ticker+day's broker activity as returned by a source.
type Result struct {
	Buyers  []Row
	Sellers []Row
	Totals  Totals
}

// ForeignFlow is one ticker+day's foreign activity as returned by a source.
// NetForeign is buy − sell (positive = foreign accumulation).
type ForeignFlow struct {
	ForeignBuy  int64
	ForeignSell int64
	NetForeign  int64
}
