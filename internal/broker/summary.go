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
// account's monthly ticker allowance is used up. It is shared by every
// quota-metered adapter so the MCP boundary can surface quota as its own
// structured error instead of a generic upstream failure. Retrying the same key
// will not help until the source's rolling reset window passes.
var ErrQuotaExhausted = errors.New("broker: quota exhausted")

// ErrDailyLimit reports that the source refused a request because the key hit
// its per-day request cap (one request per HTTP call, regardless of ticker
// count). It is deliberately distinct from ErrQuotaExhausted: the monthly
// ticker allowance is untouched, the key recovers at the next local midnight,
// and the remaining monthly units must not be discarded.
var ErrDailyLimit = errors.New("broker: daily request limit")

// ErrRateLimited reports a short-window rate limit (HTTP 429). Unlike
// ErrQuotaExhausted it clears on its own, so the caller may cool the key down
// and retry later.
var ErrRateLimited = errors.New("broker: rate limited")

// Investor lanes the pipeline stores. The vendor also exposes d (domestic) and
// or (an alias for all); neither is a pipeline lane, so they are rejected at
// the client boundary rather than stored under a misleading investor value.
const (
	InvestorAll     = "all"
	InvestorForeign = "f"
)

// MarketRegular is the only board the pipeline stores. Negotiated-market
// ingestion is out of scope, and market defaults differ between vendor
// endpoints, so every call pins this value.
const MarketRegular = "RG"

// Row is one broker's buy or sell activity for a ticker+window.
type Row struct {
	BrokerCode string
	Lot        int64
	Value      int64
	AvgPrice   int64
	Rank       int
}

// Totals is a source's summary line for one ticker+window. TVal, TLot and Avg
// are all-investor totals; in the f lane they are unobserved (zero in memory,
// written as NULL), and only FNVal is set.
type Totals struct {
	TVal  int64  // total value (rupiah)
	FNVal *int64 // foreign net value (rupiah); nil = unobserved, never zero
	TLot  int64  // total lots
	Avg   int64  // average price (rupiah per share)
}

// Result is one ticker+window's broker activity as returned by a source.
//
// Investor is the lane the rows belong to. It is carried with the result so a
// writer stamps the same lane the source was asked for; an empty value is the
// legacy all-investor lane.
type Result struct {
	Investor string
	Buyers   []Row
	Sellers  []Row
	Totals   Totals
}

// MaxBatchTickers is the vendor's hard limit on tickers per batch request. It is
// a contract constant, not a tuning knob: a larger request is rejected whole.
const MaxBatchTickers = 50

// Batch is one batch call's per-ticker outcome. Results holds the tickers that
// parsed cleanly; Invalid holds the ones the source returned but a contract
// invariant rejected (a ticker-day is per-ticker atomic, so one bad payload must
// not discard the other 49). A ticker absent from both maps was absent from the
// response and is treated as an empty (no-activity) day by the caller.
type Batch struct {
	Results map[string]*Result
	Invalid map[string]error
}
