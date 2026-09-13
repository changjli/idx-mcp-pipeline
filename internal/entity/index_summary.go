package entity

import "time"

// IndexSummary is one IDX index's daily summary (issue 18). IndexCode is the
// canonical IDX index code (e.g. COMPOSITE, LQ45, IDXENERGY); Date is the
// trading day the summary describes (the wire Date). Numeric fields are
// nullable pointers like daily_prices — the wire always carries them, but the
// read surface must not crash on an unexpected null. NumberOfStock is the
// constituent count (doubles as membership-count validation against
// ticker_indices); Volume/Value/Frequency/MarketCap are the wire's large
// integral aggregates truncated to int64 at ingest.
type IndexSummary struct {
	IndexCode     string    `db:"index_code"`
	Date          time.Time `db:"date"`
	Previous      *float64  `db:"previous"`
	High          *float64  `db:"high"`
	Low           *float64  `db:"low"`
	Close         *float64  `db:"close"`
	Change        *float64  `db:"change"`
	NumberOfStock *int32    `db:"number_of_stock"`
	Volume        *int64    `db:"volume"`
	Value         *int64    `db:"value"`
	Frequency     *int64    `db:"frequency"`
	MarketCap     *int64    `db:"market_cap"`
	StoredAt      time.Time `db:"stored_at"`
}
