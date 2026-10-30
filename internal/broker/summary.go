// Package broker holds the source-neutral data shapes for broker activity.
//
// It is the seam between the broker-summary adapters (internal/ipot today,
// internal/indexalpha next) and the usecases that consume them: adapters import
// this package, this package imports no adapter, so the dependency direction is
// adapter → broker ← usecase. Nothing here knows which vendor produced a row.
package broker

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
	TVal  int64 // total value (rupiah)
	FNVal int64 // foreign net value (rupiah)
	TLot  int64 // total lots
	Avg   int64 // average price
}

// Result is one ticker+day's broker activity as returned by a source.
type Result struct {
	Buyers  []Row
	Sellers []Row
	Totals  Totals
}
