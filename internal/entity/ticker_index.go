package entity

import "time"

// TickerIndex is one point-in-time index-membership row (issue 15b): ticker X
// was a constituent of index Y as of effective_date. Each seeder run writes a
// fresh snapshot dated with the run date, so historical screens read the
// constituents that were true then, never today's list.
type TickerIndex struct {
	TickerCode    string    `db:"ticker_code"`
	IndexCode     string    `db:"index_code"`
	EffectiveDate time.Time `db:"effective_date"`
	StoredAt      time.Time `db:"stored_at"`
}
