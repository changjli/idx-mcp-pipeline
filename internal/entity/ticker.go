package entity

import "time"

type Ticker struct {
	Code        string     `db:"code"`
	Name        string     `db:"name"`
	ListingDate *time.Time `db:"listing_date"`
	Shares      *int64     `db:"shares"`
	ListingBoard *string   `db:"listing_board"`
	Sektor      *string    `db:"sektor"`
	Industri    *string    `db:"industri"`
	Active      bool       `db:"active"`
	FirstSeenAt time.Time  `db:"first_seen_at"`
	UpdatedAt   time.Time  `db:"updated_at"`
}
