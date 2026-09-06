package entity

import "time"

// Suspension is one BEI UMA/suspension announcement event. Ticker is Kode
// (GetSuspension) / CompanyID (GetUma), unpadded, never empty (the ">1 Kode"
// aggregate rows are dropped client-side). EventDate is Date / UMADate. Type is
// the raw IDX discriminator: SPT (suspend), UPT (unsuspend), or UMA. Reason is
// the Judul (announcement title).
type Suspension struct {
	Id        int64     `db:"id"`
	Ticker    string    `db:"ticker"`
	EventDate time.Time `db:"event_date"`
	Type      string    `db:"type"`
	Reason    string    `db:"reason"`
	StoredAt  time.Time `db:"stored_at"`
}
