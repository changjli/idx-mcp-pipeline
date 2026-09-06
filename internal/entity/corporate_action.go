package entity

import "time"

// CorporateAction is one IDX corporate-action event. Id is the stable IDX
// surrogate (the GetIssuedHistory DataTables row id); EventDate is the
// listing/action date (TanggalPencatatan); JumlahSaham /
// JumlahSahamSetelahTindakan are the per-event amounts (either may be 0).
type CorporateAction struct {
	Id                         int64     `db:"id"`
	Ticker                     string    `db:"ticker"`
	EventDate                  time.Time `db:"event_date"`
	Type                       string    `db:"type"`
	JumlahSaham                int64     `db:"jumlah_saham"`
	JumlahSahamSetelahTindakan int64     `db:"jumlah_saham_setelah_tindakan"`
	StoredAt                   time.Time `db:"stored_at"`
}
