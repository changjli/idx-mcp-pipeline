package entity

import "time"

// ShareholderComposition is one KSEI balance-position row for one ticker at
// one month-end position date. The 18 per-type columns keep KSEI's own
// investor codes verbatim (IS=Insurance, CP=Corporate, PF=Pension Fund,
// IB=Financial Institution, ID=Individu, MF=Mutual Fund, SC=Securities
// Company, FD=Foundation, OT=Others). Counts are scripless (SID-held)
// positions; Total = LocalTotal + ForeignTotal.
type ShareholderComposition struct {
	Ticker       string    `db:"ticker"`
	PositionDate time.Time `db:"position_date"`
	SecNum       int64     `db:"sec_num"`
	Price        int64     `db:"price"`
	LocalIS      int64     `db:"local_is"`
	LocalCP      int64     `db:"local_cp"`
	LocalPF      int64     `db:"local_pf"`
	LocalIB      int64     `db:"local_ib"`
	LocalID      int64     `db:"local_id"`
	LocalMF      int64     `db:"local_mf"`
	LocalSC      int64     `db:"local_sc"`
	LocalFD      int64     `db:"local_fd"`
	LocalOT      int64     `db:"local_ot"`
	LocalTotal   int64     `db:"local_total"`
	ForeignIS    int64     `db:"foreign_is"`
	ForeignCP    int64     `db:"foreign_cp"`
	ForeignPF    int64     `db:"foreign_pf"`
	ForeignIB    int64     `db:"foreign_ib"`
	ForeignID    int64     `db:"foreign_id"`
	ForeignMF    int64     `db:"foreign_mf"`
	ForeignSC    int64     `db:"foreign_sc"`
	ForeignFD    int64     `db:"foreign_fd"`
	ForeignOT    int64     `db:"foreign_ot"`
	ForeignTotal int64     `db:"foreign_total"`
	Total        int64     `db:"total"`
	StoredAt     time.Time `db:"stored_at"`
}
