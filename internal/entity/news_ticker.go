package entity

type NewsTicker struct {
	NewsID      int64  `db:"news_id"`
	Ticker      string `db:"ticker"`
	MatchMethod string `db:"match_method"`
}
