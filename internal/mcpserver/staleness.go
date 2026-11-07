package mcpserver

import (
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// stalenessFor renders one source's freshness metadata. The broker source is
// special: a window ends on a trading day, so freshness is the age of the newest
// stored window end in trading days (0 = today's session, 1 = yesterday's, which
// is normal before the day's writer runs), not the wall-clock age of the last
// HTTP success.
func stalenessFor(db *sqlx.DB, repo *repository.SourceStatusRepository, source string, now time.Time) mcp.StalenessMetadata {
	if db == nil || repo == nil {
		// Test wiring or an unconfigured source: report stale rather than
		// panic on a nil DB.
		return mcp.StalenessMetadata{DataStale: true}
	}
	status, err := repo.FindBySource(db, source)
	if err != nil || status.LastSuccessAt == nil {
		return mcp.StalenessMetadata{DataStale: true}
	}

	if source == usecase.BrokerStockSummarySource {
		return brokerWindowStaleness(db, status.HighWaterMark, now)
	}

	return mcp.StalenessMetadata{
		DataStale:    status.IsStale(now),
		LastGoodDate: status.LastSuccessAt.Format("2006-01-02"),
	}
}

// brokerWindowStaleness computes the broker source's freshness from the newest
// stored window end. The live MAX is preferred over the stored watermark: a
// window written by the read path advances freshness even before the writers
// update source_status.
//
// Freshness is grain-aware because the monthly archiver is a legitimate writer:
// a newest window end at the last trading day of the last closed month is as
// fresh as month-grain storage can be. Stale means a closed month is missing,
// not that the newest row is older than one session.
func brokerWindowStaleness(db *sqlx.DB, stored *time.Time, now time.Time) mcp.StalenessMetadata {
	meta := mcp.StalenessMetadata{DataStale: true}
	var live sql.NullTime
	if err := db.Get(&live, `SELECT MAX(trading_day) FROM broker_stock_summary_totals
		WHERE investor = 'f' AND source <> 'legacy_ipot'`); err != nil {
		return meta
	}
	hwm := stored
	if live.Valid {
		hwm = &live.Time
	}
	if hwm == nil {
		return meta
	}
	meta.LastGoodDate = hwm.Format("2006-01-02")

	lastClosedMonthEnd := lastClosedMonthEnd(db, now)
	meta.DataStale = hwm.Before(lastClosedMonthEnd)
	return meta
}

// lastClosedMonthEnd returns the last stored trading day of the previous
// calendar month (WIB). It is the freshness bar for an archiver-only period.
// With no prices at all it falls back to the calendar month end, so an empty
// store never reads as fresh.
func lastClosedMonthEnd(db *sqlx.DB, now time.Time) time.Time {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		loc = time.UTC
	}
	local := now.In(loc)
	firstOfMonth := time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, loc)
	prevMonthStart := firstOfMonth.AddDate(0, -1, 0)
	calendarEnd := firstOfMonth.AddDate(0, 0, -1)

	var last sql.NullTime
	if err := db.Get(&last, `SELECT MAX(trading_day) FROM daily_prices
		WHERE trading_day >= $1::date AND trading_day <= $2::date`,
		prevMonthStart, calendarEnd); err != nil || !last.Valid {
		return calendarEnd
	}
	return time.Date(last.Time.Year(), last.Time.Month(), last.Time.Day(), 0, 0, 0, 0, loc)
}

// pipelineStaleness computes the overall staleness metadata for
// get_pipeline_status: stale when any source is stale (time-based, computed
// live), last_good_date is the most recent last_success_at across sources.
func pipelineStaleness(db *sqlx.DB, repo *repository.SourceStatusRepository, now time.Time) mcp.StalenessMetadata {
	statuses, err := repo.FindAll(db)
	if err != nil {
		return mcp.StalenessMetadata{DataStale: true}
	}
	meta := mcp.StalenessMetadata{DataStale: false}
	var newest *time.Time
	for _, s := range statuses {
		if s.IsStale(now) {
			meta.DataStale = true
		}
		if s.LastSuccessAt != nil && (newest == nil || s.LastSuccessAt.After(*newest)) {
			newest = s.LastSuccessAt
		}
	}
	if newest != nil {
		meta.LastGoodDate = newest.Format("2006-01-02")
	}
	return meta
}
