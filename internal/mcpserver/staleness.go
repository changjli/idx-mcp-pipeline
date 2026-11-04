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
		return brokerWindowStaleness(db, status.HighWaterMark)
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
func brokerWindowStaleness(db *sqlx.DB, stored *time.Time) mcp.StalenessMetadata {
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

	var age int
	if err := db.Get(&age, `SELECT COUNT(DISTINCT trading_day) FROM daily_prices WHERE trading_day > $1::date`, hwm); err != nil {
		return meta
	}
	meta.DataStale = age > 1
	return meta
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
