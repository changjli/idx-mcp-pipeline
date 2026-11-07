package usecase

import (
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
)

// FetchUsageCounter is the daily-pool seam: the concrete Postgres counter in
// production, a recording fake in unit tests. Both bounded spenders share it.
type FetchUsageCounter interface {
	Reserve(db *sqlx.DB, day time.Time, feature string, requested, cap int) (granted int, remaining int, err error)
	Units(db *sqlx.DB, day time.Time, feature string) (int, error)
}

const (
	// BrokerRefreshDailyUnits is the anomaly refresher's hard daily cap.
	BrokerRefreshDailyUnits = 30
	// ScreenerFlowDailyUnits is the screener's shared daily enrichment pool:
	// every screen call on one day draws from these units.
	ScreenerFlowDailyUnits = 10
	// ScreenerEnrichLimit is how many rows of a screen are enriched with a
	// trailing broker window before the final ranking. The initial ranking
	// excludes foreign_net, so a name outside this set cannot enter on it.
	ScreenerEnrichLimit = 10
)

// nextChunkSize returns the next batch size: bounded by what one usable key can
// pay, by the run's remaining budget, by the remaining candidates and by the
// vendor's per-request limit. Zero means no usable key can serve a request now.
func nextChunkSize(perKeyCapacity, budgetLeft, candidatesLeft int) int {
	if perKeyCapacity <= 0 || budgetLeft <= 0 || candidatesLeft <= 0 {
		return 0
	}
	n := perKeyCapacity
	if n > broker.MaxBatchTickers {
		n = broker.MaxBatchTickers
	}
	if n > budgetLeft {
		n = budgetLeft
	}
	if n > candidatesLeft {
		n = candidatesLeft
	}
	return n
}

// usageDay maps an instant onto the WIB calendar day the daily budget keys on.
// The vendor's day and the operators' day are both Asia/Jakarta, so a run after
// 17:00 UTC is already tomorrow's budget, not today's.
func usageDay(now time.Time) time.Time {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		loc = time.UTC
	}
	local := now.In(loc)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC)
}
