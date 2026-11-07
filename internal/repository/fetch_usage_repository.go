package repository

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

// Daily fetch-budget feature names. They are the `feature` key in fetch_usage,
// so a new bounded spender names itself here rather than sharing another
// feature's pool by accident.
const (
	// FetchFeatureScreenerFlow is the screener's shared enrichment pool: all
	// screen calls on one day draw from the same units.
	FetchFeatureScreenerFlow = "screener_flow"
	// FetchFeatureBrokerRefresh is the daily anomaly refresher's pool.
	FetchFeatureBrokerRefresh = "broker_refresh"
)

// FetchUsageRepository persists per-feature daily unit budgets. The vendor
// quota is monthly per key; these counters are the pipeline's own per-day
// policy caps, so they are durable rather than process-local.
type FetchUsageRepository struct {
	Log *logrus.Logger
}

func NewFetchUsageRepository(log *logrus.Logger) *FetchUsageRepository {
	return &FetchUsageRepository{Log: log}
}

// Reserve atomically grants up to requested units from a feature's daily pool,
// never exceeding cap. It returns the granted units and the units left in the
// pool after the grant. A grant of 0 means the pool is already exhausted.
//
// The read-modify-write runs under a row lock, so two concurrent screen calls
// cannot both see the same remaining count and overspend.
func (r *FetchUsageRepository) Reserve(db *sqlx.DB, day time.Time, feature string, requested, cap int) (granted int, remaining int, err error) {
	if db == nil {
		return 0, 0, fmt.Errorf("fetch usage: no database")
	}
	if feature == "" {
		return 0, 0, fmt.Errorf("fetch usage: a feature is required")
	}
	if cap < 0 {
		return 0, 0, fmt.Errorf("fetch usage: cap must be non-negative")
	}
	if requested < 0 {
		return 0, 0, fmt.Errorf("fetch usage: requested must be non-negative")
	}

	tx, err := db.Beginx()
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	if _, err := tx.Exec(`
		INSERT INTO fetch_usage (usage_day, feature, units)
		VALUES ($1::date, $2, 0)
		ON CONFLICT (usage_day, feature) DO NOTHING`,
		day.Format("2006-01-02"), feature); err != nil {
		return 0, 0, err
	}

	var used int
	if err := tx.Get(&used, `
		SELECT units FROM fetch_usage
		WHERE usage_day = $1::date AND feature = $2
		FOR UPDATE`,
		day.Format("2006-01-02"), feature); err != nil {
		return 0, 0, err
	}

	allowed := cap - used
	if allowed < 0 {
		allowed = 0
	}
	if allowed > requested {
		allowed = requested
	}
	if allowed > 0 {
		if _, err := tx.Exec(`
			UPDATE fetch_usage
			SET units = units + $3, updated_at = NOW()
			WHERE usage_day = $1::date AND feature = $2`,
			day.Format("2006-01-02"), feature, allowed); err != nil {
			return 0, 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return allowed, cap - used - allowed, nil
}

// Units returns the units already spent by a feature on a day. A missing row
// reads as 0, not an error.
func (r *FetchUsageRepository) Units(db *sqlx.DB, day time.Time, feature string) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("fetch usage: no database")
	}
	var used int
	err := db.Get(&used, `
		SELECT COALESCE((
			SELECT units FROM fetch_usage
			WHERE usage_day = $1::date AND feature = $2
		), 0)`,
		day.Format("2006-01-02"), feature)
	if err != nil {
		return 0, err
	}
	return used, nil
}
