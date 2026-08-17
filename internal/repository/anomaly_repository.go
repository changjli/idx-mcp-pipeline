package repository

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

type AnomalyRepository struct {
	*Repository[entity.Anomaly]
	Log *logrus.Logger
}

func NewAnomalyRepository(log *logrus.Logger) *AnomalyRepository {
	return &AnomalyRepository{
		Repository: &Repository[entity.Anomaly]{},
		Log:        log,
	}
}

func (r *AnomalyRepository) Insert(db *sqlx.DB, anomaly *entity.Anomaly) error {
	query := `
		INSERT INTO anomalies (ticker, trading_day, type, direction, magnitude_pct, baseline_ref, observed_value, prior_value)
		VALUES (:ticker, :trading_day, :type, :direction, :magnitude_pct, :baseline_ref, :observed_value, :prior_value)
		ON CONFLICT (ticker, trading_day, type) DO UPDATE SET
			direction = EXCLUDED.direction,
			magnitude_pct = EXCLUDED.magnitude_pct,
			baseline_ref = EXCLUDED.baseline_ref,
			observed_value = EXCLUDED.observed_value,
			prior_value = EXCLUDED.prior_value
	`
	_, err := db.NamedExec(query, anomaly)
	return err
}

func (r *AnomalyRepository) FindByDate(db *sqlx.DB, tradingDay string) ([]entity.Anomaly, error) {
	var anomalies []entity.Anomaly
	err := db.Select(&anomalies,
		"SELECT * FROM anomalies WHERE trading_day = $1 ORDER BY ticker",
		tradingDay,
	)
	return anomalies, err
}

func (r *AnomalyRepository) FindByTickerAndDate(db *sqlx.DB, ticker string, tradingDay string) ([]entity.Anomaly, error) {
	var anomalies []entity.Anomaly
	err := db.Select(&anomalies,
		"SELECT * FROM anomalies WHERE ticker = $1 AND trading_day = $2 ORDER BY type",
		ticker, tradingDay,
	)
	return anomalies, err
}

func (r *AnomalyRepository) ExistsForDate(db *sqlx.DB, tradingDay string) (bool, error) {
	var count int
	err := db.Get(&count,
		"SELECT COUNT(*) FROM anomalies WHERE trading_day = $1",
		tradingDay,
	)
	return count > 0, err
}

// AnomalyWithDisclosures is one anomaly row plus the disclosure IDs the
// read-time JOIN (ticket 10) derived for it: disclosures for the same ticker
// announced within the filter's lookback window before the anomaly's trading
// day that passed the filter. Mirrors the filter task's anomaly-gate semantics
// so an anomaly's disclosure_ids match what filter:disclosures actually passed.
type AnomalyWithDisclosures struct {
	entity.Anomaly
	DisclosureIDs pq.Int64Array `db:"disclosure_ids"`
}

// FindByDateWithDisclosures returns the anomalies for a trading day (optionally
// filtered to one ticker) with their derived disclosure_ids. The disclosure
// match window is [trading_day - DisclosureFilterLookbackDays, trading_day] —
// the same window the filter task uses — not just the same-day match, so a
// disclosure announced days before the anomaly's trading day still links.
func (r *AnomalyRepository) FindByDateWithDisclosures(db *sqlx.DB, tradingDay string, ticker *string) ([]AnomalyWithDisclosures, error) {
	day, err := time.Parse("2006-01-02", tradingDay)
	if err != nil {
		return nil, err
	}
	lookbackStart := day.AddDate(0, 0, -DisclosureFilterLookbackDays)
	var rows []AnomalyWithDisclosures
	err = db.Select(&rows, `
		SELECT a.*,
		       COALESCE(array_agg(d.id) FILTER (WHERE d.id IS NOT NULL), '{}') AS disclosure_ids
		FROM anomalies a
		LEFT JOIN disclosures d
		  ON d.ticker = a.ticker
		 AND d.announcement_date >= $2
		 AND d.announcement_date <= a.trading_day
		 AND d.passed_filter = true
		WHERE a.trading_day = $1
		  AND ($3::text IS NULL OR a.ticker = $3)
		GROUP BY a.id
		ORDER BY a.ticker
	`, tradingDay, lookbackStart, ticker)
	return rows, err
}

// ExistsForTickerInWindow reports whether an anomaly exists for the ticker
// with trading_day within [announcementDate, announcementDate + lookbackDays].
// The anomaly-gate (ticket 11): a disclosure's market impact can lag its
// announcement by days, so the match window extends forward from the
// announcement date, not just the same day.
// DeleteOlderThan deletes anomalies rows whose trading_day is older than the
// retention window. The anomaly-gate only needs a 7-day lookback, so old rows
// are pure growth. Returns rows deleted.
func (r *AnomalyRepository) DeleteOlderThan(db *sqlx.DB, days int) (int64, error) {
	res, err := db.Exec(
		"DELETE FROM anomalies WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (r *AnomalyRepository) ExistsForTickerInWindow(db *sqlx.DB, ticker string, announcementDate time.Time, lookbackDays int) (bool, error) {
	end := announcementDate.AddDate(0, 0, lookbackDays)
	var exists bool
	err := db.Get(&exists, `
		SELECT EXISTS(
			SELECT 1 FROM anomalies
			WHERE ticker = $1 AND trading_day BETWEEN $2 AND $3
		)
	`, ticker, announcementDate, end)
	return exists, err
}
