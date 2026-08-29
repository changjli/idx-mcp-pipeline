// Package pipeline holds the daily Pipeline's stage engines as deep usecases:
// anomaly detection, the 3-layer disclosure filter, and source-status
// recording. The asynq handlers in internal/tasks stay thin shells that decode
// a payload, call a stage, and chain the next task (ADR-0006).
package pipeline

import (
	"context"
	"math"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// anomalyVolumeThreshold is the multiple of the 20-day average volume
	// that flags a volume spike (2.0x).
	anomalyVolumeThreshold = 2.0
	// anomalyPriceThreshold is the minimum day-over-day close change (5%)
	// that flags a price shift.
	anomalyPriceThreshold = 0.05
	// anomalyBaselineDays is the number of prior trading days used for the
	// volume baseline. Tickers with fewer days of history are skipped for
	// volume (logged); price detection still runs.
	anomalyBaselineDays = 20
)

// DefaultADTVMinValue is the minimum today trade value (Rp) for a ticker's
// anomaly to be flagged — the ADTV liquidity filter. Filters out illiquid
// gorengan stocks where a small trade can spike volume 500%. Configurable via
// config.json ("anomaly.min_adtv_value"); <= 0 in NewAnomalyDetector falls
// back to this default.
const DefaultADTVMinValue int64 = 5_000_000_000

// DailyPriceSource supplies the anomaly-candidate rows the detector scores.
// Consumer-side interface (ADR-0006): satisfied by the sqlx-backed
// DailyPriceRepository via NewSQLDailyPriceSource; tests provide the second
// adapter.
type DailyPriceSource interface {
	AnomalyCandidates(tradingDay time.Time) ([]repository.AnomalyCandidate, error)
}

// AnomalySink persists detection output. Consumer-side interface: satisfied
// by the sqlx-backed AnomalyRepository via NewSQLAnomalySink.
type AnomalySink interface {
	Insert(anomaly *entity.Anomaly) error
}

// SQLDailyPriceSource adapts DailyPriceRepository to DailyPriceSource.
type SQLDailyPriceSource struct {
	repo *repository.DailyPriceRepository
	db   *sqlx.DB
}

// NewSQLDailyPriceSource binds a daily-price repository to its database.
func NewSQLDailyPriceSource(repo *repository.DailyPriceRepository, db *sqlx.DB) *SQLDailyPriceSource {
	return &SQLDailyPriceSource{repo: repo, db: db}
}

// AnomalyCandidates returns, per ticker that traded on the given day, the
// 20-day volume baseline and previous close the detector scores.
func (s *SQLDailyPriceSource) AnomalyCandidates(tradingDay time.Time) ([]repository.AnomalyCandidate, error) {
	return s.repo.AnomalyCandidates(s.db, tradingDay)
}

// SQLAnomalySink adapts AnomalyRepository to AnomalySink.
type SQLAnomalySink struct {
	repo *repository.AnomalyRepository
	db   *sqlx.DB
}

// NewSQLAnomalySink binds an anomaly repository to its database.
func NewSQLAnomalySink(repo *repository.AnomalyRepository, db *sqlx.DB) *SQLAnomalySink {
	return &SQLAnomalySink{repo: repo, db: db}
}

// Insert upserts one anomaly row.
func (s *SQLAnomalySink) Insert(anomaly *entity.Anomaly) error {
	return s.repo.Insert(s.db, anomaly)
}

// AnomalyDetector computes volume and price anomalies for a trading day from
// daily_prices and writes them to the anomalies table. The ADTV liquidity
// filter threshold is fixed at construction; <= 0 falls back to
// DefaultADTVMinValue.
type AnomalyDetector struct {
	dailyPrices DailyPriceSource
	anomalies   AnomalySink
	minADTV     int64
	log         *logrus.Logger
}

// NewAnomalyDetector wires a detector over its data source and sink.
func NewAnomalyDetector(dailyPrices DailyPriceSource, anomalies AnomalySink, log *logrus.Logger, minADTV int64) *AnomalyDetector {
	if minADTV <= 0 {
		minADTV = DefaultADTVMinValue
	}
	return &AnomalyDetector{
		dailyPrices: dailyPrices,
		anomalies:   anomalies,
		minADTV:     minADTV,
		log:         log,
	}
}

// Detect computes volume and price anomalies for the given trading day and
// upserts them into the anomalies table. minADTV (set at construction) is the
// ADTV liquidity filter threshold; tickers whose today trade value falls
// below it are skipped for both anomaly types. Returns the rows written, in
// insertion order. The detection day (not the wall clock) anchors every
// window: the chained date is data-presence-driven, so the engine needs no
// clock.
func (d *AnomalyDetector) Detect(_ context.Context, day time.Time) ([]*entity.Anomaly, error) {
	candidates, err := d.dailyPrices.AnomalyCandidates(day)
	if err != nil {
		return nil, err
	}

	var detected []*entity.Anomaly
	skippedVolume := 0
	skippedADTV := 0
	for _, c := range candidates {
		// ADTV liquidity filter: skip illiquid tickers entirely (both anomaly
		// types) so a small trade can't spike volume 500%.
		if !passesADTV(c, d.minADTV) {
			skippedADTV++
			continue
		}

		if a := volumeAnomaly(c, day); a != nil {
			if err := d.anomalies.Insert(a); err != nil {
				d.log.Warnf("detect:anomalies: volume insert failed for %s: %v", c.Ticker, err)
				// Original semantics preserved verbatim: a failed volume insert
				// also skips this candidate's price anomaly (fail-fast per row).
				continue
			}
			detected = append(detected, a)
		} else if c.TodayVolume != nil && c.BaselineDays < anomalyBaselineDays {
			skippedVolume++
		}

		if a := priceAnomaly(c, day); a != nil {
			if err := d.anomalies.Insert(a); err != nil {
				d.log.Warnf("detect:anomalies: price insert failed for %s: %v", c.Ticker, err)
			} else {
				detected = append(detected, a)
			}
		}
	}

	if skippedVolume > 0 {
		d.log.Infof("detect:anomalies: skipped %d ticker(s) for volume (fewer than %d days history)", skippedVolume, anomalyBaselineDays)
	}
	if skippedADTV > 0 {
		d.log.Infof("detect:anomalies: skipped %d ticker(s) below ADTV liquidity threshold (min %d)", skippedADTV, d.minADTV)
	}
	return detected, nil
}

// passesADTV reports whether a ticker's today trade value meets the minimum
// ADTV (average daily trading value) liquidity threshold. Filters out
// illiquid gorengan stocks where a Rp 2M trade can spike volume 500%. A nil
// value (missing data) does not trigger the filter. minADTV <= 0 passes every
// non-nil value here, but it is not a configuration escape hatch:
// NewAnomalyDetector rewrites a <= 0 threshold to DefaultADTVMinValue, so the
// filter cannot be disabled via config ("anomaly.min_adtv_value").
func passesADTV(c repository.AnomalyCandidate, minADTV int64) bool {
	if c.TodayValue == nil || minADTV <= 0 {
		return true
	}
	return *c.TodayValue >= minADTV
}

// volumeAnomaly returns an anomaly row when today's volume exceeds the
// 20-day baseline by the threshold. Returns nil when the ticker has fewer
// than anomalyBaselineDays days of history or the baseline is unusable.
// magnitude_pct is how far over the baseline (e.g. +180% for a 280% spike).
func volumeAnomaly(c repository.AnomalyCandidate, today time.Time) *entity.Anomaly {
	if c.TodayVolume == nil || c.BaselineVolume == nil || *c.BaselineVolume <= 0 {
		return nil
	}
	if c.BaselineDays < anomalyBaselineDays {
		return nil
	}
	ratio := float64(*c.TodayVolume) / *c.BaselineVolume
	if ratio <= anomalyVolumeThreshold {
		return nil
	}
	mag := (ratio - 1) * 100
	observed := float64(*c.TodayVolume)
	return &entity.Anomaly{
		Ticker:        c.Ticker,
		TradingDay:    today,
		Type:          "volume",
		Direction:     "up",
		MagnitudePct:  &mag,
		BaselineRef:   c.BaselineVolume,
		ObservedValue: &observed,
	}
}

// priceAnomaly returns an anomaly row when the day-over-day close change
// meets the threshold. Direction is "up" for a rise, "down" for a drop.
func priceAnomaly(c repository.AnomalyCandidate, today time.Time) *entity.Anomaly {
	if c.TodayClose == nil || c.PrevClose == nil || *c.PrevClose == 0 {
		return nil
	}
	change := (*c.TodayClose - *c.PrevClose) / *c.PrevClose
	if math.Abs(change) < anomalyPriceThreshold {
		return nil
	}
	dir := "up"
	if change < 0 {
		dir = "down"
	}
	mag := change * 100
	return &entity.Anomaly{
		Ticker:        c.Ticker,
		TradingDay:    today,
		Type:          "price",
		Direction:     dir,
		MagnitudePct:  &mag,
		ObservedValue: c.TodayClose,
		PriorValue:    c.PrevClose,
	}
}
