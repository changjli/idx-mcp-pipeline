package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/hibiken/asynq"
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
	// anomalySelfRetryDelay is the delay between self-synchronizing retries
	// while waiting for today's daily_prices rows.
	anomalySelfRetryDelay = 30 * time.Second
	// anomalyMaxSelfRetry is the self-retry budget (~10 x 30s = 5 min wait).
	anomalyMaxSelfRetry = 10
)

// DefaultADTVMinValue is the minimum today trade value (Rp) for a ticker's
// anomaly to be flagged — the ADTV liquidity filter. Filters out illiquid
// gorengan stocks where a small trade can spike volume 500%. Configurable via
// config.json ("anomaly.min_adtv_value"); <= 0 disables the filter.
const DefaultADTVMinValue int64 = 5_000_000_000

// DetectAnomaliesPayload is the payload for a detect:anomalies task.
type DetectAnomaliesPayload struct {
	Date    string `json:"date"`    // YYYY-MM-DD
	Attempt int    `json:"attempt"` // self-synchronizing retry counter
}

// EnqueueDetectAnomalies enqueues a detect:anomalies task for the given date.
// Uses a date-keyed TaskID for dedup. Returns ErrTaskIDConflict if already
// enqueued. Chained from idx:stock_summary success.
func EnqueueDetectAnomalies(client *asynq.Client, date time.Time) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeDetectAnomalies, dateKey)
	task, err := detectAnomaliesTask(dateKey, 0)
	if err != nil {
		return nil, err
	}
	return client.Enqueue(task,
		asynq.TaskID(taskKey),
		asynq.Queue("ingest"),
		asynq.MaxRetry(3),
		asynq.Retention(24*time.Hour),
	)
}

// reenqueueDetectAnomalies re-enqueues a detect:anomalies task with a delay.
// Uses a unique TaskID (no dedup key) because the current task still holds
// the date-keyed ID while active.
func reenqueueDetectAnomalies(client *asynq.Client, date string, attempt int) error {
	task, err := detectAnomaliesTask(date, attempt)
	if err != nil {
		return err
	}
	_, err = client.Enqueue(task,
		asynq.Queue("ingest"),
		asynq.ProcessIn(anomalySelfRetryDelay),
		asynq.MaxRetry(3),
		asynq.Retention(24*time.Hour),
	)
	return err
}

// detectAnomaliesTask builds the asynq task for a detect:anomalies payload.
func detectAnomaliesTask(date string, attempt int) (*asynq.Task, error) {
	payload := DetectAnomaliesPayload{Date: date, Attempt: attempt}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal detect:anomalies payload: %w", err)
	}
	return asynq.NewTask(TypeDetectAnomalies, raw), nil
}

// NewDetectAnomaliesHandler returns an asynq handler for the detect:anomalies
// task type. It computes volume spikes and price shifts from daily_prices and
// writes them to the anomalies table. Self-synchronizing: if today's
// daily_prices rows aren't present yet (stock_summary not done), it
// re-enqueues itself with a delay. minADTV is the ADTV liquidity filter
// threshold (Rp); <= 0 disables the filter.
func NewDetectAnomaliesHandler(
	log *logrus.Logger,
	client *asynq.Client,
	db *sqlx.DB,
	dailyPriceRepo *repository.DailyPriceRepository,
	anomalyRepo *repository.AnomalyRepository,
	minADTV int64,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var p DetectAnomaliesPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		// Self-synchronizing: wait for today's daily_prices rows.
		present, err := dailyPriceRepo.ExistsForDate(db, p.Date)
		if err != nil {
			return fmt.Errorf("check daily_prices presence: %w", err)
		}
		if !present {
			if p.Attempt >= anomalyMaxSelfRetry {
				log.Warnf("detect:anomalies: giving up after %d attempts, no daily_prices rows for %s", p.Attempt, p.Date)
				return nil
			}
			log.Infof("detect:anomalies: daily_prices for %s not present (attempt %d), retrying in %s", p.Date, p.Attempt, anomalySelfRetryDelay)
			if err := reenqueueDetectAnomalies(client, p.Date, p.Attempt+1); err != nil {
				return fmt.Errorf("re-enqueue detect:anomalies: %w", err)
			}
			return nil
		}

		// Detection day = the chained date (p.Date), which is the most recent
		// trading day in the normal flow: stock_summary just ingested it and
		// the presence check above confirms it. Data-presence-driven, no
		// calendar dependency. Using the chained date (not MAX(trading_day))
		// keeps a self-healed past-date stock_summary's anomalies on its own
		// date instead of silently re-computing a newer day.
		today, err := time.Parse("2006-01-02", p.Date)
		if err != nil {
			return fmt.Errorf("invalid date %q: %w", p.Date, err)
		}

		written, err := detectAnomalies(db, dailyPriceRepo, anomalyRepo, today, minADTV, log)
		if err != nil {
			return fmt.Errorf("detect anomalies: %w", err)
		}
		log.Infof("detect:anomalies: wrote %d anomaly row(s) for %s", written, today.Format("2006-01-02"))
		return nil
	}
}

// detectAnomalies computes volume and price anomalies for the given trading
// day and upserts them into the anomalies table. minADTV is the ADTV liquidity
// filter threshold; tickers whose today trade value falls below it are skipped
// for both anomaly types. Returns the number of rows written.
func detectAnomalies(
	db *sqlx.DB,
	dailyPriceRepo *repository.DailyPriceRepository,
	anomalyRepo *repository.AnomalyRepository,
	today time.Time,
	minADTV int64,
	log *logrus.Logger,
) (int, error) {
	candidates, err := dailyPriceRepo.AnomalyCandidates(db, today)
	if err != nil {
		return 0, err
	}

	written := 0
	skippedVolume := 0
	skippedADTV := 0
	for _, c := range candidates {
		// ADTV liquidity filter: skip illiquid tickers entirely (both anomaly
		// types) so a small trade can't spike volume 500%.
		if !passesADTV(c, minADTV) {
			skippedADTV++
			continue
		}

		if a := volumeAnomaly(c, today); a != nil {
			if err := anomalyRepo.Insert(db, a); err != nil {
				log.Warnf("detect:anomalies: volume insert failed for %s: %v", c.Ticker, err)
				continue
			}
			written++
		} else if c.TodayVolume != nil && c.BaselineDays < anomalyBaselineDays {
			skippedVolume++
		}

		if a := priceAnomaly(c, today); a != nil {
			if err := anomalyRepo.Insert(db, a); err != nil {
				log.Warnf("detect:anomalies: price insert failed for %s: %v", c.Ticker, err)
				continue
			}
			written++
		}
	}

	if skippedVolume > 0 {
		log.Infof("detect:anomalies: skipped %d ticker(s) for volume (fewer than %d days history)", skippedVolume, anomalyBaselineDays)
	}
	if skippedADTV > 0 {
		log.Infof("detect:anomalies: skipped %d ticker(s) below ADTV liquidity threshold (min %d)", skippedADTV, minADTV)
	}
	return written, nil
}

// passesADTV reports whether a ticker's today trade value meets the minimum
// ADTV (average daily trading value) liquidity threshold. Filters out
// illiquid gorengan stocks where a Rp 2M trade can spike volume 500%. A nil
// value (missing data) does not trigger the filter; minADTV <= 0 disables it.
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
