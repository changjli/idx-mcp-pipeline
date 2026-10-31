package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// anomalySelfRetryDelay is the delay between self-synchronizing retries
	// while waiting for today's daily_prices rows.
	anomalySelfRetryDelay = 30 * time.Second
	// anomalyMaxSelfRetry is the self-retry budget (~10 x 30s = 5 min wait).
	anomalyMaxSelfRetry = 10
)

// DetectAnomaliesPayload is the payload for a detect:anomalies task.
type DetectAnomaliesPayload struct {
	Date    string `json:"date"`    // YYYY-MM-DD
	Attempt int    `json:"attempt"` // self-synchronizing retry counter
}

// EnqueueDetectAnomalies enqueues a detect:anomalies task for the given date.
// Uses a date-keyed TaskID for dedup. Returns ErrTaskIDConflict if already
// enqueued. Chained from idx:stock_summary success.
func EnqueueDetectAnomalies(enq pipeline.Enqueuer, date time.Time) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeDetectAnomalies).Enqueue(enq, date, nil)
}

// reenqueueDetectAnomalies re-enqueues a detect:anomalies task with a delay.
// Uses a unique TaskID (no dedup key) because the current task still holds
// the date-keyed ID while active.
func reenqueueDetectAnomalies(enq pipeline.Enqueuer, date string, attempt int) error {
	stage := pipeline.NewIngestStage(TypeDetectAnomalies, nil, enq, 3)
	return stage.Reenqueue(DetectAnomaliesPayload{Date: date, Attempt: attempt}, anomalySelfRetryDelay)
}

// NewDetectAnomaliesHandler returns an asynq handler for the detect:anomalies
// task type. Detection itself lives in the pipeline.AnomalyDetector stage
// (ADR-0006); the handler synchronizes with stock_summary (self-retrying until
// today's daily_prices rows exist), runs the detector, and chains the next
// tasks. detector is built with its ADTV threshold already defaulted.
//
// Broker ingestion is deliberately not triggered here. It is liquidity-driven
// (the daily anchor-day sweep), not anomaly-gated: a fan-out per flagged ticker
// would spend Index Alpha quota outside the sweep's budget (ADR-0018).
func NewDetectAnomaliesHandler(
	log *logrus.Logger,
	enq pipeline.Enqueuer,
	db *sqlx.DB,
	dailyPriceRepo *repository.DailyPriceRepository,
	detector *pipeline.AnomalyDetector,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[DetectAnomaliesPayload](t)
		if err != nil {
			return err
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
			if err := reenqueueDetectAnomalies(enq, p.Date, p.Attempt+1); err != nil {
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
		today, err := pipeline.ParseTaskDay(p.Date)
		if err != nil {
			return err
		}

		taskID := pipeline.TaskID(ctx)
		detected, err := detector.Detect(ctx, today)
		if err != nil {
			return fmt.Errorf("detect anomalies: %w", err)
		}
		log.Infof("detect:anomalies: wrote %d anomaly row(s) for %s", len(detected), today.Format("2006-01-02"))

		// anomaly_detected events carry the task-scoped fields (task_id,
		// source) that only the handler knows; the detector stays ctx-free.
		for _, a := range detected {
			logEvent(log, logrus.InfoLevel, "anomaly_detected", fmt.Sprintf("%s anomaly detected", a.Type),
				logrus.Fields{"task_id": taskID, "source": TypeDetectAnomalies, "ticker": a.Ticker, "type": a.Type, "direction": a.Direction, "magnitude_pct": a.MagnitudePct, "date": today.Format("2006-01-02")})
		}

		// Filter disclosures unconditionally (even zero anomalies): non-anomaly
		// disclosures still need marking passed_filter=false, and passing ones
		// proceed to extraction. Chained here (not from idx:announcements)
		// because anomalies are detected after announcements in the pipeline —
		// the anomaly-gate needs today's anomaly rows to exist.
		if err := Graph.Chain(ctx, enq, TypeDetectAnomalies, today); err != nil {
			log.Warnf("detect:anomalies: chain filter:disclosures: %v", err)
		}
		return nil
	}
}
