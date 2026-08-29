package tasks

import (
	"context"
	"encoding/json"
	"errors"
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
// task type. Detection itself lives in the pipeline.AnomalyDetector stage
// (ADR-0006); the handler synchronizes with stock_summary (self-retrying until
// today's daily_prices rows exist), runs the detector, and chains the next
// tasks. detector is built with its ADTV threshold already defaulted.
func NewDetectAnomaliesHandler(
	log *logrus.Logger,
	client *asynq.Client,
	db *sqlx.DB,
	dailyPriceRepo *repository.DailyPriceRepository,
	anomalyRepo *repository.AnomalyRepository,
	detector *pipeline.AnomalyDetector,
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

		taskID, _ := asynq.GetTaskID(ctx)
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

		// Auto-trigger: each flagged ticker emits an idx:broker_stock_summary
		// signal so its per-stock broker summary is fetched and stored without
		// an AI round-trip. TaskID dedup makes repeat signals no-ops.
		if len(detected) > 0 {
			enqueueBrokerSummariesForAnomalies(client, db, anomalyRepo, today, log)
		}

		// Filter disclosures unconditionally (even zero anomalies): non-anomaly
		// disclosures still need marking passed_filter=false, and passing ones
		// proceed to extraction. Chained here (not from idx:announcements)
		// because anomalies are detected after announcements in the pipeline —
		// the anomaly-gate needs today's anomaly rows to exist.
		if _, err := EnqueueFilterDisclosures(client, today); err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) {
			log.Warnf("detect:anomalies: enqueue filter:disclosures: %v", err)
		}
		return nil
	}
}

// enqueueBrokerSummariesForAnomalies enqueues an idx:broker_stock_summary task
// for each distinct ticker flagged on the given trading day. Duplicate signals
// (ErrTaskIDConflict) are expected and ignored.
func enqueueBrokerSummariesForAnomalies(client *asynq.Client, db *sqlx.DB, anomalyRepo *repository.AnomalyRepository, day time.Time, log *logrus.Logger) {
	anomalies, err := anomalyRepo.FindByDate(db, day.Format("2006-01-02"))
	if err != nil {
		log.Warnf("detect:anomalies: query flagged tickers: %v", err)
		return
	}
	for _, a := range anomalies {
		if _, err := EnqueueBrokerStockSummary(client, a.Ticker, day); err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) {
			log.Warnf("detect:anomalies: enqueue broker summary for %s: %v", a.Ticker, err)
		}
	}
}
