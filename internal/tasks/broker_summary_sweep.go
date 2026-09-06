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
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

const (
	// SweepTaskTimeout is the task-level asynq timeout for the weekly sweep.
	// A fresh catch-up run fetches ~3,300 days at the IPOT client's shared 2s
	// pacing, so it routinely exceeds asynq's 30m default; the date-keyed
	// TaskID dedup means a re-run (self-heal or manual) skips stored days.
	// Exported because the scheduler registers the task directly (not through
	// the graph node) and must apply the same override.
	SweepTaskTimeout = 2 * time.Hour

	// sweepWindowDays is the trailing window for both eligibility and fetch:
	// ADTV is computed over it (stable liquidity identity, matches the anomaly
	// detector's 20-day baseline) and only uncovered days inside it are
	// fetched. A normal weekly run extracts ~5 new days; a missed week
	// self-heals because the next run's window still covers the gap.
	sweepWindowDays = 21

	// sweepMinDays is the minimum trading days in the window for eligibility —
	// excludes suspended/dead names that traded briefly then halted.
	sweepMinDays = 8
)

// BrokerSummarySweepPayload is the payload for an idx:broker_stock_summary_sweep
// task: one date (the sweep day), the window is derived server-side.
type BrokerSummarySweepPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// EnqueueBrokerSummarySweep enqueues an idx:broker_stock_summary_sweep task for
// a date. The TaskID is date-keyed, so a duplicate sweep for the same day is
// deduped (ErrTaskIDConflict) — at most one outstanding sweep per day.
func EnqueueBrokerSummarySweep(enq pipeline.Enqueuer, date time.Time) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeBrokerStockSummarySweep).Enqueue(enq, date, nil)
}

// NewBrokerSummarySweepHandler returns an asynq handler for the
// idx:broker_stock_summary_sweep task type (issue 14b): resolve the sweep day,
// compute the ADTV-eligible universe over the trailing window, run the shared
// sweep usecase (skip-if-stored + fetch+persist, paced by the IPOT client),
// and record source_status. minADTV is the liquidity floor (config
// anomaly.min_adtv_value); <= 0 falls back to the anomaly detector's own
// DefaultADTVMinValue so one bar spans the pipeline. A full-market failure
// (every eligible day failed) returns an error so asynq retries; partial
// failures are isolated per day inside the usecase.
func NewBrokerSummarySweepHandler(
	log *logrus.Logger,
	db *sqlx.DB,
	uc *usecase.BrokerStockSummaryUseCase,
	dailyPriceRepo *repository.DailyPriceRepository,
	recorder *pipeline.SourceStatusRecorder,
	minADTV int64,
) asynq.HandlerFunc {
	if minADTV <= 0 {
		minADTV = pipeline.DefaultADTVMinValue
	}
	stage := pipeline.NewIngestStage(TypeBrokerStockSummarySweep, log, nil, 3)
	return func(ctx context.Context, t *asynq.Task) error {
		// The weekly scheduler fires this task with a nil payload; the sweep
		// defaults to today server-side, same convention as pipeline:daily.
		date := time.Now()
		if len(t.Payload()) > 0 {
			p, err := pipeline.DecodeTask[BrokerSummarySweepPayload](t)
			if err != nil {
				return err
			}
			date, err = pipeline.ParseTaskDay(p.Date)
			if err != nil {
				return err
			}
		}
		dateStr := date.Format("2006-01-02")
		from := date.AddDate(0, 0, -sweepWindowDays)
		fromStr := from.Format("2006-01-02")

		// Universe = ADTV-eligible tickers over the trailing window, computed
		// fresh each run (membership drifts with the market). Zero IPOT calls
		// to size — pure daily_prices aggregation.
		tickers, err := dailyPriceRepo.ADTVEligibleTickers(db, from, date, sweepMinDays, minADTV)
		if err != nil {
			return fmt.Errorf("resolve ADTV-eligible tickers: %w", err)
		}

		taskID := pipeline.TaskID(ctx)
		f := stage.StartFetch(taskID, "sweeping market broker summaries",
			logrus.Fields{"date": dateStr, "window_from": fromStr, "eligible": len(tickers)})

		res, err := uc.SweepStockBrokerSummaries(ctx, tickers, from, date)
		if err != nil {
			f.Fail("broker summary sweep failed", err, logrus.Fields{"date": dateStr})
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		// Every eligible day failed (nothing stored, nothing already present) →
		// indistinguishable from an outage; return an error so asynq retries.
		if res.Tickers > 0 && res.Fetched == 0 && res.Skipped == 0 && res.Failed > 0 {
			err := fmt.Errorf("broker summary sweep %s: all %d eligible days failed", dateStr, res.Failed)
			f.Fail("broker summary sweep total failure", err, logrus.Fields{"date": dateStr})
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		// Advance the watermark only when the window actually produced/confirmed
		// data; an empty window (no eligible tickers) carries the prior
		// watermark forward.
		var lastGood *time.Time
		if res.Fetched > 0 || res.Skipped > 0 {
			lastGood = &date
		}
		recorder.Success(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, lastGood)
		f.Ok("broker summary sweep complete",
			logrus.Fields{"date": dateStr, "window_from": fromStr, "tickers": res.Tickers, "days": res.Days, "skipped": res.Skipped, "fetched": res.Fetched, "empty": res.Empty, "failed": res.Failed})
		return nil
	}
}
