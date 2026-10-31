package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// BrokerSummarySweepPayload is the payload for an idx:broker_stock_summary_sweep
// task: one date (the anchor day to sweep).
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
// idx:broker_stock_summary_sweep task type: sweep one anchor day's ADTV-eligible
// universe through Index Alpha in ≤50-ticker batches, inside the key pool's
// quota, and report through Source Status.
//
// minADTV is the liquidity floor (config anomaly.min_adtv_value); <= 0 falls
// back to the anomaly detector's own DefaultADTVMinValue so one bar spans the
// pipeline. The handler is the single writer to the idx:broker_stock_summary
// Source Status row — the same row the MCP read tools consult — so a sweep feeds
// their staleness metadata.
//
// Outcomes:
//   - completed plan → Success, watermark = anchor day.
//   - quota/daily-cap/rate-limit stop → one Failure with the real shortfall and
//     nil (no asynq retry: retrying an exhausted quota only spins), rows already
//     stored stay intact.
//   - a batch-level error, or persist failures with nothing written → Failure
//     and an error so asynq retries.
//   - an empty eligible universe (non-trading day, or prices not yet stored) is a
//     no-op Success that carries the watermark forward.
func NewBrokerSummarySweepHandler(
	log *logrus.Logger,
	uc *usecase.BrokerStockSummaryUseCase,
	recorder *pipeline.SourceStatusRecorder,
	minADTV int64,
) asynq.HandlerFunc {
	if minADTV <= 0 {
		minADTV = pipeline.DefaultADTVMinValue
	}
	stage := pipeline.NewIngestStage(TypeBrokerStockSummarySweep, log, nil, 3)
	return func(ctx context.Context, t *asynq.Task) error {
		// A scheduler/chain fires the task with the day payload; a manual
		// enqueue without one defaults to today, same convention as
		// pipeline:daily.
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
		anchor := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
		dateStr := anchor.Format("2006-01-02")

		taskID := pipeline.TaskID(ctx)
		f := stage.StartFetch(taskID, "sweeping anchor-day broker summaries",
			logrus.Fields{"date": dateStr})

		res, err := uc.SweepAnchorDay(ctx, anchor, usecase.SweepOptions{MinADTV: minADTV})
		if err != nil {
			f.Fail("broker summary sweep failed", err, logrus.Fields{"date": dateStr})
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		fields := logrus.Fields{
			"date":        dateStr,
			"window_from": res.WindowFrom,
			"eligible":    res.Eligible,
			"stored":      res.Stored,
			"candidates":  res.Candidates,
			"planned":     res.Planned,
			"attempted":   res.Attempted,
			"fetched":     res.Fetched,
			"empty":       res.Empty,
			"invalid":     res.Invalid,
			"failed":      res.Failed,
			"chunks":      res.Chunks,
			"units_spent": res.UnitsSpent,
			"shortfall":   res.Shortfall,
			"uncovered":   res.Uncovered,
		}

		// A shortfall is not a parse/network failure: record one Failure with
		// the real numbers and do not retry a known-exhausted key.
		if res.QuotaStopped || res.DailyCapped || res.RateLimited || res.NoCapacity {
			short := sweepShortfallError(res, uc.Budget.NextReset())
			f.Fail("broker summary sweep stopped short", short, fields)
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, short)
			return nil
		}

		// Everything the run requested was rejected by a contract invariant:
		// contract drift, not a transient error — retrying would spend quota
		// again for the same result.
		if res.Attempted > 0 && res.Fetched == 0 && res.Empty == 0 && res.Invalid > 0 {
			err := fmt.Errorf("broker summary sweep %s: all %d attempted ticker-days were invalid", dateStr, res.Attempted)
			f.Fail("broker summary sweep rejected by the source", err, fields)
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return nil
		}
		// Parsed but not persisted with nothing written: a DB problem that may
		// clear, so fail and let asynq retry.
		if res.Attempted > 0 && res.Fetched == 0 && res.Failed > 0 {
			err := fmt.Errorf("broker summary sweep %s: %d ticker-days could not be persisted", dateStr, res.Failed)
			f.Fail("broker summary sweep persist failure", err, fields)
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		// The watermark is the anchor day of a run that completed its plan and
		// actually produced or confirmed coverage. An all-empty day (vendor not
		// published yet) carries the watermark forward instead of claiming the
		// day.
		var watermark *time.Time
		if res.Completed() && res.Eligible > 0 && (res.Fetched > 0 || res.Stored > 0) {
			watermark = &anchor
		}
		recorder.Success(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, watermark)
		f.Ok("broker summary sweep complete", fields)
		return nil
	}
}

// sweepShortfallError explains why a run did not complete its plan. Quota and
// daily-cap exhaustion are deliberately distinct: one clears at the rolling
// period end, the other at local midnight.
func sweepShortfallError(res *usecase.BrokerSummarySweepResult, reset *time.Time) error {
	var cause string
	switch {
	case res.QuotaStopped:
		cause = "Index Alpha quota exhausted"
		if reset != nil {
			cause += " until " + reset.Format(time.RFC3339)
		}
	case res.DailyCapped:
		cause = "Index Alpha daily request cap reached; usable again tomorrow"
	case res.RateLimited:
		cause = "Index Alpha rate limited; all keys are cooling down"
	case res.NoCapacity:
		cause = "no usable Index Alpha key (quota exhausted, daily cap reached, or cooling)"
	default:
		cause = "sweep incomplete"
	}
	return fmt.Errorf("%s: swept %d of %d candidates (%d planned, %d eligible, %d already stored)",
		cause, res.Attempted, res.Candidates, res.Planned, res.Eligible, res.Stored)
}
