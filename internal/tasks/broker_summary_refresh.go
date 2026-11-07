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

// BrokerSummaryRefreshPayload is the payload for an
// idx:broker_stock_summary_refresh task: one anchor day.
type BrokerSummaryRefreshPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// EnqueueBrokerSummaryRefresh enqueues a refresh for a date. The TaskID is
// date-keyed, so a duplicate refresh for the same day is deduped.
func EnqueueBrokerSummaryRefresh(enq pipeline.Enqueuer, date time.Time) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeBrokerSummaryRefresh).Enqueue(enq, date, nil)
}

// NewBrokerSummaryRefreshHandler returns an asynq handler for the daily
// anomaly refresher. It fetches at most usecase.BrokerRefreshDailyUnits foreign
// day windows for the anchor day's anomaly hits, keeping earlier writes on a
// quota stop and recording the shortfall without an asynq retry.
func NewBrokerSummaryRefreshHandler(
	log *logrus.Logger,
	uc *usecase.BrokerStockSummaryUseCase,
	recorder *pipeline.SourceStatusRecorder,
	minADTV int64,
) asynq.HandlerFunc {
	if minADTV <= 0 {
		minADTV = pipeline.DefaultADTVMinValue
	}
	stage := pipeline.NewIngestStage(TypeBrokerSummaryRefresh, log, nil, 3)
	return func(ctx context.Context, t *asynq.Task) error {
		date := time.Now()
		if len(t.Payload()) > 0 {
			p, err := pipeline.DecodeTask[BrokerSummaryRefreshPayload](t)
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
		f := stage.StartFetch(taskID, "refreshing anomaly broker summaries",
			logrus.Fields{"date": dateStr})

		res, err := uc.RefreshAnomalyDay(ctx, anchor, usecase.BrokerRefreshOptions{MinADTV: minADTV})
		if err != nil {
			f.Fail("broker anomaly refresh failed", err, logrus.Fields{"date": dateStr})
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		fields := logrus.Fields{
			"date":        dateStr,
			"candidates":  res.Candidates,
			"stored":      res.Stored,
			"planned":     res.Planned,
			"attempted":   res.Attempted,
			"fetched":     res.Fetched,
			"empty":       res.Empty,
			"invalid":     res.Invalid,
			"failed":      res.Failed,
			"chunks":      res.Chunks,
			"units_spent": res.UnitsSpent,
			"shortfall":   res.Shortfall,
		}

		if res.BudgetCapped || res.QuotaStopped || res.DailyCapped || res.RateLimited || res.NoCapacity {
			short := refreshShortfallError(res, uc.Budget.NextReset())
			f.Fail("broker anomaly refresh stopped short", short, fields)
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, short)
			return nil
		}
		if res.Attempted > 0 && res.Fetched == 0 && res.Empty == 0 && res.Invalid > 0 {
			err := fmt.Errorf("broker anomaly refresh %s: all %d attempted ticker-days were invalid", dateStr, res.Attempted)
			f.Fail("broker anomaly refresh rejected by the source", err, fields)
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return nil
		}
		if res.Attempted > 0 && res.Fetched == 0 && res.Failed > 0 {
			err := fmt.Errorf("broker anomaly refresh %s: %d ticker-days could not be persisted", dateStr, res.Failed)
			f.Fail("broker anomaly refresh persist failure", err, fields)
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		recordBrokerWatermark(uc, recorder)
		f.Ok("broker anomaly refresh complete", fields)
		return nil
	}
}

// refreshShortfallError explains why the daily refresh did not complete its
// plan. Quota and daily-cap exhaustion are deliberately distinct.
func refreshShortfallError(res *usecase.BrokerRefreshResult, reset *time.Time) error {
	var cause string
	switch {
	case res.BudgetCapped:
		cause = "daily broker refresh budget exhausted"
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
		cause = "refresh incomplete"
	}
	return fmt.Errorf("%s: refreshed %d of %d pending candidates (%d planned, %d already stored)",
		cause, res.Attempted, res.Candidates-res.Stored, res.Planned, res.Stored)
}
