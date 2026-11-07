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

// BrokerSummaryArchivePayload is the payload for an
// idx:broker_stock_summary_archive task. The date keys the TaskID and selects
// the current month; the archiver derives the closed month from it.
type BrokerSummaryArchivePayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// EnqueueBrokerSummaryArchive enqueues an archive run for a date. Date-keyed,
// so a duplicate run for the same day is deduped.
func EnqueueBrokerSummaryArchive(enq pipeline.Enqueuer, date time.Time) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeBrokerSummaryArchive).Enqueue(enq, date, nil)
}

// NewBrokerSummaryArchiveHandler returns an asynq handler for the monthly
// archiver. It examines the most recently closed months newest-first, writes at
// most one chunk per Tier-2 ticker, and no-ops when a month is already covered.
// A quota/budget stop keeps earlier chunks and records the shortfall; the next
// daily run resumes.
func NewBrokerSummaryArchiveHandler(
	log *logrus.Logger,
	uc *usecase.BrokerStockSummaryUseCase,
	recorder *pipeline.SourceStatusRecorder,
	minADTV int64,
) asynq.HandlerFunc {
	if minADTV <= 0 {
		minADTV = pipeline.DefaultADTVMinValue
	}
	stage := pipeline.NewIngestStage(TypeBrokerSummaryArchive, log, nil, 3)
	return func(ctx context.Context, t *asynq.Task) error {
		date := time.Now()
		if len(t.Payload()) > 0 {
			p, err := pipeline.DecodeTask[BrokerSummaryArchivePayload](t)
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
		f := stage.StartFetch(taskID, "archiving closed broker months",
			logrus.Fields{"date": dateStr})

		res, err := uc.ArchiveClosedMonths(ctx, anchor, usecase.BrokerArchiveOptions{MinADTV: minADTV})
		if err != nil {
			f.Fail("broker month archive failed", err, logrus.Fields{"date": dateStr})
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, err)
			return err
		}

		fields := logrus.Fields{
			"date":        dateStr,
			"months":      len(res.Months),
			"units_spent": res.UnitsSpent,
			"completed":   res.Completed,
		}
		for _, m := range res.Months {
			fields["month_"+m.Month] = fmt.Sprintf(
				"days=%d eligible=%d stored=%d derived=%d fetched=%d empty=%d invalid=%d failed=%d units=%d shortfall=%d",
				m.TradingDays, m.Eligible, m.Stored, m.Derived, m.Fetched, m.Empty, m.Invalid, m.Failed, m.UnitsSpent, m.Shortfall)
		}

		if !res.Completed {
			short := archiveShortfallError(res, uc.Budget.NextReset())
			f.Fail("broker month archive stopped short", short, fields)
			recorder.Failure(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, dateStr, short)
			return nil
		}

		recordBrokerWatermark(uc, recorder)
		f.Ok("broker month archive complete", fields)
		return nil
	}
}

// archiveShortfallError explains why the archiver did not finish its bounded
// catch-up. Quota and daily-cap exhaustion clear on different clocks.
func archiveShortfallError(res *usecase.BrokerArchiveResult, reset *time.Time) error {
	var cause string
	stalled := usecase.BrokerArchiveMonthResult{}
	for _, m := range res.Months {
		if m.Stalled() {
			stalled = m
			break
		}
	}
	switch {
	case stalled.BudgetCapped || stalled.Shortfall > 0:
		cause = "monthly archive budget or key capacity exhausted"
	case stalled.QuotaStopped:
		cause = "Index Alpha quota exhausted"
		if reset != nil {
			cause += " until " + reset.Format(time.RFC3339)
		}
	case stalled.DailyCapped:
		cause = "Index Alpha daily request cap reached; usable again tomorrow"
	case stalled.RateLimited:
		cause = "Index Alpha rate limited; all keys are cooling down"
	case stalled.NoCapacity:
		cause = "no usable Index Alpha key (quota exhausted, daily cap reached, or cooling)"
	default:
		cause = "archive incomplete"
	}
	return fmt.Errorf("%s: %s short by %d fetched chunk(s); the next run resumes",
		cause, stalled.Month, stalled.Shortfall)
}
