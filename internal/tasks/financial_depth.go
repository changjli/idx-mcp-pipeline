package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// FinancialDepthPayload is the payload for an idx:financials_depth task.
// Date is the run date. An empty Ticker runs the monthly sweep; a ticker runs
// the on-demand single-ticker path (issue 07). Month pins the ledger month
// (YYYY-MM); empty derives it from the mode. Force re-runs a ticker whose
// ledger row is already terminal — the manual/quarterly retry path for a
// not_covered ticker (issue 07).
type FinancialDepthPayload struct {
	Date   string `json:"date"`
	Ticker string `json:"ticker,omitempty"`
	Month  string `json:"month,omitempty"`
	Force  bool   `json:"force,omitempty"`
}

// NewFinancialDepthHandler returns an asynq handler for the depth task. The
// sweep is monthly and walks the Tier-1 universe; the on-demand path enriches
// one ticker. Neither path touches the MCP request path.
func NewFinancialDepthHandler(
	log *logrus.Logger,
	uc *usecase.FinancialDepthUseCase,
	recorder *pipeline.SourceStatusRecorder,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		// The monthly scheduler fires this task with a nil payload; the run
		// date defaults to today server-side, same convention as pipeline:daily.
		runDate := time.Now()
		var p FinancialDepthPayload
		if len(t.Payload()) > 0 {
			decoded, err := pipeline.DecodeTask[FinancialDepthPayload](t)
			if err != nil {
				return err
			}
			p = *decoded
			if p.Date != "" {
				runDate, err = pipeline.ParseTaskDay(p.Date)
				if err != nil {
					return err
				}
			}
		}
		runDateKey := runDate.Format("2006-01-02")

		ticker := p.Ticker
		if ticker != "" {
			return runDepthTicker(ctx, log, uc, recorder, ticker, p.Month, runDate, p.Force)
		}

		month, err := depthRunMonth(p.Month, runDate, false)
		if err != nil {
			return err
		}
		// Force re-walks not_covered tickers: the manual/quarterly retry path
		// (issue 07). The default sweep skips them.
		res, sweepErr := uc.Sweep(ctx, month, p.Force)
		if sweepErr != nil {
			recorder.Failure(usecase.YahooFinancialsSource, usecase.YahooFinancialsMaxAgeSeconds, runDateKey, sweepErr)
			return sweepErr
		}

		// Only a run that actually observed depth data advances the source
		// clock. An all-not_covered / all-skipped sweep is not a success and
		// must not refresh last_good_date.
		dataTickers := res.Observed + res.Partial + res.YahooLagging
		if dataTickers > 0 {
			var watermark *time.Time
			if res.LatestYahoo != "" {
				if parsed, parseErr := time.Parse("2006-01-02", res.LatestYahoo); parseErr == nil {
					watermark = &parsed
				}
			}
			recorder.Success(usecase.YahooFinancialsSource, usecase.YahooFinancialsMaxAgeSeconds, watermark)
		} else if res.Failed > 0 && res.Processed > 0 && res.Failed == res.Processed {
			recorder.Failure(usecase.YahooFinancialsSource, usecase.YahooFinancialsMaxAgeSeconds, runDateKey,
				fmt.Errorf("financial depth: all %d processed ticker(s) failed", res.Failed))
		}
		return nil
	}
}

func runDepthTicker(
	ctx context.Context,
	log *logrus.Logger,
	uc *usecase.FinancialDepthUseCase,
	recorder *pipeline.SourceStatusRecorder,
	ticker, monthRaw string,
	runDate time.Time,
	force bool,
) error {
	month, err := depthRunMonth(monthRaw, runDate, true)
	if err != nil {
		return err
	}
	res, err := uc.EnrichTicker(ctx, ticker, month, force)
	if err != nil {
		recorder.Failure(usecase.YahooFinancialsSource, usecase.YahooFinancialsMaxAgeSeconds,
			runDate.Format("2006-01-02"), err)
		return err
	}
	if res.Skipped {
		log.Infof("financial depth: %s %s already %s, no fetch", ticker, month.Format("2006-01"), res.Status)
		return nil
	}
	switch res.Status {
	case financials.DepthStatusObserved, financials.DepthStatusPartial, financials.DepthStatusYahooLagging:
		var watermark *time.Time
		if res.YahooLatest != "" {
			if parsed, parseErr := time.Parse("2006-01-02", res.YahooLatest); parseErr == nil {
				watermark = &parsed
			}
		}
		recorder.Success(usecase.YahooFinancialsSource, usecase.YahooFinancialsMaxAgeSeconds, watermark)
	default:
		// not_covered: the fetch succeeded but the vendor has no data. Do not
		// advance the source clock.
	}
	log.Infof("financial depth: %s %s status=%s taxonomy=%s rows_written=%d tier1_latest=%s yahoo_latest=%s lagging=%t",
		ticker, month.Format("2006-01"), res.Status, res.Taxonomy,
		res.RowsWritten, res.TierOneLatest, res.YahooLatest, res.Lagging)
	return nil
}

// depthRunMonth resolves the ledger month. A sweep is monthly and keys on the
// previous calendar month (the newest Tier-1 snapshot); an on-demand ticker
// keys on the current month so it does not collide with the monthly batch.
func depthRunMonth(raw string, runDate time.Time, onDemand bool) (time.Time, error) {
	if raw != "" {
		month, err := time.Parse("2006-01", raw)
		if err != nil {
			return time.Time{}, err
		}
		return month, nil
	}
	if onDemand {
		return financials.FirstOfMonth(runDate), nil
	}
	return usecase.PreviousMonth(runDate), nil
}
