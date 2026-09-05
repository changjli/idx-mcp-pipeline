package usecase

import (
	"context"
	"strings"
	"time"
)

// BrokerSummarySweepResult summarizes one weekly sweep run over a window.
// The accounting is the quota contract: Days is every trading day across the
// eligible tickers in the window; the sweep only makes an upstream call for a
// day that is not already stored (Skipped otherwise). Fetched/Empty/Failed are
// the outcomes of those calls.
type BrokerSummarySweepResult struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Tickers int    `json:"tickers"` // eligible tickers considered
	Days    int    `json:"days"`    // trading days across tickers in window
	Skipped int    `json:"skipped"` // days already stored → no fetch
	Fetched int    `json:"fetched"` // days newly fetched + persisted
	Empty   int    `json:"empty"`   // fetched but IPOT returned no data (not yet published)
	Failed  int    `json:"failed"`  // upstream or persist error
}

// SweepStockBrokerSummaries backfills the per-stock broker summary for every
// eligible ticker over a window, reusing the shared fetch+parse+persist core
// (fetchAndPersistDay). It sits on top of the anomaly-gated flow without
// changing it: days already covered by the anomaly gate (or a prior sweep)
// are skipped via HasStoredDay, and the 1h IPOT cache makes a same-day
// refetch network-free. Quota discipline:
//   - the ticker universe is pre-filtered by the caller (ADTVEligibleTickers —
//     the pipeline's own liquidity floor), so dead/illiquid names cost nothing;
//   - per-day skip-if-stored means a normal weekly run fetches only the ~5
//     uncovered days since the last sweep, and a missed week self-heals (the
//     next run pulls the gap);
//   - each fetch is paced by the IPOT client's shared MinDelay, so the sweep
//     cannot burst the source no matter how the asynq pool schedules it;
//   - per-day failures are isolated (counted + logged), never aborting the
//     sweep.
//
// Concurrency note: the pipeline's asynq workers already run 10-deep, and every
// upstream call serializes on the IPOT client's pacing mutex — so parallelizing
// the sweep loop would add goroutines that just queue on that lock. The
// binding cost is IPOT pacing + quota, not CPU or DB.
func (uc *BrokerStockSummaryUseCase) SweepStockBrokerSummaries(ctx context.Context, tickers []string, from, to time.Time) (*BrokerSummarySweepResult, error) {
	if from.After(to) {
		return nil, ErrInvalidRange
	}

	res := &BrokerSummarySweepResult{
		From: from.Format("2006-01-02"),
		To:   to.Format("2006-01-02"),
	}
	for _, t := range tickers {
		ticker := strings.ToUpper(strings.TrimSpace(t))
		if !tickerPattern.MatchString(ticker) {
			uc.Log.Warnf("broker_summary sweep: skipping invalid ticker %q", t)
			continue
		}
		res.Tickers++

		days, err := uc.DailyPriceRepo.TradingDaysInRange(uc.DB, ticker, from, to)
		if err != nil {
			uc.Log.Warnf("broker_summary sweep: trading days for %s failed: %v", ticker, err)
			res.Failed++
			continue
		}
		for _, day := range days {
			res.Days++

			// Skip-if-stored: a day already covered (anomaly gate, prior sweep,
			// or on-demand tool) costs no upstream call. A concurrent refetch
			// racing this check is harmless — UpsertDay replaces the day wholesale.
			has, err := uc.Repo.HasStoredDay(uc.DB, ticker, day)
			if err != nil {
				uc.Log.Warnf("broker_summary sweep: stored-day check for %s %s failed: %v", ticker, day.Format("2006-01-02"), err)
				res.Failed++
				continue
			}
			if has {
				res.Skipped++
				continue
			}

			d, err := uc.fetchAndPersistDay(ctx, ticker, day)
			if err != nil {
				uc.Log.Warnf("broker_summary sweep: %s %s failed: %v", ticker, day.Format("2006-01-02"), err)
				res.Failed++
				continue
			}
			if len(d.Buyers) == 0 && len(d.Sellers) == 0 {
				// Trading day but IPOT not yet published (or a spurious empty).
				res.Empty++
				continue
			}
			res.Fetched++
		}
	}
	return res, nil
}
