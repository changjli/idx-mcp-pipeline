package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// BrokerSummarySweepResult summarizes one full-market broker summary sweep run
// for a date. The accounting is the quota contract: Total is every active
// ticker considered; the sweep only makes an upstream call for a ticker that
// both traded (daily_prices row, NotTraded otherwise) and has no stored rows
// yet (Skipped otherwise). Fetched/Empty/Failed are the outcomes of those
// calls.
type BrokerSummarySweepResult struct {
	Date      string `json:"date"`
	Total     int    `json:"total"`      // active tickers considered
	NotTraded int    `json:"not_traded"` // no daily_prices row for the day → not traded, no fetch
	Skipped   int    `json:"skipped"`    // broker rows already stored for the day → no fetch
	Fetched   int    `json:"fetched"`    // newly fetched + persisted
	Empty     int    `json:"empty"`      // fetched but IPOT returned no data (not yet published)
	Failed    int    `json:"failed"`     // upstream or persist error
}

// SweepStockBrokerSummaries fetches + persists the per-stock broker summary
// for every active ticker that traded on a date, reusing the shared
// fetch+parse+persist core (fetchAndPersistDay). It sits on top of the
// anomaly-gated flow without changing it: tickers already covered by the
// anomaly gate (or a prior sweep) are skipped via HasStoredDay, and the 1h
// IPOT cache makes a same-day refetch network-free. Quota discipline:
//   - the traded-ticker query (TradedTickersOnDate) is the universe filter —
//     data presence is the trading-day signal, so a weekend or a calm ticker
//     costs zero upstream calls;
//   - each fetch is paced by the IPOT client's shared MinDelay, so the sweep
//     cannot burst the source no matter how the asynq pool schedules it;
//   - per-ticker failures are isolated (counted + logged), never aborting the
//     sweep.
//
// Concurrency note: the pipeline's asynq workers already run 10-deep, and every
// upstream call serializes on the IPOT client's pacing mutex — so parallelizing
// the sweep loop would add goroutines that just queue on that lock. The
// binding cost is IPOT pacing + quota, not CPU or DB.
func (uc *BrokerStockSummaryUseCase) SweepStockBrokerSummaries(ctx context.Context, tickers []string, date time.Time) (*BrokerSummarySweepResult, error) {
	dateStr := date.Format("2006-01-02")

	// Universe filter + trading-day calendar in one query: distinct tickers
	// with a daily_prices row for the date. On a non-trading day this is empty,
	// which makes the whole sweep a zero-fetch no-op.
	traded, err := uc.DailyPriceRepo.TradedTickersOnDate(uc.DB, dateStr)
	if err != nil {
		return nil, fmt.Errorf("resolve traded tickers: %w", err)
	}
	tradedSet := make(map[string]struct{}, len(traded))
	for _, t := range traded {
		tradedSet[t] = struct{}{}
	}

	res := &BrokerSummarySweepResult{Date: dateStr}
	for _, t := range tickers {
		ticker := strings.ToUpper(strings.TrimSpace(t))
		if !tickerPattern.MatchString(ticker) {
			uc.Log.Warnf("broker_summary sweep: skipping invalid ticker %q", t)
			continue
		}
		res.Total++

		if _, ok := tradedSet[ticker]; !ok {
			res.NotTraded++
			continue
		}

		// Skip-if-stored: a day already covered (anomaly gate, prior sweep, or
		// on-demand tool) costs no upstream call. A concurrent refetch racing
		// this check is harmless — UpsertDay replaces the day wholesale.
		has, err := uc.Repo.HasStoredDay(uc.DB, ticker, date)
		if err != nil {
			uc.Log.Warnf("broker_summary sweep: stored-day check for %s failed: %v", ticker, err)
			res.Failed++
			continue
		}
		if has {
			res.Skipped++
			continue
		}

		d, err := uc.fetchAndPersistDay(ctx, ticker, date)
		if err != nil {
			uc.Log.Warnf("broker_summary sweep: %s failed: %v", ticker, err)
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
	return res, nil
}
