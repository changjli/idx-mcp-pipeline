package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// BrokerRefreshOptions tunes the daily anomaly refresher.
type BrokerRefreshOptions struct {
	// MinADTV is the liquidity floor in rupiah. The detector already applies
	// it; the refresher repeats it so a config change or an out-of-band row
	// cannot spend a unit on an illiquid name. <= 0 uses the pipeline default.
	MinADTV int64
	// MaxUnits is the hard daily cap. <= 0 uses BrokerRefreshDailyUnits.
	MaxUnits int
}

// BrokerRefreshResult is one daily anomaly refresh's accounting. Counts are
// tickers unless stated otherwise.
type BrokerRefreshResult struct {
	Date       string `json:"date"`
	Candidates int    `json:"candidates"` // anomaly tickers at/above the ADTV floor
	Stored     int    `json:"stored"`     // skipped: the anchor day is already stored
	Planned    int    `json:"planned"`    // units the daily pool granted
	Attempted  int    `json:"attempted"`  // tickers actually requested
	Fetched    int    `json:"fetched"`    // parsed and persisted
	Empty      int    `json:"empty"`      // requested but no rows
	Invalid    int    `json:"invalid"`    // rejected by a contract invariant
	Failed     int    `json:"failed"`     // parsed but could not be persisted
	Chunks     int    `json:"chunks"`     // HTTP batch calls
	UnitsSpent int    `json:"units_spent"`

	// Stop reasons. At most one is set; all mean the plan was not completed
	// and Shortfall > 0.
	BudgetCapped bool `json:"budget_capped,omitempty"`
	QuotaStopped bool `json:"quota_stopped,omitempty"`
	DailyCapped  bool `json:"daily_capped,omitempty"`
	RateLimited  bool `json:"rate_limited,omitempty"`
	NoCapacity   bool `json:"no_capacity,omitempty"`

	Shortfall int `json:"shortfall"` // Planned - Attempted
}

// Completed reports whether the run fetched everything the daily pool granted.
// A run with nothing pending is complete by construction.
func (r *BrokerRefreshResult) Completed() bool {
	return !r.BudgetCapped && !r.QuotaStopped && !r.DailyCapped && !r.RateLimited && !r.NoCapacity && r.Attempted == r.Planned
}

// RefreshAnomalyDay observes the anchor day's anomaly hits as one-day foreign
// windows.
//
// Selection is anomaly-driven, not a universe sweep:
//
//  1. Read the day's anomaly tickers, deduped, at/above the ADTV floor, already
//     ordered by largest absolute magnitude, then the day's value, then ticker.
//  2. Skip tickers whose anchor day is already stored in the foreign lane.
//  3. Reserve units from the daily pool (30 by default); a partially granted
//     plan is still run, newest/most-extreme names first.
//  4. Fetch in ≤50-ticker batches, chunked against the smallest usable key's
//     remaining units. A quota/daily/rate-limit stop keeps the rows already
//     written and returns the partial result without an error.
func (uc *BrokerStockSummaryUseCase) RefreshAnomalyDay(ctx context.Context, anchor time.Time, opts BrokerRefreshOptions) (*BrokerRefreshResult, error) {
	if uc.BatchFetcher == nil || uc.Budget == nil {
		return nil, errors.New("broker refresh: batch source and quota budget are not configured")
	}
	if uc.AnomalyRepo == nil {
		return nil, errors.New("broker refresh: no anomaly source configured")
	}
	if uc.Usage == nil {
		return nil, errors.New("broker refresh: no fetch-usage counter configured")
	}
	if opts.MinADTV <= 0 {
		opts.MinADTV = pipeline.DefaultADTVMinValue
	}
	if opts.MaxUnits <= 0 {
		opts.MaxUnits = BrokerRefreshDailyUnits
	}
	anchor = time.Date(anchor.Year(), anchor.Month(), anchor.Day(), 0, 0, 0, 0, time.UTC)

	res := &BrokerRefreshResult{Date: anchor.Format("2006-01-02")}

	if err := uc.Budget.EnsureSeeded(ctx); err != nil {
		uc.Log.Warnf("broker refresh: /usage re-seed incomplete: %v", err)
	}

	candidates, err := uc.AnomalyRepo.RefreshCandidates(uc.DB, anchor, opts.MinADTV)
	if err != nil {
		return nil, fmt.Errorf("resolve anomaly candidates: %w", err)
	}
	res.Candidates = len(candidates)
	if len(candidates) == 0 {
		return res, nil
	}

	tickers := make([]string, len(candidates))
	for i, c := range candidates {
		tickers[i] = c.Ticker
	}
	stored, err := uc.Repo.StoredForeignDayTickers(uc.DB, anchor, tickers)
	if err != nil {
		return nil, fmt.Errorf("resolve stored ticker-days: %w", err)
	}
	pending := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if stored[c.Ticker] {
			res.Stored++
			continue
		}
		pending = append(pending, c.Ticker)
	}
	if len(pending) == 0 {
		return res, nil
	}

	want := len(pending)
	if want > opts.MaxUnits {
		want = opts.MaxUnits
	}
	if available := uc.Budget.Available(); want > available {
		want = available
	}
	if want <= 0 {
		res.NoCapacity = true
		res.Shortfall = len(pending)
		return res, nil
	}

	granted, _, err := uc.Usage.Reserve(uc.DB, usageDay(uc.now()), repository.FetchFeatureBrokerRefresh, want, opts.MaxUnits)
	if err != nil {
		return nil, fmt.Errorf("reserve daily refresh budget: %w", err)
	}
	res.Planned = granted
	if granted == 0 {
		res.BudgetCapped = true
		res.Shortfall = len(pending)
		return res, nil
	}

	for i := 0; i < len(pending) && res.Attempted < granted; {
		chunk := nextChunkSize(uc.Budget.BatchCapacity(), granted-res.Attempted, len(pending)-i)
		if chunk == 0 {
			res.NoCapacity = true
			break
		}
		batchTickers := pending[i : i+chunk]
		batch, err := uc.BatchFetcher.FetchBatchRange(ctx, batchTickers, anchor, anchor, broker.InvestorForeign)
		if err != nil {
			switch {
			case errors.Is(err, broker.ErrQuotaExhausted):
				res.QuotaStopped = true
			case errors.Is(err, broker.ErrDailyLimit):
				res.DailyCapped = true
			case errors.Is(err, broker.ErrRateLimited):
				res.RateLimited = true
			default:
				res.Shortfall = res.Planned - res.Attempted
				return res, err
			}
			break
		}

		res.Chunks++
		res.UnitsSpent += len(batchTickers)
		res.Attempted += len(batchTickers)
		i += chunk

		for _, ticker := range batchTickers {
			if parseErr, bad := batch.Invalid[ticker]; bad {
				res.Invalid++
				uc.Log.Warnf("broker refresh: %s %s rejected: %v", ticker, res.Date, parseErr)
				continue
			}
			r, ok := batch.Results[ticker]
			if !ok || (len(r.Buyers) == 0 && len(r.Sellers) == 0) {
				res.Empty++
				continue
			}
			if err := uc.persist(ticker, anchor, r, entity.BrokerSourceSweep); err != nil {
				res.Failed++
				uc.Log.Warnf("broker refresh: persist %s %s failed: %v", ticker, res.Date, err)
				continue
			}
			res.Fetched++
		}
	}

	res.Shortfall = res.Planned - res.Attempted
	if res.Shortfall < 0 {
		res.Shortfall = 0
	}
	return res, nil
}

func (uc *BrokerStockSummaryUseCase) now() time.Time {
	if uc.Now == nil {
		return time.Now()
	}
	return uc.Now()
}
