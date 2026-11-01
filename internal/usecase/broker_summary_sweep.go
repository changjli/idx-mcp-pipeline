package usecase

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// SweepWindowDays is the trailing window the ADTV universe is computed
	// over. A stable window is what makes liquidity a property of the name
	// rather than of one noisy session.
	SweepWindowDays = 21

	// SweepMinDays is the minimum number of trading days in the window for
	// eligibility — it excludes suspended/dead names that traded briefly and
	// then stopped.
	SweepMinDays = 8
)

// SweepOptions tunes the anchor-day sweep's universe filter.
type SweepOptions struct {
	// MinADTV is the liquidity floor in rupiah of average daily trade value.
	// It is the anomaly detector's own bar, so one definition spans the
	// pipeline. Must be positive.
	MinADTV int64
	// MinDays is the minimum trading days in the window; <= 0 uses SweepMinDays.
	MinDays int
}

// BrokerSummarySweepResult is one anchor-day sweep's accounting. Every field is
// a count of ticker-days unless stated otherwise, so a run can be reconciled:
// Eligible = Stored + Planned + (candidates the budget could not cover), and
// Attempted = Chunks' ticker total, split into Fetched / Empty / Invalid /
// Failed.
type BrokerSummarySweepResult struct {
	Date       string `json:"date"`
	WindowFrom string `json:"window_from"`
	Eligible   int    `json:"eligible"`   // ADTV-eligible tickers
	Stored     int    `json:"stored"`     // skipped: already stored for the anchor day
	Candidates int    `json:"candidates"` // Eligible - Stored
	Planned    int    `json:"planned"`    // ticker-days this run intended to fetch
	Attempted  int    `json:"attempted"`  // ticker-days actually requested
	Fetched    int    `json:"fetched"`    // parsed and persisted
	Empty      int    `json:"empty"`      // requested but no rows (no activity / not published)
	Invalid    int    `json:"invalid"`    // returned but rejected by a contract invariant
	Failed     int    `json:"failed"`     // parsed but could not be persisted
	Chunks     int    `json:"chunks"`     // HTTP batch calls
	UnitsSpent int    `json:"units_spent"`

	// Stop reasons. At most one is set; all of them mean the plan was not
	// completed and Shortfall > 0.
	QuotaStopped bool `json:"quota_stopped,omitempty"`
	DailyCapped  bool `json:"daily_capped,omitempty"`
	RateLimited  bool `json:"rate_limited,omitempty"`
	NoCapacity   bool `json:"no_capacity,omitempty"`

	Shortfall int `json:"shortfall"` // Planned - Attempted: the unfinished plan
	Uncovered int `json:"uncovered"` // Candidates - Attempted: the day's coverage gap
}

// HasCandidates reports whether any eligible ticker still needed a fetch for
// the anchor day. False means the day was already fully covered.
func (r *BrokerSummarySweepResult) HasCandidates() bool {
	return r.Eligible > 0 && r.Stored < r.Eligible
}

// Completed reports whether the run fetched everything it planned. A run with
// no candidates is complete by construction.
func (r *BrokerSummarySweepResult) Completed() bool {
	return !r.QuotaStopped && !r.DailyCapped && !r.RateLimited && !r.NoCapacity && r.Attempted == r.Planned
}

// SweepAnchorDay runs the daily Broker Stock Summary sweep for one trading day.
//
// Selection is the whole point, because the source meters one unit per ticker
// and a full ADTV universe does not fit a month's quota:
//
//  1. Universe: ADTV-eligible tickers over the trailing window, most liquid
//     first (daily_prices only — zero upstream cost to size).
//  2. Skip: ticker-days already stored for the anchor day. A re-run of the same
//     date therefore resumes where the previous one stopped.
//  3. Rotate: tickers never observed first, then least-recently-observed, with
//     ADTV as the tie-break. Stored rows ARE the rotation state, so no cursor
//     table is needed.
//  4. Budget: capacity = min(candidates, the pool's available units). When the
//     pool is funded for the universe this is full daily coverage; when it is
//     not, the most liquid unobserved names go first.
//  5. Fetch: ≤50-ticker batches, chunked against the smallest usable key's
//     remaining units so a request can never exceed what a key can pay (a
//     quota 403 would park that key until the period resets). Chunk sizes are
//     re-derived before every call, so concurrent on-demand spend and header
//     corrections are respected.
//
// A past anchor day is never revisited: the next run is a new day. Past holes
// are the history backfill's job. Per-ticker payload failures are isolated; only
// a batch-level failure (non-2xx, success=false, undecodable envelope) stops the
// run, and a quota/daily-cap/rate-limit stop records the shortfall and returns
// the partial result without an error.
func (uc *BrokerStockSummaryUseCase) SweepAnchorDay(ctx context.Context, anchor time.Time, opts SweepOptions) (*BrokerSummarySweepResult, error) {
	if uc.BatchFetcher == nil || uc.Budget == nil {
		return nil, errors.New("broker sweep: batch source and quota budget are not configured")
	}
	if opts.MinADTV <= 0 {
		return nil, errors.New("broker sweep: a positive MinADTV is required")
	}
	if opts.MinDays <= 0 {
		opts.MinDays = SweepMinDays
	}
	// Normalise to UTC midnight: trading_day is a date, and the payload's date
	// components (not its instant) are what the sweep means.
	anchor = time.Date(anchor.Year(), anchor.Month(), anchor.Day(), 0, 0, 0, 0, time.UTC)
	from := anchor.AddDate(0, 0, -SweepWindowDays)

	res := &BrokerSummarySweepResult{
		Date:       anchor.Format("2006-01-02"),
		WindowFrom: from.Format("2006-01-02"),
	}

	// A fresh process whose boot seed failed has only unknown keys, and unknown
	// keys contribute zero to Available(). Re-seed for free before planning;
	// failure is non-fatal and leaves the keys optimistically usable.
	if err := uc.Budget.EnsureSeeded(ctx); err != nil {
		uc.Log.Warnf("broker sweep: /usage re-seed incomplete: %v", err)
	}

	universe, err := uc.DailyPriceRepo.ADTVEligibleTickers(uc.DB, from, anchor, opts.MinDays, opts.MinADTV)
	if err != nil {
		return nil, fmt.Errorf("resolve ADTV universe: %w", err)
	}
	res.Eligible = len(universe)
	if len(universe) == 0 {
		return res, nil
	}

	allTickers := make([]string, len(universe))
	for i, u := range universe {
		allTickers[i] = u.Ticker
	}
	stored, err := uc.Repo.StoredTickersForDay(uc.DB, anchor, allTickers)
	if err != nil {
		return nil, fmt.Errorf("resolve stored ticker-days: %w", err)
	}
	pending := make([]repository.LiquidityTicker, 0, len(universe))
	for _, u := range universe {
		if stored[u.Ticker] {
			res.Stored++
			continue
		}
		pending = append(pending, u)
	}
	if len(pending) == 0 {
		return res, nil
	}
	res.Candidates = len(pending)

	pendingTickers := make([]string, len(pending))
	for i, u := range pending {
		pendingTickers[i] = u.Ticker
	}
	lastSeen, err := uc.Repo.LastObservedByTicker(uc.DB, pendingTickers, anchor)
	if err != nil {
		return nil, fmt.Errorf("resolve last-observed days: %w", err)
	}
	ordered := orderSweepCandidates(pending, lastSeen)

	capacity := uc.Budget.Available()
	if capacity > len(ordered) {
		capacity = len(ordered)
	}
	if capacity < 0 {
		capacity = 0
	}
	res.Planned = capacity
	if capacity == 0 {
		// Candidates exist but the pool can spend nothing: a played-out quota,
		// a daily request cap or a cooling pool. The handler records it.
		res.NoCapacity = true
		res.Uncovered = res.Candidates
		return res, nil
	}

	for i := 0; i < len(ordered) && res.Attempted < capacity; {
		chunk := nextChunkSize(uc.Budget.BatchCapacity(), capacity-res.Attempted, len(ordered)-i)
		if chunk == 0 {
			// No usable key can afford even one ticker.
			res.NoCapacity = true
			break
		}

		batchTickers := ordered[i : i+chunk]
		batch, err := uc.BatchFetcher.FetchBatch(ctx, batchTickers, anchor)
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
				res.Uncovered = res.Candidates - res.Attempted
				return res, err
			}
			uc.Log.Warnf("broker sweep: stopping at %d/%d ticker-days: %v", res.Attempted, res.Planned, err)
			break
		}

		res.Chunks++
		res.UnitsSpent += len(batchTickers)
		res.Attempted += len(batchTickers)
		i += chunk

		for _, ticker := range batchTickers {
			if parseErr, bad := batch.Invalid[ticker]; bad {
				res.Invalid++
				uc.Log.Warnf("broker sweep: %s %s rejected: %v", ticker, res.Date, parseErr)
				continue
			}
			r, ok := batch.Results[ticker]
			if !ok || (len(r.Buyers) == 0 && len(r.Sellers) == 0) {
				res.Empty++
				continue
			}
			if err := uc.persist(ticker, anchor, r, entity.BrokerSourceSweep); err != nil {
				res.Failed++
				uc.Log.Warnf("broker sweep: persist %s %s failed: %v", ticker, res.Date, err)
				continue
			}
			res.Fetched++
		}
	}

	res.Shortfall = res.Planned - res.Attempted
	if res.Shortfall < 0 {
		res.Shortfall = 0
	}
	res.Uncovered = res.Candidates - res.Attempted
	if res.Uncovered < 0 {
		res.Uncovered = 0
	}
	return res, nil
}

// nextChunkSize returns the next batch size: bounded by what one usable key can
// pay, by the run's remaining budget, by the remaining candidates and by the
// vendor's per-request limit. Zero means no usable key can serve a request now.
func nextChunkSize(perKeyCapacity, budgetLeft, candidatesLeft int) int {
	if perKeyCapacity <= 0 || budgetLeft <= 0 || candidatesLeft <= 0 {
		return 0
	}
	n := perKeyCapacity
	if n > broker.MaxBatchTickers {
		n = broker.MaxBatchTickers
	}
	if n > budgetLeft {
		n = budgetLeft
	}
	if n > candidatesLeft {
		n = candidatesLeft
	}
	return n
}

// orderSweepCandidates orders the rotation: never observed first, then the
// least recently observed, with ADTV desc as the tie-break (and the ticker code
// last, for determinism). The stored rows are the rotation cursor, so a
// budget-limited sweep walks the whole universe across days instead of
// re-fetching the same names.
func orderSweepCandidates(candidates []repository.LiquidityTicker, lastSeen map[string]time.Time) []string {
	sorted := make([]repository.LiquidityTicker, len(candidates))
	copy(sorted, candidates)
	sort.SliceStable(sorted, func(i, j int) bool {
		ti, seenI := lastSeen[sorted[i].Ticker]
		tj, seenJ := lastSeen[sorted[j].Ticker]
		if seenI != seenJ {
			return !seenI // never observed sorts first
		}
		if seenI && !ti.Equal(tj) {
			return ti.Before(tj)
		}
		if sorted[i].ADTV != sorted[j].ADTV {
			return sorted[i].ADTV > sorted[j].ADTV
		}
		return sorted[i].Ticker < sorted[j].Ticker
	})
	out := make([]string, len(sorted))
	for i, c := range sorted {
		out[i] = c.Ticker
	}
	return out
}
