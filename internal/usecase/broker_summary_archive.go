package usecase

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// BrokerArchiveMonthsPerRun is how many closed months one catch-up run may
	// examine. Newest first; an incomplete month is retried next run.
	BrokerArchiveMonthsPerRun = 3
	// BrokerArchiveMinDays is the ADTV predicate's minimum trading days.
	BrokerArchiveMinDays = 8
)

// BrokerArchiveOptions tunes the monthly archiver.
type BrokerArchiveOptions struct {
	// MinADTV is the Tier-2 liquidity floor. <= 0 uses the pipeline default.
	MinADTV int64
	// MinDays is the ADTV predicate's minimum days. <= 0 uses
	// BrokerArchiveMinDays.
	MinDays int
	// MaxMonths is the catch-up cap. <= 0 uses BrokerArchiveMonthsPerRun.
	MaxMonths int
	// MaxTickers caps the units spent in one run. 0 means only the pool caps it.
	MaxTickers int
}

// BrokerArchiveMonthResult is one month's accounting.
type BrokerArchiveMonthResult struct {
	Month       string `json:"month"` // YYYY-MM
	From        string `json:"from"`
	To          string `json:"to"`
	TradingDays int    `json:"trading_days"`
	Eligible    int    `json:"eligible"` // Tier-2 tickers
	Stored      int    `json:"stored"`   // skipped: a chunk already covers the month
	Derived     int    `json:"derived"`  // written from exact-covering day rows, 0 units
	Fetched     int    `json:"fetched"`  // fetched and persisted as a chunk
	Empty       int    `json:"empty"`    // requested but no rows; retried next run
	Invalid     int    `json:"invalid"`
	Failed      int    `json:"failed"`
	UnitsSpent  int    `json:"units_spent"`

	BudgetCapped bool `json:"budget_capped,omitempty"`
	QuotaStopped bool `json:"quota_stopped,omitempty"`
	DailyCapped  bool `json:"daily_capped,omitempty"`
	RateLimited  bool `json:"rate_limited,omitempty"`
	NoCapacity   bool `json:"no_capacity,omitempty"`

	Shortfall int `json:"shortfall"` // pending fetches the run could not start
}

// Stalled reports whether the month needs another run for budget/quota reasons.
func (m *BrokerArchiveMonthResult) Stalled() bool {
	return m.BudgetCapped || m.QuotaStopped || m.DailyCapped || m.RateLimited || m.NoCapacity || m.Shortfall > 0
}

// BrokerArchiveResult is one archiver run across up to MaxMonths closed months.
type BrokerArchiveResult struct {
	Months     []BrokerArchiveMonthResult `json:"months"`
	UnitsSpent int                        `json:"units_spent"`
	Completed  bool                       `json:"completed"`
}

// ArchiveClosedMonths writes month-aligned foreign chunks for the ADTV
// universe, starting with the most recently closed month and walking back at
// most MaxMonths.
//
// For each month M:
//
//  1. Its range is the first..last stored trading day of M, not the calendar
//     dates, so a month query exact-covers for free.
//  2. Tier 2 is the existing ADTV predicate over the 21 trading days ending on
//     M's last trading day (reaching into the prior month for a short M).
//  3. A ticker whose archive/backfill chunk already covers M is skipped.
//  4. If M's foreign day rows exactly cover its market days, the chunk is
//     derived for zero units; otherwise the whole month is fetched in ≤50-ticker
//     batches, one unit per ticker.
//
// Membership is unconditional: the derive path is an optimization, never a
// gate. A quota/daily/rate-limit stop keeps earlier chunks and reports the
// shortfall; the next daily run resumes.
func (uc *BrokerStockSummaryUseCase) ArchiveClosedMonths(ctx context.Context, runDate time.Time, opts BrokerArchiveOptions) (*BrokerArchiveResult, error) {
	if uc.BatchFetcher == nil || uc.Budget == nil {
		return nil, errors.New("broker archive: batch source and quota budget are not configured")
	}
	if opts.MinADTV <= 0 {
		opts.MinADTV = pipeline.DefaultADTVMinValue
	}
	if opts.MinDays <= 0 {
		opts.MinDays = BrokerArchiveMinDays
	}
	if opts.MaxMonths <= 0 {
		opts.MaxMonths = BrokerArchiveMonthsPerRun
	}

	runDate = time.Date(runDate.Year(), runDate.Month(), runDate.Day(), 0, 0, 0, 0, time.UTC)
	firstOfCurrent := time.Date(runDate.Year(), runDate.Month(), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := firstOfCurrent.AddDate(0, 0, -1)

	res := &BrokerArchiveResult{Completed: true}
	for i := 0; i < opts.MaxMonths; i++ {
		monthStart := time.Date(monthEnd.Year(), monthEnd.Month(), 1, 0, 0, 0, 0, time.UTC)
		mr, err := uc.archiveMonth(ctx, monthStart, monthEnd, opts)
		if err != nil {
			return res, err
		}
		res.Months = append(res.Months, *mr)
		res.UnitsSpent += mr.UnitsSpent
		if mr.Stalled() {
			res.Completed = false
			break // newest first: do not skip ahead of an unfinished month
		}
		monthEnd = monthStart.AddDate(0, 0, -1)
	}
	return res, nil
}

func (uc *BrokerStockSummaryUseCase) archiveMonth(ctx context.Context, monthStart, monthEnd time.Time, opts BrokerArchiveOptions) (*BrokerArchiveMonthResult, error) {
	res := &BrokerArchiveMonthResult{Month: monthStart.Format("2006-01")}

	days, err := uc.DailyPriceRepo.TradingDaysInRangeAll(uc.DB, monthStart, monthEnd)
	if err != nil {
		return nil, fmt.Errorf("resolve %s trading days: %w", res.Month, err)
	}
	if len(days) == 0 {
		return res, nil
	}
	first, last := days[0], days[len(days)-1]
	res.From, res.To = first.Format("2006-01-02"), last.Format("2006-01-02")
	res.TradingDays = len(days)

	tier2, err := uc.tier2ForMonth(last, opts)
	if err != nil {
		return nil, fmt.Errorf("resolve %s ADTV universe: %w", res.Month, err)
	}
	res.Eligible = len(tier2)

	pending := make([]string, 0, len(tier2))
	for _, t := range tier2 {
		covered, err := uc.Repo.HasPartitionChunk(uc.DB, t.Ticker, first, last)
		if err != nil {
			return nil, fmt.Errorf("check %s %s chunk: %w", t.Ticker, res.Month, err)
		}
		if covered {
			res.Stored++
			continue
		}
		pending = append(pending, t.Ticker)
	}
	if len(pending) == 0 {
		return res, nil
	}

	// Derive first: a complete day-row cover costs nothing and is independent of
	// the quota, so it runs even when the pool is spent.
	fetch := make([]string, 0, len(pending))
	for _, ticker := range pending {
		derived, err := uc.deriveMonthChunk(ticker, first, last, days)
		if err != nil {
			return nil, fmt.Errorf("derive %s %s: %w", ticker, res.Month, err)
		}
		if derived {
			res.Derived++
			continue
		}
		fetch = append(fetch, ticker)
	}
	if len(fetch) == 0 {
		return res, nil
	}

	if err := uc.Budget.EnsureSeeded(ctx); err != nil {
		uc.Log.Warnf("broker archive: /usage re-seed incomplete: %v", err)
	}
	capacity := len(fetch)
	if available := uc.Budget.Available(); capacity > available {
		capacity = available
	}
	if opts.MaxTickers > 0 && capacity > opts.MaxTickers {
		capacity = opts.MaxTickers
	}
	if capacity < len(fetch) {
		res.BudgetCapped = true
	}
	if capacity <= 0 {
		res.NoCapacity = true
		res.Shortfall = len(fetch)
		return res, nil
	}

	attempted := 0
	for i := 0; i < len(fetch) && i < capacity; {
		chunk := nextChunkSize(uc.Budget.BatchCapacity(), capacity-i, len(fetch)-i)
		if chunk == 0 {
			res.NoCapacity = true
			break
		}
		batchTickers := fetch[i : i+chunk]
		batch, err := uc.BatchFetcher.FetchBatchRange(ctx, batchTickers, first, last, broker.InvestorForeign)
		if err != nil {
			switch {
			case errors.Is(err, broker.ErrQuotaExhausted):
				res.QuotaStopped = true
			case errors.Is(err, broker.ErrDailyLimit):
				res.DailyCapped = true
			case errors.Is(err, broker.ErrRateLimited):
				res.RateLimited = true
			default:
				return res, err
			}
			break
		}

		res.UnitsSpent += len(batchTickers)
		attempted += len(batchTickers)
		i += chunk

		for _, ticker := range batchTickers {
			if parseErr, bad := batch.Invalid[ticker]; bad {
				res.Invalid++
				uc.Log.Warnf("broker archive: %s %s rejected: %v", ticker, res.Month, parseErr)
				continue
			}
			r, ok := batch.Results[ticker]
			if !ok || (len(r.Buyers) == 0 && len(r.Sellers) == 0) {
				// No rows is not a chunk: the next run retries rather than
				// freezing an empty month forever.
				res.Empty++
				continue
			}
			if err := persistBrokerWindow(uc.DB, uc.Repo, ticker, first, last, r, broker.InvestorForeign, entity.BrokerSourceArchive); err != nil {
				res.Failed++
				uc.Log.Warnf("broker archive: persist %s %s failed: %v", ticker, res.Month, err)
				continue
			}
			res.Fetched++
		}
	}
	res.Shortfall = len(fetch) - attempted
	if res.Shortfall < 0 {
		res.Shortfall = 0
	}
	return res, nil
}

// tier2ForMonth applies the existing ADTV predicate to the 21 trading days
// ending on the month's last trading day.
func (uc *BrokerStockSummaryUseCase) tier2ForMonth(monthLast time.Time, opts BrokerArchiveOptions) ([]repository.LiquidityTicker, error) {
	lookbackStart := monthLast.AddDate(0, 0, -70)
	days, err := uc.DailyPriceRepo.TradingDaysInRangeAll(uc.DB, lookbackStart, monthLast)
	if err != nil {
		return nil, err
	}
	if len(days) == 0 {
		return nil, nil
	}
	if len(days) > 21 {
		days = days[len(days)-21:]
	}
	return uc.DailyPriceRepo.ADTVEligibleTickers(uc.DB, days[0], monthLast, opts.MinDays, opts.MinADTV)
}

// deriveMonthChunk writes a month chunk from exact-covering foreign day rows.
// It returns false when the month is not exactly covered (or the rows are
// missing), leaving the fetch path to buy it.
func (uc *BrokerStockSummaryUseCase) deriveMonthChunk(ticker string, monthFirst, monthLast time.Time, days []time.Time) (bool, error) {
	spans, err := uc.Repo.FindWindowSpans(uc.DB, []string{ticker}, broker.InvestorForeign, monthFirst, monthLast)
	if err != nil {
		return false, err
	}
	daySpans := make([]broker.Span, 0, len(spans))
	for _, s := range spans {
		if s.WinFrom.Equal(s.TradingDay) {
			daySpans = append(daySpans, s.Span())
		}
	}
	if len(daySpans) == 0 {
		return false, nil
	}
	selected, ok := broker.ExactCover(daySpans, monthFirst, monthLast, days)
	if !ok {
		return false, nil
	}
	agg, err := uc.Repo.SumRowsInSpans(uc.DB, ticker, broker.InvestorForeign, selected)
	if err != nil {
		return false, err
	}
	res := resultFromAggregated(agg)
	if err := persistBrokerWindow(uc.DB, uc.Repo, ticker, monthFirst, monthLast, res, broker.InvestorForeign, entity.BrokerSourceArchive); err != nil {
		return false, err
	}
	return true, nil
}

// resultFromAggregated turns SQL-composed broker sums into the neutral Result
// the shared window writer consumes. Rank is recomputed per side; avg is the
// window VWAP, derived from value and lot, never summed.
func resultFromAggregated(agg []repository.BrokerAggregatedRow) *broker.Result {
	buyers := make([]broker.Row, 0, len(agg))
	sellers := make([]broker.Row, 0, len(agg))
	for _, r := range agg {
		var avg int64
		if r.Lot > 0 {
			avg = r.Value / (r.Lot * 100)
		}
		row := broker.Row{BrokerCode: r.BrokerCode, Lot: r.Lot, Value: r.Value, AvgPrice: avg}
		if r.Side == "sell" {
			sellers = append(sellers, row)
		} else {
			buyers = append(buyers, row)
		}
	}
	rankResultRows(buyers)
	rankResultRows(sellers)
	return &broker.Result{Investor: broker.InvestorForeign, Buyers: buyers, Sellers: sellers}
}

func rankResultRows(rows []broker.Row) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Value != rows[j].Value {
			return rows[i].Value > rows[j].Value
		}
		return rows[i].BrokerCode < rows[j].BrokerCode
	})
	for i := range rows {
		rows[i].Rank = i + 1
	}
}
