package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// tickerPattern matches IDX ticker codes (2-6 uppercase letters).
var tickerPattern = regexp.MustCompile(`^[A-Z]{2,6}$`)

// BrokerSummaryFetcher fetches per-ticker broker activity from a market-data
// source. FetchRange is the primitive: a day is the degenerate window, and a
// range costs the same one unit as a day. *indexalpha.Pool satisfies this.
type BrokerSummaryFetcher interface {
	FetchRange(ctx context.Context, ticker string, from, to time.Time, investor string) (*broker.Result, error)
}

// BrokerSummaryBatchFetcher fetches one batch of at most broker.MaxBatchTickers
// tickers for one window and one investor lane. *indexalpha.Pool satisfies it;
// the batch is metered by the number of tickers requested.
type BrokerSummaryBatchFetcher interface {
	FetchBatch(ctx context.Context, tickers []string, date time.Time) (*broker.Batch, error)
	FetchBatchRange(ctx context.Context, tickers []string, from, to time.Time, investor string) (*broker.Batch, error)
}

// BrokerSweepBudget is the quota view the daily writers plan against:
// available monthly units (reserve already deducted), the largest batch a
// single usable key can afford right now, the next rolling reset, and a free
// re-seed for keys that have never been measured.
type BrokerSweepBudget interface {
	Available() int
	BatchCapacity() int
	NextReset() *time.Time
	EnsureSeeded(ctx context.Context) error
}

// BrokerStockSummaryUseCase orchestrates the per-stock broker summary flow: the
// database answers first, and only a genuine cover miss spends one unit to fetch
// a whole window and cache it.
type BrokerStockSummaryUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	Validate       *validator.Validate
	Fetcher        BrokerSummaryFetcher
	BatchFetcher   BrokerSummaryBatchFetcher
	Budget         BrokerSweepBudget
	Repo           *repository.BrokerStockSummaryRepository
	DailyPriceRepo *repository.DailyPriceRepository
	AnomalyRepo    *repository.AnomalyRepository
	Usage          FetchUsageCounter
	// Now is the clock the daily budget keys on. Tests inject a fixed clock;
	// production uses time.Now.
	Now func() time.Time
}

// BrokerStockSummaryOption configures optional dependencies. The on-demand
// paths need only Fetcher; the writers also need a batch fetcher, a quota
// budget, and the daily usage counter.
type BrokerStockSummaryOption func(*BrokerStockSummaryUseCase)

// WithBatchSource installs the batch fetcher and quota budget used by the
// writers.
func WithBatchSource(f BrokerSummaryBatchFetcher, b BrokerSweepBudget) BrokerStockSummaryOption {
	return func(uc *BrokerStockSummaryUseCase) {
		uc.BatchFetcher = f
		uc.Budget = b
	}
}

// WithAnomalySource installs the anomaly read behind the daily refresher.
func WithAnomalySource(repo *repository.AnomalyRepository) BrokerStockSummaryOption {
	return func(uc *BrokerStockSummaryUseCase) {
		uc.AnomalyRepo = repo
	}
}

// WithFetchUsage overrides the daily fetch-budget counter (tests).
func WithFetchUsage(repo FetchUsageCounter) BrokerStockSummaryOption {
	return func(uc *BrokerStockSummaryUseCase) {
		uc.Usage = repo
	}
}

func NewBrokerStockSummaryUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	fetcher BrokerSummaryFetcher,
	repo *repository.BrokerStockSummaryRepository,
	dailyPriceRepo *repository.DailyPriceRepository,
	opts ...BrokerStockSummaryOption,
) *BrokerStockSummaryUseCase {
	uc := &BrokerStockSummaryUseCase{
		DB:             db,
		Log:            log,
		Validate:       validate,
		Fetcher:        fetcher,
		Repo:           repo,
		DailyPriceRepo: dailyPriceRepo,
		Usage:          repository.NewFetchUsageRepository(log),
		Now:            time.Now,
	}
	for _, opt := range opts {
		opt(uc)
	}
	return uc
}

// BrokerSummaryRow is one broker's buy or sell activity in the response. On a
// window it is the window's aggregate for that broker; avg_price is a window
// VWAP, not any single day's price.
type BrokerSummaryRow struct {
	BrokerCode string `json:"broker_code"`
	Lot        int64  `json:"lot"`
	Value      int64  `json:"value"`
	AvgPrice   int64  `json:"avg_price"`
	Rank       int    `json:"rank"`
}

// brokerSummaryTopN is the read-time projection: every broker is stored, but the
// tools keep their historical top-ten-per-side shape.
const brokerSummaryTopN = 10

// ServedFrom declares where an answer came from: the stored partition ("store",
// zero quota) or a fresh upstream fetch ("upstream", one unit).
const (
	servedFromStore    = "store"
	servedFromUpstream = "upstream"
)

// Grain values. A window whose first day equals its end is a day entry; any
// other span is a window entry; a table holding both reads "mixed".
const (
	grainDay    = "day"
	grainWindow = "window"
	grainMixed  = "mixed"
	grainNone   = "none"
)

// BrokerSummaryCoverage declares what a stored window observes. In the foreign
// lane Complete is structural — the vendor returns the full broker set for the
// window, and there is no all-investor t_val anchor to reconcile against.
// Reconciling a window requires a separate all-lane fetch, which no read does
// implicitly.
type BrokerSummaryCoverage struct {
	Lane             string `json:"lane"` // "foreign" | "all"
	Brokers          int    `json:"brokers"`
	Buyers           int    `json:"buyers"`
	Sellers          int    `json:"sellers"`
	BuyValueCovered  int64  `json:"buy_value_covered"`
	SellValueCovered int64  `json:"sell_value_covered"`
	Complete         bool   `json:"complete"`
}

// BrokerSummaryTotals is the summary line in the response. In the foreign lane
// t_val/t_lot/avg are null — an f payload has no all-investor totals — and
// f_nval is the foreign net over the window. Null always means unobserved,
// never zero.
type BrokerSummaryTotals struct {
	TVal  *int64 `json:"t_val"`
	FNVal *int64 `json:"f_nval"`
	TLot  *int64 `json:"t_lot"`
	Avg   *int64 `json:"avg"`
}

// BrokerStockSummaryWindow is one stored window in a response. From/To are its
// range (To is the trading_day column, the window end) and Grain says whether it
// is a day or an aggregated window. TotalBuyValue/TotalSellValue are the gross
// rows over the full stored set — foreign gross in the f lane.
type BrokerStockSummaryWindow struct {
	From           string                `json:"from"`
	To             string                `json:"to"`
	Grain          string                `json:"grain"`
	TotalBuyValue  int64                 `json:"total_buy_value"`
	TotalSellValue int64                 `json:"total_sell_value"`
	Coverage       BrokerSummaryCoverage `json:"coverage"`
	Totals         BrokerSummaryTotals   `json:"totals"`
	Buyers         []BrokerSummaryRow    `json:"buyers"`
	Sellers        []BrokerSummaryRow    `json:"sellers"`
}

// BrokerStockSummaryResponse is the structured MCP tool result for one window.
// AsOf + Cause are set only when the requested day returned empty and a prior
// stored day answered instead (or explained the empty). ServedFrom says whether
// the call cost quota.
type BrokerStockSummaryResponse struct {
	Ticker         string                `json:"ticker"`
	From           string                `json:"from"`
	To             string                `json:"to"`
	Grain          string                `json:"grain"`
	TradingDay     string                `json:"trading_day"`
	AsOf           string                `json:"as_of,omitempty"`
	Cause          string                `json:"cause,omitempty"`
	ServedFrom     string                `json:"served_from"`
	TotalBuyValue  int64                 `json:"total_buy_value"`
	TotalSellValue int64                 `json:"total_sell_value"`
	Coverage       BrokerSummaryCoverage `json:"coverage"`
	Totals         BrokerSummaryTotals   `json:"totals"`
	Buyers         []BrokerSummaryRow    `json:"buyers"`
	Sellers        []BrokerSummaryRow    `json:"sellers"`
}

// BrokerStockSummaryHistoryResponse is the structured result of a stored
// history read. Windows lists every stored window that intersects the range,
// each with its own from/to/grain; Grain is "mixed" when both day rows and
// aggregated windows are present, so a table is never presented as a daily
// series.
type BrokerStockSummaryHistoryResponse struct {
	Ticker  string                     `json:"ticker"`
	From    string                     `json:"from"`
	To      string                     `json:"to"`
	Grain   string                     `json:"grain"`
	Windows []BrokerStockSummaryWindow `json:"windows"`
}

// BrokerStockSummaryRangeResponse is the result of a range/backfill run. A
// covered range is served from the partition with no quota; a miss fetches the
// whole requested range in one call and stores it as an answer cache.
type BrokerStockSummaryRangeResponse struct {
	Ticker     string                     `json:"ticker"`
	Start      string                     `json:"start"`
	End        string                     `json:"end"`
	Grain      string                     `json:"grain"`
	ServedFrom string                     `json:"served_from"`
	Fetched    int                        `json:"fetched"`
	Failed     int                        `json:"failed"`
	Empty      int                        `json:"empty"`
	Windows    []BrokerStockSummaryWindow `json:"windows"`
}

// GetStockBrokerSummary answers one ticker+day. An exact stored one-day window
// costs zero quota; a miss spends exactly one unit on that day's foreign lane
// and caches it. Empty results fall back over stored prior days only — never an
// upstream probe, which would spend up to five units to rediscover what the
// store already answers.
func (uc *BrokerStockSummaryUseCase) GetStockBrokerSummary(ctx context.Context, ticker string, date *time.Time) (*BrokerStockSummaryResponse, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}

	var day time.Time
	if date != nil {
		day = *date
	} else {
		latest, err := uc.DailyPriceRepo.LatestTradingDay(uc.DB, ticker)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrNoTradingDay
			}
			return nil, fmt.Errorf("resolve latest trading day: %w", err)
		}
		day = *latest
	}

	// Zero-unit path: the exact one-day window is stored.
	totals, rows, err := uc.Repo.FindStoredWindow(uc.DB, ticker, brokerReadLane, day, day)
	if err == nil {
		win := windowFromStored(day, day, rows, totals)
		return responseFromWindow(ticker, win, servedFromStore, "", ""), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("read stored broker window: %w", err)
	}

	// One-unit path: fetch the day, persist it as the f lane's live day window.
	res, err := uc.fetchRange(ctx, ticker, day, day)
	if err != nil {
		return nil, fmt.Errorf("broker summary fetch: %w", err)
	}
	if len(res.Buyers) == 0 && len(res.Sellers) == 0 {
		return uc.fallbackStored(ticker, day)
	}
	if err := persistBrokerWindow(uc.DB, uc.Repo, ticker, day, day, res, brokerReadLane, entity.BrokerSourceLive); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPersist, err)
	}
	win := windowFromResult(day, day, res, brokerReadLane)
	return responseFromWindow(ticker, win, servedFromUpstream, "", ""), nil
}

// fetchRange is the one-unit upstream read, always on the foreign lane. A
// single-day read is the degenerate window, not a separate request path.
func (uc *BrokerStockSummaryUseCase) fetchRange(ctx context.Context, ticker string, from, to time.Time) (*broker.Result, error) {
	if uc.Fetcher == nil {
		return nil, fmt.Errorf("%w: no broker summary fetcher configured", ErrInvalidArgument)
	}
	return uc.Fetcher.FetchRange(ctx, ticker, from, to, brokerReadLane)
}

// brokerSummaryFallbackMaxCalendarDays bounds the stored-day fallback so a
// sparse ticker cannot return a months-old window as if it were "prior".
const brokerSummaryFallbackMaxCalendarDays = 30

// fallbackStored explains an empty upstream answer from the store: if the
// requested day has no data but a recent stored window exists, return it tagged
// as_of + cause. Nothing is fetched for the probe.
func (uc *BrokerStockSummaryUseCase) fallbackStored(ticker string, requested time.Time) (*BrokerStockSummaryResponse, error) {
	cause := "not_yet_published"
	if !uc.isTradingDay(ticker, requested) {
		cause = "non_trading_day"
	}

	latest, err := uc.Repo.FindLatestWindowBefore(uc.DB, ticker, brokerReadLane, requested)
	if err == nil && !latest.TradingDay.Before(requested.AddDate(0, 0, -brokerSummaryFallbackMaxCalendarDays)) {
		totals, rows, err := uc.Repo.FindStoredWindow(uc.DB, ticker, brokerReadLane, latest.WinFrom, latest.TradingDay)
		if err == nil {
			win := windowFromStored(latest.WinFrom, latest.TradingDay, rows, totals)
			return responseFromWindow(ticker, win, servedFromStore, win.To, cause), nil
		}
		uc.Log.Warnf("broker_stock_summary fallback: read stored window %s..%s failed: %v",
			latest.WinFrom.Format("2006-01-02"), latest.TradingDay.Format("2006-01-02"), err)
	}

	// Trading day but nothing stored across the fallback window → no activity.
	if cause == "not_yet_published" {
		cause = "no_activity"
	}
	return &BrokerStockSummaryResponse{
		Ticker:     ticker,
		From:       requested.Format("2006-01-02"),
		To:         requested.Format("2006-01-02"),
		Grain:      grainDay,
		TradingDay: requested.Format("2006-01-02"),
		Cause:      cause,
		ServedFrom: servedFromUpstream,
		Totals:     BrokerSummaryTotals{},
		Buyers:     []BrokerSummaryRow{},
		Sellers:    []BrokerSummaryRow{},
		Coverage:   BrokerSummaryCoverage{Lane: laneName(brokerReadLane)},
	}, nil
}

// isTradingDay reports whether the ticker has a stored EOD row for the date.
// Data presence is the trading-day signal (no separate calendar).
func (uc *BrokerStockSummaryUseCase) isTradingDay(ticker string, day time.Time) bool {
	_, err := uc.DailyPriceRepo.FindByTickerAndDay(uc.DB, ticker, day.Format("2006-01-02"))
	return err == nil
}

// GetStockBrokerSummaryHistory reads every stored window intersecting [from, to]
// and returns each with its own range and grain. Pure DB read — the history tool
// never spends quota; a gap is a gap. Legacy top-10 rows are invisible.
func (uc *BrokerStockSummaryUseCase) GetStockBrokerSummaryHistory(ctx context.Context, ticker string, from, to time.Time) (*BrokerStockSummaryHistoryResponse, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}
	if from.After(to) {
		return nil, ErrInvalidRange
	}

	spans, err := uc.Repo.FindWindowSpans(uc.DB, []string{ticker}, brokerReadLane, from, to)
	if err != nil {
		return nil, fmt.Errorf("read broker summary windows: %w", err)
	}
	rows, err := uc.Repo.FindRowsInWindows(uc.DB, []string{ticker}, brokerReadLane, from, to)
	if err != nil {
		return nil, fmt.Errorf("read broker summary rows: %w", err)
	}
	totals, err := uc.Repo.FindTotalsInWindows(uc.DB, []string{ticker}, brokerReadLane, from, to)
	if err != nil {
		return nil, fmt.Errorf("read broker summary totals: %w", err)
	}

	resp := &BrokerStockSummaryHistoryResponse{
		Ticker:  ticker,
		From:    from.Format("2006-01-02"),
		To:      to.Format("2006-01-02"),
		Grain:   grainNone,
		Windows: []BrokerStockSummaryWindow{},
	}

	totalsByWindow := make(map[string]*entity.BrokerStockSummaryTotals, len(totals))
	for i := range totals {
		totalsByWindow[windowKey(totals[i].WinFrom, totals[i].TradingDay)] = &totals[i]
	}
	rowsByWindow := make(map[string][]entity.BrokerStockSummary)
	for _, r := range rows {
		k := windowKey(r.WinFrom, r.TradingDay)
		rowsByWindow[k] = append(rowsByWindow[k], r)
	}

	sort.Slice(spans, func(i, j int) bool {
		if !spans[i].TradingDay.Equal(spans[j].TradingDay) {
			return spans[i].TradingDay.Before(spans[j].TradingDay)
		}
		return spans[i].WinFrom.Before(spans[j].WinFrom)
	})
	for _, s := range spans {
		k := windowKey(s.WinFrom, s.TradingDay)
		t := totalsByWindow[k]
		if t == nil {
			continue // a window without a footer cannot declare its totals
		}
		resp.Windows = append(resp.Windows, windowFromStored(s.WinFrom, s.TradingDay, rowsByWindow[k], t))
	}
	resp.Grain = responseGrain(resp.Windows)
	return resp, nil
}

// GetStockBrokerSummaryRange answers a range. An exact cover of stored disjoint
// windows costs zero units; anything else fetches the whole requested range in
// one call and stores it as a window answer cache. Partial covers are never
// built — a gap fill costs the same unit as the whole range.
func (uc *BrokerStockSummaryUseCase) GetStockBrokerSummaryRange(ctx context.Context, ticker string, start, end time.Time) (*BrokerStockSummaryRangeResponse, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}
	if start.After(end) {
		return nil, ErrInvalidRange
	}

	resp := &BrokerStockSummaryRangeResponse{
		Ticker:  ticker,
		Start:   start.Format("2006-01-02"),
		End:     end.Format("2006-01-02"),
		Grain:   grainNone,
		Windows: []BrokerStockSummaryWindow{},
	}

	days, err := brokerMarketDays(uc.DB, uc.DailyPriceRepo, start, end)
	if err != nil {
		return nil, fmt.Errorf("resolve trading days: %w", err)
	}
	spans, err := uc.Repo.FindWindowSpans(uc.DB, []string{ticker}, brokerReadLane, start, end)
	if err != nil {
		return nil, fmt.Errorf("read stored windows: %w", err)
	}
	cover := buildWindowCover(spans, start, end, days, true)
	if selected := cover.spans[ticker]; len(selected) > 0 {
		// An exact union of disjoint stored windows answers for zero units. The
		// sum runs in SQL; the response is one window for the requested range,
		// never a list of pieces that a reader could mistake for days.
		agg, err := uc.Repo.SumRowsInSpans(uc.DB, ticker, brokerReadLane, selected)
		if err != nil {
			return nil, fmt.Errorf("sum covered windows %s..%s: %w",
				start.Format("2006-01-02"), end.Format("2006-01-02"), err)
		}
		resp.ServedFrom = servedFromStore
		resp.Windows = append(resp.Windows, windowFromAggregated(start, end, agg))
		resp.Grain = responseGrain(resp.Windows)
		return resp, nil
	}

	// A miss fetches the whole requested range once, never day by day.
	res, err := uc.fetchRange(ctx, ticker, start, end)
	if err != nil {
		uc.Log.Warnf("broker_stock_summary range: %s %s..%s failed: %v",
			ticker, start.Format("2006-01-02"), end.Format("2006-01-02"), err)
		resp.Failed++
		return resp, nil
	}
	if len(res.Buyers) == 0 && len(res.Sellers) == 0 {
		uc.Log.Warnf("broker_stock_summary range: %s %s..%s empty (not yet published or no activity)",
			ticker, start.Format("2006-01-02"), end.Format("2006-01-02"))
		resp.Empty++
		return resp, nil
	}
	if err := persistBrokerWindow(uc.DB, uc.Repo, ticker, start, end, res, brokerReadLane, entity.BrokerSourceWindow); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPersist, err)
	}
	resp.ServedFrom = servedFromUpstream
	resp.Fetched++
	resp.Windows = append(resp.Windows, windowFromResult(start, end, res, brokerReadLane))
	resp.Grain = responseGrain(resp.Windows)
	return resp, nil
}

// persist writes one fetched day for the daily refresher, honoring the result's
// lane. The read paths call persistBrokerWindow directly with an explicit lane.
func (uc *BrokerStockSummaryUseCase) persist(ticker string, day time.Time, res *broker.Result, source string) error {
	investor := res.Investor
	if investor == "" {
		investor = broker.InvestorAll
	}
	return persistBrokerWindow(uc.DB, uc.Repo, ticker, day, day, res, investor, source)
}

// windowFromStored builds a response entry from stored rows and totals. Ranks
// are recomputed over the full set because per-window ranks are not comparable
// once windows are composed or read back at a different grain.
func windowFromStored(from, to time.Time, rows []entity.BrokerStockSummary, totals *entity.BrokerStockSummaryTotals) BrokerStockSummaryWindow {
	buyers := make([]BrokerSummaryRow, 0, len(rows))
	sellers := make([]BrokerSummaryRow, 0, len(rows))
	for _, r := range rows {
		row := BrokerSummaryRow{
			BrokerCode: r.BrokerCode,
			Lot:        deref(r.Lot),
			Value:      deref(r.Value),
			AvgPrice:   deref(r.AvgPrice),
		}
		if r.Side == "buy" {
			buyers = append(buyers, row)
		} else {
			sellers = append(sellers, row)
		}
	}
	rankSummaryRows(buyers)
	rankSummaryRows(sellers)

	lane := broker.InvestorAll
	var tval *int64
	if totals != nil {
		lane = totals.Investor
		tval = totals.TVal
	}
	win := BrokerStockSummaryWindow{
		From:           from.Format("2006-01-02"),
		To:             to.Format("2006-01-02"),
		Grain:          grainFor(from, to),
		TotalBuyValue:  sumValues(buyers),
		TotalSellValue: sumValues(sellers),
		Coverage:       coverageForWindow(lane, buyers, sellers, tval),
		Totals:         totalsFromEntity(totals),
		Buyers:         topN(buyers),
		Sellers:        topN(sellers),
	}
	return win
}

// windowFromAggregated builds one entry from per-broker sums over a disjoint
// window selection. Average price is derived from the summed value and lot — a
// window VWAP — because per-window averages are not summable.
func windowFromAggregated(from, to time.Time, rows []repository.BrokerAggregatedRow) BrokerStockSummaryWindow {
	buyers := make([]BrokerSummaryRow, 0, len(rows))
	sellers := make([]BrokerSummaryRow, 0, len(rows))
	for _, r := range rows {
		var avg int64
		if r.Lot > 0 {
			avg = r.Value / (r.Lot * 100)
		}
		row := BrokerSummaryRow{BrokerCode: r.BrokerCode, Lot: r.Lot, Value: r.Value, AvgPrice: avg}
		if r.Side == "sell" {
			sellers = append(sellers, row)
		} else {
			buyers = append(buyers, row)
		}
	}
	rankSummaryRows(buyers)
	rankSummaryRows(sellers)

	net := sumValues(buyers) - sumValues(sellers)
	win := BrokerStockSummaryWindow{
		From:           from.Format("2006-01-02"),
		To:             to.Format("2006-01-02"),
		Grain:          grainFor(from, to),
		TotalBuyValue:  sumValues(buyers),
		TotalSellValue: sumValues(sellers),
		Coverage:       coverageForWindow(brokerReadLane, buyers, sellers, nil),
		Totals:         BrokerSummaryTotals{FNVal: &net},
		Buyers:         topN(buyers),
		Sellers:        topN(sellers),
	}
	return win
}

// windowFromResult builds a response entry from a fresh source result.
func windowFromResult(from, to time.Time, res *broker.Result, investor string) BrokerStockSummaryWindow {
	buyers := make([]BrokerSummaryRow, 0, len(res.Buyers))
	sellers := make([]BrokerSummaryRow, 0, len(res.Sellers))
	for _, b := range res.Buyers {
		buyers = append(buyers, BrokerSummaryRow{BrokerCode: b.BrokerCode, Lot: b.Lot, Value: b.Value, AvgPrice: b.AvgPrice})
	}
	for _, s := range res.Sellers {
		sellers = append(sellers, BrokerSummaryRow{BrokerCode: s.BrokerCode, Lot: s.Lot, Value: s.Value, AvgPrice: s.AvgPrice})
	}
	rankSummaryRows(buyers)
	rankSummaryRows(sellers)

	tvals := totalsFromResult(res, investor)
	win := BrokerStockSummaryWindow{
		From:           from.Format("2006-01-02"),
		To:             to.Format("2006-01-02"),
		Grain:          grainFor(from, to),
		TotalBuyValue:  sumValues(buyers),
		TotalSellValue: sumValues(sellers),
		Coverage:       coverageForWindow(investor, buyers, sellers, tvals.TVal),
		Totals:         tvals,
		Buyers:         topN(buyers),
		Sellers:        topN(sellers),
	}
	return win
}

// responseFromWindow lifts a window entry into the single-window tool response.
func responseFromWindow(ticker string, win BrokerStockSummaryWindow, servedFrom, asOf, cause string) *BrokerStockSummaryResponse {
	return &BrokerStockSummaryResponse{
		Ticker:         ticker,
		From:           win.From,
		To:             win.To,
		Grain:          win.Grain,
		TradingDay:     win.To,
		AsOf:           asOf,
		Cause:          cause,
		ServedFrom:     servedFrom,
		TotalBuyValue:  win.TotalBuyValue,
		TotalSellValue: win.TotalSellValue,
		Coverage:       win.Coverage,
		Totals:         win.Totals,
		Buyers:         win.Buyers,
		Sellers:        win.Sellers,
	}
}

// totalsFromEntity converts a stored footer to the response shape.
func totalsFromEntity(t *entity.BrokerStockSummaryTotals) BrokerSummaryTotals {
	if t == nil {
		return BrokerSummaryTotals{}
	}
	return BrokerSummaryTotals{TVal: t.TVal, FNVal: t.FNVal, TLot: t.TLot, Avg: t.Avg}
}

// totalsFromResult converts a source result to the response shape. In the
// foreign lane only f_nval is observable; the all-investor totals stay null.
func totalsFromResult(res *broker.Result, investor string) BrokerSummaryTotals {
	if investor == broker.InvestorForeign {
		net := foreignNetFromRows(res.Buyers, res.Sellers)
		return BrokerSummaryTotals{FNVal: &net}
	}
	return BrokerSummaryTotals{
		TVal:  i64p(res.Totals.TVal),
		FNVal: res.Totals.FNVal,
		TLot:  i64p(res.Totals.TLot),
		Avg:   i64p(res.Totals.Avg),
	}
}

// coverageForWindow declares what a window's stored rows observe. The foreign
// lane is structurally complete (the vendor returns the full set); the all lane
// reconciles to t_val when one is present.
func coverageForWindow(investor string, buyers, sellers []BrokerSummaryRow, tVal *int64) BrokerSummaryCoverage {
	seen := make(map[string]struct{}, len(buyers)+len(sellers))
	cov := BrokerSummaryCoverage{Lane: laneName(investor), Buyers: len(buyers), Sellers: len(sellers)}
	for _, b := range buyers {
		seen[b.BrokerCode] = struct{}{}
		cov.BuyValueCovered += b.Value
	}
	for _, s := range sellers {
		seen[s.BrokerCode] = struct{}{}
		cov.SellValueCovered += s.Value
	}
	cov.Brokers = len(seen)
	if investor == broker.InvestorForeign {
		cov.Complete = true
		return cov
	}
	cov.Complete = tVal != nil && *tVal > 0 && cov.BuyValueCovered == *tVal && cov.SellValueCovered == *tVal
	return cov
}

// laneName renders an investor lane for the response.
func laneName(investor string) string {
	if investor == broker.InvestorForeign {
		return "foreign"
	}
	return "all"
}

// rankSummaryRows assigns dense 1..N ranks by value desc over one side, so a
// composed or recomputed set keeps the source's projection semantics.
func rankSummaryRows(rows []BrokerSummaryRow) {
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

func sumValues(rows []BrokerSummaryRow) int64 {
	var total int64
	for _, r := range rows {
		total += r.Value
	}
	return total
}

// topN projects a ranked side to the read-time top-N cap.
func topN(rows []BrokerSummaryRow) []BrokerSummaryRow {
	if len(rows) > brokerSummaryTopN {
		return rows[:brokerSummaryTopN]
	}
	return rows
}

// grainFor classifies one stored window.
func grainFor(from, to time.Time) string {
	if from.Equal(to) {
		return grainDay
	}
	return grainWindow
}

// responseGrain folds per-entry grains into the table's grain.
func responseGrain(windows []BrokerStockSummaryWindow) string {
	if len(windows) == 0 {
		return grainNone
	}
	day, window := false, false
	for _, w := range windows {
		if w.Grain == grainWindow {
			window = true
		} else {
			day = true
		}
	}
	switch {
	case day && window:
		return grainMixed
	case window:
		return grainWindow
	default:
		return grainDay
	}
}

// windowKey identifies one stored window (start + end).
func windowKey(from, to time.Time) string {
	return from.Format("2006-01-02") + "|" + to.Format("2006-01-02")
}

func toEntity(ticker string, winFrom, day time.Time, investor, side string, r broker.Row, source string) entity.BrokerStockSummary {
	rank := int32(r.Rank)
	return entity.BrokerStockSummary{
		Ticker:     ticker,
		BrokerCode: r.BrokerCode,
		Side:       side,
		TradingDay: day,
		WinFrom:    winFrom,
		Investor:   investor,
		Market:     broker.MarketRegular,
		Lot:        i64p(r.Lot),
		Value:      i64p(r.Value),
		AvgPrice:   i64p(r.AvgPrice),
		Rank:       &rank,
		Source:     source,
	}
}

func i64p(v int64) *int64 { return &v }

func deref(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func deref32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
