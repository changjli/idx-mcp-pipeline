package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// tickerPattern matches IDX ticker codes (2-6 uppercase letters).
var tickerPattern = regexp.MustCompile(`^[A-Z]{2,6}$`)

// BrokerSummaryFetcher fetches a per-ticker broker summary from IPOT.
// *ipot.Client satisfies this; tests use a fake.
type BrokerSummaryFetcher interface {
	Fetch(ctx context.Context, ticker string, date time.Time) (*ipot.Result, error)
}

// BrokerStockSummaryUseCase orchestrates the on-demand per-stock broker
// summary flow: resolve the trading day, fetch from IPOT, persist, return.
type BrokerStockSummaryUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	Validate       *validator.Validate
	Fetcher        BrokerSummaryFetcher
	Repo           *repository.BrokerStockSummaryRepository
	DailyPriceRepo *repository.DailyPriceRepository
}

func NewBrokerStockSummaryUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	fetcher BrokerSummaryFetcher,
	repo *repository.BrokerStockSummaryRepository,
	dailyPriceRepo *repository.DailyPriceRepository,
) *BrokerStockSummaryUseCase {
	return &BrokerStockSummaryUseCase{
		DB:             db,
		Log:            log,
		Validate:       validate,
		Fetcher:        fetcher,
		Repo:           repo,
		DailyPriceRepo: dailyPriceRepo,
	}
}

// BrokerSummaryRow is one broker's buy or sell activity in the response.
type BrokerSummaryRow struct {
	BrokerCode string `json:"broker_code"`
	Lot        int64  `json:"lot"`
	Value      int64  `json:"value"`
	AvgPrice   int64  `json:"avg_price"`
	Rank       int    `json:"rank"`
}

// BrokerSummaryTotals is the footer summary line in the response.
type BrokerSummaryTotals struct {
	TVal  int64 `json:"t_val"`
	FNVal int64 `json:"f_nval"`
	TLot  int64 `json:"t_lot"`
	Avg   int64 `json:"avg"`
}

// BrokerStockSummaryResponse is the structured MCP tool result.
// AsOf + Cause are set only when the requested date returned empty and the
// publish-lag fallback served a prior day's data (or explained the empty).
type BrokerStockSummaryResponse struct {
	Ticker     string              `json:"ticker"`
	TradingDay string              `json:"trading_day"`
	AsOf       string              `json:"as_of,omitempty"`
	Cause      string              `json:"cause,omitempty"`
	Totals     BrokerSummaryTotals `json:"totals"`
	Buyers     []BrokerSummaryRow  `json:"buyers"`
	Sellers    []BrokerSummaryRow  `json:"sellers"`
}

// BrokerStockSummaryDay is one trading day's stored top-N + totals in a
// history response.
type BrokerStockSummaryDay struct {
	TradingDay string              `json:"trading_day"`
	Totals     BrokerSummaryTotals `json:"totals"`
	Buyers     []BrokerSummaryRow  `json:"buyers"`
	Sellers    []BrokerSummaryRow  `json:"sellers"`
}

// BrokerStockSummaryHistoryResponse is the structured result of a stored
// history read over a date range.
type BrokerStockSummaryHistoryResponse struct {
	Ticker string                  `json:"ticker"`
	From   string                  `json:"from"`
	To     string                  `json:"to"`
	Days   []BrokerStockSummaryDay `json:"days"`
}

// BrokerStockSummaryRangeResponse is the result of a range/backfill run:
// per-day results plus success/failure/empty counts. Failed days (upstream
// error) and empty days (not yet published / no activity) are logged and
// excluded from Days, never aborting the range — the caller sees Fetched <
// expected and can re-run later for the missing days.
type BrokerStockSummaryRangeResponse struct {
	Ticker  string                  `json:"ticker"`
	Start   string                  `json:"start"`
	End     string                  `json:"end"`
	Fetched int                     `json:"fetched"`
	Failed  int                     `json:"failed"`
	Empty   int                     `json:"empty"`
	Days    []BrokerStockSummaryDay `json:"days"`
}

// GetStockBrokerSummary fetches the per-stock broker summary for a ticker on a
// trading day (defaulting to the ticker's latest stored trading day), persists
// it, and returns the structured top-N. An empty IPOT result (non-trading day,
// or not yet published) returns an empty response and writes nothing.
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

	d, err := uc.fetchAndPersistDay(ctx, ticker, day)
	if err != nil {
		if errors.Is(err, ErrPersist) {
			return nil, err
		}
		return nil, fmt.Errorf("ipot fetch: %w", err)
	}

	// Empty result (non-trading day, or IPOT not yet published) → publish-lag
	// fallback: probe backward for the most recent day IPOT has data for.
	if len(d.Buyers) == 0 && len(d.Sellers) == 0 {
		return uc.fallbackEmpty(ctx, ticker, day)
	}

	return &BrokerStockSummaryResponse{
		Ticker:     ticker,
		TradingDay: d.TradingDay,
		Totals:     d.Totals,
		Buyers:     d.Buyers,
		Sellers:    d.Sellers,
	}, nil
}

// brokerSummaryFallbackProbeDays is how many prior trading days the publish-lag
// fallback probes backward before giving up.
const brokerSummaryFallbackProbeDays = 5

// brokerSummaryFallbackMaxCalendarDays bounds the backward walk so a sparse
// ticker (long gaps between trading days) can't loop forever.
const brokerSummaryFallbackMaxCalendarDays = 30

// fallbackEmpty handles an empty IPOT result for the requested date. It probes
// backward up to brokerSummaryFallbackProbeDays trading days and returns the
// first non-empty day tagged with as_of + cause, so the caller is never handed
// a bare empty indistinguishable from a real zero. Nothing is written for the
// empty requested date; a found prior day is persisted normally.
func (uc *BrokerStockSummaryUseCase) fallbackEmpty(ctx context.Context, ticker string, requested time.Time) (*BrokerStockSummaryResponse, error) {
	cause := "not_yet_published"
	if !uc.isTradingDay(ticker, requested) {
		cause = "non_trading_day"
	}

	// Walk backward day by day, fetching only trading days, until the probe
	// budget (5 trading days) is spent or a non-empty day is found.
	probed := 0
	deadline := requested.AddDate(0, 0, -brokerSummaryFallbackMaxCalendarDays)
	for day := requested.AddDate(0, 0, -1); probed < brokerSummaryFallbackProbeDays && day.After(deadline); day = day.AddDate(0, 0, -1) {
		if !uc.isTradingDay(ticker, day) {
			continue
		}
		probed++
		d, err := uc.fetchAndPersistDay(ctx, ticker, day)
		if err != nil {
			uc.Log.Warnf("broker_stock_summary fallback: probe day %s failed: %v", day.Format("2006-01-02"), err)
			continue
		}
		if len(d.Buyers) > 0 || len(d.Sellers) > 0 {
			return &BrokerStockSummaryResponse{
				Ticker:     ticker,
				TradingDay: d.TradingDay,
				AsOf:       d.TradingDay,
				Cause:      cause,
				Totals:     d.Totals,
				Buyers:     d.Buyers,
				Sellers:    d.Sellers,
			}, nil
		}
	}

	// Trading day but nothing anywhere in the probe window → no activity.
	if cause == "not_yet_published" {
		cause = "no_activity"
	}
	return &BrokerStockSummaryResponse{
		Ticker:     ticker,
		TradingDay: requested.Format("2006-01-02"),
		Cause:      cause,
		Totals:     BrokerSummaryTotals{},
		Buyers:     []BrokerSummaryRow{},
		Sellers:    []BrokerSummaryRow{},
	}, nil
}

// isTradingDay reports whether the ticker has a stored EOD row for the date.
// Data presence is the trading-day signal (no separate calendar).
func (uc *BrokerStockSummaryUseCase) isTradingDay(ticker string, day time.Time) bool {
	_, err := uc.DailyPriceRepo.FindByTickerAndDay(uc.DB, ticker, day.Format("2006-01-02"))
	return err == nil
}

// GetStockBrokerSummaryRange fetches and persists the per-stock broker summary
// for a ticker over a date range, iterating trading days only (data presence
// in daily_prices is the trading-day signal — weekends/holidays have no row).
// Each day reuses the shared fetch+parse+persist core and its 1h cache. A
// partial failure (one day 5xx/parse-drift) does not abort the range: the
// failed day is logged and counted, successful days are kept.
func (uc *BrokerStockSummaryUseCase) GetStockBrokerSummaryRange(ctx context.Context, ticker string, start, end time.Time) (*BrokerStockSummaryRangeResponse, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}
	if start.After(end) {
		return nil, ErrInvalidRange
	}

	days, err := uc.DailyPriceRepo.TradingDaysInRange(uc.DB, ticker, start, end)
	if err != nil {
		return nil, fmt.Errorf("resolve trading days: %w", err)
	}

	resp := &BrokerStockSummaryRangeResponse{
		Ticker: ticker,
		Start:  start.Format("2006-01-02"),
		End:    end.Format("2006-01-02"),
		Days:   []BrokerStockSummaryDay{},
	}
	for _, day := range days {
		d, err := uc.fetchAndPersistDay(ctx, ticker, day)
		if err != nil {
			uc.Log.Warnf("broker_stock_summary range: day %s failed: %v", day.Format("2006-01-02"), err)
			resp.Failed++
			continue
		}
		if len(d.Buyers) == 0 && len(d.Sellers) == 0 {
			// Not yet published (or no activity) — not a successful store.
			uc.Log.Warnf("broker_stock_summary range: day %s empty (not yet published or no activity)", day.Format("2006-01-02"))
			resp.Empty++
			continue
		}
		resp.Days = append(resp.Days, *d)
		resp.Fetched++
	}
	return resp, nil
}

// fetchAndPersistDay fetches one ticker+day from IPOT, persists it, and returns
// the day-level result. An empty IPOT result (non-trading day or not yet
// published) returns an empty day — nothing persisted, no error.
func (uc *BrokerStockSummaryUseCase) fetchAndPersistDay(ctx context.Context, ticker string, day time.Time) (*BrokerStockSummaryDay, error) {
	res, err := uc.Fetcher.Fetch(ctx, ticker, day)
	if err != nil {
		return nil, err
	}
	if len(res.Buyers) == 0 && len(res.Sellers) == 0 {
		return &BrokerStockSummaryDay{TradingDay: day.Format("2006-01-02")}, nil
	}
	if err := uc.persist(ticker, day, res); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPersist, err)
	}
	return dayFromResult(ticker, day, res), nil
}

// GetStockBrokerSummaryHistory reads the persisted broker summary rows for a
// ticker over a date range and returns them grouped by trading day with
// per-day totals. Pure DB read — never touches the IPOT fetcher. An empty
// range returns an empty Days list, no error.
func (uc *BrokerStockSummaryUseCase) GetStockBrokerSummaryHistory(ctx context.Context, ticker string, from, to time.Time) (*BrokerStockSummaryHistoryResponse, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(ticker) {
		return nil, ErrInvalidTicker
	}
	if from.After(to) {
		return nil, ErrInvalidRange
	}

	rows, err := uc.Repo.FindByTickerAndDateRange(uc.DB, ticker, from, to)
	if err != nil {
		return nil, fmt.Errorf("read broker summary history: %w", err)
	}
	totals, err := uc.Repo.FindTotalsByTickerAndDateRange(uc.DB, ticker, from, to)
	if err != nil {
		return nil, fmt.Errorf("read broker summary totals: %w", err)
	}

	resp := &BrokerStockSummaryHistoryResponse{
		Ticker: ticker,
		From:   from.Format("2006-01-02"),
		To:     to.Format("2006-01-02"),
		Days:   []BrokerStockSummaryDay{},
	}

	// Group rows by trading day; totals keyed by day.
	totalsByDay := make(map[string]BrokerSummaryTotals, len(totals))
	for _, t := range totals {
		key := t.TradingDay.Format("2006-01-02")
		totalsByDay[key] = BrokerSummaryTotals{
			TVal:  deref(t.TVal),
			FNVal: deref(t.FNVal),
			TLot:  deref(t.TLot),
			Avg:   deref(t.Avg),
		}
	}

	dayIndex := make(map[string]int)
	for _, r := range rows {
		key := r.TradingDay.Format("2006-01-02")
		idx, ok := dayIndex[key]
		if !ok {
			idx = len(resp.Days)
			dayIndex[key] = idx
			resp.Days = append(resp.Days, BrokerStockSummaryDay{
				TradingDay: key,
				Totals:     totalsByDay[key],
				Buyers:     []BrokerSummaryRow{},
				Sellers:    []BrokerSummaryRow{},
			})
		}
		row := BrokerSummaryRow{
			BrokerCode: r.BrokerCode,
			Lot:        deref(r.Lot),
			Value:      deref(r.Value),
			AvgPrice:   deref(r.AvgPrice),
			Rank:       int(deref32(r.Rank)),
		}
		if r.Side == "buy" {
			resp.Days[idx].Buyers = append(resp.Days[idx].Buyers, row)
		} else {
			resp.Days[idx].Sellers = append(resp.Days[idx].Sellers, row)
		}
	}
	return resp, nil
}

// persist writes the fetched rows and totals for one ticker+day atomically.
func (uc *BrokerStockSummaryUseCase) persist(ticker string, day time.Time, res *ipot.Result) error {
	rows := make([]entity.BrokerStockSummary, 0, len(res.Buyers)+len(res.Sellers))
	for _, b := range res.Buyers {
		rows = append(rows, toEntity(ticker, day, "buy", b))
	}
	for _, s := range res.Sellers {
		rows = append(rows, toEntity(ticker, day, "sell", s))
	}

	totals := &entity.BrokerStockSummaryTotals{
		Ticker:     ticker,
		TradingDay: day,
		TVal:       i64p(res.Totals.TVal),
		FNVal:      i64p(res.Totals.FNVal),
		TLot:       i64p(res.Totals.TLot),
		Avg:        i64p(res.Totals.Avg),
	}
	return uc.Repo.UpsertDay(uc.DB, rows, totals)
}

func toEntity(ticker string, day time.Time, side string, r ipot.Row) entity.BrokerStockSummary {
	rank := int32(r.Rank)
	return entity.BrokerStockSummary{
		Ticker:     ticker,
		BrokerCode: r.BrokerCode,
		Side:       side,
		TradingDay: day,
		Lot:        i64p(r.Lot),
		Value:      i64p(r.Value),
		AvgPrice:   i64p(r.AvgPrice),
		Rank:       &rank,
	}
}

// dayFromResult builds a day-level result from a fetched IPOT result.
func dayFromResult(ticker string, day time.Time, res *ipot.Result) *BrokerStockSummaryDay {
	d := &BrokerStockSummaryDay{
		TradingDay: day.Format("2006-01-02"),
		Totals: BrokerSummaryTotals{
			TVal:  res.Totals.TVal,
			FNVal: res.Totals.FNVal,
			TLot:  res.Totals.TLot,
			Avg:   res.Totals.Avg,
		},
		Buyers:  make([]BrokerSummaryRow, 0, len(res.Buyers)),
		Sellers: make([]BrokerSummaryRow, 0, len(res.Sellers)),
	}
	for _, b := range res.Buyers {
		d.Buyers = append(d.Buyers, BrokerSummaryRow{
			BrokerCode: b.BrokerCode,
			Lot:        b.Lot,
			Value:      b.Value,
			AvgPrice:   b.AvgPrice,
			Rank:       b.Rank,
		})
	}
	for _, s := range res.Sellers {
		d.Sellers = append(d.Sellers, BrokerSummaryRow{
			BrokerCode: s.BrokerCode,
			Lot:        s.Lot,
			Value:      s.Value,
			AvgPrice:   s.AvgPrice,
			Rank:       s.Rank,
		})
	}
	return d
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
