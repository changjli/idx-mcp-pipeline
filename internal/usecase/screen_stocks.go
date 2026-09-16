package usecase

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/indicator"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Screener defaults and bounds. Every one of them is a call parameter: the
// funnel's shape is tuned per session, never by editing code.
const (
	// defaultScreenerMinValue is the liquidity floor, Rp 5bn of daily
	// transaction value — the same bar the pipeline's anomaly detector uses as
	// its ADTV minimum (pipeline.DefaultADTVMinValue), so "liquid" means one
	// thing across the pipeline. It is compared against the anchor day's stored
	// value, not an average: the screen asks whether the stock traded today.
	defaultScreenerMinValue int64 = 5_000_000_000
	// maxScreenerMinValue is a sanity ceiling, far above any real day's value
	// on IDX, so an out-of-range floor is rejected rather than silently
	// emptying the funnel.
	maxScreenerMinValue int64 = 1_000_000_000_000_000

	// defaultScreenerLimit caps the returned rows; maxScreenerLimit is the
	// stated ceiling (an over-cap call is rejected, never silently truncated —
	// the compute_indicators rule).
	defaultScreenerLimit = 30
	maxScreenerLimit     = 50

	// defaultSuspensionWindowDays is how many trading days back a
	// suspension-related event still disqualifies a ticker.
	defaultSuspensionWindowDays = 5
	maxSuspensionWindowDays     = 60

	// screenerSortValue is the sort key for the anchor day's transaction value;
	// any other key must be one of the requested indicator keys.
	screenerSortValue = "value"
	sortOrderAsc      = "asc"
	sortOrderDesc     = "desc"
)

// ScreenerDefaultIndicators is the stage-1 column set (spec story 17): what a
// shortlist needs to be usable without follow-up reads — where price sits
// against its moving averages, where it sits in its range, whether volume is
// contracting, and momentum. Exported so the MCP tool description names exactly
// the columns the usecase computes, rather than paraphrasing them.
var ScreenerDefaultIndicators = []string{"ma_distance:20:50", "range_position:60", "volume_ratio:20", "rsi:14"}

// ScreenStocksReader is the read surface the MCP server depends on, so its
// handler tests can wire a fake and run without a DB (ComputeIndicatorsReader
// precedent).
type ScreenStocksReader interface {
	ScreenStocks(ctx context.Context, req ScreenStocksRequest) (*ScreenStocksResponse, error)
}

// ScreenerSource is the read source the screener funnels through: the SQL hard
// filters, the survivor price windows, and the anchor day. The concrete
// *repository.DailyPriceRepository satisfies it; usecase tests wire a fake over
// stored rows (spec Test Seam 2 — no DB-verify tests for this feature).
type ScreenerSource interface {
	ScreenUniverse(db *sqlx.DB, anchor time.Time, window int) (int, error)
	ScreenCandidates(db *sqlx.DB, anchor time.Time, minValue int64, suspensionDays int) ([]repository.ScreenCandidate, error)
	FindByTickersUpTo(db *sqlx.DB, tickers []string, to time.Time, limit int) ([]entity.DailyPrice, error)
	LatestTradingDayAll(db *sqlx.DB) (*time.Time, error)
}

// ScreenStocksRequest is a parsed caller request. Every field is optional: the
// zero value screens with the shipped defaults.
type ScreenStocksRequest struct {
	AsOf *time.Time
	// Window is the lookback in stored trading days: the population the funnel
	// counts, and the history each survivor's indicators are computed over.
	Window int
	// MinValue is the anchor day's minimum transaction value; nil uses the
	// default floor, and 0 is a legitimate "no floor" (a ticker with no stored
	// value is still excluded — unknown liquidity is not liquidity).
	MinValue *int64
	// Limit caps returned rows; 0 uses the default.
	Limit int
	// SuspensionWindowDays is how many trading days back a suspension-related
	// event disqualifies a ticker; 0 uses the default.
	SuspensionWindowDays int
	// Indicators are registry specs for the row columns; empty uses
	// ScreenerDefaultIndicators.
	Indicators []string
	// Sort is screenerSortValue (default) or one of the requested indicator keys.
	Sort string
	// Order is "desc" (default) or "asc".
	Order string
}

// ScreenStocksFunnel is the mandatory per-stage count block: a filter set that
// keeps everything or nothing is visible here rather than guessed at from the
// shortlist.
type ScreenStocksFunnel struct {
	// Universe is every ticker with a stored row inside the window.
	Universe int `json:"universe"`
	// AfterValue is how many cleared the hard filters — still trading on the
	// anchor day, liquid enough, and not recently suspended. Ticket 05 adds the
	// structural stage after this one.
	AfterValue int `json:"after_value"`
}

// ScreenStocksRow is one survivor: the ticker, the anchor day's transaction
// value and close (what the ranking and the reader see first), and its
// indicator columns. Insufficient lists the keys whose stored history is
// shorter than their warm-up — their value is null, never a short-window number
// that would read as fully warmed.
type ScreenStocksRow struct {
	Ticker       string              `json:"ticker"`
	Value        *int64              `json:"value"`
	Close        *float64            `json:"close"`
	Values       map[string]*float64 `json:"values"`
	Insufficient []string            `json:"insufficient"`
	HistoryRows  int                 `json:"history_rows"`
	RequiredRows int                 `json:"required_rows"`
}

// ScreenStocksResponse is the structured MCP tool result. The effective
// parameters are echoed so a shortlist is reproducible from its own response,
// and the funnel says how much the filters cut.
type ScreenStocksResponse struct {
	AsOf                 string             `json:"as_of"`
	Window               int                `json:"window"`
	MinValue             int64              `json:"min_value"`
	SuspensionWindowDays int                `json:"suspension_window_days"`
	Indicators           []string           `json:"indicators"`
	Sort                 string             `json:"sort"`
	Order                string             `json:"order"`
	Limit                int                `json:"limit"`
	Funnel               ScreenStocksFunnel `json:"funnel"`
	// TotalMatches is how many tickers passed every filter; Count is how many
	// are returned after the row cap. TotalMatches > Count is the cut.
	TotalMatches int               `json:"total_matches"`
	Count        int               `json:"count"`
	Rows         []ScreenStocksRow `json:"rows"`
}

// ScreenStocksUseCase runs the whole-universe funnel — the deterministic stage
// 1 of the daily flow: one call turns the persisted market into a ranked
// shortlist. Pure DB read, no upstream call, no persist (Heroku H12
// constraint, ADR-0009). The hard filters run in SQL, so the expensive part
// (per-ticker indicator computation) only ever touches survivors.
type ScreenStocksUseCase struct {
	DB     *sqlx.DB
	Log    *logrus.Logger
	Source ScreenerSource
}

func NewScreenStocksUseCase(db *sqlx.DB, log *logrus.Logger, source ScreenerSource) *ScreenStocksUseCase {
	return &ScreenStocksUseCase{DB: db, Log: log, Source: source}
}

// ScreenStocks validates the request, then runs the funnel in order: SQL hard
// filters over the whole universe, indicator computation on survivors only,
// deterministic ranking, and a row cap. Validation happens before any read, so
// a bad parameter costs one round trip and returns the same structured error
// whether it arrived over MCP or from another caller.
func (uc *ScreenStocksUseCase) ScreenStocks(ctx context.Context, req ScreenStocksRequest) (*ScreenStocksResponse, error) {
	window, err := resolveIndicatorWindow(req.Window)
	if err != nil {
		return nil, err
	}
	minValue, err := resolveScreenerMinValue(req.MinValue)
	if err != nil {
		return nil, err
	}
	suspensionDays, err := resolveSuspensionWindowDays(req.SuspensionWindowDays)
	if err != nil {
		return nil, err
	}
	limit, err := resolveScreenerLimit(req.Limit)
	if err != nil {
		return nil, err
	}
	requests, err := parseComputeIndicators(screenerIndicatorSpecs(req.Indicators))
	if err != nil {
		return nil, err
	}
	sortKey, err := resolveScreenerSort(req.Sort, requests)
	if err != nil {
		return nil, err
	}
	order, err := resolveScreenerOrder(req.Order)
	if err != nil {
		return nil, err
	}

	anchor, err := resolveAnchorDay(uc.Source, uc.DB, req.AsOf)
	if err != nil {
		return nil, err
	}

	universe, err := uc.Source.ScreenUniverse(uc.DB, *anchor, window)
	if err != nil {
		return nil, fmt.Errorf("count screener universe: %w", err)
	}

	candidates, err := uc.Source.ScreenCandidates(uc.DB, *anchor, minValue, suspensionDays)
	if err != nil {
		return nil, fmt.Errorf("read screener candidates: %w", err)
	}

	// Survivors only: the price windows are read after the hard filters, so the
	// universe's history is never loaded to answer a screen that discards it.
	tickers := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		tickers = append(tickers, candidate.Ticker)
	}
	prices, err := uc.Source.FindByTickersUpTo(uc.DB, tickers, *anchor, window)
	if err != nil {
		return nil, fmt.Errorf("read survivor prices: %w", err)
	}
	windows := groupPricesByTicker(prices)
	required := bindingRequiredRows(requests)

	rows := make([]ScreenStocksRow, 0, len(candidates))
	for _, candidate := range candidates {
		series, _ := usableSeries(windows[candidate.Ticker])

		values := make(map[string]*float64, len(requests))
		insufficient := []string{}
		for _, r := range requests {
			value, ok := indicator.Value(r, series)
			if !ok {
				values[r.Key()] = nil
				insufficient = append(insufficient, r.Key())
				continue
			}
			v := value
			values[r.Key()] = &v
		}

		rows = append(rows, ScreenStocksRow{
			Ticker:       candidate.Ticker,
			Value:        candidate.Value,
			Close:        candidate.Close,
			Values:       values,
			Insufficient: insufficient,
			HistoryRows:  series.Len(),
			RequiredRows: required,
		})
	}

	rankRows(rows, sortKey, order == sortOrderDesc)
	totalMatches := len(rows)
	if totalMatches > limit {
		rows = rows[:limit]
	}

	keys := make([]string, 0, len(requests))
	for _, r := range requests {
		keys = append(keys, r.Key())
	}

	return &ScreenStocksResponse{
		AsOf:                 anchor.Format("2006-01-02"),
		Window:               window,
		MinValue:             minValue,
		SuspensionWindowDays: suspensionDays,
		Indicators:           keys,
		Sort:                 sortKey,
		Order:                order,
		Limit:                limit,
		Funnel: ScreenStocksFunnel{
			Universe:   universe,
			AfterValue: len(candidates),
		},
		TotalMatches: totalMatches,
		Count:        len(rows),
		Rows:         rows,
	}, nil
}

// rankRows orders the shortlist deterministically: by the sort key first, with
// unobserved values last in either direction (a row that has no value for the
// key has no claim to the top of a ranking it cannot compete in), then by the
// anchor day's transaction value descending, then by ticker ascending. The
// final two are what make identical inputs produce identical order — never a
// map iteration or a database's incidental row order showing through.
func rankRows(rows []ScreenStocksRow, sortKey string, desc bool) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]

		aValue, aOK := rowSortValue(a, sortKey)
		bValue, bOK := rowSortValue(b, sortKey)
		if aOK != bOK {
			return aOK
		}
		if aOK && aValue != bValue {
			if desc {
				return aValue > bValue
			}
			return aValue < bValue
		}

		if c := compareValueDesc(a.Value, b.Value); c != 0 {
			return c > 0
		}
		return a.Ticker < b.Ticker
	})
}

// rowSortValue reads a row's value for the sort key: the anchor day's
// transaction value for screenerSortValue, otherwise the indicator column. A
// null value (an indicator short of its warm-up) reports as unobserved.
func rowSortValue(row ScreenStocksRow, key string) (float64, bool) {
	if key == screenerSortValue {
		if row.Value == nil {
			return 0, false
		}
		return float64(*row.Value), true
	}
	value, ok := row.Values[key]
	if !ok || value == nil {
		return 0, false
	}
	return *value, true
}

// compareValueDesc compares two anchor-day values with a missing value ranked
// last in both directions.
func compareValueDesc(a, b *int64) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	case *a > *b:
		return 1
	case *a < *b:
		return -1
	default:
		return 0
	}
}

// groupPricesByTicker buckets the survivor read per ticker, keeping the
// repository's ascending-by-trading-day order inside each bucket — the order
// the registry computes over.
func groupPricesByTicker(prices []entity.DailyPrice) map[string][]entity.DailyPrice {
	byTicker := make(map[string][]entity.DailyPrice, len(prices))
	for _, price := range prices {
		byTicker[price.Ticker] = append(byTicker[price.Ticker], price)
	}
	return byTicker
}

// screenerIndicatorSpecs applies the default column set when the caller named
// no columns, so the one-call shortlist of the spec's stage 1 needs no
// follow-up read to be useful.
func screenerIndicatorSpecs(specs []string) []string {
	if len(specs) == 0 {
		return ScreenerDefaultIndicators
	}
	return specs
}

// resolveScreenerMinValue resolves the liquidity floor: the caller's value, or
// the default. Zero is allowed (no floor) but never negative, and never so
// large it could only be a mistake.
func resolveScreenerMinValue(minValue *int64) (int64, error) {
	if minValue == nil {
		return defaultScreenerMinValue, nil
	}
	if *minValue < 0 || *minValue > maxScreenerMinValue {
		return 0, fmt.Errorf("%w: min_value must be 0..%d, got %d",
			ErrInvalidArgument, maxScreenerMinValue, *minValue)
	}
	return *minValue, nil
}

// resolveSuspensionWindowDays resolves how far back a suspension-related event
// disqualifies a ticker. The window is counted in trading days by the SQL, so
// weekends and holidays never shorten it.
func resolveSuspensionWindowDays(days int) (int, error) {
	if days == 0 {
		return defaultSuspensionWindowDays, nil
	}
	if days < 1 || days > maxSuspensionWindowDays {
		return 0, fmt.Errorf("%w: suspension_window_days must be 1..%d, got %d",
			ErrInvalidArgument, maxSuspensionWindowDays, days)
	}
	return days, nil
}

// resolveScreenerLimit validates the row cap. An over-cap request is rejected
// rather than clamped: a caller asking for more rows than the ceiling must know
// it got fewer.
func resolveScreenerLimit(limit int) (int, error) {
	if limit == 0 {
		return defaultScreenerLimit, nil
	}
	if limit < 1 || limit > maxScreenerLimit {
		return 0, fmt.Errorf("%w: limit must be 1..%d, got %d",
			ErrInvalidArgument, maxScreenerLimit, limit)
	}
	return limit, nil
}

// resolveScreenerSort validates the sort key against the columns this call
// actually computes: the anchor day's value, or a requested indicator key. A
// key that is not a column would rank rows by nothing, so it is rejected with
// the valid keys listed.
func resolveScreenerSort(sortKey string, requests []indicator.Request) (string, error) {
	keys := make([]string, 0, len(requests))
	for _, r := range requests {
		keys = append(keys, r.Key())
	}

	norm := strings.ToLower(strings.TrimSpace(sortKey))
	if norm == "" || norm == screenerSortValue {
		return screenerSortValue, nil
	}
	for _, key := range keys {
		if norm == strings.ToLower(key) {
			return key, nil
		}
	}
	return "", fmt.Errorf("%w: sort must be %s or one of the requested indicators (%s), got %q",
		ErrInvalidArgument, screenerSortValue, strings.Join(keys, ", "), sortKey)
}

// resolveScreenerOrder validates the sort direction.
func resolveScreenerOrder(order string) (string, error) {
	switch norm := strings.ToLower(strings.TrimSpace(order)); norm {
	case "", sortOrderDesc:
		return sortOrderDesc, nil
	case sortOrderAsc:
		return sortOrderAsc, nil
	default:
		return "", fmt.Errorf("%w: order must be %s or %s, got %q",
			ErrInvalidArgument, sortOrderAsc, sortOrderDesc, order)
	}
}

// compile-time check: the concrete repository satisfies the screener source.
var _ ScreenerSource = (*repository.DailyPriceRepository)(nil)
