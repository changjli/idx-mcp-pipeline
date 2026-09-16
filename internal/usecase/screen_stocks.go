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

	// defaultFlowWindowDays is how many trading days the broker-flow column
	// reaches back — about a month, the span a stage-2 accumulation read is
	// usually made over. maxFlowWindowDays is the ceiling: stored broker rows are
	// retained 90 calendar days, so a longer ask can never be observed and is
	// rejected rather than returned structurally truncated.
	defaultFlowWindowDays = 20
	maxFlowWindowDays     = 60

	// screenerSortValue is the sort key for the anchor day's transaction value
	// and screenerSortForeignNet for the broker-flow column; any other key must be
	// one of the requested indicator keys.
	screenerSortValue      = "value"
	screenerSortForeignNet = "foreign_net"
	sortOrderAsc           = "asc"
	sortOrderDesc          = "desc"

	// coverageForeignNet is the broker-flow column's name, both as a sort key and
	// as the key it is flagged under in a row's coverage block.
	coverageForeignNet = screenerSortForeignNet
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

// ScreenerFlowSource is the stored broker-flow read behind the row's
// foreign-net column. It is a seam of its own rather than part of
// ScreenerSource because it reads the broker tables, not the price tables: the
// two repositories are separate, so a screener test can cover the flow column
// (including its sparsity) without a broker database. The concrete
// *repository.BrokerStockSummaryRepository satisfies it.
type ScreenerFlowSource interface {
	SumForeignNetByTickers(db *sqlx.DB, tickers []string, to time.Time, tradingDays int) (*repository.ForeignNetWindow, error)
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
	// FlowWindowDays is how many stored trading days the foreign-net column
	// reaches back over, ending on the anchor day; 0 uses the default.
	FlowWindowDays int
	// Indicators are registry specs for the row columns; empty uses
	// ScreenerDefaultIndicators.
	Indicators []string
	// Filters is the structural filter set (funnel stage 3). nil means the
	// caller said nothing, so ScreenerDefaultFilters runs; a non-nil empty slice
	// means the caller asked for no structural filters at all; a populated slice
	// replaces the default set entirely. The filter indicators join the columns.
	Filters *[]ScreenStocksFilter
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
	// anchor day, liquid enough, and not recently suspended.
	AfterValue int `json:"after_value"`
	// AfterStructure is how many of those cleared the structural filters. The
	// gap between it and AfterValue is exactly what the filter set removed, and
	// a row whose filter column was unobserved counts here, never silently
	// re-admitted.
	AfterStructure int `json:"after_structure"`
}

// ScreenColumnCoverage declares what a row's optional column actually observes:
// Observed is false when nothing was stored for this ticker inside the window,
// and Days is how many of the window's trading days carried a value. It is what
// keeps a missing number from reading as a zero — the column is null and its
// flag says why.
type ScreenColumnCoverage struct {
	Observed bool `json:"observed"`
	Days     int  `json:"days"`
}

// ScreenStocksRow is one survivor: the ticker, the anchor day's transaction
// value and close (what the ranking and the reader see first), its indicator
// columns, and the broker-flow column. Insufficient lists the keys whose stored
// history is shorter than their warm-up — their value is null, never a
// short-window number that would read as fully warmed.
type ScreenStocksRow struct {
	Ticker       string              `json:"ticker"`
	Value        *int64              `json:"value"`
	Close        *float64            `json:"close"`
	Values       map[string]*float64 `json:"values"`
	Insufficient []string            `json:"insufficient"`
	HistoryRows  int                 `json:"history_rows"`
	RequiredRows int                 `json:"required_rows"`
	// ForeignNet is the stored foreign net (Σ f_nval) over the flow window —
	// null when unobserved, never zero standing in for "no data". Coverage says
	// how many of the window's trading days it rests on.
	ForeignNet *int64 `json:"foreign_net"`
	// Coverage is keyed by optional column name, so a column added later lands
	// here without changing the shape a reader parses.
	Coverage map[string]ScreenColumnCoverage `json:"coverage"`
}

// ScreenStocksResponse is the structured MCP tool result. The effective
// parameters are echoed so a shortlist is reproducible from its own response,
// and the funnel says how much the filters cut.
type ScreenStocksResponse struct {
	AsOf                 string `json:"as_of"`
	Window               int    `json:"window"`
	MinValue             int64  `json:"min_value"`
	SuspensionWindowDays int    `json:"suspension_window_days"`
	// FlowWindowDays is the requested broker-flow lookback; FlowDaysInWindow is
	// how many market-wide trading days that window actually spans. The pair is
	// the denominator a row's coverage days reads against — "7 of 20" — so an
	// over-long ask shows up as a short window rather than as a thin ticker.
	FlowWindowDays   int                  `json:"flow_window_days"`
	FlowDaysInWindow int                  `json:"flow_days_in_window"`
	Indicators       []string             `json:"indicators"`
	Filters          []ScreenStocksFilter `json:"filters"`
	Sort             string               `json:"sort"`
	Order            string               `json:"order"`
	Limit            int                  `json:"limit"`
	Funnel           ScreenStocksFunnel   `json:"funnel"`
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
	// Flow is the stored broker read behind the row's foreign-net column, which
	// is always joined — the column is flagged per row, never toggled per call.
	Flow ScreenerFlowSource
}

func NewScreenStocksUseCase(db *sqlx.DB, log *logrus.Logger, source ScreenerSource, flow ScreenerFlowSource) *ScreenStocksUseCase {
	return &ScreenStocksUseCase{DB: db, Log: log, Source: source, Flow: flow}
}

// ScreenStocks validates the request, then runs the funnel in order: SQL hard
// filters over the whole universe, indicator computation and the broker-flow
// join on survivors only, the structural filter set, deterministic ranking, and
// a row cap. Validation —
// including every filter's indicator, operator, and bounds — happens before any
// read, so a bad parameter costs one round trip and returns the same structured
// error whether it arrived over MCP or from another caller.
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
	flowDays, err := resolveFlowWindowDays(req.FlowWindowDays)
	if err != nil {
		return nil, err
	}
	requests, err := parseComputeIndicators(screenerIndicatorSpecs(req.Indicators))
	if err != nil {
		return nil, err
	}
	filters := resolveScreenerFilters(req.Filters)
	compiled, err := compileScreenerFilters(filters)
	if err != nil {
		return nil, err
	}
	// The filters' indicators join the column set before the sort key is
	// resolved, so a filter's column is sortable like any other — and so the row
	// can show the number it was judged on.
	requests = unionRequests(requests, compiled)
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
	// The broker-flow column, also read for the survivors only. Its own window is
	// independent of the indicator one, and the read declares how many trading
	// days it covered so each row's day count has a denominator.
	flow, err := uc.Flow.SumForeignNetByTickers(uc.DB, tickers, *anchor, flowDays)
	if err != nil {
		return nil, fmt.Errorf("read screener foreign net: %w", err)
	}
	foreignNet := make(map[string]repository.TickerForeignNet, len(flow.Rows))
	for _, observed := range flow.Rows {
		foreignNet[observed.Ticker] = observed
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

		// An absent ticker and a ticker observed over no days are the same
		// answer — nothing stored — and both render as a null column with a
		// false flag rather than as a zero net.
		net, coverage := int64(0), ScreenColumnCoverage{}
		if observed, ok := foreignNet[candidate.Ticker]; ok {
			net, coverage = observed.ForeignNet, ScreenColumnCoverage{Observed: true, Days: observed.Days}
		}
		var netPtr *int64
		if coverage.Observed {
			netPtr = &net
		}

		rows = append(rows, ScreenStocksRow{
			Ticker:       candidate.Ticker,
			Value:        candidate.Value,
			Close:        candidate.Close,
			Values:       values,
			Insufficient: insufficient,
			HistoryRows:  series.Len(),
			RequiredRows: required,
			ForeignNet:   netPtr,
			Coverage:     map[string]ScreenColumnCoverage{coverageForeignNet: coverage},
		})
	}

	// Funnel stage 3: the structural filters, AND-combined, applied to the
	// computed columns. Everything below counts survivors of this stage.
	rows = applyStructuralFilters(rows, compiled)

	rankRows(rows, sortKey, order == sortOrderDesc)
	afterStructure := len(rows)
	totalMatches := afterStructure
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
		FlowWindowDays:       flowDays,
		FlowDaysInWindow:     flow.TradeDays,
		Indicators:           keys,
		Filters:              filters,
		Sort:                 sortKey,
		Order:                order,
		Limit:                limit,
		Funnel: ScreenStocksFunnel{
			Universe:       universe,
			AfterValue:     len(candidates),
			AfterStructure: afterStructure,
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
// transaction value for screenerSortValue, the stored foreign net for
// coverageForeignNet, otherwise the indicator column. A null value (an
// indicator short of its warm-up, or a flow window the ticker has nothing
// stored in) reports as unobserved, which is what ranks the row last.
func rowSortValue(row ScreenStocksRow, key string) (float64, bool) {
	if key == screenerSortValue {
		if row.Value == nil {
			return 0, false
		}
		return float64(*row.Value), true
	}
	if key == coverageForeignNet {
		if row.ForeignNet == nil {
			return 0, false
		}
		return float64(*row.ForeignNet), true
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
// actually computes: the anchor day's value, the always-joined broker-flow
// column, or a requested indicator key. A key that is not a column would rank
// rows by nothing, so it is rejected with the valid keys listed.
func resolveScreenerSort(sortKey string, requests []indicator.Request) (string, error) {
	keys := make([]string, 0, len(requests))
	for _, r := range requests {
		keys = append(keys, r.Key())
	}

	norm := strings.ToLower(strings.TrimSpace(sortKey))
	if norm == "" || norm == screenerSortValue {
		return screenerSortValue, nil
	}
	if norm == coverageForeignNet {
		return coverageForeignNet, nil
	}
	for _, key := range keys {
		if norm == strings.ToLower(key) {
			return key, nil
		}
	}
	return "", fmt.Errorf("%w: sort must be %s, %s, or one of the requested indicators (%s), got %q",
		ErrInvalidArgument, screenerSortValue, coverageForeignNet, strings.Join(keys, ", "), sortKey)
}

// resolveFlowWindowDays resolves the broker-flow lookback: the caller's value,
// or the default. The ceiling is the retention limit rather than a preference —
// broker rows older than it are deleted, so a longer window could only ever be
// reported as mostly unobserved, and a caller asking for it is told instead.
func resolveFlowWindowDays(days int) (int, error) {
	if days == 0 {
		return defaultFlowWindowDays, nil
	}
	if days < 1 || days > maxFlowWindowDays {
		return 0, fmt.Errorf("%w: flow_window_days must be 1..%d, got %d",
			ErrInvalidArgument, maxFlowWindowDays, days)
	}
	return days, nil
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

// compile-time checks: the concrete repositories satisfy the screener's two
// read seams — prices for the funnel, broker summaries for the flow column.
var (
	_ ScreenerSource     = (*repository.DailyPriceRepository)(nil)
	_ ScreenerFlowSource = (*repository.BrokerStockSummaryRepository)(nil)
)
