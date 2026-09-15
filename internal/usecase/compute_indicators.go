package usecase

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/indicator"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Compute-indicator modes. Screen mode compares many tickers at their latest
// values; series mode reads one ticker's trajectory, which is what a stage-2
// deep-dive needs ("is RSI rising, was the volume spike one day or three").
const (
	computeModeScreen = "screen"
	computeModeSeries = "series"
)

// maxScreenTickers caps one screen-mode call. Screen rows are wide (one column
// per requested indicator), so the cap keeps a response inside an AI's context;
// the tool description states it, and an over-cap call is rejected rather than
// truncated — a silently shortened ticker list would read as "these are all the
// matches". Series mode is one ticker by definition, whatever the cap.
const maxScreenTickers = 50

// window bounds (trading days of stored history loaded per ticker). The
// default covers MA200 warm-up with room to spare; the ceiling keeps a
// 50-ticker call from reading the whole retention window.
const (
	defaultIndicatorWindow = 250
	minIndicatorWindow     = 2
	maxIndicatorWindow     = 500
)

// ComputeIndicatorsReader is the read surface the MCP server depends on, so its
// handler tests can wire a fake and run without a DB (TickerMetadataReader
// precedent).
type ComputeIndicatorsReader interface {
	ComputeIndicators(ctx context.Context, req ComputeIndicatorsRequest) (*ComputeIndicatorsResponse, error)
}

// DailyPriceSeriesReader is the price seam the usecase reads through. The
// concrete *repository.DailyPriceRepository satisfies it; usecase tests wire a
// fake over stored rows (spec Test Seam 2 — no DB-verify tests for this
// feature).
type DailyPriceSeriesReader interface {
	FindByTickerUpTo(db *sqlx.DB, ticker string, to time.Time, limit int) ([]entity.DailyPrice, error)
	LatestTradingDayAll(db *sqlx.DB) (*time.Time, error)
}

// ComputeIndicatorsRequest is a parsed caller request. Mode screen returns the
// latest value per indicator, one row per ticker; mode series returns one
// ticker's full arrays over the window.
type ComputeIndicatorsRequest struct {
	Mode       string
	Tickers    []string
	Indicators []string // specs, e.g. "sma:20", "rsi" (entry default period)
	AsOf       *time.Time
	Window     int // trading days of history; 0 = default
}

// ComputeIndicatorsRow is one ticker's latest values. Values is keyed by the
// canonical indicator key ("sma:20"); a key whose series is shorter than its
// warm-up requirement is null AND listed in Insufficient — never a value
// computed on a short window, which would read as fully warmed.
type ComputeIndicatorsRow struct {
	Ticker       string              `json:"ticker"`
	Values       map[string]*float64 `json:"values"`
	Insufficient []string            `json:"insufficient"`
	// HistoryRows is the number of usable closes the values are based on (rows
	// with a stored close inside the window).
	HistoryRows int `json:"history_rows"`
	// RequiredRows is the binding warm-up bar across the requested indicators —
	// the longest lookback asked for, so a row's history can be judged against
	// one number.
	RequiredRows int `json:"required_rows"`
}

// ComputeIndicatorsSeriesEntry is one indicator's trajectory over the window:
// Values is parallel to the response's Dates, so index i is that trading day's
// value, or null where the indicator has none — inside its warm-up, on a day
// whose stored row lacked a column the formula reads, or where the formula
// itself yields no number (a flat range).
//
// RequiredRows is the warm-up bar and ObservedRows how many days in the window
// carry a value, so the basis is declared rather than implied. Insufficient
// means the history never cleared the bar (the whole array is null), which is
// the same rule screen mode flags — a series is never computed on a short
// window and reported as warmed. It reads false on a sufficient series that
// happens to have no value anywhere (a flat range every day), so the reason for
// an empty array stays readable from RequiredRows and ObservedRows.
type ComputeIndicatorsSeriesEntry struct {
	Key          string     `json:"key"`
	Values       []*float64 `json:"values"`
	ObservedRows int        `json:"observed_rows"`
	RequiredRows int        `json:"required_rows"`
	Insufficient bool       `json:"insufficient"`
}

// ComputeIndicatorsResponse is the structured MCP tool result. AsOf is the
// trading day the values anchor to (the anchor is shared by every row of a
// screen call, so a multi-ticker comparison is same-day by construction).
//
// The two modes fill different halves: screen mode fills Count and Rows,
// series mode fills Ticker, Dates, HistoryRows and Series. The empty half is
// omitted from the JSON rather than sent as an empty list, so a reader can tell
// "this mode does not carry rows" from "this call found none".
type ComputeIndicatorsResponse struct {
	Mode       string   `json:"mode"`
	AsOf       string   `json:"as_of"`
	Window     int      `json:"window"`
	Indicators []string `json:"indicators"`

	// Screen mode: one row per ticker, latest values only.
	Count int                    `json:"count,omitempty"`
	Rows  []ComputeIndicatorsRow `json:"rows,omitempty"`

	// Series mode: exactly one ticker, full per-day arrays over the window.
	Ticker string `json:"ticker,omitempty"`
	// Dates are the trading days the arrays run over, ascending — the axis every
	// Values array in Series is parallel to. It holds the stored days the values
	// rest on, so a gap in the history shows as a shorter axis, not as padding.
	Dates       []string                       `json:"dates,omitempty"`
	HistoryRows int                            `json:"history_rows,omitempty"`
	Series      []ComputeIndicatorsSeriesEntry `json:"series,omitempty"`
}

// ComputeIndicatorsUseCase computes registered indicators over stored Daily
// Price rows — pure DB read, no upstream call, no persist (Heroku H12
// constraint, ADR-0009). The math lives in internal/indicator; this layer
// validates the request, reads the window, and shapes the response.
type ComputeIndicatorsUseCase struct {
	DB        *sqlx.DB
	Log       *logrus.Logger
	PriceRepo DailyPriceSeriesReader
}

func NewComputeIndicatorsUseCase(db *sqlx.DB, log *logrus.Logger, priceRepo DailyPriceSeriesReader) *ComputeIndicatorsUseCase {
	return &ComputeIndicatorsUseCase{DB: db, Log: log, PriceRepo: priceRepo}
}

// ComputeIndicators resolves the request into the shape its mode asks for: one
// latest-value row per ticker (screen) or one ticker's full arrays (series).
// Validation happens before any read: an unknown indicator name, bad period, or
// a mode/ticker combination that cannot be served is a structured error listing
// the registry's valid names.
func (uc *ComputeIndicatorsUseCase) ComputeIndicators(ctx context.Context, req ComputeIndicatorsRequest) (*ComputeIndicatorsResponse, error) {
	mode, err := resolveComputeMode(req.Mode)
	if err != nil {
		return nil, err
	}
	tickers, err := normalizeComputeTickers(mode, req.Tickers)
	if err != nil {
		return nil, err
	}
	requests, err := parseComputeIndicators(req.Indicators)
	if err != nil {
		return nil, err
	}
	window, err := resolveIndicatorWindow(req.Window)
	if err != nil {
		return nil, err
	}

	anchor, err := uc.anchorDay(req.AsOf)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(requests))
	for _, r := range requests {
		keys = append(keys, r.Key())
	}

	base := &ComputeIndicatorsResponse{
		Mode:       mode,
		AsOf:       anchor.Format("2006-01-02"),
		Window:     window,
		Indicators: keys,
	}
	if mode == computeModeSeries {
		return uc.seriesResponse(base, tickers[0], requests, anchor, window)
	}
	return uc.screenResponse(base, tickers, requests, anchor, window)
}

// screenResponse reads one window per ticker and reports each one's latest
// values — the compact shape a multi-ticker comparison needs.
func (uc *ComputeIndicatorsUseCase) screenResponse(base *ComputeIndicatorsResponse, tickers []string, requests []indicator.Request, anchor *time.Time, window int) (*ComputeIndicatorsResponse, error) {
	required := bindingRequiredRows(requests)

	rows := make([]ComputeIndicatorsRow, 0, len(tickers))
	for _, ticker := range tickers {
		prices, err := uc.PriceRepo.FindByTickerUpTo(uc.DB, ticker, *anchor, window)
		if err != nil {
			return nil, fmt.Errorf("read daily prices for %s: %w", ticker, err)
		}
		series, _ := usableSeries(prices)

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

		rows = append(rows, ComputeIndicatorsRow{
			Ticker:       ticker,
			Values:       values,
			Insufficient: insufficient,
			HistoryRows:  series.Len(),
			RequiredRows: required,
		})
	}

	base.Count = len(rows)
	base.Rows = rows
	return base, nil
}

// seriesResponse reads the one requested ticker's window and reports every
// indicator as a full array over it, so the caller can read trajectory instead
// of a single point. The arrays share one date axis (the stored trading days of
// the window) and each carries its own warm-up basis.
func (uc *ComputeIndicatorsUseCase) seriesResponse(base *ComputeIndicatorsResponse, ticker string, requests []indicator.Request, anchor *time.Time, window int) (*ComputeIndicatorsResponse, error) {
	prices, err := uc.PriceRepo.FindByTickerUpTo(uc.DB, ticker, *anchor, window)
	if err != nil {
		return nil, fmt.Errorf("read daily prices for %s: %w", ticker, err)
	}
	series, days := usableSeries(prices)

	base.Ticker = ticker
	base.HistoryRows = series.Len()
	base.Dates = make([]string, 0, len(days))
	for _, day := range days {
		base.Dates = append(base.Dates, day.Format("2006-01-02"))
	}

	base.Series = make([]ComputeIndicatorsSeriesEntry, 0, len(requests))
	for _, r := range requests {
		evaluated := indicator.Evaluate(r, series)
		values, observed := observedValues(evaluated.Array)
		base.Series = append(base.Series, ComputeIndicatorsSeriesEntry{
			Key:          r.Key(),
			Values:       values,
			ObservedRows: observed,
			RequiredRows: evaluated.RequiredRows,
			Insufficient: !evaluated.Sufficient(),
		})
	}
	return base, nil
}

// anchorDay resolves the trading day the values anchor to: the caller's asOf,
// or the latest stored market-wide trading day. Market-wide (not per-ticker)
// keeps a multi-ticker comparison same-day — a halted ticker reads as
// insufficient history rather than as a value from a different day.
func (uc *ComputeIndicatorsUseCase) anchorDay(asOf *time.Time) (*time.Time, error) {
	if asOf != nil {
		return asOf, nil
	}
	day, err := uc.PriceRepo.LatestTradingDayAll(uc.DB)
	if err != nil {
		return nil, fmt.Errorf("resolve latest trading day: %w", err)
	}
	if day == nil {
		return nil, fmt.Errorf("%w: no stored trading day to anchor indicator values on", ErrNotFound)
	}
	return day, nil
}

// resolveComputeMode validates the explicit mode parameter.
func resolveComputeMode(mode string) (string, error) {
	switch norm := strings.ToLower(strings.TrimSpace(mode)); norm {
	case "", computeModeScreen:
		return computeModeScreen, nil
	case computeModeSeries:
		return computeModeSeries, nil
	default:
		return "", fmt.Errorf("%w: mode must be %s or %s, got %q",
			ErrInvalidArgument, computeModeScreen, computeModeSeries, mode)
	}
}

// normalizeComputeTickers validates and normalizes the ticker list, preserving
// the caller's order (the response rows follow the request) and dropping
// duplicates so one ticker never costs two windows.
//
// Series mode serves exactly one ticker, so a multi-ticker series request is
// rejected naming that rule — never silently narrowed to the first ticker,
// which would answer a question the caller did not ask.
func normalizeComputeTickers(mode string, tickers []string) ([]string, error) {
	if len(tickers) == 0 {
		return nil, fmt.Errorf("%w: at least one ticker is required", ErrInvalidArgument)
	}
	if len(tickers) > maxScreenTickers {
		return nil, fmt.Errorf("%w: at most %d tickers per call, got %d",
			ErrInvalidArgument, maxScreenTickers, len(tickers))
	}

	seen := make(map[string]bool, len(tickers))
	normalized := make([]string, 0, len(tickers))
	for _, ticker := range tickers {
		norm := strings.ToUpper(strings.TrimSpace(ticker))
		norm = strings.TrimSuffix(norm, ".JK")
		if !tickerPattern.MatchString(norm) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidTicker, ticker)
		}
		if seen[norm] {
			continue
		}
		seen[norm] = true
		normalized = append(normalized, norm)
	}

	if mode == computeModeSeries && len(normalized) > 1 {
		return nil, fmt.Errorf("%w: mode %s reads exactly one ticker, got %d (%s)",
			ErrInvalidArgument, computeModeSeries, len(normalized), strings.Join(normalized, ", "))
	}
	return normalized, nil
}

// parseComputeIndicators parses the registry specs, wrapping the registry's own
// validation error so its enumeration of valid names reaches the caller through
// the INVALID_ARGUMENT envelope.
func parseComputeIndicators(specs []string) ([]indicator.Request, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("%w: at least one indicator is required; valid: %s",
			ErrInvalidArgument, strings.Join(indicator.Names(), ", "))
	}

	requests := make([]indicator.Request, 0, len(specs))
	seen := make(map[string]bool, len(specs))
	for _, spec := range specs {
		req, err := indicator.Parse(spec)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, err)
		}
		if seen[req.Key()] {
			continue
		}
		seen[req.Key()] = true
		requests = append(requests, req)
	}
	return requests, nil
}

// resolveIndicatorWindow validates the trading-day window.
func resolveIndicatorWindow(window int) (int, error) {
	if window == 0 {
		return defaultIndicatorWindow, nil
	}
	if window < minIndicatorWindow || window > maxIndicatorWindow {
		return 0, fmt.Errorf("%w: window must be %d..%d trading days, got %d",
			ErrInvalidArgument, minIndicatorWindow, maxIndicatorWindow, window)
	}
	return window, nil
}

// bindingRequiredRows is the longest warm-up bar across the requested
// indicators — the one number a row's history can be judged against.
func bindingRequiredRows(requests []indicator.Request) int {
	required := 0
	for _, r := range requests {
		if rows := indicator.RequiredRows(r); rows > required {
			required = rows
		}
	}
	return required
}

// usableSeries builds the registry's compute input from stored rows, ascending,
// and returns the trading day of each row it kept, parallel to the series — the
// axis a series-mode response zips its values against.
//
// A row with no stored close is dropped — every registry entry reads close, so
// such a row can carry no value — which also keeps history_rows counting the
// same usable closes it always has. A missing high, low, or volume rides as NaN
// rather than as a zero: a zero would read as a real (and maximally bearish)
// observation, whereas the registry drops that row for the entries that need
// the column, leaving a shorter series and an insufficient flag.
func usableSeries(prices []entity.DailyPrice) (indicator.Series, []time.Time) {
	highs := make([]float64, 0, len(prices))
	lows := make([]float64, 0, len(prices))
	closes := make([]float64, 0, len(prices))
	volumes := make([]float64, 0, len(prices))
	days := make([]time.Time, 0, len(prices))
	for _, p := range prices {
		if p.Close == nil {
			continue
		}
		highs = append(highs, missingPrice(p.High))
		lows = append(lows, missingPrice(p.Low))
		closes = append(closes, *p.Close)
		volumes = append(volumes, missingVolume(p.Volume))
		days = append(days, p.TradingDay)
	}
	return indicator.Series{High: highs, Low: lows, Close: closes, Volume: volumes}, days
}

// observedValues turns the registry's array into the response shape: a value
// per row as a nullable pointer, with every row the registry left empty (NaN or
// an infinity) null. JSON cannot carry a NaN, and a zero would read as a real
// observation, so "no value" has exactly one spelling in the response.
// observed counts the rows that carry one.
func observedValues(values []float64) ([]*float64, int) {
	out := make([]*float64, 0, len(values))
	observed := 0
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			out = append(out, nil)
			continue
		}
		v := value
		out = append(out, &v)
		observed++
	}
	return out, observed
}

// missingPrice is the NaN a price column carries when the row did not store it.
func missingPrice(value *float64) float64 {
	if value == nil {
		return math.NaN()
	}
	return *value
}

// missingVolume is the same for volume, which is stored as an integer count.
func missingVolume(value *int64) float64 {
	if value == nil {
		return math.NaN()
	}
	return float64(*value)
}

// compile-time check: the concrete repository satisfies the series seam.
var _ DailyPriceSeriesReader = (*repository.DailyPriceRepository)(nil)
