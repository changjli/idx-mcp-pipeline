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

// Compute-indicator modes. Ticket 01 ships screen mode; series mode is the
// ticket-03 follow-up and is rejected explicitly rather than half-implemented.
const (
	computeModeScreen = "screen"
)

// maxScreenTickers caps one screen-mode call. Screen rows are wide (one column
// per requested indicator), so the cap keeps a response inside an AI's context;
// the tool description states it, and an over-cap call is rejected rather than
// truncated — a silently shortened ticker list would read as "these are all the
// matches".
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

// ComputeIndicatorsRequest is a parsed caller request. Mode screen (the only
// mode in ticket 01) returns the latest value per indicator, one row per
// ticker; Series* fields land with ticket 03.
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

// ComputeIndicatorsResponse is the structured MCP tool result. AsOf is the
// trading day the latest values anchor to (the anchor is shared by every row,
// so a multi-ticker comparison is same-day by construction).
type ComputeIndicatorsResponse struct {
	Mode       string                 `json:"mode"`
	AsOf       string                 `json:"as_of"`
	Window     int                    `json:"window"`
	Indicators []string               `json:"indicators"`
	Count      int                    `json:"count"`
	Rows       []ComputeIndicatorsRow `json:"rows"`
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

// ComputeIndicators resolves the request into one row per ticker. Validation
// happens before any read: an unknown indicator name or bad period is a
// structured error enumerating the registry's valid names.
func (uc *ComputeIndicatorsUseCase) ComputeIndicators(ctx context.Context, req ComputeIndicatorsRequest) (*ComputeIndicatorsResponse, error) {
	mode, err := resolveComputeMode(req.Mode)
	if err != nil {
		return nil, err
	}
	tickers, err := normalizeComputeTickers(req.Tickers)
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

	required := bindingRequiredRows(requests)
	keys := make([]string, 0, len(requests))
	for _, r := range requests {
		keys = append(keys, r.Key())
	}

	rows := make([]ComputeIndicatorsRow, 0, len(tickers))
	for _, ticker := range tickers {
		prices, err := uc.PriceRepo.FindByTickerUpTo(uc.DB, ticker, *anchor, window)
		if err != nil {
			return nil, fmt.Errorf("read daily prices for %s: %w", ticker, err)
		}
		series := usableSeries(prices)

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

	return &ComputeIndicatorsResponse{
		Mode:       mode,
		AsOf:       anchor.Format("2006-01-02"),
		Window:     window,
		Indicators: keys,
		Count:      len(rows),
		Rows:       rows,
	}, nil
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
	case "series":
		return "", fmt.Errorf("%w: mode series is not implemented yet; use screen", ErrInvalidArgument)
	default:
		return "", fmt.Errorf("%w: mode must be screen, got %q", ErrInvalidArgument, mode)
	}
}

// normalizeComputeTickers validates and normalizes the ticker list, preserving
// the caller's order (the response rows follow the request) and dropping
// duplicates so one ticker never costs two windows.
func normalizeComputeTickers(tickers []string) ([]string, error) {
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

// usableSeries builds the registry's compute input from stored rows, ascending.
// A row with no stored close is dropped — every registry entry reads close, so
// such a row can carry no value — which also keeps history_rows counting the
// same usable closes it always has. A missing high, low, or volume rides as NaN
// rather than as a zero: a zero would read as a real (and maximally bearish)
// observation, whereas the registry drops that row for the entries that need
// the column, leaving a shorter series and an insufficient flag.
func usableSeries(prices []entity.DailyPrice) indicator.Series {
	highs := make([]float64, 0, len(prices))
	lows := make([]float64, 0, len(prices))
	closes := make([]float64, 0, len(prices))
	volumes := make([]float64, 0, len(prices))
	for _, p := range prices {
		if p.Close == nil {
			continue
		}
		highs = append(highs, missingPrice(p.High))
		lows = append(lows, missingPrice(p.Low))
		closes = append(closes, *p.Close)
		volumes = append(volumes, missingVolume(p.Volume))
	}
	return indicator.Series{High: highs, Low: lows, Close: closes, Volume: volumes}
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
