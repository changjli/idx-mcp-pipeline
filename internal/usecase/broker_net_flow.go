package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// Net flow window defaults.
const (
	// brokerNetFlowDefaultWindowDays is the lookback applied when from is
	// omitted: 30 calendar days ending at the latest trading day.
	brokerNetFlowDefaultWindowDays = 30
	// brokerNetFlowMaxWindowDays caps how far back a window may reach so one
	// request can't scan months of stored rows.
	brokerNetFlowMaxWindowDays = 180

	netFlowModeTicker = "ticker"
	netFlowModeMarket = "market"
)

// BrokerNetFlowRow is one broker's cumulative flow over the window.
type BrokerNetFlowRow struct {
	BrokerCode string `json:"broker_code"`
	Buy        int64  `json:"buy"`
	Sell       int64  `json:"sell"`
	Net        int64  `json:"net"` // buy − sell; positive = net accumulation
	// Per-ticker mode: distinct trading days the broker appeared in any
	// top-10 list. Days below top-10 are invisible, never inferred.
	DaysShown int `json:"days_shown,omitempty"`
	// Market-wide mode: breadth (sessions = ticker-day listings) and depth
	// (tickers = distinct tickers where the broker appeared), plus the
	// per-ticker breakdown so the stance is attributable — which stocks the
	// broker accumulated, not just how much.
	Sessions int                         `json:"sessions,omitempty"`
	Tickers  int                         `json:"tickers,omitempty"`
	ByTicker []BrokerNetFlowTickerDetail `json:"by_ticker,omitempty"`
}

// BrokerNetFlowTickerDetail is one broker's flow within a single ticker in
// market-wide mode — the attribution the aggregate alone can't give.
type BrokerNetFlowTickerDetail struct {
	Ticker    string `json:"ticker"`
	Buy       int64  `json:"buy"`
	Sell      int64  `json:"sell"`
	Net       int64  `json:"net"`
	DaysShown int    `json:"days_shown"`
}

// BrokerNetFlowResponse is the structured result of get_broker_net_flow.
// Coverage is the honesty contract: stored broker rows exist only for
// anomaly-passing days and on-demand fetches, so trade_days_in_window vs
// covered_days declares how much of the window this answer actually observes
// (see CONTEXT.md "Coverage"). OthersNet is the window's unlisted tail —
// positive = the quiet tail net-bought (same sign as a row's Net).
type BrokerNetFlowResponse struct {
	Mode              string             `json:"mode"` // "ticker" | "market"
	Ticker            string             `json:"ticker,omitempty"`
	From              string             `json:"from"`
	To                string             `json:"to"`
	TradeDaysInWindow int                `json:"trade_days_in_window"`
	CoveredDays       int                `json:"covered_days"`
	TickersCovered    int                `json:"tickers_covered,omitempty"` // market-wide mode
	OthersNet         int64              `json:"others_net"`
	Rows              []BrokerNetFlowRow `json:"rows"`
}

// netFlowAccumulator accumulates one broker's totals over the window. days is
// used in ticker mode; sessions + tickers + byTicker in market mode.
type netFlowAccumulator struct {
	buy, sell int64
	days      map[string]struct{}
	sessions  map[string]struct{}
	tickers   map[string]struct{}
	byTicker  map[string]*netFlowAccumulator // market mode: per-ticker detail
}

func newNetFlowAccumulator() *netFlowAccumulator {
	return &netFlowAccumulator{
		days:     make(map[string]struct{}),
		sessions: make(map[string]struct{}),
		tickers:  make(map[string]struct{}),
		byTicker: make(map[string]*netFlowAccumulator),
	}
}

// GetBrokerNetFlow aggregates stored per-stock broker rows over a window into
// per-broker cumulative net flow. Pure DB read — no upstream call, no
// inference: a broker below top-10 on a day is not counted that day, and its
// flow lives in the window's others_net tail. Two modes:
//   - ticker mode (pass ticker): per-broker net over one ticker's window.
//   - market mode (omit ticker): per-broker net across every ticker with
//     stored rows in the window. The population is anomaly-gated + on-demand
//     fetches (bias toward busy tickers); tickers_covered declares it.
//
// from/to default to brokerNetFlowDefaultWindowDays ending at the latest
// trading day (per-ticker in ticker mode, market-wide in market mode).
// Windows longer than brokerNetFlowMaxWindowDays are rejected.
func (uc *BrokerStockSummaryUseCase) GetBrokerNetFlow(ctx context.Context, ticker *string, from, to *time.Time) (*BrokerNetFlowResponse, error) {
	mode, normalized, err := uc.netFlowMode(ticker)
	if err != nil {
		return nil, err
	}

	fromT, toT, err := uc.resolveNetFlowWindow(normalized, mode == netFlowModeMarket, from, to)
	if err != nil {
		return nil, err
	}
	if fromT.After(toT) {
		return nil, ErrInvalidRange
	}
	if windowTooLong(fromT, toT) {
		return nil, fmt.Errorf("%w: window exceeds %d calendar days", ErrInvalidRange, brokerNetFlowMaxWindowDays)
	}

	resp := &BrokerNetFlowResponse{
		Mode: mode,
		From: fromT.Format("2006-01-02"),
		To:   toT.Format("2006-01-02"),
		Rows: []BrokerNetFlowRow{},
	}

	tradeDays, err := uc.netFlowTradeDays(mode == netFlowModeMarket, normalized, fromT, toT)
	if err != nil {
		return nil, err
	}
	resp.TradeDaysInWindow = tradeDays

	rows, err := uc.netFlowRows(mode == netFlowModeMarket, normalized, fromT, toT)
	if err != nil {
		return nil, err
	}

	acc := make(map[string]*netFlowAccumulator)
	covered := make(map[string]struct{})
	tickersCovered := make(map[string]struct{})
	var totalBuy, totalSell int64
	market := mode == netFlowModeMarket
	for _, r := range rows {
		a, ok := acc[r.BrokerCode]
		if !ok {
			a = newNetFlowAccumulator()
			acc[r.BrokerCode] = a
		}
		val := deref(r.Value)
		if r.Side == "sell" {
			a.sell += val
			totalSell += val
		} else {
			a.buy += val
			totalBuy += val
		}
		day := r.TradingDay.Format("2006-01-02")
		covered[day] = struct{}{}
		tickersCovered[r.Ticker] = struct{}{}
		if market {
			a.sessions[r.Ticker+"|"+day] = struct{}{}
			a.tickers[r.Ticker] = struct{}{}
			sub, ok := a.byTicker[r.Ticker]
			if !ok {
				sub = newNetFlowAccumulator()
				a.byTicker[r.Ticker] = sub
			}
			if r.Side == "sell" {
				sub.sell += val
			} else {
				sub.buy += val
			}
			sub.days[day] = struct{}{}
		} else {
			a.days[day] = struct{}{}
		}
	}

	resp.CoveredDays = len(covered)
	if market {
		resp.TickersCovered = len(tickersCovered)
	}
	// Window tail = Σ listed sell − Σ listed buy across the window's rows,
	// the same recompute the history reader uses per day — positive = the
	// unlisted tail net-bought. Summing per-day tails collapses to the global
	// difference, so no per-day grouping is needed.
	resp.OthersNet = totalSell - totalBuy

	for code, a := range acc {
		row := BrokerNetFlowRow{
			BrokerCode: code,
			Buy:        a.buy,
			Sell:       a.sell,
			Net:        a.buy - a.sell,
		}
		if market {
			row.Sessions = len(a.sessions)
			row.Tickers = len(a.tickers)
			row.ByTicker = make([]BrokerNetFlowTickerDetail, 0, len(a.byTicker))
			for tk, sub := range a.byTicker {
				row.ByTicker = append(row.ByTicker, BrokerNetFlowTickerDetail{
					Ticker:    tk,
					Buy:       sub.buy,
					Sell:      sub.sell,
					Net:       sub.buy - sub.sell,
					DaysShown: len(sub.days),
				})
			}
			sort.Slice(row.ByTicker, func(i, j int) bool {
				if row.ByTicker[i].Net != row.ByTicker[j].Net {
					return row.ByTicker[i].Net > row.ByTicker[j].Net
				}
				return row.ByTicker[i].Ticker < row.ByTicker[j].Ticker
			})
		} else {
			row.DaysShown = len(a.days)
		}
		resp.Rows = append(resp.Rows, row)
	}
	// Accumulators first: the tool answers "who accumulated" — sellers sink.
	sort.Slice(resp.Rows, func(i, j int) bool {
		if resp.Rows[i].Net != resp.Rows[j].Net {
			return resp.Rows[i].Net > resp.Rows[j].Net
		}
		return resp.Rows[i].BrokerCode < resp.Rows[j].BrokerCode
	})
	return resp, nil
}

// netFlowMode resolves the response mode and validates the ticker when one is
// given. nil ticker = market-wide mode.
func (uc *BrokerStockSummaryUseCase) netFlowMode(ticker *string) (mode, normalized string, err error) {
	if ticker == nil {
		return netFlowModeMarket, "", nil
	}
	normalized = strings.ToUpper(strings.TrimSpace(*ticker))
	if !tickerPattern.MatchString(normalized) {
		return "", "", ErrInvalidTicker
	}
	return netFlowModeTicker, normalized, nil
}

// resolveNetFlowWindow applies the date defaults to the resolved range: to
// defaults to the latest trading day (ticker-specific, or market-wide when
// market=true); from defaults to (to − brokerNetFlowDefaultWindowDays).
// When to is supplied the latest-trading-day query is skipped entirely.
func (uc *BrokerStockSummaryUseCase) resolveNetFlowWindow(ticker string, market bool, from, to *time.Time) (time.Time, time.Time, error) {
	toT, err := uc.netFlowAnchor(ticker, market, to)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	fromT := toT.AddDate(0, 0, -brokerNetFlowDefaultWindowDays)
	if from != nil {
		fromT = *from
	}
	return fromT, toT, nil
}

// netFlowAnchor returns the window end: the caller's to when given, otherwise
// the latest trading day (per-ticker or market-wide). No stored day → ErrNoTradingDay.
func (uc *BrokerStockSummaryUseCase) netFlowAnchor(ticker string, market bool, to *time.Time) (time.Time, error) {
	if to != nil {
		return *to, nil
	}
	var latest *time.Time
	var err error
	if market {
		latest, err = uc.DailyPriceRepo.LatestTradingDayAll(uc.DB)
	} else {
		latest, err = uc.DailyPriceRepo.LatestTradingDay(uc.DB, ticker)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, ErrNoTradingDay
		}
		return time.Time{}, fmt.Errorf("resolve latest trading day: %w", err)
	}
	return *latest, nil
}

func windowTooLong(from, to time.Time) bool {
	return from.AddDate(0, 0, brokerNetFlowMaxWindowDays).Before(to)
}

// netFlowTradeDays counts the distinct trading days in the window per the
// daily_prices calendar (the coverage denominator).
func (uc *BrokerStockSummaryUseCase) netFlowTradeDays(market bool, ticker string, from, to time.Time) (int, error) {
	var days []time.Time
	var err error
	if market {
		days, err = uc.DailyPriceRepo.TradingDaysInRangeAll(uc.DB, from, to)
	} else {
		days, err = uc.DailyPriceRepo.TradingDaysInRange(uc.DB, ticker, from, to)
	}
	if err != nil {
		return 0, fmt.Errorf("resolve trading days: %w", err)
	}
	return len(days), nil
}

// netFlowRows loads the stored broker rows feeding the window aggregation.
func (uc *BrokerStockSummaryUseCase) netFlowRows(market bool, ticker string, from, to time.Time) ([]entity.BrokerStockSummary, error) {
	var rows []entity.BrokerStockSummary
	var err error
	if market {
		rows, err = uc.Repo.FindByDateRangeAll(uc.DB, from, to)
	} else {
		rows, err = uc.Repo.FindByTickerAndDateRange(uc.DB, ticker, from, to)
	}
	if err != nil {
		return nil, fmt.Errorf("read broker summary rows for net flow: %w", err)
	}
	return rows, nil
}
