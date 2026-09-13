package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Sector grouping dimensions — the tickers taxonomy columns the 15b seeder
// owns (sektor / sub_sektor / industri, the screener's canonical English
// labels).
const (
	groupBySektor    = "sektor"
	groupBySubSector = "sub_sector"
	groupByIndustry  = "industry"
)

// sectorFlowUnclassified is the bucket for a ticker whose taxonomy label is
// missing (NULL/empty column, or no tickers row at all). Bucketing rather than
// dropping keeps the sector listed nets reconcilable with the market-wide read.
const sectorFlowUnclassified = "UNCLASSIFIED"

// sectorFlowTopBrokers caps the per-sector broker breakdown — the biggest
// accumulators first, so a sector with sixty brokers doesn't dump all of them.
// BrokersCount declares the untruncated total. It also caps shape B's
// top_tickers list, where TickersCovered declares the total.
const sectorFlowTopBrokers = 5

// sectorFlowNestedLimit caps one level deeper than sectorFlowTopBrokers — the
// per-ticker blocks inside shape A's brokers and shape B's tickers, where the
// sibling TickersCount / BrokersCount declares the untruncated total.
const sectorFlowNestedLimit = 3

// Nested-detail shapes for the sector rows. "" (the default) returns the plain
// sector rows; broker adds each top broker's per-ticker detail (shape A);
// ticker adds the sector's per-ticker rows, carrying the tail and foreign net
// that broker grain structurally cannot (shape B).
const (
	breakdownNone   = ""
	breakdownBroker = "broker"
	breakdownTicker = "ticker"
)

// SectorFlowReader is the read surface the MCP server depends on, so its
// handler tests can wire a fake and run without a DB.
type SectorFlowReader interface {
	GetSectorFlow(ctx context.Context, req SectorFlowRequest) (*SectorFlowResponse, error)
}

// SectorFlowUseCase aggregates stored per-stock broker rows to sector level by
// joining them to the seeded ticker taxonomy. Pure DB read — no upstream call,
// no new ingestion: the population is whatever broker rows the anomaly gate and
// on-demand fetches have stored, which the coverage fields declare. Sector
// totals come from two sources: the listed top-10 rows (broker-attributable)
// and each ticker-day's footer totals (the unlisted tail + foreign net).
type SectorFlowUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	BrokerRepo     *repository.BrokerStockSummaryRepository
	DailyPriceRepo *repository.DailyPriceRepository
	TickerRepo     *repository.TickerRepository
}

func NewSectorFlowUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	brokerRepo *repository.BrokerStockSummaryRepository,
	dailyPriceRepo *repository.DailyPriceRepository,
	tickerRepo *repository.TickerRepository,
) *SectorFlowUseCase {
	return &SectorFlowUseCase{
		DB:             db,
		Log:            log,
		BrokerRepo:     brokerRepo,
		DailyPriceRepo: dailyPriceRepo,
		TickerRepo:     tickerRepo,
	}
}

// SectorFlowRequest is one get_sector_flow query. From/To default like
// get_broker_net_flow (30 calendar days ending at the latest trading day, max
// 180). Sector filters the grouped label (case-insensitive exact match) and is
// echoed back; nil = every group. GroupBy picks the taxonomy dimension —
// "" defaults to sektor, otherwise sektor | sub_sector | industry. Breakdown
// picks the nested detail — "" (default) sector rows only, "broker" shape A,
// "ticker" shape B.
type SectorFlowRequest struct {
	From      *time.Time
	To        *time.Time
	Sector    *string
	GroupBy   string
	Breakdown string
}

// SectorFlowBrokerRow is one broker's contribution inside a sector — or inside
// one ticker, in shape B's nested block. DaysShown is populated only in the
// nested-ticker context; TickersCount/ByTicker only in shape A.
type SectorFlowBrokerRow struct {
	BrokerCode   string               `json:"broker_code"`
	Buy          int64                `json:"buy"`
	Sell         int64                `json:"sell"`
	Net          int64                `json:"net"`
	DaysShown    int                  `json:"days_shown,omitempty"`
	TickersCount int                  `json:"tickers_count,omitempty"`
	ByTicker     []SectorFlowByTicker `json:"by_ticker,omitempty"`
}

// SectorFlowByTicker is one ticker's contribution inside one broker (shape A).
type SectorFlowByTicker struct {
	Ticker    string `json:"ticker"`
	Buy       int64  `json:"buy"`
	Sell      int64  `json:"sell"`
	Net       int64  `json:"net"`
	DaysShown int    `json:"days_shown"`
}

// SectorFlowTickerRow is one ticker's flow inside a sector (shape B). Unlike a
// broker row it can carry the tail (OthersNet) and the foreign net, because
// those are ticker-day facts from the footer totals — a broker cannot own them.
type SectorFlowTickerRow struct {
	Ticker       string                `json:"ticker"`
	Buy          int64                 `json:"buy"`
	Sell         int64                 `json:"sell"`
	Net          int64                 `json:"net"`
	OthersNet    int64                 `json:"others_net"`
	ForeignNet   int64                 `json:"foreign_net"`
	DaysShown    int                   `json:"days_shown"`
	BrokersCount int                   `json:"brokers_count"`
	TopBrokers   []SectorFlowBrokerRow `json:"top_brokers"`
}

// SectorFlowRow is one sector's flow over the window. Buy/Sell are the listed
// top-10 rows; OthersNet is the sector's unlisted tail (Σ per-ticker footer
// others_net); Net = Buy − Sell + OthersNet, the sector's true net flow.
// ForeignNet is Σ per-ticker f_nval for the sector — positive = foreign net
// buying. Coverage declares how thin the population is: TickersCovered counts
// distinct tickers with stored rows in the sector, DaysCovered the distinct
// days they cover. TopTickers is populated only for breakdown=ticker.
type SectorFlowRow struct {
	Sector         string                `json:"sector"`
	Buy            int64                 `json:"buy"`
	Sell           int64                 `json:"sell"`
	Net            int64                 `json:"net"`
	OthersNet      int64                 `json:"others_net"`
	ForeignNet     int64                 `json:"foreign_net"`
	TickersCovered int                   `json:"tickers_covered"`
	DaysCovered    int                   `json:"days_covered"`
	BrokersCount   int                   `json:"brokers_count"`
	TopBrokers     []SectorFlowBrokerRow `json:"top_brokers"`
	TopTickers     []SectorFlowTickerRow `json:"top_tickers,omitempty"`
}

// SectorFlowResponse is the structured MCP tool result. Response-level totals
// and coverage describe the returned rows only — a sector filter narrows them
// to that sector. Rows are sorted by net desc (rotation leaders first), then
// label asc.
type SectorFlowResponse struct {
	From              string          `json:"from"`
	To                string          `json:"to"`
	GroupBy           string          `json:"group_by"`
	Breakdown         string          `json:"breakdown,omitempty"`
	Sector            string          `json:"sector,omitempty"`
	TradeDaysInWindow int             `json:"trade_days_in_window"`
	CoveredDays       int             `json:"covered_days"`
	TickersCovered    int             `json:"tickers_covered"`
	OthersNet         int64           `json:"others_net"`
	ForeignNet        int64           `json:"foreign_net"`
	Rows              []SectorFlowRow `json:"rows"`
}

// flowNode is one bucket in the sector → broker → ticker tree, and — because
// the same walk fills the tickers and brokers maps at every level — also the
// sector → ticker → broker tree. That duplicate walk is what lets either
// breakdown project without a second pass over the rows. Leaf nodes carry only
// buy/sell + their day set; the tail and foreign net land on sector and ticker
// nodes (the footer totals' grain).
type flowNode struct {
	buy, sell             int64
	othersNet, foreignNet int64
	days                  map[string]struct{}
	brokers               map[string]*flowNode
	tickers               map[string]*flowNode
}

func newFlowNode() *flowNode {
	return &flowNode{
		days:    make(map[string]struct{}),
		brokers: make(map[string]*flowNode),
		tickers: make(map[string]*flowNode),
	}
}

// getOrCreate returns the nested node for key in m, creating it on first use.
func getOrCreate(m map[string]*flowNode, key string) *flowNode {
	n, ok := m[key]
	if !ok {
		n = newFlowNode()
		m[key] = n
	}
	return n
}

// addFlow books one row's value into a node: side total + the day it traded.
func (n *flowNode) addFlow(day string, val int64, sell bool) {
	if sell {
		n.sell += val
	} else {
		n.buy += val
	}
	n.days[day] = struct{}{}
}

// net is the node's net flow: listed buy − sell plus its own tail (zero for
// leaf and broker nodes, which have no footer grain).
func (n *flowNode) net() int64 { return n.buy - n.sell + n.othersNet }

// GetSectorFlow aggregates stored broker flow to sector level over a window by
// joining each ticker to the taxonomy label of the requested dimension. The
// per-ticker tail (others_net) and foreign net come from the stored footer
// totals, so a sector's net is the whole sector's flow, not just the top-10
// slice of it. Empty window → empty rows with coverage 0, not an error.
func (uc *SectorFlowUseCase) GetSectorFlow(ctx context.Context, req SectorFlowRequest) (*SectorFlowResponse, error) {
	groupBy, err := resolveGroupBy(req.GroupBy)
	if err != nil {
		return nil, err
	}
	breakdown, err := resolveBreakdown(req.Breakdown)
	if err != nil {
		return nil, err
	}

	from, to, err := uc.resolveSectorFlowWindow(req.From, req.To)
	if err != nil {
		return nil, err
	}
	if from.After(to) {
		return nil, ErrInvalidRange
	}
	if windowTooLong(from, to) {
		return nil, fmt.Errorf("%w: window exceeds %d calendar days", ErrInvalidRange, brokerNetFlowMaxWindowDays)
	}

	tradeDays, err := marketTradingDayCount(uc.DB, uc.DailyPriceRepo, from, to)
	if err != nil {
		return nil, err
	}

	rows, err := uc.BrokerRepo.FindByDateRangeAll(uc.DB, from, to)
	if err != nil {
		return nil, fmt.Errorf("read broker summary rows for sector flow: %w", err)
	}
	totals, err := uc.BrokerRepo.FindTotalsByDateRangeAll(uc.DB, from, to)
	if err != nil {
		return nil, fmt.Errorf("read broker summary totals for sector flow: %w", err)
	}
	labels, err := uc.sectorLabels()
	if err != nil {
		return nil, err
	}

	acc := make(map[string]*flowNode)
	for _, r := range rows {
		s := getOrCreate(acc, sectorLabelFor(labels, r.Ticker, groupBy))
		day := r.TradingDay.Format("2006-01-02")
		val := deref(r.Value)
		sell := r.Side == "sell"

		s.addFlow(day, val, sell)
		// Both directions of the same row: sector→broker→ticker feeds shape A,
		// sector→ticker→broker feeds shape B. Neither breakdown pays for the
		// other, and the default projection ignores both.
		b := getOrCreate(s.brokers, r.BrokerCode)
		tk := getOrCreate(s.tickers, r.Ticker)
		b.addFlow(day, val, sell)
		tk.addFlow(day, val, sell)
		getOrCreate(b.tickers, r.Ticker).addFlow(day, val, sell)
		getOrCreate(tk.brokers, r.BrokerCode).addFlow(day, val, sell)
	}
	// The footer totals carry what the top-10 rows can't see: the unlisted tail
	// and the foreign net. Both are ticker-day facts — no broker grain — so they
	// land on the sector and ticker nodes only.
	for _, t := range totals {
		s := getOrCreate(acc, sectorLabelFor(labels, t.Ticker, groupBy))
		others, foreign := deref(t.OthersNet), deref(t.FNVal)
		s.othersNet += others
		s.foreignNet += foreign
		tk := getOrCreate(s.tickers, t.Ticker)
		tk.othersNet += others
		tk.foreignNet += foreign
	}

	filter := ""
	if req.Sector != nil {
		filter = strings.TrimSpace(*req.Sector)
	}

	resp := &SectorFlowResponse{
		From:              from.Format("2006-01-02"),
		To:                to.Format("2006-01-02"),
		GroupBy:           groupBy,
		Breakdown:         breakdown,
		Sector:            filter,
		TradeDaysInWindow: tradeDays,
		Rows:              []SectorFlowRow{},
	}
	covered := make(map[string]struct{})
	tickersCovered := make(map[string]struct{})
	for label, s := range acc {
		if filter != "" && !strings.EqualFold(label, filter) {
			continue
		}
		row := SectorFlowRow{
			Sector:         label,
			Buy:            s.buy,
			Sell:           s.sell,
			Net:            s.net(),
			OthersNet:      s.othersNet,
			ForeignNet:     s.foreignNet,
			TickersCovered: len(s.tickers),
			DaysCovered:    len(s.days),
			BrokersCount:   len(s.brokers),
			TopBrokers:     sectorBrokerRows(s, breakdown),
		}
		if breakdown == breakdownTicker {
			row.TopTickers = tickerRows(s)
		}
		resp.Rows = append(resp.Rows, row)
		resp.OthersNet += row.OthersNet
		resp.ForeignNet += row.ForeignNet
		for d := range s.days {
			covered[d] = struct{}{}
		}
		for tk := range s.tickers {
			tickersCovered[tk] = struct{}{}
		}
	}
	resp.CoveredDays = len(covered)
	resp.TickersCovered = len(tickersCovered)

	// Lead with the sectors money is rotating into — distributors sink.
	sort.Slice(resp.Rows, func(i, j int) bool {
		if resp.Rows[i].Net != resp.Rows[j].Net {
			return resp.Rows[i].Net > resp.Rows[j].Net
		}
		return resp.Rows[i].Sector < resp.Rows[j].Sector
	})
	return resp, nil
}

// resolveBreakdown validates the nested-detail shape; "" = sector rows only.
func resolveBreakdown(breakdown string) (string, error) {
	switch norm := strings.ToLower(strings.TrimSpace(breakdown)); norm {
	case breakdownNone, breakdownBroker, breakdownTicker:
		return norm, nil
	default:
		return "", fmt.Errorf("%w: breakdown must be broker or ticker", ErrInvalidArgument)
	}
}

// resolveGroupBy validates the grouping dimension; "" defaults to sektor.
func resolveGroupBy(groupBy string) (string, error) {
	switch norm := strings.ToLower(strings.TrimSpace(groupBy)); norm {
	case "":
		return groupBySektor, nil
	case groupBySektor, groupBySubSector, groupByIndustry:
		return norm, nil
	default:
		return "", fmt.Errorf("%w: group_by must be sektor, sub_sector or industry", ErrInvalidArgument)
	}
}

// resolveSectorFlowWindow applies the window defaults: to defaults to the
// latest market-wide trading day, from to (to − 30 calendar days). A supplied
// to skips the latest-trading-day query entirely.
func (uc *SectorFlowUseCase) resolveSectorFlowWindow(from, to *time.Time) (time.Time, time.Time, error) {
	toT := time.Time{}
	if to != nil {
		toT = *to
	} else {
		latest, err := latestMarketTradingDay(uc.DB, uc.DailyPriceRepo)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		toT = latest
	}
	fromT := toT.AddDate(0, 0, -brokerNetFlowDefaultWindowDays)
	if from != nil {
		fromT = *from
	}
	return fromT, toT, nil
}

// sectorLabels loads the taxonomy of every ticker (active or not — a stored
// flow row for a delisted ticker still needs its sector).
func (uc *SectorFlowUseCase) sectorLabels() (map[string]entity.Ticker, error) {
	tickers, err := uc.TickerRepo.FindClassifications(uc.DB)
	if err != nil {
		return nil, fmt.Errorf("read ticker taxonomy for sector flow: %w", err)
	}
	labels := make(map[string]entity.Ticker, len(tickers))
	for _, t := range tickers {
		labels[t.Code] = t
	}
	return labels, nil
}

// sectorLabelFor resolves one ticker's label for the grouping dimension,
// falling back to UNCLASSIFIED when the ticker row or its column is missing.
func sectorLabelFor(labels map[string]entity.Ticker, ticker, groupBy string) string {
	t, ok := labels[ticker]
	if !ok {
		return sectorFlowUnclassified
	}
	var label *string
	switch groupBy {
	case groupBySubSector:
		label = t.SubSektor
	case groupByIndustry:
		label = t.Industri
	default:
		label = t.Sektor
	}
	if label == nil {
		return sectorFlowUnclassified
	}
	if trimmed := strings.TrimSpace(*label); trimmed != "" {
		return trimmed
	}
	return sectorFlowUnclassified
}

// sortedByNet returns the node keys of m ordered net desc, key asc on ties —
// the accumulation-leaders-first order every projection uses.
func sortedByNet(m map[string]*flowNode) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ni, nj := m[keys[i]].net(), m[keys[j]].net()
		if ni != nj {
			return ni > nj
		}
		return keys[i] < keys[j]
	})
	return keys
}

// capKeys truncates a sorted key list to limit; the sibling count field in the
// response declares what the cap hid.
func capKeys(keys []string, limit int) []string {
	if len(keys) > limit {
		return keys[:limit]
	}
	return keys
}

// sectorBrokerRows projects a sector node's brokers. breakdown=broker attaches
// shape A's per-ticker detail (with the broker's true ticker count); otherwise
// the plain row the sector grain has always returned.
func sectorBrokerRows(s *flowNode, breakdown string) []SectorFlowBrokerRow {
	keys := capKeys(sortedByNet(s.brokers), sectorFlowTopBrokers)
	rows := make([]SectorFlowBrokerRow, 0, len(keys))
	for _, code := range keys {
		b := s.brokers[code]
		row := SectorFlowBrokerRow{
			BrokerCode: code,
			Buy:        b.buy,
			Sell:       b.sell,
			Net:        b.net(),
		}
		if breakdown == breakdownBroker {
			row.TickersCount = len(b.tickers)
			row.ByTicker = byTickerRows(b)
		}
		rows = append(rows, row)
	}
	return rows
}

// byTickerRows projects one broker's ticker buckets (shape A's nested block).
func byTickerRows(b *flowNode) []SectorFlowByTicker {
	keys := capKeys(sortedByNet(b.tickers), sectorFlowNestedLimit)
	rows := make([]SectorFlowByTicker, 0, len(keys))
	for _, tk := range keys {
		t := b.tickers[tk]
		rows = append(rows, SectorFlowByTicker{
			Ticker:    tk,
			Buy:       t.buy,
			Sell:      t.sell,
			Net:       t.net(),
			DaysShown: len(t.days),
		})
	}
	return rows
}

// tickerRows projects one sector's ticker buckets (shape B) — the only grain
// where the footer tail and foreign net are attributable, plus each ticker's
// own top brokers.
func tickerRows(s *flowNode) []SectorFlowTickerRow {
	keys := capKeys(sortedByNet(s.tickers), sectorFlowTopBrokers)
	rows := make([]SectorFlowTickerRow, 0, len(keys))
	for _, tk := range keys {
		t := s.tickers[tk]
		rows = append(rows, SectorFlowTickerRow{
			Ticker:       tk,
			Buy:          t.buy,
			Sell:         t.sell,
			Net:          t.net(),
			OthersNet:    t.othersNet,
			ForeignNet:   t.foreignNet,
			DaysShown:    len(t.days),
			BrokersCount: len(t.brokers),
			TopBrokers:   tickerBrokerRows(t),
		})
	}
	return rows
}

// tickerBrokerRows is the leaf block inside a shape-B ticker row: which brokers
// moved that ticker, and on how many days.
func tickerBrokerRows(t *flowNode) []SectorFlowBrokerRow {
	keys := capKeys(sortedByNet(t.brokers), sectorFlowNestedLimit)
	rows := make([]SectorFlowBrokerRow, 0, len(keys))
	for _, code := range keys {
		b := t.brokers[code]
		rows = append(rows, SectorFlowBrokerRow{
			BrokerCode: code,
			Buy:        b.buy,
			Sell:       b.sell,
			Net:        b.net(),
			DaysShown:  len(b.days),
		})
	}
	return rows
}

// latestMarketTradingDay resolves the most recent trading day across the market
// (the default window anchor for the market-wide reads). No stored day →
// ErrNoTradingDay.
func latestMarketTradingDay(db *sqlx.DB, repo *repository.DailyPriceRepository) (time.Time, error) {
	latest, err := repo.LatestTradingDayAll(db)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, ErrNoTradingDay
		}
		return time.Time{}, fmt.Errorf("resolve latest trading day: %w", err)
	}
	return *latest, nil
}

// marketTradingDayCount counts the distinct trading days in the window per the
// market-wide daily_prices calendar (the coverage denominator).
func marketTradingDayCount(db *sqlx.DB, repo *repository.DailyPriceRepository, from, to time.Time) (int, error) {
	days, err := repo.TradingDaysInRangeAll(db, from, to)
	if err != nil {
		return 0, fmt.Errorf("resolve trading days: %w", err)
	}
	return len(days), nil
}
