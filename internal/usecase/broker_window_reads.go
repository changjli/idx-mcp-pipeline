package usecase

import (
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// brokerReadLane is the investor lane every tool reads. The all-investor lane is
// written only by the sweep until ticket 10c moves it, and is never aggregated
// with the foreign lane (the two are not summable).
const brokerReadLane = broker.InvestorForeign

// brokerMarketDays returns the market-wide trading days in [from, to] from the
// stored daily_prices calendar. It is the denominator the interval-cover solver
// tiles and the coverage a read declares.
func brokerMarketDays(db *sqlx.DB, repo *repository.DailyPriceRepository, from, to time.Time) ([]time.Time, error) {
	return repo.TradingDaysInRangeAll(db, from, to)
}

// windowCover is the solver's outcome for one requested range: the trading days
// in scope and, per ticker, the disjoint stored windows that answer it.
type windowCover struct {
	days  []time.Time
	spans map[string][]broker.Span
}

// buildWindowCover solves the requested range for every ticker in spans.
// exact selects the answering rule (an exact tiling is required, and a ticker
// without one is simply absent) versus the aggregation rule (a maximal disjoint
// subset, with coverage declared). Neither mode ever double-counts a day.
func buildWindowCover(spans []repository.BrokerWindowSpan, from, to time.Time, days []time.Time, exact bool) *windowCover {
	cover := &windowCover{days: days, spans: make(map[string][]broker.Span)}
	byTicker := make(map[string][]broker.Span, len(spans))
	for _, s := range spans {
		byTicker[s.Ticker] = append(byTicker[s.Ticker], s.Span())
	}
	for ticker, candidates := range byTicker {
		if exact {
			selected, ok := broker.ExactCover(candidates, from, to, days)
			if !ok {
				continue
			}
			cover.spans[ticker] = selected
			continue
		}
		if selected := broker.DisjointCover(candidates, from, to, days); len(selected) > 0 {
			cover.spans[ticker] = selected
		}
	}
	return cover
}

// selected reports whether a stored row/totals window belongs to the solved
// cover. Rows carry their own window key, so membership is an exact range match.
func (c *windowCover) selected(ticker string, winFrom, day time.Time) bool {
	for _, s := range c.spans[ticker] {
		if s.From.Equal(winFrom) && s.To.Equal(day) {
			return true
		}
	}
	return false
}

// dayIndices returns the positions in the market calendar that one stored
// window represents. A window row aggregates all of them, so every day-based
// count (covered_days, days_shown, sessions) expands to the whole span.
func (c *windowCover) dayIndices(ticker string, winFrom, day time.Time) []int {
	if !c.selected(ticker, winFrom, day) {
		return nil
	}
	return dayIndicesForSpan(broker.Span{From: winFrom, To: day}, c.days)
}

// dayIndicesForSpan returns the calendar positions inside one span.
func dayIndicesForSpan(span broker.Span, days []time.Time) []int {
	out := make([]int, 0, 4)
	for i, day := range days {
		if !day.Before(span.From) && !day.After(span.To) {
			out = append(out, i)
		}
	}
	return out
}

// filterRowsToCover drops rows whose window the solver did not select, so a
// cached window that overlaps the partition can never be summed.
func filterRowsToCover(rows []entity.BrokerStockSummary, cover *windowCover) []entity.BrokerStockSummary {
	out := make([]entity.BrokerStockSummary, 0, len(rows))
	for _, r := range rows {
		if cover.selected(r.Ticker, r.WinFrom, r.TradingDay) {
			out = append(out, r)
		}
	}
	return out
}

// filterTotalsToCover is filterRowsToCover for footer summaries.
func filterTotalsToCover(totals []entity.BrokerStockSummaryTotals, cover *windowCover) []entity.BrokerStockSummaryTotals {
	out := make([]entity.BrokerStockSummaryTotals, 0, len(totals))
	for _, t := range totals {
		if cover.selected(t.Ticker, t.WinFrom, t.TradingDay) {
			out = append(out, t)
		}
	}
	return out
}

// persistBrokerWindow writes one ticker+window+investor atomically. It is the
// single write seam shared by the on-demand read path, the range/backfill path
// and the screener's trailing-window cache, so all three stamp the same grain,
// lane and provenance.
//
// In the foreign lane the f payload has no all-investor totals: t_val, t_lot and
// avg are written NULL, and f_nval is derived from the rows (Σbuy − Σsell),
// never taken from a foreign gross.
func persistBrokerWindow(db *sqlx.DB, repo *repository.BrokerStockSummaryRepository, ticker string, winFrom, day time.Time, res *broker.Result, investor, source string) error {
	rows := make([]entity.BrokerStockSummary, 0, len(res.Buyers)+len(res.Sellers))
	for _, b := range res.Buyers {
		rows = append(rows, toEntity(ticker, winFrom, day, investor, "buy", b, source))
	}
	for _, s := range res.Sellers {
		rows = append(rows, toEntity(ticker, winFrom, day, investor, "sell", s, source))
	}

	var tval, tlot, avg, fnet *int64
	if investor == broker.InvestorAll {
		tval, tlot, avg = i64p(res.Totals.TVal), i64p(res.Totals.TLot), i64p(res.Totals.Avg)
		fnet = res.Totals.FNVal
	} else {
		net := foreignNetFromRows(res.Buyers, res.Sellers)
		fnet = &net
	}
	totals := &entity.BrokerStockSummaryTotals{
		Ticker:     ticker,
		TradingDay: day,
		WinFrom:    winFrom,
		Investor:   investor,
		Market:     broker.MarketRegular,
		TVal:       tval,
		FNVal:      fnet,
		TLot:       tlot,
		Avg:        avg,
		Source:     source,
	}
	return repo.UpsertWindow(db, rows, totals)
}

// foreignNetFromRows is Σbuy value − Σsell value over the f rows.
func foreignNetFromRows(buyers, sellers []broker.Row) int64 {
	var net int64
	for _, b := range buyers {
		net += b.Value
	}
	for _, s := range sellers {
		net -= s.Value
	}
	return net
}
