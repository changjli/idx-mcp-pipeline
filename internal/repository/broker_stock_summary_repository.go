package repository

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// BrokerStockSummaryRepository persists per-stock broker summaries. It does
// NOT embed the generic Repository — the table has a composite PK and no id
// column, so the promoted FindByID/DeleteByID/Count would fail at runtime.
type BrokerStockSummaryRepository struct {
	Log *logrus.Logger
}

func NewBrokerStockSummaryRepository(log *logrus.Logger) *BrokerStockSummaryRepository {
	return &BrokerStockSummaryRepository{Log: log}
}

// UpsertDay atomically replaces one ticker+day's broker rows and totals in a
// single transaction. It is the legacy day/all wrapper around UpsertWindow: it
// fills win_from = trading_day, investor = all and market = RG, so every
// existing caller keeps writing one-day all-investor rows.
func (r *BrokerStockSummaryRepository) UpsertDay(db *sqlx.DB, rows []entity.BrokerStockSummary, totals *entity.BrokerStockSummaryTotals) error {
	for i := range rows {
		if rows[i].WinFrom.IsZero() {
			rows[i].WinFrom = rows[i].TradingDay
		}
		if rows[i].Investor == "" {
			rows[i].Investor = broker.InvestorAll
		}
		if rows[i].Market == "" {
			rows[i].Market = broker.MarketRegular
		}
	}
	if totals != nil {
		if totals.WinFrom.IsZero() {
			totals.WinFrom = totals.TradingDay
		}
		if totals.Investor == "" {
			totals.Investor = broker.InvestorAll
		}
		if totals.Market == "" {
			totals.Market = broker.MarketRegular
		}
	}
	return r.UpsertWindow(db, rows, totals)
}

// UpsertWindow atomically replaces one ticker+window+investor's broker rows and
// totals in a single transaction. The key is
// (ticker, trading_day, win_from, investor): a window never deletes a day row,
// and the f lane never deletes the all lane. Rows that disappeared from a
// refetch are deleted (not accumulated), and totals are written in the same
// transaction so rows and totals cannot diverge. Idempotent.
//
// TradingDay is the window end; WinFrom is its start (equal for a day). A
// caller that leaves Source empty is seeding a legacy-shaped row (the DB tests
// do this); production writers state live, sweep, backfill, archive or window.
func (r *BrokerStockSummaryRepository) UpsertWindow(db *sqlx.DB, rows []entity.BrokerStockSummary, totals *entity.BrokerStockSummaryTotals) error {
	// The window's key comes from the rows or the totals row.
	var ticker string
	var day, winFrom time.Time
	var investor, market string
	switch {
	case len(rows) > 0:
		ticker, day, winFrom = rows[0].Ticker, rows[0].TradingDay, rows[0].WinFrom
		investor, market = rows[0].Investor, rows[0].Market
	case totals != nil:
		ticker, day, winFrom = totals.Ticker, totals.TradingDay, totals.WinFrom
		investor, market = totals.Investor, totals.Market
	default:
		return nil // nothing to write
	}

	// The key is explicit: a new writer must state its window and lane rather
	// than inheriting a silent default that could mislabel an f row as all.
	if investor != broker.InvestorAll && investor != broker.InvestorForeign {
		return fmt.Errorf("broker stock summary: upsert window %s %s: investor %q must be %q or %q",
			ticker, day.Format("2006-01-02"), investor, broker.InvestorAll, broker.InvestorForeign)
	}
	if winFrom.IsZero() {
		return fmt.Errorf("broker stock summary: upsert window %s %s: win_from is required", ticker, day.Format("2006-01-02"))
	}
	if market != broker.MarketRegular {
		return fmt.Errorf("broker stock summary: upsert window %s %s: market %q must be %q",
			ticker, day.Format("2006-01-02"), market, broker.MarketRegular)
	}
	if investor == broker.InvestorForeign && totals != nil &&
		(totals.TVal != nil || totals.TLot != nil || totals.Avg != nil) {
		return fmt.Errorf("broker stock summary: upsert window %s %s: f-lane totals must leave t_val/t_lot/avg NULL",
			ticker, day.Format("2006-01-02"))
	}
	if winFrom.After(day) {
		return fmt.Errorf("broker stock summary: upsert window %s: win_from %s is after trading_day %s",
			ticker, winFrom.Format("2006-01-02"), day.Format("2006-01-02"))
	}
	for i := range rows {
		if rows[i].Ticker != ticker || !rows[i].TradingDay.Equal(day) || !rows[i].WinFrom.Equal(winFrom) ||
			rows[i].Investor != investor || rows[i].Market != market {
			return fmt.Errorf("broker stock summary: upsert window %s: row %d (%s %s %s..%s %s) does not match the window key",
				ticker, i, rows[i].Ticker, rows[i].Investor, rows[i].WinFrom.Format("2006-01-02"),
				rows[i].TradingDay.Format("2006-01-02"), rows[i].Market)
		}
	}
	if totals != nil {
		if totals.Ticker != ticker || !totals.TradingDay.Equal(day) || !totals.WinFrom.Equal(winFrom) ||
			totals.Investor != investor || totals.Market != market {
			return fmt.Errorf("broker stock summary: upsert window %s: totals row does not match the window key", ticker)
		}
	}

	for i := range rows {
		if rows[i].Source == "" {
			// legacy_ipot is the old scraped top-10 set; an f row is never
			// that. Require the new lane to state its writer rather than
			// inheriting a provenance label that is actively false.
			if investor == broker.InvestorForeign {
				return fmt.Errorf("broker stock summary: upsert window %s %s: f-lane rows require an explicit source", ticker, day.Format("2006-01-02"))
			}
			rows[i].Source = entity.BrokerSourceLegacyIPOT
		}
	}
	if totals != nil && totals.Source == "" {
		if investor == broker.InvestorForeign {
			return fmt.Errorf("broker stock summary: upsert window %s %s: f-lane totals require an explicit source", ticker, day.Format("2006-01-02"))
		}
		totals.Source = entity.BrokerSourceLegacyIPOT
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Replace the window's rows wholesale so brokers that disappeared from a
	// refetch are removed, not kept as stale rows. Runs even when rows is empty
	// (a caller clearing a window) so rows and totals can never diverge.
	if _, err := tx.Exec(
		"DELETE FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2 AND win_from = $3 AND investor = $4",
		ticker, day, winFrom, investor,
	); err != nil {
		return err
	}

	if len(rows) > 0 {
		query, args, err := buildMultiRowUpsert(rows)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(query, args...); err != nil {
			return err
		}
	}

	if totals != nil {
		if _, err := tx.NamedExec(`
			INSERT INTO broker_stock_summary_totals (ticker, trading_day, win_from, investor, market, t_val, f_nval, t_lot, avg, source)
			VALUES (:ticker, :trading_day, :win_from, :investor, :market, :t_val, :f_nval, :t_lot, :avg, :source)
			ON CONFLICT (ticker, trading_day, win_from, investor) DO UPDATE SET
				market = EXCLUDED.market,
				t_val = EXCLUDED.t_val,
				f_nval = EXCLUDED.f_nval,
				t_lot = EXCLUDED.t_lot,
				avg = EXCLUDED.avg,
				source = EXCLUDED.source
		`, totals); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// buildMultiRowUpsert builds a single multi-row INSERT ... ON CONFLICT for a
// batch of broker rows (one round trip instead of one per row).
func buildMultiRowUpsert(rows []entity.BrokerStockSummary) (string, []interface{}, error) {
	const cols = 12 // ticker, broker_code, side, trading_day, win_from, investor, market, lot, value, avg_price, rank, source
	valueStrings := make([]string, 0, len(rows))
	args := make([]interface{}, 0, len(rows)*cols)
	for i, r := range rows {
		base := i * cols
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11, base+12,
		))
		args = append(args, r.Ticker, r.BrokerCode, r.Side, r.TradingDay, r.WinFrom, r.Investor, r.Market,
			r.Lot, r.Value, r.AvgPrice, r.Rank, r.Source)
	}
	query := fmt.Sprintf(`
		INSERT INTO broker_stock_summaries (ticker, broker_code, side, trading_day, win_from, investor, market, lot, value, avg_price, rank, source)
		VALUES %s
		ON CONFLICT (ticker, trading_day, win_from, investor, broker_code, side) DO UPDATE SET
			market = EXCLUDED.market,
			lot = EXCLUDED.lot,
			value = EXCLUDED.value,
			avg_price = EXCLUDED.avg_price,
			rank = EXCLUDED.rank,
			source = EXCLUDED.source
	`, strings.Join(valueStrings, ","))
	return query, args, nil
}

// HasStoredDay reports whether any broker_stock_summaries rows exist for a
// ticker+day. Cheap EXISTS check — the sweep's skip-if-stored guard.
// Idempotency note: rows present means the day is already covered, so the
// sweep skips the ticker entirely; a race between the sweep and an on-demand
// refetch is harmless because UpsertDay replaces wholesale.
func (r *BrokerStockSummaryRepository) HasStoredDay(db *sqlx.DB, ticker string, day time.Time) (bool, error) {
	var exists bool
	err := db.Get(&exists,
		"SELECT EXISTS(SELECT 1 FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2 AND win_from = $2 AND investor = $3)",
		ticker, day, broker.InvestorAll,
	)
	return exists, err
}

// StoredTickersForDay reports which of the given tickers already have stored
// rows for a trading day, in one query. It is the batch form of HasStoredDay:
// the sweep resolves the whole candidate list's coverage before planning, so a
// re-run of the same anchor day (manual, self-heal, or a second worker) never
// pays for a ticker-day twice.
func (r *BrokerStockSummaryRepository) StoredTickersForDay(db *sqlx.DB, day time.Time, tickers []string) (map[string]bool, error) {
	out := make(map[string]bool, len(tickers))
	if len(tickers) == 0 {
		return out, nil
	}
	var stored []string
	err := db.Select(&stored, `
		SELECT DISTINCT ticker
		FROM broker_stock_summaries
		WHERE trading_day = $1::date
		  AND win_from = $1::date
		  AND investor = $3
		  AND ticker = ANY(string_to_array($2, ','))`,
		day.Format("2006-01-02"), strings.Join(tickers, ","), broker.InvestorAll)
	if err != nil {
		return nil, err
	}
	for _, t := range stored {
		out[t] = true
	}
	return out, nil
}

// LastObservedByTicker returns, for each given ticker, the most recent trading
// day with a stored tally at or before upTo. It is the sweep's rotation key:
// tickers observed longest ago sort first, so a budget-limited sweep cycles
// through the universe instead of re-fetching the same liquid names every day.
// A ticker with no stored day is absent from the map (never observed).
func (r *BrokerStockSummaryRepository) LastObservedByTicker(db *sqlx.DB, tickers []string, upTo time.Time) (map[string]time.Time, error) {
	out := make(map[string]time.Time, len(tickers))
	if len(tickers) == 0 {
		return out, nil
	}
	var rows []struct {
		Ticker string    `db:"ticker"`
		Last   time.Time `db:"last_day"`
	}
	err := db.Select(&rows, `
		SELECT ticker, MAX(trading_day) AS last_day
		FROM broker_stock_summary_totals
		WHERE trading_day <= $1::date
		  AND win_from = trading_day
		  AND investor = $3
		  AND ticker = ANY(string_to_array($2, ','))
		GROUP BY ticker`,
		upTo.Format("2006-01-02"), strings.Join(tickers, ","), broker.InvestorAll)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[r.Ticker] = r.Last
	}
	return out, nil
}

// FindByTickerAndDay returns the stored broker rows for a ticker+day,
// ordered by side then rank.
func (r *BrokerStockSummaryRepository) FindByTickerAndDay(db *sqlx.DB, ticker string, day time.Time) ([]entity.BrokerStockSummary, error) {
	var rows []entity.BrokerStockSummary
	err := db.Select(&rows,
		"SELECT * FROM broker_stock_summaries WHERE ticker = $1 AND trading_day = $2 AND win_from = $2 AND investor = $3 ORDER BY side, rank",
		ticker, day, broker.InvestorAll,
	)
	return rows, err
}

// FindTotalsByTickerAndDay returns the stored footer summary for a ticker+day.
func (r *BrokerStockSummaryRepository) FindTotalsByTickerAndDay(db *sqlx.DB, ticker string, day time.Time) (*entity.BrokerStockSummaryTotals, error) {
	var totals entity.BrokerStockSummaryTotals
	err := db.Get(&totals,
		"SELECT * FROM broker_stock_summary_totals WHERE ticker = $1 AND trading_day = $2 AND win_from = $2 AND investor = $3",
		ticker, day, broker.InvestorAll,
	)
	if err != nil {
		return nil, err
	}
	return &totals, nil
}

// FindByTickerAndDateRange returns the stored broker rows for a ticker between
// two trading days (inclusive), ordered by trading day then side/rank. An
// empty range returns an empty slice, no error.
func (r *BrokerStockSummaryRepository) FindByTickerAndDateRange(db *sqlx.DB, ticker string, from, to time.Time) ([]entity.BrokerStockSummary, error) {
	var rows []entity.BrokerStockSummary
	err := db.Select(&rows,
		"SELECT * FROM broker_stock_summaries WHERE ticker = $1 AND trading_day BETWEEN $2 AND $3 AND win_from = trading_day AND investor = $4 ORDER BY trading_day, side, rank",
		ticker, from, to, broker.InvestorAll,
	)
	return rows, err
}

// FindByDateRangeAll returns the stored broker rows for every ticker between
// two trading days (inclusive), ordered by trading day then ticker. Feeds
// get_broker_net_flow's market-wide mode.
func (r *BrokerStockSummaryRepository) FindByDateRangeAll(db *sqlx.DB, from, to time.Time) ([]entity.BrokerStockSummary, error) {
	var rows []entity.BrokerStockSummary
	err := db.Select(&rows,
		"SELECT * FROM broker_stock_summaries WHERE trading_day BETWEEN $1 AND $2 AND win_from = trading_day AND investor = $3 ORDER BY trading_day, ticker, side, rank",
		from, to, broker.InvestorAll,
	)
	return rows, err
}

// FindTotalsByDateRangeAll returns the stored footer summaries for every
// ticker between two trading days (inclusive), ordered by trading day then
// ticker. Feeds get_sector_flow's foreign net and per-ticker observed-day
// coverage.
func (r *BrokerStockSummaryRepository) FindTotalsByDateRangeAll(db *sqlx.DB, from, to time.Time) ([]entity.BrokerStockSummaryTotals, error) {
	var totals []entity.BrokerStockSummaryTotals
	err := db.Select(&totals,
		"SELECT * FROM broker_stock_summary_totals WHERE trading_day BETWEEN $1 AND $2 AND win_from = trading_day AND investor = $3 ORDER BY trading_day, ticker",
		from, to, broker.InvestorAll,
	)
	return totals, err
}

// FindTotalsByTickerAndDateRange returns the stored footer summaries for a
// ticker between two trading days (inclusive), ordered by trading day.
func (r *BrokerStockSummaryRepository) FindTotalsByTickerAndDateRange(db *sqlx.DB, ticker string, from, to time.Time) ([]entity.BrokerStockSummaryTotals, error) {
	var totals []entity.BrokerStockSummaryTotals
	err := db.Select(&totals,
		"SELECT * FROM broker_stock_summary_totals WHERE ticker = $1 AND trading_day BETWEEN $2 AND $3 AND win_from = trading_day AND investor = $4 ORDER BY trading_day",
		ticker, from, to, broker.InvestorAll,
	)
	return totals, err
}

// TickerForeignNet is one ticker's stored foreign net over a flow window, with
// the day count that makes it readable: Days is how many of the window's
// trading days had a stored foreign net at all. A ticker with no stored broker
// rows is absent from the result rather than present with a zero — "not
// observed" and "net zero" are different answers, and the screener renders them
// differently.
type TickerForeignNet struct {
	Ticker     string `db:"ticker"`
	ForeignNet int64  `db:"foreign_net"`
	Days       int    `db:"days"`
}

// ForeignNetWindow is the window a foreign-net read actually covered: its first
// stored trading day and how many market-wide trading days it spans. The
// denominator is what makes a row's day count interpretable ("7 of 20") instead
// of an unanchored number, so a caller can tell a thin window from a thin
// ticker.
type ForeignNetWindow struct {
	From      time.Time
	TradeDays int
	Rows      []TickerForeignNet
}

// BrokerWindowSpan is one stored window's key and range for a ticker, as the
// interval-cover solver sees it. Source is the writer's provenance; FNVal is the
// window's stored foreign net (nil when unobserved).
type BrokerWindowSpan struct {
	Ticker     string    `db:"ticker"`
	WinFrom    time.Time `db:"win_from"`
	TradingDay time.Time `db:"trading_day"`
	Source     string    `db:"source"`
	FNVal      *int64    `db:"f_nval"`
}

// Span is the window's range, as the pure cover solver consumes it.
func (s BrokerWindowSpan) Span() broker.Span {
	return broker.Span{From: s.WinFrom, To: s.TradingDay}
}

// BrokerAggregatedRow is one broker+side summed over a set of disjoint windows.
// Rank is deliberately absent: it is recomputed by the read projection after
// composition, because per-window ranks are not comparable.
type BrokerAggregatedRow struct {
	BrokerCode string `db:"broker_code"`
	Side       string `db:"side"`
	Lot        int64  `db:"lot"`
	Value      int64  `db:"value"`
}

// FindWindowSpans returns the stored windows overlapping [from, to] for the
// given tickers in one investor lane, excluding the legacy top-10 rows (which
// are never read). The solver is what decides whether a span is usable; this is
// the candidate set.
func (r *BrokerStockSummaryRepository) FindWindowSpans(db *sqlx.DB, tickers []string, investor string, from, to time.Time) ([]BrokerWindowSpan, error) {
	if len(tickers) == 0 {
		return nil, nil
	}
	var spans []BrokerWindowSpan
	err := db.Select(&spans, `
		SELECT ticker, win_from, trading_day, source, f_nval
		FROM broker_stock_summary_totals
		WHERE ticker = ANY(string_to_array($1, ','))
		  AND investor = $2
		  AND source <> 'legacy_ipot'
		  AND win_from <= $4::date
		  AND trading_day >= $3::date
		ORDER BY ticker, win_from, trading_day`,
		strings.Join(tickers, ","), investor, from.Format("2006-01-02"), to.Format("2006-01-02"))
	return spans, err
}

// FindAllWindowSpans is FindWindowSpans without a ticker filter — the
// market-wide read behind get_broker_net_flow and get_sector_flow.
func (r *BrokerStockSummaryRepository) FindAllWindowSpans(db *sqlx.DB, investor string, from, to time.Time) ([]BrokerWindowSpan, error) {
	var spans []BrokerWindowSpan
	err := db.Select(&spans, `
		SELECT ticker, win_from, trading_day, source, f_nval
		FROM broker_stock_summary_totals
		WHERE investor = $1
		  AND source <> 'legacy_ipot'
		  AND win_from <= $3::date
		  AND trading_day >= $2::date
		ORDER BY ticker, win_from, trading_day`,
		investor, from.Format("2006-01-02"), to.Format("2006-01-02"))
	return spans, err
}

// FindRowsInWindows returns the stored per-broker rows overlapping [from, to]
// for the given tickers and lane. Callers filter the result to the spans the
// cover solver selected; one read serves both single-ticker and market-wide
// aggregation without a per-window round trip.
func (r *BrokerStockSummaryRepository) FindRowsInWindows(db *sqlx.DB, tickers []string, investor string, from, to time.Time) ([]entity.BrokerStockSummary, error) {
	if len(tickers) == 0 {
		return nil, nil
	}
	var rows []entity.BrokerStockSummary
	err := db.Select(&rows, `
		SELECT * FROM broker_stock_summaries
		WHERE ticker = ANY(string_to_array($1, ','))
		  AND investor = $2
		  AND source <> 'legacy_ipot'
		  AND win_from <= $4::date
		  AND trading_day >= $3::date
		ORDER BY ticker, win_from, trading_day, side, rank`,
		strings.Join(tickers, ","), investor, from.Format("2006-01-02"), to.Format("2006-01-02"))
	return rows, err
}

// FindAllRowsInWindows is FindRowsInWindows without a ticker filter.
func (r *BrokerStockSummaryRepository) FindAllRowsInWindows(db *sqlx.DB, investor string, from, to time.Time) ([]entity.BrokerStockSummary, error) {
	var rows []entity.BrokerStockSummary
	err := db.Select(&rows, `
		SELECT * FROM broker_stock_summaries
		WHERE investor = $1
		  AND source <> 'legacy_ipot'
		  AND win_from <= $3::date
		  AND trading_day >= $2::date
		ORDER BY ticker, win_from, trading_day, side, rank`,
		investor, from.Format("2006-01-02"), to.Format("2006-01-02"))
	return rows, err
}

// FindTotalsInWindows returns the stored footer summaries overlapping [from, to]
// for the given tickers and lane (legacy rows excluded).
func (r *BrokerStockSummaryRepository) FindTotalsInWindows(db *sqlx.DB, tickers []string, investor string, from, to time.Time) ([]entity.BrokerStockSummaryTotals, error) {
	if len(tickers) == 0 {
		return nil, nil
	}
	var totals []entity.BrokerStockSummaryTotals
	err := db.Select(&totals, `
		SELECT * FROM broker_stock_summary_totals
		WHERE ticker = ANY(string_to_array($1, ','))
		  AND investor = $2
		  AND source <> 'legacy_ipot'
		  AND win_from <= $4::date
		  AND trading_day >= $3::date
		ORDER BY ticker, win_from, trading_day`,
		strings.Join(tickers, ","), investor, from.Format("2006-01-02"), to.Format("2006-01-02"))
	return totals, err
}

// FindAllTotalsInWindows is FindTotalsInWindows without a ticker filter.
func (r *BrokerStockSummaryRepository) FindAllTotalsInWindows(db *sqlx.DB, investor string, from, to time.Time) ([]entity.BrokerStockSummaryTotals, error) {
	var totals []entity.BrokerStockSummaryTotals
	err := db.Select(&totals, `
		SELECT * FROM broker_stock_summary_totals
		WHERE investor = $1
		  AND source <> 'legacy_ipot'
		  AND win_from <= $3::date
		  AND trading_day >= $2::date
		ORDER BY ticker, win_from, trading_day`,
		investor, from.Format("2006-01-02"), to.Format("2006-01-02"))
	return totals, err
}

// FindLatestWindowBefore returns the most recent stored window ending before
// `before` for one ticker and lane. It is the publish-lag fallback's stored-only
// probe: an empty day falls back to what is already stored, never to an upstream
// fetch that would spend quota rediscovering it.
func (r *BrokerStockSummaryRepository) FindLatestWindowBefore(db *sqlx.DB, ticker, investor string, before time.Time) (*entity.BrokerStockSummaryTotals, error) {
	var totals entity.BrokerStockSummaryTotals
	err := db.Get(&totals, `
		SELECT * FROM broker_stock_summary_totals
		WHERE ticker = $1 AND investor = $2 AND source <> 'legacy_ipot'
		  AND trading_day < $3::date
		ORDER BY trading_day DESC, win_from DESC
		LIMIT 1`,
		ticker, investor, before.Format("2006-01-02"))
	if err != nil {
		return nil, err
	}
	return &totals, nil
}

// FindStoredWindow returns one exact stored window's footer summary plus rows.
// It is the zero-unit path of get_stock_broker_summary: an exact one-day window
// answers without touching the source.
func (r *BrokerStockSummaryRepository) FindStoredWindow(db *sqlx.DB, ticker, investor string, winFrom, day time.Time) (*entity.BrokerStockSummaryTotals, []entity.BrokerStockSummary, error) {
	var totals entity.BrokerStockSummaryTotals
	err := db.Get(&totals, `
		SELECT * FROM broker_stock_summary_totals
		WHERE ticker = $1 AND investor = $2 AND win_from = $3::date AND trading_day = $4::date
		  AND source <> 'legacy_ipot'`,
		ticker, investor, winFrom.Format("2006-01-02"), day.Format("2006-01-02"))
	if err != nil {
		return nil, nil, err
	}
	var rows []entity.BrokerStockSummary
	if err := db.Select(&rows, `
		SELECT * FROM broker_stock_summaries
		WHERE ticker = $1 AND investor = $2 AND win_from = $3::date AND trading_day = $4::date
		ORDER BY side, rank`,
		ticker, investor, winFrom.Format("2006-01-02"), day.Format("2006-01-02")); err != nil {
		return nil, nil, err
	}
	return &totals, rows, nil
}

// SumRowsInSpans sums one ticker's per-broker value and lot over the given
// disjoint windows in SQL. Composition never reads the same day twice: the
// caller passes the solver's selection, which is overlap-free by construction.
// Avg price is not returned — it is a window VWAP and must be derived from the
// summed value and lot, never summed.
func (r *BrokerStockSummaryRepository) SumRowsInSpans(db *sqlx.DB, ticker, investor string, spans []broker.Span) ([]BrokerAggregatedRow, error) {
	if len(spans) == 0 {
		return nil, nil
	}
	preds := make([]string, 0, len(spans))
	args := []interface{}{ticker, investor}
	for _, s := range spans {
		base := len(args)
		preds = append(preds, fmt.Sprintf("(win_from = $%d::date AND trading_day = $%d::date)", base+1, base+2))
		args = append(args, s.From.Format("2006-01-02"), s.To.Format("2006-01-02"))
	}
	query := fmt.Sprintf(`
		SELECT broker_code, side,
			COALESCE(SUM(lot), 0)::bigint AS lot,
			COALESCE(SUM(value), 0)::bigint AS value
		FROM broker_stock_summaries
		WHERE ticker = $1 AND investor = $2 AND (%s)
		GROUP BY broker_code, side
		ORDER BY side, value DESC, broker_code`, strings.Join(preds, " OR "))

	var rows []BrokerAggregatedRow
	err := db.Select(&rows, query, args...)
	return rows, err
}

// MaxWindowEnd returns the newest stored window end in one lane, or sql.ErrNoRows
// when nothing is stored. It is the live Source Status high-water read: the
// newest window end, not the last HTTP success, is what "as fresh as" means.
func (r *BrokerStockSummaryRepository) MaxWindowEnd(db *sqlx.DB, investor string) (*time.Time, error) {
	var day sql.NullTime
	err := db.Get(&day, `
		SELECT MAX(trading_day) FROM broker_stock_summary_totals
		WHERE investor = $1 AND source <> 'legacy_ipot'`,
		investor)
	if err != nil {
		return nil, err
	}
	if !day.Valid {
		return nil, sql.ErrNoRows
	}
	return &day.Time, nil
}

// SumForeignNetByTickers sums the stored foreign net (f_nval) of the footer
// totals for each given ticker over the `tradingDays` most recent market-wide
// trading days at or before `to`. It is the screener's broker-flow column and
// Broker Confirmation's read.
//
// A ticker is answered only when its stored windows *exactly cover* that
// trading-day window (the interval-cover rule): windows outside the request are
// unusable because aggregation cannot be decomposed, and overlapping windows
// can never be summed. A partial cover is not partially composed — the ticker is
// absent from Rows and renders unobserved. A stored footer whose f_nval is null
// cannot witness a net either, so it does not participate.
func (r *BrokerStockSummaryRepository) SumForeignNetByTickers(db *sqlx.DB, tickers []string, to time.Time, tradingDays int) (*ForeignNetWindow, error) {
	window := &ForeignNetWindow{Rows: []TickerForeignNet{}}

	// The trading days come from the market-wide daily_prices calendar, so the
	// denominator counts days the market actually had, not days the sparsely
	// covered broker table happens to have.
	var days []time.Time
	err := db.Select(&days, `
		SELECT trading_day FROM (
			SELECT DISTINCT trading_day FROM daily_prices
			WHERE trading_day <= $1::date
			ORDER BY trading_day DESC
			LIMIT $2
		) recent
		ORDER BY trading_day`,
		to.Format("2006-01-02"), tradingDays)
	if err != nil {
		return nil, err
	}
	if len(days) == 0 {
		// No stored trading day at or before `to` — nothing to sum over, and the
		// caller's rows are all unobserved. Not an error: an empty market is a
		// legitimate (if empty) answer.
		return window, nil
	}
	window.From = days[0]
	window.TradeDays = len(days)

	if len(tickers) == 0 {
		return window, nil
	}

	spans, err := r.FindWindowSpans(db, tickers, broker.InvestorForeign, window.From, to)
	if err != nil {
		return nil, err
	}
	byTicker := make(map[string][]BrokerWindowSpan, len(tickers))
	for _, s := range spans {
		if s.FNVal == nil {
			continue // an unobserved net cannot witness one
		}
		byTicker[s.Ticker] = append(byTicker[s.Ticker], s)
	}

	for ticker, candidates := range byTicker {
		spansOnly := make([]broker.Span, 0, len(candidates))
		nets := make(map[string]int64, len(candidates))
		for _, s := range candidates {
			p := s.Span()
			spansOnly = append(spansOnly, p)
			nets[s.WinFrom.Format("2006-01-02")+"|"+s.TradingDay.Format("2006-01-02")] = *s.FNVal
		}
		selected, ok := broker.ExactCover(spansOnly, window.From, to, days)
		if !ok {
			continue
		}
		var net int64
		for _, s := range selected {
			net += nets[s.From.Format("2006-01-02")+"|"+s.To.Format("2006-01-02")]
		}
		window.Rows = append(window.Rows, TickerForeignNet{Ticker: ticker, ForeignNet: net, Days: len(days)})
	}
	sort.Slice(window.Rows, func(i, j int) bool { return window.Rows[i].Ticker < window.Rows[j].Ticker })
	return window, nil
}

// DeleteOlderThan deletes broker_stock_summaries rows whose window end is
// older than the retention window. Returns rows deleted. Ticket 10c replaces
// this blanket delete with day-row expiry that exempts archive chunks and the
// all lane; 10a only writes one-day rows, so the existing behaviour is safe
// until then.
func (r *BrokerStockSummaryRepository) DeleteOlderThan(db *sqlx.DB, days int) (int64, error) {
	res, err := db.Exec(
		"DELETE FROM broker_stock_summaries WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DeleteTotalsOlderThan deletes broker_stock_summary_totals rows whose
// trading_day is older than the retention window. Returns rows deleted.
func (r *BrokerStockSummaryRepository) DeleteTotalsOlderThan(db *sqlx.DB, days int) (int64, error) {
	res, err := db.Exec(
		"DELETE FROM broker_stock_summary_totals WHERE trading_day < NOW() - make_interval(days => $1)",
		days,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
