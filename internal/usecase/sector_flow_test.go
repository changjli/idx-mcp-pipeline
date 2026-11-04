package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Sector-flow tests run over a future window (2027-03) so the seeded world is
// self-contained: no real pipeline rows share it, which keeps the market-wide
// reconciliation assertions deterministic.
var (
	sfDay1 = time.Date(2027, 3, 15, 0, 0, 0, 0, time.UTC)
	sfDay2 = time.Date(2027, 3, 16, 0, 0, 0, 0, time.UTC)
)

func newSectorFlowTestUC(t *testing.T, db *sqlx.DB) *SectorFlowUseCase {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return NewSectorFlowUseCase(
		db, log,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
		repository.NewTickerRepository(log),
	)
}

// seedSectorTicker writes a ticker's sector labels + a trading day. Empty
// label strings are written as NULL (an unclassified ticker).
func seedSectorTicker(t *testing.T, db *sqlx.DB, ticker string, day time.Time, sektor, subSector, industry string) {
	t.Helper()
	db.MustExec(`
		INSERT INTO tickers (code, name, sektor, sub_sektor, industri, active)
		VALUES ($1, $1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), true)
		ON CONFLICT (code) DO UPDATE SET
			sektor = EXCLUDED.sektor, sub_sektor = EXCLUDED.sub_sektor,
			industri = EXCLUDED.industri, active = true`,
		ticker, sektor, subSector, industry)
	db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)
		VALUES ($1, $2, 100, 101, 99, 100, 1000, 100000, 10, 'idx')
		ON CONFLICT (ticker, trading_day) DO NOTHING`, ticker, day)
}

// seedSectorDay writes one ticker+day's stored foreign rows plus its footer
// totals (the foreign net the sector aggregation reads).
func seedSectorDay(t *testing.T, db *sqlx.DB, repo *repository.BrokerStockSummaryRepository, ticker string, day time.Time, foreignNet int64, rows ...entity.BrokerStockSummary) {
	t.Helper()
	for i := range rows {
		rows[i].WinFrom = day
		rows[i].Investor = broker.InvestorForeign
		rows[i].Market = broker.MarketRegular
		if rows[i].Source == "" {
			rows[i].Source = entity.BrokerSourceLive
		}
	}
	totals := &entity.BrokerStockSummaryTotals{
		Ticker: ticker, TradingDay: day, WinFrom: day,
		Investor: broker.InvestorForeign, Market: broker.MarketRegular,
		FNVal: i64p(foreignNet), Source: entity.BrokerSourceLive,
	}
	if err := repo.UpsertWindow(db, rows, totals); err != nil {
		t.Fatalf("seedSectorDay %s %s: %v", ticker, day.Format("2006-01-02"), err)
	}
}

func cleanupSectorTickers(t *testing.T, db *sqlx.DB, tickers ...string) {
	t.Helper()
	t.Cleanup(func() {
		for _, tk := range tickers {
			db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", tk)
			db.MustExec("DELETE FROM tickers WHERE code = $1", tk)
		}
	})
}

func findSectorRow(t *testing.T, rows []SectorFlowRow, sector string) SectorFlowRow {
	t.Helper()
	for _, r := range rows {
		if r.Sector == sector {
			return r
		}
	}
	t.Fatalf("sector %q not in rows: %+v", sector, rows)
	return SectorFlowRow{}
}

// TestGetSectorFlow_GroupsAndReconciles — the core read: two tickers in two
// sectors over two days, grouped by sektor. Per-sector listed net, per-ticker
// tail, foreign net, coverage, top-broker breakdown — and the ticket's sanity
// check: the sector listed nets sum back to market-wide get_broker_net_flow
// over the same window (no row dropped, none double-counted).
func TestGetSectorFlow_GroupsAndReconciles(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newSectorFlowTestUC(t, db)
	netFlowUC := newHistoryTestUC(t, db)

	ta, tb := "TESTA", "TESTB"
	cleanupSectorTickers(t, db, ta, tb)
	seedSectorTicker(t, db, ta, sfDay1, "Financials", "Banks", "Banks")
	seedSectorTicker(t, db, ta, sfDay2, "Financials", "Banks", "Banks")
	seedSectorTicker(t, db, tb, sfDay1, "Energy", "Oil & Gas", "Oil & Gas")
	seedSectorTicker(t, db, tb, sfDay2, "Energy", "Oil & Gas", "Oil & Gas")

	// Financials: TESTA — AK accumulates both days, XL sells day 1.
	seedSectorDay(t, db, uc.BrokerRepo, ta, sfDay1, -2_000_000_000,
		nfRow(ta, "AK", "buy", sfDay1, nfBuy10), nfRow(ta, "XL", "sell", sfDay1, nfSell6))
	seedSectorDay(t, db, uc.BrokerRepo, ta, sfDay2, 1_000_000_000,
		nfRow(ta, "AK", "buy", sfDay2, nfSell4))
	// Energy: TESTB — rows only on day 1 (day 2 is a coverage gap).
	seedSectorDay(t, db, uc.BrokerRepo, tb, sfDay1, 2_000_000_000,
		nfRow(tb, "AK", "buy", sfDay1, nfSell7), nfRow(tb, "MN", "sell", sfDay1, nfSell3))

	res, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay2})
	if err != nil {
		t.Fatalf("GetSectorFlow: %v", err)
	}

	if res.GroupBy != "sektor" {
		t.Errorf("group_by = %q, want sektor (default)", res.GroupBy)
	}
	if res.From != "2027-03-15" || res.To != "2027-03-16" {
		t.Errorf("window = %s..%s, want 2027-03-15..2027-03-16", res.From, res.To)
	}
	if res.TradeDaysInWindow != 2 || res.CoveredDays != 2 {
		t.Errorf("coverage = %d/%d, want 2/2", res.CoveredDays, res.TradeDaysInWindow)
	}
	if res.TickersCovered != 2 {
		t.Errorf("tickers_covered = %d, want 2", res.TickersCovered)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d, want 2: %+v", len(res.Rows), res.Rows)
	}

	fin := findSectorRow(t, res.Rows, "Financials")
	// Gross buy/sell over the full broker set; no aggregate net by design.
	if fin.Buy != nfBuy10+nfSell4 || fin.Sell != nfSell6 {
		t.Errorf("Financials buy/sell = %d/%d, want 14B/6B", fin.Buy, fin.Sell)
	}
	if deref(fin.ForeignNet) != -1_000_000_000 || fin.ForeignNetDays != 2 {
		t.Errorf("Financials foreign_net = %v over %d days, want −1B over 2", fin.ForeignNet, fin.ForeignNetDays)
	}
	if fin.TickersCovered != 1 || fin.DaysCovered != 2 {
		t.Errorf("Financials coverage = %d tickers/%d days, want 1/2", fin.TickersCovered, fin.DaysCovered)
	}
	if fin.BrokersCount != 2 || len(fin.TopBrokers) != 2 {
		t.Fatalf("Financials brokers = %d/top %d, want 2/2", fin.BrokersCount, len(fin.TopBrokers))
	}
	if fin.TopBrokers[0].BrokerCode != "AK" || fin.TopBrokers[0].Net != nfBuy10+nfSell4 {
		t.Errorf("Financials top_brokers[0] = %+v, want AK net 14B (accumulator first)", fin.TopBrokers[0])
	}
	if fin.TopBrokers[1].BrokerCode != "XL" || fin.TopBrokers[1].Net != -nfSell6 {
		t.Errorf("Financials top_brokers[1] = %+v, want XL net −6B", fin.TopBrokers[1])
	}

	energy := findSectorRow(t, res.Rows, "Energy")
	if energy.Buy != nfSell7 || energy.Sell != nfSell3 {
		t.Errorf("Energy = %+v, want buy 7B sell 3B", energy)
	}
	if deref(energy.ForeignNet) != 2_000_000_000 || energy.ForeignNetDays != 1 {
		t.Errorf("Energy foreign_net = %v over %d days, want 2B over 1", energy.ForeignNet, energy.ForeignNetDays)
	}
	if energy.TickersCovered != 1 || energy.DaysCovered != 1 {
		t.Errorf("Energy coverage = %d tickers/%d days, want 1/1 (day 2 gap)", energy.TickersCovered, energy.DaysCovered)
	}
	// Rows are ordered foreign_net desc (unobserved last): Energy +2B > Financials −1B.
	if res.Rows[0].Sector != "Energy" {
		t.Errorf("rows[0] = %s, want Energy (top foreign net first)", res.Rows[0].Sector)
	}

	// Reconciliation: every stored row lands in exactly one sector bucket, and
	// the per-sector foreign nets sum to the response.
	market, err := netFlowUC.GetBrokerNetFlow(context.Background(), nil, &sfDay1, &sfDay2)
	if err != nil {
		t.Fatalf("GetBrokerNetFlow: %v", err)
	}
	var marketListedNet, sectorListedNet, sectorForeign int64
	for _, r := range market.Rows {
		marketListedNet += r.Net
	}
	for _, r := range res.Rows {
		sectorListedNet += r.Buy - r.Sell
		sectorForeign += deref(r.ForeignNet)
	}
	if sectorListedNet != marketListedNet {
		t.Errorf("sector listed net = %d, market-wide = %d — rows dropped or double-counted", sectorListedNet, marketListedNet)
	}
	if sectorForeign != deref(res.ForeignNet) || res.ForeignNetDays != 3 {
		t.Errorf("per-sector foreign %d / %d days != response %v / %d", sectorForeign, res.ForeignNetDays, res.ForeignNet, res.ForeignNetDays)
	}
}

// TestGetSectorFlow_FullSetReconcilesAndOrdersByForeignNet — with every broker
// stored, each ticker-day's buy and sell sides are equal, so a sector reconciles
// on gross buy/sell and carries no aggregate net. Ordering leads with foreign
// net (unobserved last), then gross buy.
func TestGetSectorFlow_FullSetReconcilesAndOrdersByForeignNet(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newSectorFlowTestUC(t, db)

	tq, tr, ts, tu := "TESTQ", "TESTR", "TESTS", "TESTU"
	cleanupSectorTickers(t, db, tq, tr, ts, tu)
	seedSectorTicker(t, db, tq, sfDay1, "Financials", "Banks", "Banks")
	seedSectorTicker(t, db, tr, sfDay1, "Energy", "Oil & Gas", "Oil & Gas")
	seedSectorTicker(t, db, ts, sfDay1, "Health", "Pharma", "Pharma")
	seedSectorTicker(t, db, tu, sfDay1, "Tech", "Software", "Software")

	seedFullSetDay := func(ticker string, value, foreign int64) {
		t.Helper()
		seedSectorDay(t, db, uc.BrokerRepo, ticker, sfDay1, foreign,
			nfRow(ticker, "AK", "buy", sfDay1, value),
			nfRow(ticker, "XL", "sell", sfDay1, value))
	}
	seedFullSetDay(tq, 10_000_000_000, 2_000_000_000)
	seedFullSetDay(tr, 8_000_000_000, -1_000_000_000)
	seedFullSetDay(ts, 9_000_000_000, 0)
	seedFullSetDay(tu, 4_000_000_000, 0)
	// TESTS is a stored ticker-day with no observed foreign net.
	db.MustExec("UPDATE broker_stock_summary_totals SET f_nval = NULL WHERE ticker = $1", ts)

	res, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1})
	if err != nil {
		t.Fatalf("GetSectorFlow: %v", err)
	}
	if len(res.Rows) != 4 {
		t.Fatalf("rows = %+v, want 4 sectors", res.Rows)
	}
	for _, row := range res.Rows {
		if row.Buy != row.Sell {
			t.Errorf("%s buy/sell = %d/%d, want equal over the full broker set", row.Sector, row.Buy, row.Sell)
		}
	}
	// foreign_net desc, unobserved last: Financials +2B, Tech 0, Energy −1B, Health nil.
	wantOrder := []string{"Financials", "Tech", "Energy", "Health"}
	for i, want := range wantOrder {
		if res.Rows[i].Sector != want {
			t.Fatalf("rows[%d] = %s, want %s (order %v): %+v", i, res.Rows[i].Sector, want, wantOrder, res.Rows)
		}
	}
	health := findSectorRow(t, res.Rows, "Health")
	if health.ForeignNet != nil || health.ForeignNetDays != 0 {
		t.Errorf("Health foreign = %v/%d days, want nil/0 (never zero-imputed)", health.ForeignNet, health.ForeignNetDays)
	}
	if res.ForeignNet == nil || *res.ForeignNet != 1_000_000_000 || res.ForeignNetDays != 3 {
		t.Errorf("response foreign = %v over %d days, want +1B over 3", res.ForeignNet, res.ForeignNetDays)
	}
}

// TestGetSectorFlow_FilterAndGroupBy — group_by=sub_sector regroups the same
// rows, and the sector filter is a case-insensitive exact match on the grouped
// label that returns only the matching row (no match → empty, not an error).
func TestGetSectorFlow_FilterAndGroupBy(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newSectorFlowTestUC(t, db)

	ta, tb := "TESTA", "TESTB"
	cleanupSectorTickers(t, db, ta, tb)
	seedSectorTicker(t, db, ta, sfDay1, "Financials", "Banks", "Financials")
	seedSectorTicker(t, db, tb, sfDay1, "Financials", "Financing Service", "Financials")
	seedSectorDay(t, db, uc.BrokerRepo, ta, sfDay1, 0, nfRow(ta, "AK", "buy", sfDay1, nfBuy10))
	seedSectorDay(t, db, uc.BrokerRepo, tb, sfDay1, 0, nfRow(tb, "AK", "buy", sfDay1, nfBuy8))

	res, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1, GroupBy: "sub_sector"})
	if err != nil {
		t.Fatalf("GetSectorFlow sub_sector: %v", err)
	}
	if res.GroupBy != "sub_sector" || len(res.Rows) != 2 {
		t.Fatalf("group_by = %q rows = %d, want sub_sector/2: %+v", res.GroupBy, len(res.Rows), res.Rows)
	}
	if got := findSectorRow(t, res.Rows, "Financing Service").Buy; got != nfBuy8 {
		t.Errorf("Financing Service buy = %d, want 8B", got)
	}

	// Same taxonomy at industry level merges both into "Financials".
	res, err = uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1, GroupBy: "industry"})
	if err != nil {
		t.Fatalf("GetSectorFlow industry: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0].Sector != "Financials" || res.Rows[0].Buy != nfBuy10+nfBuy8 {
		t.Errorf("industry rows = %+v, want one Financials row buy 18B", res.Rows)
	}

	// Filter: case-insensitive exact match on the grouped label.
	filter := "banks"
	res, err = uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1, GroupBy: "sub_sector", Sector: &filter})
	if err != nil {
		t.Fatalf("GetSectorFlow filtered: %v", err)
	}
	if res.Sector != "banks" {
		t.Errorf("sector echo = %q, want banks", res.Sector)
	}
	if len(res.Rows) != 1 || res.Rows[0].Sector != "Banks" {
		t.Fatalf("filtered rows = %+v, want only Banks", res.Rows)
	}

	none := "Not A Sector"
	res, err = uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1, Sector: &none})
	if err != nil {
		t.Fatalf("no-match filter must not error, got: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("no-match rows = %+v, want empty", res.Rows)
	}
}

// TestGetSectorFlow_Unclassified — a ticker with NULL sector labels buckets as
// UNCLASSIFIED so its flow still reconciles instead of vanishing from the read.
func TestGetSectorFlow_Unclassified(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newSectorFlowTestUC(t, db)

	tk := "TESTC"
	cleanupSectorTickers(t, db, tk)
	seedSectorTicker(t, db, tk, sfDay1, "", "", "")
	seedSectorDay(t, db, uc.BrokerRepo, tk, sfDay1, 0, nfRow(tk, "AK", "buy", sfDay1, nfBuy2))

	res, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1})
	if err != nil {
		t.Fatalf("GetSectorFlow: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %+v, want one UNCLASSIFIED row", res.Rows)
	}
	if res.Rows[0].Sector != "UNCLASSIFIED" || res.Rows[0].Buy != nfBuy2 {
		t.Errorf("row = %+v, want UNCLASSIFIED buy 2B", res.Rows[0])
	}
}

// TestGetSectorFlow_EmptyWindow — no stored rows in the window: empty rows +
// coverage 0, not an error (matches get_broker_net_flow's empty contract).
func TestGetSectorFlow_EmptyWindow(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newSectorFlowTestUC(t, db)

	tk := "TESTD"
	cleanupSectorTickers(t, db, tk)
	seedSectorTicker(t, db, tk, sfDay1, "Financials", "Banks", "Banks")

	res, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay2})
	if err != nil {
		t.Fatalf("empty window must not error, got: %v", err)
	}
	if res.Rows == nil || len(res.Rows) != 0 {
		t.Fatalf("rows = %v, want empty non-nil array", res.Rows)
	}
	if res.CoveredDays != 0 || res.TradeDaysInWindow != 1 || res.ForeignNet != nil {
		t.Errorf("empty window = covered %d/%d foreign %v, want 0/1/nil", res.CoveredDays, res.TradeDaysInWindow, res.ForeignNet)
	}
}

// Validation-path tests below touch no DB (the checks run before any query).
func TestGetSectorFlow_InvalidGroupBy(t *testing.T) {
	uc := &SectorFlowUseCase{}
	_, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{GroupBy: "ticker"})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestGetSectorFlow_InvalidBreakdown(t *testing.T) {
	uc := &SectorFlowUseCase{}
	_, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{Breakdown: "sector"})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got %v", err)
	}
}

// seedBreakdownWorld seeds one sector (Financials) with four tickers and two
// brokers, so both breakdown shapes can be asserted from the same world:
//
//	AK buy 10B TA, 8B TB, 2B TD, sell 4B TC  → net 16B, 4 tickers, days 1
//	YP sell 6B TA                            → net −6B, 1 ticker
//	TA footer: foreign −2B
//
// AK's by_ticker ordering (net desc) is TA +10B, TB +8B, TD +2B, TC −4B, so a
// 3-entry cap must drop TC while tickers_count stays 4.
func seedBreakdownWorld(t *testing.T, db *sqlx.DB, uc *SectorFlowUseCase) {
	t.Helper()
	ta, tb, tc, td := "TESTA", "TESTB", "TESTC", "TESTD"
	cleanupSectorTickers(t, db, ta, tb, tc, td)
	for _, tk := range []string{ta, tb, tc, td} {
		seedSectorTicker(t, db, tk, sfDay1, "Financials", "Banks", "Financials")
	}
	seedSectorDay(t, db, uc.BrokerRepo, ta, sfDay1, -2_000_000_000,
		nfRow(ta, "AK", "buy", sfDay1, nfBuy10), nfRow(ta, "YP", "sell", sfDay1, nfSell6))
	seedSectorDay(t, db, uc.BrokerRepo, tb, sfDay1, 0, nfRow(tb, "AK", "buy", sfDay1, nfBuy8))
	seedSectorDay(t, db, uc.BrokerRepo, tc, sfDay1, 0, nfRow(tc, "AK", "sell", sfDay1, nfSell4))
	seedSectorDay(t, db, uc.BrokerRepo, td, sfDay1, 0, nfRow(td, "AK", "buy", sfDay1, nfBuy2))
}

// TestGetSectorFlow_BreakdownBroker — shape A: each sector top_broker carries
// its per-ticker detail, capped, with tickers_count declaring the true breadth.
func TestGetSectorFlow_BreakdownBroker(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newSectorFlowTestUC(t, db)
	seedBreakdownWorld(t, db, uc)

	res, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1, Breakdown: "broker"})
	if err != nil {
		t.Fatalf("GetSectorFlow: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %+v, want one sector", res.Rows)
	}
	fin := res.Rows[0]
	if fin.Sector != "Financials" || fin.Buy != nfBuy10+nfBuy8+nfBuy2 || fin.Sell != nfSell6+nfSell4 {
		t.Fatalf("sector row = %+v, want Financials buy 20B sell 10B", fin)
	}
	if fin.TopTickers != nil {
		t.Errorf("top_tickers = %+v, want nil under breakdown=broker", fin.TopTickers)
	}
	if len(fin.TopBrokers) != 2 {
		t.Fatalf("top_brokers = %+v, want AK + YP", fin.TopBrokers)
	}

	ak := fin.TopBrokers[0]
	if ak.BrokerCode != "AK" || ak.Net != nfBuy10+nfBuy8+nfBuy2-nfSell4 {
		t.Fatalf("top_brokers[0] = %+v, want AK net 16B", ak)
	}
	if ak.TickersCount != 4 {
		t.Errorf("AK tickers_count = %d, want 4", ak.TickersCount)
	}
	if len(ak.ByTicker) != 3 {
		t.Fatalf("AK by_ticker = %d entries, want 3 (cap): %+v", len(ak.ByTicker), ak.ByTicker)
	}
	if ak.ByTicker[0].Ticker != "TESTA" || ak.ByTicker[0].Net != nfBuy10 || ak.ByTicker[0].DaysShown != 1 {
		t.Errorf("AK by_ticker[0] = %+v, want TESTA net 10B days 1", ak.ByTicker[0])
	}
	if ak.ByTicker[2].Ticker != "TESTD" || ak.ByTicker[2].Net != nfBuy2 {
		t.Errorf("AK by_ticker[2] = %+v, want TESTD net 2B (TC −4B falls past the cap)", ak.ByTicker[2])
	}

	yp := fin.TopBrokers[1]
	if yp.BrokerCode != "YP" || yp.TickersCount != 1 || len(yp.ByTicker) != 1 || yp.ByTicker[0].Ticker != "TESTA" {
		t.Errorf("top_brokers[1] = %+v, want YP net −6B on TESTA only", yp)
	}

	// Default (no breakdown) must stay byte-identical to the shipped shape.
	plain, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1})
	if err != nil {
		t.Fatalf("GetSectorFlow default: %v", err)
	}
	if plain.Rows[0].TopBrokers[0].ByTicker != nil || plain.Rows[0].TopTickers != nil {
		t.Errorf("default breakdown leaked nesting: %+v", plain.Rows[0])
	}
}

// TestGetSectorFlow_BreakdownTicker — shape B: the sector carries per-ticker
// rows with the tail/foreign net the broker rows can't attribute, each with its
// own top brokers.
func TestGetSectorFlow_BreakdownTicker(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newSectorFlowTestUC(t, db)
	seedBreakdownWorld(t, db, uc)

	res, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay1, To: &sfDay1, Breakdown: "ticker"})
	if err != nil {
		t.Fatalf("GetSectorFlow: %v", err)
	}
	fin := res.Rows[0]
	if fin.TopBrokers[0].ByTicker != nil {
		t.Errorf("by_ticker = %+v, want nil under breakdown=ticker", fin.TopBrokers[0].ByTicker)
	}
	if len(fin.TopTickers) != 4 {
		t.Fatalf("top_tickers = %d entries, want 4: %+v", len(fin.TopTickers), fin.TopTickers)
	}
	// Ordered by gross buy: TESTA 10B, TESTB 8B, TESTD 2B, TESTC 0 (sell only).
	if fin.TopTickers[0].Ticker != "TESTA" || fin.TopTickers[0].Buy != nfBuy10 {
		t.Errorf("top_tickers[0] = %+v, want TESTA buy 10B", fin.TopTickers[0])
	}

	ta := fin.TopTickers[0]
	if ta.Buy != nfBuy10 || ta.Sell != nfSell6 {
		t.Errorf("TESTA buy/sell = %d/%d, want 10B/6B", ta.Buy, ta.Sell)
	}
	if deref(ta.ForeignNet) != -2_000_000_000 || ta.ForeignNetDays != 1 {
		t.Errorf("TESTA foreign = %v over %d days, want −2B over 1", ta.ForeignNet, ta.ForeignNetDays)
	}
	if ta.DaysShown != 1 || ta.BrokersCount != 2 {
		t.Errorf("TESTA coverage = %d days/%d brokers, want 1/2", ta.DaysShown, ta.BrokersCount)
	}
	if len(ta.TopBrokers) != 2 || ta.TopBrokers[0].BrokerCode != "AK" || ta.TopBrokers[0].DaysShown != 1 {
		t.Errorf("TESTA top_brokers = %+v, want AK first with days_shown 1", ta.TopBrokers)
	}

	// Sector-level fields stay the same under either breakdown.
	if fin.Buy != nfBuy10+nfBuy8+nfBuy2 || fin.Sell != nfSell6+nfSell4 {
		t.Errorf("sector row = %+v, want buy 20B sell 10B regardless of breakdown", fin)
	}
}

func TestGetSectorFlow_BackwardsRange(t *testing.T) {
	uc := &SectorFlowUseCase{}
	_, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &sfDay2, To: &sfDay1})
	if !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange, got %v", err)
	}
}

func TestGetSectorFlow_WindowTooLong(t *testing.T) {
	uc := &SectorFlowUseCase{}
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	_, err := uc.GetSectorFlow(context.Background(), SectorFlowRequest{From: &from, To: &to})
	if !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange (window > 180d), got %v", err)
	}
}
