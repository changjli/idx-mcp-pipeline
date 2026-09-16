package repository

import (
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// The screener's SQL hard filters are one statement, and this is the only place
// they run against a real Postgres: the funnel's stage-1 contract (still
// trading on the anchor day, liquid enough, not recently suspended) either holds
// in SQL or the shortlist is silently wrong. Skipped unless IDX_MCP_DB_DSN is
// set; cleanup is scoped to the TESTS* tickers only.
const (
	screenTickerLiquid    = "TESTSA" // trades on the anchor day, 10B — survives
	screenTickerThin      = "TESTSB" // trades on the anchor day, 1B — under the floor
	screenTickerHalted    = "TESTSC" // last row 3 days before the anchor — not trading
	screenTickerSuspended = "TESTSD" // liquid, but suspended 2 trading days before the anchor
	screenTickerOldEvent  = "TESTSE" // liquid, suspended 7 trading days before the anchor
	screenTickerNoValue   = "TESTSF" // trades on the anchor day with a null value
)

var screenTickers = []string{
	screenTickerLiquid, screenTickerThin, screenTickerHalted,
	screenTickerSuspended, screenTickerOldEvent, screenTickerNoValue,
}

// screenAnchor is the last of the seeded trading days; the seeded market is 8
// consecutive stored days, so "the last N trading days" is exactly N days back.
var screenAnchor = time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)

// screenDay is the seeded day `back` days before the anchor; 0 is the anchor.
func screenDay(back int) time.Time { return screenAnchor.AddDate(0, 0, -back) }

// seedScreenMarket inserts the six test tickers and their rows, and returns the
// connection. Days run from the anchor back to day 7 for the tickers that are
// still trading; the halted ticker's newest row is day 3, so it has rows only
// inside the window's older half.
func seedScreenMarket(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	priceRepo := NewDailyPriceRepository(logrus.New())
	suspensionRepo := NewSuspensionRepository(logrus.New())

	cleanup := func() {
		db.MustExec("DELETE FROM daily_prices WHERE ticker = ANY($1)", screenTickers)
		db.MustExec("DELETE FROM suspensions WHERE ticker = ANY($1)", screenTickers)
		db.MustExec("DELETE FROM tickers WHERE code = ANY($1)", screenTickers)
	}
	cleanup()
	t.Cleanup(cleanup)

	for _, code := range screenTickers {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", code, "Screener test "+code)
	}

	seedDays(t, db, priceRepo, screenTickerLiquid, 0, 7, 10_000_000_000, false)
	seedDays(t, db, priceRepo, screenTickerThin, 0, 7, 1_000_000_000, false)
	seedDays(t, db, priceRepo, screenTickerHalted, 3, 7, 10_000_000_000, false)
	seedDays(t, db, priceRepo, screenTickerSuspended, 0, 7, 10_000_000_000, false)
	seedDays(t, db, priceRepo, screenTickerOldEvent, 0, 7, 10_000_000_000, false)
	seedDays(t, db, priceRepo, screenTickerNoValue, 0, 7, 0, true)

	mustSuspend(t, db, suspensionRepo, screenTickerSuspended, screenDay(2))
	mustSuspend(t, db, suspensionRepo, screenTickerOldEvent, screenDay(7))

	return db
}

// seedDays stores one row per day from `newest` back to `oldest` inclusive,
// counted in days before the anchor.
func seedDays(t *testing.T, db *sqlx.DB, repo *DailyPriceRepository, code string, newest, oldest int, value int64, nullValue bool) {
	t.Helper()
	for back := oldest; back >= newest; back-- {
		var stored *int64
		if !nullValue {
			stored = i64(value)
		}
		if err := repo.Upsert(db, &entity.DailyPrice{
			Ticker:     code,
			TradingDay: screenDay(back),
			Open:       f64(100),
			High:       f64(110),
			Low:        f64(90),
			Close:      f64(105),
			Volume:     i64(1_000_000),
			Value:      stored,
			Frequency:  i32(1000),
			Source:     "idx",
		}); err != nil {
			t.Fatalf("Upsert %s %s: %v", code, screenDay(back).Format("2006-01-02"), err)
		}
	}
}

// mustSuspend records a suspension-type event for a ticker.
func mustSuspend(t *testing.T, db *sqlx.DB, repo *SuspensionRepository, ticker string, day time.Time) {
	t.Helper()
	if err := repo.Upsert(db, []entity.Suspension{{
		Ticker:    ticker,
		EventDate: day,
		Type:      "SPT",
		Reason:    "Screener test suspension",
	}}); err != nil {
		t.Fatalf("seed suspension for %s: %v", ticker, err)
	}
}

// TestScreenCandidates_HardFilters verifies the funnel's SQL stage: a survivor
// trades on the anchor day, clears the liquidity floor, and has no
// suspension-related event inside the window.
func TestScreenCandidates_HardFilters(t *testing.T) {
	db := seedScreenMarket(t)
	repo := NewDailyPriceRepository(logrus.New())

	assertSurvivors := func(t *testing.T, suspensionDays int, want []string) {
		t.Helper()
		candidates, err := repo.ScreenCandidates(db, screenAnchor, 5_000_000_000, suspensionDays)
		if err != nil {
			t.Fatalf("ScreenCandidates: %v", err)
		}
		got := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			if isScreenTicker(candidate.Ticker) {
				got = append(got, candidate.Ticker)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("suspensionDays %d: survivors = %v, want %v", suspensionDays, got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("suspensionDays %d: survivors = %v, want %v (ascending by ticker)", suspensionDays, got, want)
			}
		}
	}

	// The default window covers days 0..4 back, so the day-2 event disqualifies
	// that ticker while the day-7 event is long past.
	assertSurvivors(t, 5, []string{screenTickerLiquid, screenTickerOldEvent})
	// Boundary, inside: a 3-day window reaches day 2 back, catching the event.
	assertSurvivors(t, 3, []string{screenTickerLiquid, screenTickerOldEvent})
	// Boundary, outside: one day shorter and the same event falls out of the
	// window, so the ticker is back in the shortlist. This is what makes the
	// parameter meaningful rather than decorative.
	assertSurvivors(t, 2, []string{screenTickerLiquid, screenTickerSuspended, screenTickerOldEvent})
	// A long window reaches the day-7 event too.
	assertSurvivors(t, 8, []string{screenTickerLiquid})

	// The floor: a higher one empties the funnel without erroring.
	candidates, err := repo.ScreenCandidates(db, screenAnchor, 20_000_000_000, 5)
	if err != nil {
		t.Fatalf("ScreenCandidates at a higher floor: %v", err)
	}
	for _, candidate := range candidates {
		if isScreenTicker(candidate.Ticker) {
			t.Fatalf("survivor %s cleared a 20B floor on a 10B day", candidate.Ticker)
		}
	}

	// Survivors carry the anchor day's own value and close, which is what the
	// ranking and the reader see.
	candidates, err = repo.ScreenCandidates(db, screenAnchor, 5_000_000_000, 5)
	if err != nil {
		t.Fatalf("ScreenCandidates: %v", err)
	}
	found := false
	for _, candidate := range candidates {
		if candidate.Ticker != screenTickerLiquid {
			continue
		}
		found = true
		if candidate.Value == nil || *candidate.Value != 10_000_000_000 {
			t.Fatalf("value = %v, want the anchor day's 10B", candidate.Value)
		}
		if candidate.Close == nil || *candidate.Close != 105 {
			t.Fatalf("close = %v, want the anchor day's 105", candidate.Close)
		}
	}
	if !found {
		t.Fatal("the liquid ticker did not survive")
	}
}

// TestScreenUniverse_CountsTheWindow verifies the funnel's first number: only
// tickers with a row inside the window count, and a shorter window can only
// shrink the population.
func TestScreenUniverse_CountsTheWindow(t *testing.T) {
	db := seedScreenMarket(t)
	repo := NewDailyPriceRepository(logrus.New())

	wide, err := repo.ScreenUniverse(db, screenAnchor, 8)
	if err != nil {
		t.Fatalf("ScreenUniverse: %v", err)
	}
	narrow, err := repo.ScreenUniverse(db, screenAnchor, 1)
	if err != nil {
		t.Fatalf("ScreenUniverse: %v", err)
	}

	// All six seeded tickers have rows inside an 8-day window.
	if wide < len(screenTickers) {
		t.Fatalf("universe(8) = %d, want at least the %d seeded tickers", wide, len(screenTickers))
	}
	// The halted ticker traded only on days 3..7 back, so the one-day universe
	// cannot see it: the wide window counts strictly more.
	if narrow >= wide {
		t.Fatalf("universe(1) = %d, universe(8) = %d, want the wide window to count more", narrow, wide)
	}
}

// TestFindByTickersUpTo_PerTickerWindow verifies the survivor read: a per-ticker
// limit, ascending by trading day within each ticker, one round trip, and no
// error for a ticker with no stored rows.
func TestFindByTickersUpTo_PerTickerWindow(t *testing.T) {
	db := seedScreenMarket(t)
	repo := NewDailyPriceRepository(logrus.New())

	rows, err := repo.FindByTickersUpTo(db, []string{screenTickerLiquid, "TESTSZ"}, screenAnchor, 3)
	if err != nil {
		t.Fatalf("FindByTickersUpTo: %v", err)
	}

	byTicker := map[string][]time.Time{}
	for _, row := range rows {
		byTicker[row.Ticker] = append(byTicker[row.Ticker], row.TradingDay)
	}
	if len(byTicker["TESTSZ"]) != 0 {
		t.Fatalf("read %d rows for an unknown ticker, want none", len(byTicker["TESTSZ"]))
	}

	got := byTicker[screenTickerLiquid]
	if len(got) != 3 {
		t.Fatalf("rows for %s = %d, want the per-ticker limit of 3", screenTickerLiquid, len(got))
	}
	for i := 1; i < len(got); i++ {
		if !got[i].After(got[i-1]) {
			t.Fatalf("rows not ascending: %v", got)
		}
	}
	if !got[len(got)-1].Equal(screenAnchor) {
		t.Fatalf("latest row = %s, want the anchor %s", got[len(got)-1].Format("2006-01-02"), screenAnchor.Format("2006-01-02"))
	}

	empty, err := repo.FindByTickersUpTo(db, nil, screenAnchor, 3)
	if err != nil {
		t.Fatalf("FindByTickersUpTo with no tickers: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("read %d rows for an empty ticker list, want none", len(empty))
	}
}

func isScreenTicker(code string) bool {
	for _, ticker := range screenTickers {
		if ticker == code {
			return true
		}
	}
	return false
}
