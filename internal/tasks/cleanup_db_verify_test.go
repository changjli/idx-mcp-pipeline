package tasks

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestCleanup_EndToEnd verifies the full retention cleanup path against a real
// Postgres: expired rows deleted, fresh rows kept, R2 objects evicted with
// raw_files marked deleted_at, and disclosure text evicted with the metadata
// row kept. Skipped unless IDX_MCP_DB_DSN is set.
//
// Note: the run deletes every row past its retention window in the target DB,
// not just the synthetic ones — that is the cleanup task's job.
func TestCleanup_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean slate for the synthetic rows.
	cleanup := func() {
		db.MustExec("DELETE FROM news_tickers WHERE ticker IN ('CLNPA','CLNPB')")
		db.MustExec("DELETE FROM news_items WHERE url LIKE 'https://example.com/cleanup-%'")
		db.MustExec("DELETE FROM alerts WHERE source = 'cleanup-test'")
		db.MustExec("DELETE FROM raw_files WHERE storage_key LIKE 'cleanup-test/%'")
		db.MustExec("DELETE FROM disclosures WHERE pdf_url LIKE 'https://example.com/cleanup-%'")
		db.MustExec("DELETE FROM anomalies WHERE ticker IN ('CLNPA','CLNPB')")
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker LIKE 'CLNP%'")
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker LIKE 'CLNP%'")
		db.MustExec("DELETE FROM broker_summaries WHERE broker_code IN ('CLNPA','CLNPB')")
		db.MustExec("DELETE FROM daily_prices WHERE ticker LIKE 'CLNP%'")
		db.MustExec("DELETE FROM tickers WHERE code LIKE 'CLNP%'")
	}
	cleanup()
	t.Cleanup(cleanup)

	// Tickers (FK parents).
	for _, code := range []string{"CLNPA", "CLNPB", "CLNPC", "CLNPD", "CLNPE", "CLNPT"} {
		db.MustExec("INSERT INTO tickers (code, name) VALUES ($1, $1)", code)
	}

	old := time.Now().AddDate(-3, 0, 0)     // past every retention window
	old90 := time.Now().AddDate(0, 0, -100) // past the 90-day window
	fresh := time.Now().AddDate(0, 0, -1)   // inside every window

	// daily_prices: CLNPA expired (3y), CLNPB fresh.
	db.MustExec("INSERT INTO daily_prices (ticker, trading_day, close, source) VALUES ($1,$2,100,'idx')", "CLNPA", old)
	db.MustExec("INSERT INTO daily_prices (ticker, trading_day, close, source) VALUES ($1,$2,100,'idx')", "CLNPB", fresh)

	// broker_summaries: CLNPA expired (100d), CLNPB fresh.
	db.MustExec("INSERT INTO broker_summaries (broker_code, trading_day) VALUES ($1,$2)", "CLNPA", old90)
	db.MustExec("INSERT INTO broker_summaries (broker_code, trading_day) VALUES ($1,$2)", "CLNPB", fresh)

	// broker_stock_summaries + totals: five retention cases.
	insertBroker := func(ticker, investor, source string, day, winFrom time.Time) {
		db.MustExec(`INSERT INTO broker_stock_summaries
			(ticker, broker_code, side, trading_day, win_from, investor, market, source)
			VALUES ($1,'AA','buy',$2,$3,$4,'RG',$5)`,
			ticker, day, winFrom, investor, source)
		db.MustExec(`INSERT INTO broker_stock_summary_totals
			(ticker, trading_day, win_from, investor, market, source)
			VALUES ($1,$2,$3,$4,'RG',$5)`,
			ticker, day, winFrom, investor, source)
	}
	// Expired foreign day row, non-Tier-2 month → deleted.
	insertBroker("CLNPA", "f", "live", old90, old90)
	// Fresh foreign day row → kept.
	insertBroker("CLNPB", "f", "live", fresh, fresh)
	// Expired archive chunk → kept (permanent partition member).
	insertBroker("CLNPC", "f", "archive", old90, old90.AddDate(0, 0, -5))
	// Expired new-lane all window → kept.
	insertBroker("CLNPD", "all", "live", old90, old90.AddDate(0, 0, -5))
	// Expired legacy row → deleted.
	insertBroker("CLNPE", "all", "legacy_ipot", old90, old90)

	// CLNPT is Tier 2 for its month but has no archive chunk: its expired day
	// row must be retained until the archiver replaces it. Seed the ADTV window
	// ending on that month's last day.
	monthStart := time.Date(old90.Year(), old90.Month(), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, -1)
	for d := monthStart.AddDate(0, 0, -40); !d.After(monthEnd); d = d.AddDate(0, 0, 1) {
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			continue
		}
		db.MustExec("INSERT INTO daily_prices (ticker, trading_day, close, value, source) VALUES ($1,$2,100,10000000000,'idx')", "CLNPT", d)
	}
	insertBroker("CLNPT", "f", "live", old90, old90)

	// anomalies: CLNPA expired, CLNPB fresh.
	db.MustExec("INSERT INTO anomalies (ticker, trading_day, type, direction) VALUES ($1,$2,'volume','up')", "CLNPA", old90)
	db.MustExec("INSERT INTO anomalies (ticker, trading_day, type, direction) VALUES ($1,$2,'volume','up')", "CLNPB", fresh)

	// news_items + news_tickers: CLNPA expired, CLNPB fresh.
	db.MustExec("INSERT INTO news_items (title, url, source, published_at) VALUES ('old','https://example.com/cleanup-old-news','cleanup-test',$1)", old90)
	db.MustExec("INSERT INTO news_items (title, url, source, published_at) VALUES ('new','https://example.com/cleanup-new-news','cleanup-test',$1)", fresh)
	var oldNewsID, newNewsID int64
	db.Get(&oldNewsID, "SELECT id FROM news_items WHERE url = 'https://example.com/cleanup-old-news'")
	db.Get(&newNewsID, "SELECT id FROM news_items WHERE url = 'https://example.com/cleanup-new-news'")
	db.MustExec("INSERT INTO news_tickers (news_id, ticker, match_method) VALUES ($1,'CLNPA','code')", oldNewsID)
	db.MustExec("INSERT INTO news_tickers (news_id, ticker, match_method) VALUES ($1,'CLNPB','code')", newNewsID)

	// alerts: one expired, one fresh.
	db.MustExec("INSERT INTO alerts (source, alert_type, message, raised_at) VALUES ('cleanup-test','test','old',$1)", old90)
	db.MustExec("INSERT INTO alerts (source, alert_type, message, raised_at) VALUES ('cleanup-test','test','new',$1)", fresh)

	// raw_files: one expired (stored 100d ago, retention 30), one fresh.
	store := newMemStore()
	store.PutObject(context.Background(), "cleanup-test/old.pdf", []byte("x"))
	store.PutObject(context.Background(), "cleanup-test/new.pdf", []byte("x"))
	db.MustExec("INSERT INTO raw_files (storage_key, kind, stored_at, retention_days) VALUES ('cleanup-test/old.pdf','pdf',$1,30)", old90)
	db.MustExec("INSERT INTO raw_files (storage_key, kind, stored_at, retention_days) VALUES ('cleanup-test/new.pdf','pdf',$1,30)", fresh)

	// disclosures: one evictable (ok, extracted 100d ago), one fresh, one
	// already-evicted (must be left alone).
	db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, extraction_status, text_r2_key, extracted_at)
		VALUES ('CLNPA', $1, 'old', 'https://example.com/cleanup-old-disc', 'ok', 'cleanup-test/old.txt', $2)`, old, old90)
	db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, extraction_status, text_r2_key, extracted_at)
		VALUES ('CLNPB', $1, 'new', 'https://example.com/cleanup-new-disc', 'ok', 'cleanup-test/new.txt', $2)`, fresh, fresh)
	db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, extraction_status, text_r2_key, extracted_at)
		VALUES ('CLNPA', $1, 'evicted', 'https://example.com/cleanup-evicted-disc', 'evicted', NULL, $2)`, old, old90)
	store.PutObject(context.Background(), "cleanup-test/old.txt", []byte("text"))
	store.PutObject(context.Background(), "cleanup-test/new.txt", []byte("text"))

	// Run cleanup.
	r := &cleanupRunner{
		log:                    log,
		db:                     db,
		dailyPriceRepo:         repository.NewDailyPriceRepository(log),
		brokerRepo:             repository.NewBrokerRepository(log),
		brokerStockSummaryRepo: repository.NewBrokerStockSummaryRepository(log),
		newsRepo:               repository.NewNewsRepository(log),
		newsTickerRepo:         repository.NewNewsTickerRepository(log),
		alertRepo:              repository.NewAlertRepository(log),
		anomalyRepo:            repository.NewAnomalyRepository(log),
		rawFileRepo:            repository.NewRawFileRepository(log),
		disclosureRepo:         repository.NewDisclosureRepository(log),
		r2Store:                store,
		minADTV:                pipeline.DefaultADTVMinValue,
	}
	if err := r.run(context.Background(), "test"); err != nil {
		t.Fatalf("cleanup run: %v", err)
	}

	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		if err := db.Get(&n, query, args...); err != nil {
			t.Fatalf("count query: %v", err)
		}
		return n
	}

	// daily_prices: expired gone, fresh kept.
	if n := count("SELECT COUNT(*) FROM daily_prices WHERE ticker = 'CLNPA'"); n != 0 {
		t.Errorf("expired daily_prices row not deleted (count=%d)", n)
	}
	if n := count("SELECT COUNT(*) FROM daily_prices WHERE ticker = 'CLNPB'"); n != 1 {
		t.Errorf("fresh daily_prices row deleted (count=%d)", n)
	}

	// broker_summaries: expired gone, fresh kept.
	if n := count("SELECT COUNT(*) FROM broker_summaries WHERE broker_code = 'CLNPA'"); n != 0 {
		t.Errorf("expired broker_summaries row not deleted (count=%d)", n)
	}
	if n := count("SELECT COUNT(*) FROM broker_summaries WHERE broker_code = 'CLNPB'"); n != 1 {
		t.Errorf("fresh broker_summaries row deleted (count=%d)", n)
	}

	// broker_stock_summaries + totals: the five retention cases.
	for _, ticker := range []string{"CLNPA", "CLNPE"} {
		if n := count("SELECT COUNT(*) FROM broker_stock_summaries WHERE ticker = $1", ticker); n != 0 {
			t.Errorf("%s expired row not deleted (count=%d)", ticker, n)
		}
		if n := count("SELECT COUNT(*) FROM broker_stock_summary_totals WHERE ticker = $1", ticker); n != 0 {
			t.Errorf("%s expired totals not deleted (count=%d)", ticker, n)
		}
	}
	for _, ticker := range []string{"CLNPB", "CLNPC", "CLNPD", "CLNPT"} {
		if n := count("SELECT COUNT(*) FROM broker_stock_summaries WHERE ticker = $1", ticker); n != 1 {
			t.Errorf("%s row deleted (count=%d, want 1)", ticker, n)
		}
		if n := count("SELECT COUNT(*) FROM broker_stock_summary_totals WHERE ticker = $1", ticker); n != 1 {
			t.Errorf("%s totals deleted (count=%d, want 1)", ticker, n)
		}
	}

	// anomalies: expired gone, fresh kept.
	if n := count("SELECT COUNT(*) FROM anomalies WHERE ticker = 'CLNPA'"); n != 0 {
		t.Errorf("expired anomalies row not deleted (count=%d)", n)
	}
	if n := count("SELECT COUNT(*) FROM anomalies WHERE ticker = 'CLNPB'"); n != 1 {
		t.Errorf("fresh anomalies row deleted (count=%d)", n)
	}

	// news_items + news_tickers: expired gone, fresh kept.
	if n := count("SELECT COUNT(*) FROM news_items WHERE url = 'https://example.com/cleanup-old-news'"); n != 0 {
		t.Errorf("expired news_item not deleted (count=%d)", n)
	}
	if n := count("SELECT COUNT(*) FROM news_items WHERE url = 'https://example.com/cleanup-new-news'"); n != 1 {
		t.Errorf("fresh news_item deleted (count=%d)", n)
	}
	if n := count("SELECT COUNT(*) FROM news_tickers WHERE news_id = $1", oldNewsID); n != 0 {
		t.Errorf("expired news_ticker join not deleted (count=%d)", n)
	}
	if n := count("SELECT COUNT(*) FROM news_tickers WHERE news_id = $1", newNewsID); n != 1 {
		t.Errorf("fresh news_ticker join deleted (count=%d)", n)
	}

	// alerts: expired gone, fresh kept.
	if n := count("SELECT COUNT(*) FROM alerts WHERE source = 'cleanup-test' AND message = 'old'"); n != 0 {
		t.Errorf("expired alert not deleted (count=%d)", n)
	}
	if n := count("SELECT COUNT(*) FROM alerts WHERE source = 'cleanup-test' AND message = 'new'"); n != 1 {
		t.Errorf("fresh alert deleted (count=%d)", n)
	}

	// raw_files: expired marked deleted_at, fresh not; R2 objects follow.
	var deletedAt *time.Time
	db.Get(&deletedAt, "SELECT deleted_at FROM raw_files WHERE storage_key = 'cleanup-test/old.pdf'")
	if deletedAt == nil {
		t.Error("expired raw_file not marked deleted_at")
	}
	db.Get(&deletedAt, "SELECT deleted_at FROM raw_files WHERE storage_key = 'cleanup-test/new.pdf'")
	if deletedAt != nil {
		t.Error("fresh raw_file marked deleted_at")
	}
	if _, err := store.GetObject(context.Background(), "cleanup-test/old.pdf"); err == nil {
		t.Error("expired R2 object not deleted")
	}
	if _, err := store.GetObject(context.Background(), "cleanup-test/new.pdf"); err != nil {
		t.Error("fresh R2 object deleted")
	}

	// disclosures: evictable one evicted + key nulled, fresh still ok,
	// already-evicted untouched.
	var status string
	var key *string
	db.Get(&status, "SELECT extraction_status FROM disclosures WHERE pdf_url = 'https://example.com/cleanup-old-disc'")
	if status != "evicted" {
		t.Errorf("evictable disclosure status = %s, want evicted", status)
	}
	db.Get(&key, "SELECT text_r2_key FROM disclosures WHERE pdf_url = 'https://example.com/cleanup-old-disc'")
	if key != nil {
		t.Errorf("evictable disclosure text_r2_key not nulled (got %v)", *key)
	}
	db.Get(&status, "SELECT extraction_status FROM disclosures WHERE pdf_url = 'https://example.com/cleanup-new-disc'")
	if status != "ok" {
		t.Errorf("fresh disclosure status = %s, want ok", status)
	}
	db.Get(&status, "SELECT extraction_status FROM disclosures WHERE pdf_url = 'https://example.com/cleanup-evicted-disc'")
	if status != "evicted" {
		t.Errorf("already-evicted disclosure status = %s, want evicted (untouched)", status)
	}
	if _, err := store.GetObject(context.Background(), "cleanup-test/old.txt"); err == nil {
		t.Error("expired disclosure text object not deleted")
	}
	if _, err := store.GetObject(context.Background(), "cleanup-test/new.txt"); err != nil {
		t.Error("fresh disclosure text object deleted")
	}
}
