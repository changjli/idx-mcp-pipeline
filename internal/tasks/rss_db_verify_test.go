package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/hibiken/asynq"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// fakeObjectStore records claim-check uploads without touching R2.
type fakeObjectStore struct {
	mu   sync.Mutex
	keys []string
}

func (f *fakeObjectStore) PutObject(ctx context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, key)
	return nil
}

// GetObject completes the ObjectStore interface; the RSS path never reads
// back, so any lookup is an error.
func (f *fakeObjectStore) GetObject(context.Context, string) ([]byte, error) {
	return nil, errors.New("rss fake store is write-only")
}

// DeleteObject completes the ObjectStore interface; the RSS path never deletes.
func (f *fakeObjectStore) DeleteObject(context.Context, string) error {
	return nil
}

func (f *fakeObjectStore) storedKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.keys))
	copy(out, f.keys)
	return out
}

// TestRSSIngest_EndToEnd runs the full rss:ingest handler against a real
// Postgres: three httptest feeds, code+name matching, news_items/news_tickers
// storage, unmatched discard, R2 claim-check, and re-run idempotency. Skipped
// unless IDX_MCP_DB_DSN is set. Cleanup is scoped strictly to rows the test
// created (test-only URL pattern, captured storage keys, the rss source_status
// row) so a shared dev DB is never touched beyond that.
func TestRSSIngest_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	tickerRepo := repository.NewTickerRepository(log)
	newsRepo := repository.NewNewsRepository(log)
	newsTickerRepo := repository.NewNewsTickerRepository(log)
	sourceStatusRepo := repository.NewSourceStatusRepository(log)
	alertRepo := repository.NewAlertRepository(log)
	rawFileRepo := repository.NewRawFileRepository(log)

	const urlPattern = "https://example.com/rss-test-%d"
	feedXML := map[string]string{
		"cnbc": `<rss version="2.0"><channel>` +
			`<item><title>Bank Central Asia Raup Laba Bersih</title><link>` + fmt.Sprintf(urlPattern, 1) + `</link><pubDate>Mon, 10 Aug 2026 09:00:00 +0700</pubDate><description>BBCA membukukan laba.</description></item>` +
			`<item><title>TLKM Jual Obligasi</title><link>` + fmt.Sprintf(urlPattern, 2) + `</link><pubDate>Mon, 10 Aug 2026 10:00:00 +0700</pubDate><description>Emiten telekomunikasi menerbitkan surat utang.</description></item>` +
			`<item><title>Inflasi Indonesia Naik di Agustus</title><link>` + fmt.Sprintf(urlPattern, 3) + `</link><pubDate>Mon, 10 Aug 2026 11:00:00 +0700</pubDate><description>Badan pusat statistik rilis data.</description></item>` +
			`</channel></rss>`,
		"kontan": `<rss version="2.0"><channel>` +
			`<item><title>Saham BUMI Menguat ke Level Tertinggi</title><link>` + fmt.Sprintf(urlPattern, 4) + `</link><pubDate>Mon, 10 Aug 2026 09:30:00 +0700</pubDate><description>Harga saham tambang batu bara.</description></item>` +
			`<item><title>Energi dari dalam bumi tetap dibutuhkan</title><link>` + fmt.Sprintf(urlPattern, 5) + `</link><pubDate>Mon, 10 Aug 2026 09:40:00 +0700</pubDate><description>Artikel umum tanpa kode.</description></item>` +
			`</channel></rss>`,
		"bisnis": `<rss version="2.0"><channel>` +
			`<item><title>Telkom Siapkan Belanja Modal</title><link>` + fmt.Sprintf(urlPattern, 6) + `</link><pubDate>Mon, 10 Aug 2026 10:15:00 +0700</pubDate><description>Investasi jaringan.</description></item>` +
			`</channel></rss>`,
	}

	feeds := make([]RSSFeed, 0, len(feedXML))
	var servers []*httptest.Server
	for name, xml := range feedXML {
		name, xml := name, xml
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/rss+xml")
			w.Write([]byte(xml))
		}))
		servers = append(servers, srv)
		feeds = append(feeds, RSSFeed{Name: name, URL: srv.URL})
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Close()
		}
	})

	// The matcher reads its universe from the DB tickers table, so the codes
	// this test's feeds reference must exist. InsertIfAbsent seeds them on a
	// fresh DB and leaves an existing row's metadata untouched on a shared one.
	for _, tk := range []struct{ code, name string }{
		{"BBCA", "Bank Central Asia Tbk."},
		{"TLKM", "Telkom Indonesia (Persero) Tbk."},
		{"BUMI", "Bumi Resources Tbk."},
	} {
		if err := tickerRepo.InsertIfAbsent(db, tk.code, tk.name); err != nil {
			t.Fatalf("seed ticker %s: %v", tk.code, err)
		}
	}

	store := &fakeObjectStore{}
	handler := NewRSSHandler(
		log,
		&http.Client{Timeout: RSSHTTPTimeout},
		store,
		feeds,
		db,
		tickerRepo, newsRepo, newsTickerRepo, sourceStatusRepo, alertRepo, rawFileRepo,
	)

	// Scoped cleanup: test-only news rows, the exact raw_files keys this test
	// created, and the rss source_status row. Tickers seeded by the matcher are
	// left in place (real embed names, harmless in a shared DB).
	t.Cleanup(func() {
		db.MustExec("DELETE FROM news_tickers WHERE news_id IN (SELECT id FROM news_items WHERE url LIKE '%rss-test-%')")
		db.MustExec("DELETE FROM news_items WHERE url LIKE '%rss-test-%'")
		for _, k := range store.storedKeys() {
			db.MustExec("DELETE FROM raw_files WHERE storage_key = $1", k)
		}
		db.MustExec("DELETE FROM source_status WHERE source = 'rss'")
		db.MustExec("DELETE FROM alerts WHERE source = 'rss' AND message LIKE '%rss-test%'")
	})

	payload := RSSPayload{Date: "2026-08-10"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeRSS, raw)

	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler run 1: %v", err)
	}

	// ─── Matched articles present with correct tickers + methods ───────
	var rows []struct {
		NewsID      int64  `db:"news_id"`
		Ticker      string `db:"ticker"`
		MatchMethod string `db:"match_method"`
		Title       string `db:"title"`
		URL         string `db:"url"`
		Source      string `db:"source"`
	}
	if err := db.Select(&rows, `
		SELECT nt.news_id, nt.ticker, nt.match_method, ni.title, ni.url, ni.source
		FROM news_tickers nt
		JOIN news_items ni ON ni.id = nt.news_id
		WHERE ni.url LIKE '%rss-test-%'
		ORDER BY nt.news_id, nt.ticker`); err != nil {
		t.Fatalf("query news rows: %v", err)
	}

	if len(rows) != 4 {
		t.Fatalf("expected 4 news_tickers rows, got %d: %+v", len(rows), rows)
	}

	expected := []struct {
		ticker, method, url, source string
	}{
		{"BBCA", "name", fmt.Sprintf(urlPattern, 1), "cnbc"},
		{"TLKM", "code", fmt.Sprintf(urlPattern, 2), "cnbc"},
		{"BUMI", "code", fmt.Sprintf(urlPattern, 4), "kontan"},
		{"TLKM", "name", fmt.Sprintf(urlPattern, 6), "bisnis"},
	}
	for i, exp := range expected {
		if rows[i].Ticker != exp.ticker || rows[i].MatchMethod != exp.method || rows[i].URL != exp.url || rows[i].Source != exp.source {
			t.Errorf("row %d: got ticker=%s method=%s url=%s source=%s, want %+v",
				i, rows[i].Ticker, rows[i].MatchMethod, rows[i].URL, rows[i].Source, exp)
		}
	}

	// Unmatched items (urls 3, 5) must not be stored.
	var stored int
	if err := db.Get(&stored, "SELECT COUNT(*) FROM news_items WHERE url LIKE '%rss-test-%'"); err != nil {
		t.Fatalf("count news_items: %v", err)
	}
	if stored != 4 {
		t.Errorf("expected 4 news_items (2 unmatched discarded), got %d", stored)
	}

	// ─── Raw feed XML claim-checked to R2 (DISABLED 2026-08-10) ────────
	// The claim-check is commented out in the handler — raw feed XML duplicates
	// the DB rows for matched articles and adds nothing the AI consumes. The
	// assertions below are preserved (commented) so they can be re-enabled
	// alongside the claim-check if recovery-from-XML ever gets built.
	// keys := store.storedKeys()
	// if len(keys) != 3 {
	// 	t.Fatalf("expected 3 claim-check uploads (one per feed), got %d: %v", len(keys), keys)
	// }
	// var rawRows int
	// if err := db.Get(&rawRows, "SELECT COUNT(*) FROM raw_files WHERE storage_key = ANY($1)", keys); err != nil {
	// 	t.Fatalf("count raw_files: %v", err)
	// }
	// if rawRows != 3 {
	// 	t.Errorf("expected 3 raw_files rows, got %d", rawRows)
	// }

	// source_status row recorded healthy.
	var ss struct {
		Source    string  `db:"source"`
		Stale     bool    `db:"stale"`
		LastError *string `db:"last_error"`
	}
	if err := db.Get(&ss, "SELECT source, stale, last_error FROM source_status WHERE source = 'rss'"); err != nil {
		t.Fatalf("fetch source_status: %v", err)
	}
	if ss.Stale {
		t.Error("expected source_status stale=false after success")
	}
	if ss.LastError != nil {
		t.Errorf("expected no last_error, got %q", *ss.LastError)
	}

	// ─── Re-run is idempotent ──────────────────────────────────────────
	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler run 2: %v", err)
	}
	if err := db.Get(&stored, "SELECT COUNT(*) FROM news_items WHERE url LIKE '%rss-test-%'"); err != nil {
		t.Fatalf("count news_items after re-run: %v", err)
	}
	if stored != 4 {
		t.Errorf("expected 4 news_items after re-run (idempotent), got %d", stored)
	}
	var joinCount int
	if err := db.Get(&joinCount, "SELECT COUNT(*) FROM news_tickers WHERE news_id IN (SELECT id FROM news_items WHERE url LIKE '%rss-test-%')"); err != nil {
		t.Fatalf("count news_tickers after re-run: %v", err)
	}
	if joinCount != 4 {
		t.Errorf("expected 4 news_tickers after re-run, got %d", joinCount)
	}
}

// TestRSSIngest_AllFeedsFail verifies a total feed failure records
// source_status failure and returns an error (asynq will retry).
func TestRSSIngest_AllFeedsFail(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	tickerRepo := repository.NewTickerRepository(log)
	newsRepo := repository.NewNewsRepository(log)
	newsTickerRepo := repository.NewNewsTickerRepository(log)
	sourceStatusRepo := repository.NewSourceStatusRepository(log)
	alertRepo := repository.NewAlertRepository(log)
	rawFileRepo := repository.NewRawFileRepository(log)

	// Every feed returns 500.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	feeds := []RSSFeed{
		{Name: "cnbc", URL: srv.URL},
		{Name: "kontan", URL: srv.URL},
		{Name: "bisnis", URL: srv.URL},
	}

	handler := NewRSSHandler(
		log,
		&http.Client{Timeout: RSSHTTPTimeout},
		nil,
		feeds,
		db,
		tickerRepo, newsRepo, newsTickerRepo, sourceStatusRepo, alertRepo, rawFileRepo,
	)

	payload := RSSPayload{Date: "2026-08-10"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeRSS, raw)

	if err := handler(context.Background(), task); err == nil {
		t.Fatal("expected error when all feeds fail")
	}

	var ss struct {
		Source              string  `db:"source"`
		ConsecutiveFailures int32   `db:"consecutive_failures"`
		LastError           *string `db:"last_error"`
	}
	if err := db.Get(&ss, "SELECT source, consecutive_failures, last_error FROM source_status WHERE source = 'rss'"); err != nil {
		t.Fatalf("fetch source_status: %v", err)
	}
	if ss.Source != "rss" {
		t.Errorf("expected source rss, got %s", ss.Source)
	}
	if ss.ConsecutiveFailures < 1 {
		t.Errorf("expected consecutive_failures >= 1, got %d", ss.ConsecutiveFailures)
	}
	if ss.LastError == nil || !strings.Contains(*ss.LastError, "boom") {
		t.Errorf("expected last_error mentioning feed failure, got %v", ss.LastError)
	}

	// Clean up the failure state this test wrote.
	t.Cleanup(func() {
		db.MustExec("DELETE FROM alerts WHERE source = 'rss' AND message LIKE '%2026-08-10%'")
		db.MustExec("DELETE FROM source_status WHERE source = 'rss'")
	})
}
