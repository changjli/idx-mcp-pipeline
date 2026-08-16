package tasks

import (
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestRunFilter_EndToEnd verifies the 3-layer filter against a real Postgres:
// anomaly-gate, keyword whitelist, exclusion, sticky-true, delayed-reaction
// re-check, and extract enqueue per passing disclosure. Skipped unless
// IDX_MCP_DB_DSN is set.
func TestRunFilter_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	disclosureRepo := repository.NewDisclosureRepository(log)
	anomalyRepo := repository.NewAnomalyRepository(log)

	// Clean slate.
	db.MustExec("DELETE FROM disclosures WHERE pdf_url LIKE '%filter-test-%'")
	db.MustExec("DELETE FROM anomalies WHERE ticker LIKE 'FILT%'")
	db.MustExec("DELETE FROM tickers WHERE code LIKE 'FILT%'")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url LIKE '%filter-test-%'")
		db.MustExec("DELETE FROM anomalies WHERE ticker LIKE 'FILT%'")
		db.MustExec("DELETE FROM tickers WHERE code LIKE 'FILT%'")
	})

	for _, code := range []string{"FILTA", "FILTB", "FILTC", "FILTD", "FILTE", "FILTF"} {
		db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $1, true)", code)
	}

	// Anomaly on 2026-08-10 for FILTA and FILTE (within the 7-day lookback of
	// disclosures announced 2026-08-05).
	db.MustExec(`INSERT INTO anomalies (ticker, trading_day, type, direction) VALUES ('FILTA', '2026-08-10', 'volume', 'up')`)
	db.MustExec(`INSERT INTO anomalies (ticker, trading_day, type, direction) VALUES ('FILTE', '2026-08-10', 'price', 'up')`)

	seed := func(ticker, date, title, url string, passed *bool, status string) {
		var pf any
		if passed != nil {
			pf = *passed
		}
		db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			ticker, date, title, url, pf, status)
	}

	// FILTA: anomaly + whitelist title -> pass.
	seed("FILTA", "2026-08-05", "Pemanggilan RUPS Tahunan PT ABC", "https://example.com/filter-test-a.pdf", nil, "pending")
	// FILTB: anomaly + non-material title -> reject.
	seed("FILTB", "2026-08-05", "Laporan Bulanan Registrasi Pemegang Efek", "https://example.com/filter-test-b.pdf", nil, "pending")
	// FILTC: anomaly + exclusion wins -> reject.
	seed("FILTC", "2026-08-05", "Laporan Keuangan dan Informasi dan Fakta Material", "https://example.com/filter-test-c.pdf", nil, "pending")
	// FILTD: no anomaly -> reject.
	seed("FILTD", "2026-08-05", "Pemanggilan RUPS Tahunan", "https://example.com/filter-test-d.pdf", nil, "pending")
	// FILTE: anomaly + whitelist -> pass.
	seed("FILTE", "2026-08-05", "Pembagian Dividen Interim", "https://example.com/filter-test-e.pdf", nil, "pending")
	// FILTF: already rejected, announced in window, anomaly now exists -> flip to pass.
	seed("FILTF", "2026-08-05", "Pemanggilan RUPS Tahunan", "https://example.com/filter-test-f.pdf", boolPtr(false), "pending")
	db.MustExec(`INSERT INTO anomalies (ticker, trading_day, type, direction) VALUES ('FILTF', '2026-08-10', 'volume', 'up')`)
	// Sticky: already passed (categories set by a prior run), still pending
	// extraction -> extract re-enqueued, gate NOT re-evaluated (its current
	// title is non-material, so re-evaluation would wrongly reject it).
	db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, categories, extraction_status)
		VALUES ('FILTA', '2026-08-06', 'Laporan Bulanan Registrasi Pemegang Efek', 'https://example.com/filter-test-sticky.pdf', true, '{Dividen}', 'pending')`)

	today := mustParseDate(t, "2026-08-10")
	var enqueued []int64
	enqueue := func(id int64) { enqueued = append(enqueued, id) }

	if err := runFilter(log, db, disclosureRepo, anomalyRepo, today, enqueue); err != nil {
		t.Fatalf("runFilter: %v", err)
	}

	assertVerdict := func(url string, wantPassed bool) {
		t.Helper()
		var passed *bool
		var cats pq.StringArray
		if err := db.Get(&passed, "SELECT passed_filter FROM disclosures WHERE pdf_url = $1", url); err != nil {
			t.Fatalf("fetch passed_filter for %s: %v", url, err)
		}
		if passed == nil {
			t.Errorf("%s: expected passed_filter set, got NULL", url)
			return
		}
		if *passed != wantPassed {
			t.Errorf("%s: expected passed_filter=%v, got %v", url, wantPassed, *passed)
		}
		if err := db.Get(&cats, "SELECT categories FROM disclosures WHERE pdf_url = $1", url); err != nil {
			t.Fatalf("fetch categories for %s: %v", url, err)
		}
		if wantPassed {
			if len(cats) == 0 {
				t.Errorf("%s: expected categories populated, got empty", url)
			}
		} else if cats != nil {
			t.Errorf("%s: expected NULL categories, got %v", url, cats)
		}
	}

	assertVerdict("https://example.com/filter-test-a.pdf", true)
	assertVerdict("https://example.com/filter-test-b.pdf", false)
	assertVerdict("https://example.com/filter-test-c.pdf", false)
	assertVerdict("https://example.com/filter-test-d.pdf", false)
	assertVerdict("https://example.com/filter-test-e.pdf", true)
	// Delayed reaction: FILTF was rejected on announcement day, flipped when
	// the anomaly appeared within the lookback window.
	assertVerdict("https://example.com/filter-test-f.pdf", true)
	// Sticky true: gate not re-evaluated (title is non-material), extract
	// re-enqueued because still pending.
	assertVerdict("https://example.com/filter-test-sticky.pdf", true)

	assertVerdict("https://example.com/filter-test-a.pdf", true)
	assertVerdict("https://example.com/filter-test-b.pdf", false)
	assertVerdict("https://example.com/filter-test-c.pdf", false)
	assertVerdict("https://example.com/filter-test-d.pdf", false)
	assertVerdict("https://example.com/filter-test-e.pdf", true)
	// Delayed reaction: FILTF was rejected on announcement day, flipped when
	// the anomaly appeared within the lookback window.
	assertVerdict("https://example.com/filter-test-f.pdf", true)
	// Sticky true: gate not re-evaluated (title is non-material), extract
	// re-enqueued because still pending.
	assertVerdict("https://example.com/filter-test-sticky.pdf", true)

	// Extract enqueued for every passing disclosure. The DB may hold real
	// disclosures too, so assert the FILT% passing rows are present (and the
	// rejected ones absent) rather than an exact count.
	var passingIDs []int64
	if err := db.Select(&passingIDs, `SELECT id FROM disclosures
		WHERE pdf_url LIKE '%filter-test-%' AND passed_filter = true`); err != nil {
		t.Fatalf("fetch passing ids: %v", err)
	}
	if len(passingIDs) != 4 {
		t.Fatalf("expected 4 passing FILT disclosures, got %d", len(passingIDs))
	}
	enqueuedSet := make(map[int64]bool, len(enqueued))
	for _, id := range enqueued {
		enqueuedSet[id] = true
	}
	for _, id := range passingIDs {
		if !enqueuedSet[id] {
			t.Errorf("expected extract enqueued for passing disclosure %d", id)
		}
	}
	var rejectedIDs []int64
	if err := db.Select(&rejectedIDs, `SELECT id FROM disclosures
		WHERE pdf_url LIKE '%filter-test-%' AND passed_filter = false`); err != nil {
		t.Fatalf("fetch rejected ids: %v", err)
	}
	for _, id := range rejectedIDs {
		if enqueuedSet[id] {
			t.Errorf("expected no extract enqueue for rejected disclosure %d", id)
		}
	}
}

func boolPtr(b bool) *bool { return &b }
