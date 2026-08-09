package tasks

import (
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestAnnouncementsUpsert_EndToEnd verifies the disclosure upsert path against
// a real Postgres: multi-attachment flattening, ticker auto-discovery, and
// idempotency via pdf_url. Skipped unless IDX_MCP_DB_DSN is set.
func TestAnnouncementsUpsert_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	tickerRepo := repository.NewTickerRepository(log)
	disclosureRepo := repository.NewDisclosureRepository(log)

	// Clean slate.
	db.MustExec("DELETE FROM disclosures WHERE pdf_url LIKE '%announce-test-%'")
	db.MustExec("DELETE FROM tickers WHERE code IN ('DISCX', 'DISCY')")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url LIKE '%announce-test-%'")
		db.MustExec("DELETE FROM tickers WHERE code IN ('DISCX', 'DISCY')")
	})

	// One announcement for DISCX (already in tickers) with 2 PDF attachments;
	// one for DISCY (unknown issuer, auto-discovered as placeholder).
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('DISCX', 'Disc Test X Tbk.', true)")

	replies := []AnnouncementReply{
		{
			Pengumuman: AnnouncementMeta{
				ID2:             "test-1",
				TglPengumuman:   "2026-08-08T17:22:09",
				JudulPengumuman: "Laporan Bulanan Registrasi Pemegang Efek",
				KodeEmiten:      "DISCX",
			},
			Attachments: []AnnouncementAttachment{
				{PDFFilename: "a.pdf", FullSavePath: "https://example.com/announce-test-1a.pdf"},
				{PDFFilename: "b.pdf", FullSavePath: "https://example.com/announce-test-1b.pdf"},
			},
		},
		{
			Pengumuman: AnnouncementMeta{
				ID2:             "test-2",
				TglPengumuman:   "2026-08-09T09:30:00",
				JudulPengumuman: "Rencana Aksi Perusahaan",
				KodeEmiten:      "DISCY",
			},
			Attachments: []AnnouncementAttachment{
				{PDFFilename: "c.pdf", FullSavePath: "https://example.com/announce-test-2.pdf"},
			},
		},
	}

	first, err := upsertDisclosureRows(db, tickerRepo, disclosureRepo, replies, log)
	if err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if first != 3 {
		t.Fatalf("expected 3 rows upserted on first run, got %d", first)
	}

	// Re-run is idempotent: same 3 rows, no duplicates.
	second, err := upsertDisclosureRows(db, tickerRepo, disclosureRepo, replies, log)
	if err != nil {
		t.Fatalf("re-run upsert: %v", err)
	}
	if second != 3 {
		t.Fatalf("expected 3 rows upserted on re-run, got %d", second)
	}

	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM disclosures WHERE pdf_url LIKE '%announce-test-%'"); err != nil {
		t.Fatalf("count disclosures: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 disclosure rows total, got %d", count)
	}

	// Metadata + defaults verified.
	var row struct {
		Ticker           string `db:"ticker"`
		Title            string `db:"title"`
		AnnouncementDate string `db:"announcement_date"`
		PdfURL           string `db:"pdf_url"`
		AttachmentIdx    int32  `db:"attachment_idx"`
		PassedFilter     bool   `db:"passed_filter"`
		ExtractionStatus string `db:"extraction_status"`
		FetchedAt        string `db:"fetched_at"`
	}
	if err := db.Get(&row, "SELECT ticker, title, announcement_date::text, pdf_url, attachment_idx, passed_filter, extraction_status, fetched_at::text FROM disclosures WHERE pdf_url = $1", "https://example.com/announce-test-1a.pdf"); err != nil {
		t.Fatalf("fetch DISCX row: %v", err)
	}
	if row.Ticker != "DISCX" {
		t.Errorf("expected ticker DISCX, got %s", row.Ticker)
	}
	if row.Title != "Laporan Bulanan Registrasi Pemegang Efek" {
		t.Errorf("unexpected title %q", row.Title)
	}
	if row.AnnouncementDate != "2026-08-08" {
		t.Errorf("expected announcement_date 2026-08-08, got %s", row.AnnouncementDate)
	}
	if row.AttachmentIdx != 0 {
		t.Errorf("expected attachment_idx 0, got %d", row.AttachmentIdx)
	}
	if row.PassedFilter {
		t.Error("expected passed_filter false")
	}
	if row.ExtractionStatus != "pending" {
		t.Errorf("expected extraction_status pending, got %s", row.ExtractionStatus)
	}
	if row.FetchedAt == "" {
		t.Error("expected fetched_at set")
	}

	// Second attachment carries idx 1.
	var rowB struct {
		AttachmentIdx int32 `db:"attachment_idx"`
	}
	if err := db.Get(&rowB, "SELECT attachment_idx FROM disclosures WHERE pdf_url = $1", "https://example.com/announce-test-1b.pdf"); err != nil {
		t.Fatalf("fetch DISCX second row: %v", err)
	}
	if rowB.AttachmentIdx != 1 {
		t.Errorf("expected attachment_idx 1, got %d", rowB.AttachmentIdx)
	}

	// Unknown issuer auto-discovered as placeholder ticker.
	var rowY struct {
		Ticker string `db:"ticker"`
	}
	if err := db.Get(&rowY, "SELECT ticker FROM disclosures WHERE pdf_url = $1", "https://example.com/announce-test-2.pdf"); err != nil {
		t.Fatalf("fetch DISCY row: %v", err)
	}
	if rowY.Ticker != "DISCY" {
		t.Errorf("expected ticker DISCY, got %s", rowY.Ticker)
	}
	var name string
	if err := db.Get(&name, "SELECT name FROM tickers WHERE code = 'DISCY'"); err != nil {
		t.Fatalf("fetch placeholder ticker: %v", err)
	}
	if name != "DISCY" {
		t.Errorf("expected placeholder name DISCY, got %s", name)
	}
}

// TestDisclosureUpsert_PreservesFilterResult verifies that re-ingesting a
// disclosure does not clobber passed_filter / extraction_status set by the
// downstream filter pipeline.
func TestDisclosureUpsert_PreservesFilterResult(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	disclosureRepo := repository.NewDisclosureRepository(log)

	const pdfURL = "https://example.com/announce-test-filter.pdf"
	db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
	})

	d := &entity.Disclosure{
		AnnouncementDate: mustParseDate(t, "2026-08-08"),
		Title:            "Filtered Announcement",
		PdfURL:           pdfURL,
		AttachmentIdx:    0,
	}
	if err := disclosureRepo.Upsert(db, d); err != nil {
		t.Fatalf("initial upsert: %v", err)
	}

	// Filter pipeline marks the row as passing + extracted.
	db.MustExec("UPDATE disclosures SET passed_filter = true, extraction_status = 'ok' WHERE pdf_url = $1", pdfURL)

	// Re-ingest metadata — must not clobber filter results.
	if err := disclosureRepo.Upsert(db, d); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}

	var passed bool
	var status string
	if err := db.Get(&passed, "SELECT passed_filter FROM disclosures WHERE pdf_url = $1", pdfURL); err != nil {
		t.Fatalf("fetch passed_filter: %v", err)
	}
	if err := db.Get(&status, "SELECT extraction_status FROM disclosures WHERE pdf_url = $1", pdfURL); err != nil {
		t.Fatalf("fetch extraction_status: %v", err)
	}
	if !passed {
		t.Error("expected passed_filter preserved after re-ingest")
	}
	if status != "ok" {
		t.Errorf("expected extraction_status ok preserved, got %s", status)
	}
}

func mustParseDate(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatalf("parse date %s: %v", s, err)
	}
	return tt
}
