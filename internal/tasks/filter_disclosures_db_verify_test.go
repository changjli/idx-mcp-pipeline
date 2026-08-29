package tasks

import (
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestExistsFetchedOnDate verifies the "idx:announcements ran today" marker
// that filter:disclosures self-syncs on. Skipped unless IDX_MCP_DB_DSN is set.
func TestExistsFetchedOnDate(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	repo := repository.NewDisclosureRepository(log)

	const url = "https://example.com/filter-test-fetched.pdf"
	ticker := "FILTFETCH"
	db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", url)
	db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", url)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $1, true)", ticker)

	today := time.Now().Format("2006-01-02")
	tomorrow := time.Now().AddDate(0, 0, 1).Format("2006-01-02")

	// Before any upsert: no row fetched today.
	if ok, err := repo.ExistsFetchedOnDate(db, today); err != nil {
		t.Fatalf("ExistsFetchedOnDate pre-upsert: %v", err)
	} else if ok {
		t.Error("expected false before any upsert for today")
	}

	// Upsert stamps fetched_at = NOW() (today). Use the repo's own Upsert so the
	// marker reflects the real announcements write path.
	announced := mustParseDate(t, "2026-08-05")
	d := &entity.Disclosure{
		Ticker:           &ticker,
		AnnouncementDate: announced,
		Title:            "Pemanggilan RUPS Tahunan",
		PdfURL:           url,
	}
	if err := repo.Upsert(db, d); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if ok, err := repo.ExistsFetchedOnDate(db, today); err != nil {
		t.Fatalf("ExistsFetchedOnDate post-upsert: %v", err)
	} else if !ok {
		t.Error("expected true after upsert fetched today")
	}
	if ok, err := repo.ExistsFetchedOnDate(db, tomorrow); err != nil {
		t.Fatalf("ExistsFetchedOnDate tomorrow: %v", err)
	} else if ok {
		t.Error("expected false for tomorrow (no row fetched then)")
	}
}
