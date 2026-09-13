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

// TestTickerRepository_UpsertProfiles_EndToEnd verifies the profile upsert
// against a real Postgres: rows land with the profile fields, re-running the
// same codes is idempotent (update, not duplicate), the upsert leaves
// shares/sektor/industri untouched (populated by other sources), and a nil
// listing date can't clobber a previously-good value. Skipped unless
// IDX_MCP_DB_DSN is set. Cleanup is scoped to the TESTPA/TESTPB tickers.
func TestTickerRepository_UpsertProfiles_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewTickerRepository(log)

	cleanup := func() {
		db.MustExec("DELETE FROM tickers WHERE code IN ('TESTPA', 'TESTPB')")
	}
	cleanup()
	t.Cleanup(cleanup)

	// Pre-seed shares/sektor/industri via the full Upsert so the profile
	// upsert's non-touch guarantee is observable.
	date := time.Date(2024, 12, 5, 0, 0, 0, 0, time.UTC)
	seeded := &entity.Ticker{
		Code:     "TESTPA",
		Name:     "Old Name",
		Shares:   int64Ptr(1_000_000),
		Sektor:   strPtr("Energi"),
		Industri: strPtr("Batu Bara"),
		Active:   true,
	}
	if err := repo.Upsert(db, seeded); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}

	rows := []entity.Ticker{
		{Code: "TESTPA", Name: "PT Test Alpha Tbk", ListingDate: &date, ListingBoard: strPtr("Utama"), Active: true},
		{Code: "TESTPB", Name: "PT Test Beta Tbk", ListingDate: &date, ListingBoard: strPtr("Pengembangan"), Active: false},
	}
	if err := repo.UpsertProfiles(db, rows); err != nil {
		t.Fatalf("UpsertProfiles: %v", err)
	}

	// Profile fields landed; shares/sektor/industri survived untouched.
	got, err := repo.FindByCode(db, "TESTPA")
	if err != nil {
		t.Fatalf("FindByCode: %v", err)
	}
	if got.Name != "PT Test Alpha Tbk" {
		t.Errorf("name = %q, want PT Test Alpha Tbk", got.Name)
	}
	if got.ListingBoard == nil || *got.ListingBoard != "Utama" {
		t.Errorf("listing_board = %v, want Utama", got.ListingBoard)
	}
	if got.ListingDate == nil || got.ListingDate.Format("2006-01-02") != "2024-12-05" {
		t.Errorf("listing_date = %v, want 2024-12-05", got.ListingDate)
	}
	if got.Shares == nil || *got.Shares != 1_000_000 {
		t.Errorf("shares = %v, want 1000000 (untouched)", got.Shares)
	}
	if got.Sektor == nil || *got.Sektor != "Energi" {
		t.Errorf("sektor = %v, want Energi (untouched)", got.Sektor)
	}
	if got.Industri == nil || *got.Industri != "Batu Bara" {
		t.Errorf("industri = %v, want Batu Bara (untouched)", got.Industri)
	}

	// TESTPB landed with active=false.
	gotB, err := repo.FindByCode(db, "TESTPB")
	if err != nil {
		t.Fatalf("FindByCode TESTPB: %v", err)
	}
	if gotB.Active {
		t.Errorf("TESTPB active = true, want false")
	}

	// Re-run same codes → idempotent, no duplicate, count unchanged.
	if err := repo.UpsertProfiles(db, rows); err != nil {
		t.Fatalf("second UpsertProfiles: %v", err)
	}
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM tickers WHERE code IN ('TESTPA', 'TESTPB')"); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("row count after re-run = %d, want 2", count)
	}

	// A re-run with a nil listing date (unparseable wire date) must NOT clobber
	// the previously-good value — COALESCE keeps the stored date.
	nilDate := []entity.Ticker{
		{Code: "TESTPA", Name: "PT Test Alpha Tbk", ListingDate: nil, ListingBoard: strPtr("Utama"), Active: true},
	}
	if err := repo.UpsertProfiles(db, nilDate); err != nil {
		t.Fatalf("nil-date UpsertProfiles: %v", err)
	}
	got, err = repo.FindByCode(db, "TESTPA")
	if err != nil {
		t.Fatalf("FindByCode after nil-date: %v", err)
	}
	if got.ListingDate == nil || got.ListingDate.Format("2006-01-02") != "2024-12-05" {
		t.Errorf("listing_date after nil-date re-run = %v, want 2024-12-05 preserved", got.ListingDate)
	}

	// Empty upsert is a no-op.
	if err := repo.UpsertProfiles(db, nil); err != nil {
		t.Fatalf("empty UpsertProfiles: %v", err)
	}
}

// TestTickerRepository_Upsert_PreservesOtherSourcesColumns verifies the daily
// ticker-refresh path (stock_summary/disclosure ingests call Upsert with only
// name/shares/active set) does NOT wipe the metadata columns other sources own:
// listing_date/listing_board (11b profile seeder) and sektor/industri (15b
// sector seeder). Skipped unless IDX_MCP_DB_DSN is set. Cleanup scoped to
// TESTPC.
func TestTickerRepository_Upsert_PreservesOtherSourcesColumns(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewTickerRepository(log)

	cleanup := func() {
		db.MustExec("DELETE FROM tickers WHERE code = 'TESTPC'")
	}
	cleanup()
	t.Cleanup(cleanup)

	// Seed the full metadata set as the 11b + 15b seeders would: profile
	// columns via Upsert, sector columns via UpsertSectorIndex (Upsert's column
	// list doesn't include the sub_* levels).
	date := time.Date(2024, 12, 5, 0, 0, 0, 0, time.UTC)
	seeded := &entity.Ticker{
		Code: "TESTPC", Name: "PT Test Preserve Tbk",
		ListingDate: &date, ListingBoard: strPtr("Utama"),
		Sektor: strPtr("Energy"), Industri: strPtr("Coal"),
		Active: true,
	}
	if err := repo.Upsert(db, seeded); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}
	if err := repo.UpsertSectorIndex(db, []entity.Ticker{{
		Code: "TESTPC", Sektor: strPtr("Energy"), Industri: strPtr("Coal"),
		SubSektor: strPtr("Oil, Gas & Coal"), SubIndustry: strPtr("Coal Production"), SubIndustryCode: strPtr("A121"),
	}}); err != nil {
		t.Fatalf("seed UpsertSectorIndex: %v", err)
	}

	// Daily refresh: only name/shares/active set, everything else nil — the
	// exact shape tickerFromSummaryItem produces.
	daily := &entity.Ticker{Code: "TESTPC", Name: "PT Test Preserve Tbk", Shares: int64Ptr(9_000_000), Active: true}
	if err := repo.Upsert(db, daily); err != nil {
		t.Fatalf("daily Upsert: %v", err)
	}

	got, err := repo.FindByCode(db, "TESTPC")
	if err != nil {
		t.Fatalf("FindByCode: %v", err)
	}
	if got.ListingDate == nil || got.ListingDate.Format("2006-01-02") != "2024-12-05" {
		t.Errorf("listing_date = %v, want 2024-12-05 preserved", got.ListingDate)
	}
	if got.ListingBoard == nil || *got.ListingBoard != "Utama" {
		t.Errorf("listing_board = %v, want Utama preserved", got.ListingBoard)
	}
	if got.Sektor == nil || *got.Sektor != "Energy" {
		t.Errorf("sektor = %v, want Energy preserved", got.Sektor)
	}
	if got.Industri == nil || *got.Industri != "Coal" {
		t.Errorf("industri = %v, want Coal preserved", got.Industri)
	}
	if got.SubSektor == nil || *got.SubSektor != "Oil, Gas & Coal" {
		t.Errorf("sub_sektor = %v, want Oil, Gas & Coal preserved", got.SubSektor)
	}
	if got.Shares == nil || *got.Shares != 9_000_000 {
		t.Errorf("shares = %v, want 9000000 (daily refresh still writes shares)", got.Shares)
	}
}

func int64Ptr(v int64) *int64 { return &v }
func strPtr(v string) *string { return &v }
