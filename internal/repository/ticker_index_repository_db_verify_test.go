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

// TestTickerIndexRepository_ReplaceMembership_EndToEnd verifies the
// point-in-time replace against a real Postgres: rows land with the
// effective_date, a same-day re-run replaces the snapshot (a dropped index is
// removed, not orphaned), and a different effective_date coexists as a
// separate historical snapshot. Skipped unless IDX_MCP_DB_DSN is set. Cleanup
// is scoped to the TESTIA/TESTIB tickers.
func TestTickerIndexRepository_ReplaceMembership_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewTickerIndexRepository(log)

	cleanup := func() {
		db.MustExec("DELETE FROM ticker_indices WHERE ticker_code IN ('TESTIA', 'TESTIB')")
	}
	cleanup()
	t.Cleanup(cleanup)

	day1 := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	rows := []entity.TickerIndex{
		{TickerCode: "TESTIA", IndexCode: "LQ45", EffectiveDate: day1},
		{TickerCode: "TESTIA", IndexCode: "COMPOSITE", EffectiveDate: day1},
		{TickerCode: "TESTIB", IndexCode: "LQ45", EffectiveDate: day1},
	}
	if err := repo.ReplaceMembership(db, day1, rows); err != nil {
		t.Fatalf("ReplaceMembership: %v", err)
	}

	// All rows landed with the effective_date.
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM ticker_indices WHERE effective_date = $1", day1); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Errorf("row count = %d, want 3", count)
	}

	// Re-run same day with TESTIA dropping COMPOSITE → snapshot replaced, the
	// dropped membership is gone (not orphaned).
	rerun := []entity.TickerIndex{
		{TickerCode: "TESTIA", IndexCode: "LQ45", EffectiveDate: day1},
		{TickerCode: "TESTIB", IndexCode: "LQ45", EffectiveDate: day1},
	}
	if err := repo.ReplaceMembership(db, day1, rerun); err != nil {
		t.Fatalf("re-run ReplaceMembership: %v", err)
	}
	var composite int
	if err := db.Get(&composite, "SELECT COUNT(*) FROM ticker_indices WHERE ticker_code = 'TESTIA' AND index_code = 'COMPOSITE' AND effective_date = $1", day1); err != nil {
		t.Fatalf("composite count: %v", err)
	}
	if composite != 0 {
		t.Errorf("dropped COMPOSITE membership still present (%d rows), want 0", composite)
	}

	// A later effective_date coexists as a separate historical snapshot.
	day2 := time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC)
	if err := repo.ReplaceMembership(db, day2, []entity.TickerIndex{
		{TickerCode: "TESTIA", IndexCode: "LQ45", EffectiveDate: day2},
	}); err != nil {
		t.Fatalf("day2 ReplaceMembership: %v", err)
	}
	var day1Count, day2Count int
	if err := db.Get(&day1Count, "SELECT COUNT(*) FROM ticker_indices WHERE effective_date = $1", day1); err != nil {
		t.Fatalf("day1 count: %v", err)
	}
	if err := db.Get(&day2Count, "SELECT COUNT(*) FROM ticker_indices WHERE effective_date = $1", day2); err != nil {
		t.Fatalf("day2 count: %v", err)
	}
	if day1Count != 2 || day2Count != 1 {
		t.Errorf("snapshots = day1:%d day2:%d, want 2 and 1 (coexisting)", day1Count, day2Count)
	}

	// Empty slice clears the day's snapshot.
	if err := repo.ReplaceMembership(db, day2, nil); err != nil {
		t.Fatalf("empty ReplaceMembership: %v", err)
	}
	if err := db.Get(&day2Count, "SELECT COUNT(*) FROM ticker_indices WHERE effective_date = $1", day2); err != nil {
		t.Fatalf("day2 count after clear: %v", err)
	}
	if day2Count != 0 {
		t.Errorf("day2 rows after empty replace = %d, want 0", day2Count)
	}
}

// TestTickerRepository_UpsertSectorIndex_EndToEnd verifies the sector-column
// upsert against a real Postgres: sector fields land, non-sector columns
// (name, shares) survive untouched, a brand-new code is inserted with
// name = code, and a re-run is idempotent. Skipped unless IDX_MCP_DB_DSN is
// set. Cleanup is scoped to the TESTSA/TESTSB tickers.
func TestTickerRepository_UpsertSectorIndex_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	repo := NewTickerRepository(log)

	cleanup := func() {
		db.MustExec("DELETE FROM tickers WHERE code IN ('TESTSA', 'TESTSB')")
	}
	cleanup()
	t.Cleanup(cleanup)

	// Pre-seed TESTSA with a name + shares so the non-touch guarantee is
	// observable.
	seeded := &entity.Ticker{
		Code:   "TESTSA",
		Name:   "PT Test Sector Alpha Tbk",
		Shares: int64Ptr(5_000_000),
		Active: true,
	}
	if err := repo.Upsert(db, seeded); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}

	rows := []entity.Ticker{
		{Code: "TESTSA", Sektor: strPtr("Energy"), Industri: strPtr("Coal"), SubSektor: strPtr("Oil, Gas & Coal"), SubIndustry: strPtr("Coal Production"), SubIndustryCode: strPtr("A121")},
		{Code: "TESTSB", Sektor: strPtr("Financials"), Industri: strPtr("Banks"), SubSektor: strPtr("Banks"), SubIndustry: strPtr("Banks"), SubIndustryCode: strPtr("G111")},
	}
	if err := repo.UpsertSectorIndex(db, rows); err != nil {
		t.Fatalf("UpsertSectorIndex: %v", err)
	}

	// Sector fields landed; name/shares survived untouched.
	got, err := repo.FindByCode(db, "TESTSA")
	if err != nil {
		t.Fatalf("FindByCode: %v", err)
	}
	if got.Sektor == nil || *got.Sektor != "Energy" {
		t.Errorf("sektor = %v, want Energy", got.Sektor)
	}
	if got.Industri == nil || *got.Industri != "Coal" {
		t.Errorf("industri = %v, want Coal", got.Industri)
	}
	if got.SubSektor == nil || *got.SubSektor != "Oil, Gas & Coal" {
		t.Errorf("sub_sektor = %v, want Oil, Gas & Coal", got.SubSektor)
	}
	if got.SubIndustry == nil || *got.SubIndustry != "Coal Production" {
		t.Errorf("sub_industry = %v, want Coal Production", got.SubIndustry)
	}
	if got.SubIndustryCode == nil || *got.SubIndustryCode != "A121" {
		t.Errorf("sub_industry_code = %v, want A121", got.SubIndustryCode)
	}
	if got.Name != "PT Test Sector Alpha Tbk" {
		t.Errorf("name = %q, want untouched", got.Name)
	}
	if got.Shares == nil || *got.Shares != 5_000_000 {
		t.Errorf("shares = %v, want 5000000 (untouched)", got.Shares)
	}

	// TESTSB was absent → inserted with name = code placeholder.
	gotB, err := repo.FindByCode(db, "TESTSB")
	if err != nil {
		t.Fatalf("FindByCode TESTSB: %v", err)
	}
	if gotB.Name != "TESTSB" {
		t.Errorf("TESTSB name = %q, want code placeholder", gotB.Name)
	}
	if gotB.Sektor == nil || *gotB.Sektor != "Financials" {
		t.Errorf("TESTSB sektor = %v, want Financials", gotB.Sektor)
	}

	// Re-run → idempotent, no duplicate.
	if err := repo.UpsertSectorIndex(db, rows); err != nil {
		t.Fatalf("second UpsertSectorIndex: %v", err)
	}
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM tickers WHERE code IN ('TESTSA', 'TESTSB')"); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("row count after re-run = %d, want 2", count)
	}

	// Empty upsert is a no-op.
	if err := repo.UpsertSectorIndex(db, nil); err != nil {
		t.Fatalf("empty UpsertSectorIndex: %v", err)
	}
}
