package usecase

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

// TestTickerMetadata_Demo_BBRI is the ticket-16 demo: one query returns
// BBRI's sector + index membership from the seeded data. It seeds from the
// saved 960-row screener sample (get.json — no live IDX call) via the 15b
// seeder, reads through GetTickerMetadata, then restores the tickers state.
// Skipped unless IDX_MCP_DB_DSN is set.
func TestTickerMetadata_Demo_BBRI(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed demo")
	}
	path := os.Getenv("SCREENER_SAMPLE")
	if path == "" {
		path = "../../get.json"
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("screener sample %s not present; skipping demo", path)
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Snapshot the pre-demo tickers state so the cleanup can restore it
	// exactly (same reasoning as TestSectorIndexDemo_RealSample: other tests
	// share this DB — a leaked footprint breaks the RSS news matcher).
	type sectorState struct {
		Code            string
		Sektor          *string `db:"sektor"`
		Industri        *string `db:"industri"`
		SubSektor       *string `db:"sub_sektor"`
		SubIndustry     *string `db:"sub_industry"`
		SubIndustryCode *string `db:"sub_industry_code"`
	}
	var before []sectorState
	if err := db.Select(&before, "SELECT code, sektor, industri, sub_sektor, sub_industry, sub_industry_code FROM tickers"); err != nil {
		t.Fatalf("snapshot tickers: %v", err)
	}
	preCodes := make([]string, 0, len(before))
	for _, s := range before {
		preCodes = append(preCodes, s.Code)
	}

	cleanup := func() {
		db.MustExec("DELETE FROM ticker_indices WHERE effective_date = '2026-09-09'")
		db.MustExec("DELETE FROM source_status WHERE source = $1", sectorIndexSource)
		for _, s := range before {
			db.MustExec("UPDATE tickers SET sektor=$2, industri=$3, sub_sektor=$4, sub_industry=$5, sub_industry_code=$6 WHERE code=$1",
				s.Code, s.Sektor, s.Industri, s.SubSektor, s.SubIndustry, s.SubIndustryCode)
		}
		db.MustExec("DELETE FROM tickers WHERE NOT (code = ANY($1))", preCodes)
	}
	cleanup()
	t.Cleanup(cleanup)

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	seeder := NewSectorIndexUseCase(
		db, log, &fileScreenerFetcher{path: path},
		repository.NewTickerRepository(log), repository.NewTickerIndexRepository(log), recorder,
	)
	runDate := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	if _, _, err := seeder.SeedSectorIndex(context.Background(), runDate); err != nil {
		t.Fatalf("SeedSectorIndex: %v", err)
	}

	uc := NewTickerMetadataUseCase(db, log, repository.NewTickerRepository(log), repository.NewTickerIndexRepository(log))

	bbri := "BBRI"
	resp, err := uc.GetTickerMetadata(context.Background(), &bbri, nil)
	if err != nil {
		t.Fatalf("GetTickerMetadata(BBRI): %v", err)
	}
	if resp.Count != 1 || len(resp.Tickers) != 1 {
		t.Fatalf("count = %d tickers = %d, want 1", resp.Count, len(resp.Tickers))
	}
	m := resp.Tickers[0]
	if m.Ticker != "BBRI" {
		t.Errorf("ticker = %q, want BBRI", m.Ticker)
	}
	if m.Sector == nil || *m.Sector != "Financials" {
		t.Errorf("sector = %v, want Financials", m.Sector)
	}
	if len(m.Indices) == 0 {
		t.Errorf("indices empty, want at least one (BBRI is on major indices)")
	}
	if resp.EffectiveDate != "2026-09-09" {
		t.Errorf("effective_date = %q, want 2026-09-09 (seeder run date)", resp.EffectiveDate)
	}
	t.Logf("BBRI: sector=%s industry=%s sub_industry=%s indices=%v effective_date=%s",
		strPtrStr(m.Sector), strPtrStr(m.Industry), strPtrStr(m.SubIndustry), m.Indices, resp.EffectiveDate)
}

func strPtrStr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
