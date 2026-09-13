package usecase

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// fileScreenerFetcher reads a saved stock-screener/get response (get.json, the
// real 960-row sample pulled 2026-09-09) — a live-free demo of the seeder.
type fileScreenerFetcher struct {
	path string
}

func (f *fileScreenerFetcher) FetchScreener(ctx context.Context) ([]client.ScreenerRow, error) {
	raw, err := os.ReadFile(f.path)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Results []struct {
			StockCode       string  `json:"stockCode"`
			Sector          string  `json:"sector"`
			SubSector       string  `json:"subSector"`
			Industry        *string `json:"industry"`
			SubIndustry     *string `json:"subIndustry"`
			SubIndustryCode *string `json:"subIndustryCode"`
			IndexCode       *string `json:"indexCode"`
		} `json:"results"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	rows := make([]client.ScreenerRow, 0, len(resp.Results))
	for _, r := range resp.Results {
		rows = append(rows, client.ScreenerRow{
			StockCode: r.StockCode, Sector: r.Sector, SubSector: r.SubSector,
			Industry: r.Industry, SubIndustry: r.SubIndustry, SubIndustryCode: r.SubIndustryCode,
			IndexCode: r.IndexCode,
		})
	}
	return rows, nil
}

// TestSectorIndexDemo_RealSample runs the seeder against the saved 960-row
// screener sample and verifies the issue's demo criteria: BBRI's tickers row
// carries sector + indices, and ticker_indices has LQ45 membership with an
// effective_date. Skipped unless IDX_MCP_DB_DSN is set.
func TestSectorIndexDemo_RealSample(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping demo")
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

	// Snapshot the pre-demo tickers state (codes + sector columns) so the
	// cleanup can restore it exactly: the seeder upserts sector columns on
	// existing rows and inserts brand-new rows (name = code placeholder), and
	// other tests share this DB — a leaked footprint breaks the RSS news
	// matcher (it matches title words against ticker codes).
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
		// Restore sector columns on pre-existing rows, then drop the rows the
		// seeder inserted (codes that were not present before the demo).
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
	uc := NewSectorIndexUseCase(
		db, log, &fileScreenerFetcher{path: path},
		repository.NewTickerRepository(log), repository.NewTickerIndexRepository(log), recorder,
	)

	runDate := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	n, m, err := uc.SeedSectorIndex(context.Background(), runDate)
	if err != nil {
		t.Fatalf("SeedSectorIndex: %v", err)
	}
	t.Logf("seeded %d ticker rows, %d membership rows", n, m)

	// Demo criterion 1: BBRI tickers row has sector + indices.
	repo := repository.NewTickerRepository(log)
	bbri, err := repo.FindByCode(db, "BBRI")
	if err != nil {
		t.Fatalf("FindByCode BBRI: %v", err)
	}
	if bbri.Sektor == nil || *bbri.Sektor != "Financials" {
		t.Errorf("BBRI sektor = %v, want Financials", bbri.Sektor)
	}
	if bbri.SubSektor == nil || *bbri.SubSektor != "Banks" {
		t.Errorf("BBRI sub_sektor = %v, want Banks", bbri.SubSektor)
	}
	if bbri.SubIndustryCode == nil || *bbri.SubIndustryCode != "G111" {
		t.Errorf("BBRI sub_industry_code = %v, want G111", bbri.SubIndustryCode)
	}

	// Demo criterion 2: ticker_indices has LQ45 membership with effective_date.
	var lq45 int
	if err := db.Get(&lq45, "SELECT COUNT(*) FROM ticker_indices WHERE ticker_code = 'BBRI' AND index_code = 'LQ45' AND effective_date = $1", runDate); err != nil {
		t.Fatalf("LQ45 count: %v", err)
	}
	if lq45 != 1 {
		t.Errorf("BBRI LQ45 membership rows = %d, want 1", lq45)
	}

	// Dirty-row cleanup verified against the real sample: KETR sector nil,
	// VICI mapped, 8 null-indexCode rows produced no membership.
	ketr, err := repo.FindByCode(db, "KETR")
	if err != nil {
		t.Fatalf("FindByCode KETR: %v", err)
	}
	if ketr.Sektor != nil {
		t.Errorf("KETR sektor = %v, want nil (No Sector)", ketr.Sektor)
	}
	vici, err := repo.FindByCode(db, "VICI")
	if err != nil {
		t.Fatalf("FindByCode VICI: %v", err)
	}
	if vici.Sektor == nil || *vici.Sektor != "Consumer Non-Cyclicals" {
		t.Errorf("VICI sektor = %v, want Consumer Non-Cyclicals", vici.Sektor)
	}
	var nullIdx int
	if err := db.Get(&nullIdx, "SELECT COUNT(*) FROM ticker_indices WHERE ticker_code IN ('FIMP','FLMC','FREN','GOTOM','KING','MENN','MSIE','PGJO')"); err != nil {
		t.Fatalf("null-indexCode count: %v", err)
	}
	if nullIdx != 0 {
		t.Errorf("null-indexCode tickers produced %d membership rows, want 0", nullIdx)
	}
}
