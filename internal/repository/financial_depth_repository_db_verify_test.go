package repository

import (
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

func depthPtrF(v float64) *float64 { return &v }
func depthPtrS(v string) *string   { return &v }

func depthPtrT(v time.Time) *time.Time { return &v }

// TestFinancialDepthRepository_ApplyIdempotent exercises the depth columns,
// the append-only depth trail and the run/taxonomy ledgers against a real
// Postgres. Skipped unless IDX_MCP_DB_DSN is set; test rows use 2095 so
// production data is never touched.
func TestFinancialDepthRepository_ApplyIdempotent(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := NewFinancialDepthRepository(log)
	finRepo := NewFinancialsRepository(log)

	cleanup := func() {
		db.MustExec(`DELETE FROM financial_statement_depth_versions WHERE ticker = 'TESTFD'`)
		db.MustExec(`DELETE FROM financial_depth_runs WHERE ticker = 'TESTFD'`)
		db.MustExec(`DELETE FROM ticker_financial_taxonomy WHERE ticker = 'TESTFD'`)
		db.MustExec(`DELETE FROM financial_statement_versions WHERE ticker = 'TESTFD'`)
		db.MustExec(`DELETE FROM financial_statements WHERE ticker = 'TESTFD'`)
		db.MustExec(`DELETE FROM financial_snapshot_runs WHERE snapshot_month = '2095-01-01'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	period := time.Date(2095, time.September, 30, 0, 0, 0, 0, time.UTC)
	month := time.Date(2095, time.October, 1, 0, 0, 0, 0, time.UTC)
	st := finTestStatement("TESTFD", 5000000000000)
	st.PeriodEnd = period
	st.FiscalYearEnd = time.Date(2095, time.December, 31, 0, 0, 0, 0, time.UTC)
	st.DurationMonths = 9
	st.SourceFSDate = period
	if _, err := finRepo.IngestSnapshot(db, finSnapshot(month, st), finRun(month)); err != nil {
		t.Fatalf("seed Tier-1: %v", err)
	}

	cash := 123.5
	gross := 456.75
	hash := "depth-hash-1"
	coverage := `{"cash":"observed","gross_profit":"observed"}`
	raw := `{"cash":{"amount":123.5,"currency":"IDR"}}`
	conflicts := `[]`
	snapshot := `{"ticker":"TESTFD","values":{"cash":123.5}}`
	row := entity.FinancialStatementDepth{
		Ticker: "TESTFD", PeriodEnd: period,
		Cash: &cash, GrossProfit: &gross,
		Currency: depthPtrS("IDR"), FXRate: depthPtrF(1),
		FXDate: depthPtrT(period), PeriodBasis: depthPtrS(financials.PeriodBasisCumulativeYTD),
		Methodology:   depthPtrS(financials.MethodologyMorningstar),
		DepthCoverage: &coverage, DepthRaw: &raw, DepthConflicts: &conflicts,
		DepthStatus:      depthPtrS(financials.DepthStatusObserved),
		DepthFetchedAt:   depthPtrT(time.Date(2095, time.October, 2, 0, 0, 0, 0, time.UTC)),
		DepthContentHash: &hash,
		ObservedMonth:    depthPtrT(month), DepthSnapshot: &snapshot,
	}

	written, err := repo.ApplyDepth(db, month, []entity.FinancialStatementDepth{row})
	if err != nil {
		t.Fatalf("ApplyDepth: %v", err)
	}
	if written != 1 {
		t.Fatalf("written = %d, want 1", written)
	}
	read, err := finRepo.FindByTicker(db, "TESTFD")
	if err != nil {
		t.Fatalf("FindByTicker: %v", err)
	}
	if len(read) != 1 || read[0].Cash == nil || *read[0].Cash != cash {
		t.Fatalf("cash = %+v, want %v", read[0].Cash, cash)
	}
	if read[0].GrossProfit == nil || *read[0].GrossProfit != gross {
		t.Errorf("gross_profit = %v, want %v", read[0].GrossProfit, gross)
	}
	if read[0].DepthStatus == nil || *read[0].DepthStatus != financials.DepthStatusObserved {
		t.Errorf("depth_status = %v", read[0].DepthStatus)
	}

	versions, err := repo.FindDepthVersions(db, "TESTFD", period)
	if err != nil {
		t.Fatalf("FindDepthVersions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("depth versions = %d, want 1", len(versions))
	}

	// Same content hash: no write, no new version.
	written, err = repo.ApplyDepth(db, month, []entity.FinancialStatementDepth{row})
	if err != nil {
		t.Fatalf("second ApplyDepth: %v", err)
	}
	if written != 0 {
		t.Errorf("idempotent apply written = %d, want 0", written)
	}

	// Changed value: update the row and overwrite that month's version.
	newCash := 999.0
	row.Cash = &newCash
	row.DepthContentHash = depthPtrS("depth-hash-2")
	written, err = repo.ApplyDepth(db, month, []entity.FinancialStatementDepth{row})
	if err != nil {
		t.Fatalf("changed ApplyDepth: %v", err)
	}
	if written != 1 {
		t.Errorf("changed apply written = %d, want 1", written)
	}
	versions, err = repo.FindDepthVersions(db, "TESTFD", period)
	if err != nil {
		t.Fatalf("FindDepthVersions after change: %v", err)
	}
	if len(versions) != 1 {
		t.Errorf("depth versions after same-month change = %d, want 1", len(versions))
	}
	read, _ = finRepo.FindByTicker(db, "TESTFD")
	if read[0].Cash == nil || *read[0].Cash != newCash {
		t.Errorf("cash after change = %v, want %v", read[0].Cash, newCash)
	}

	// Taxonomy and run ledgers.
	tax := &entity.TickerFinancialTaxonomy{
		Ticker: "TESTFD", Taxonomy: string(financials.TaxonomyGeneral154),
		DiscoveredAt: month, Source: "discriminant_probe",
		DepthStatus: financials.DepthStatusObserved, LastDepthMonth: &month,
	}
	if err := repo.UpsertTaxonomy(db, tax); err != nil {
		t.Fatalf("UpsertTaxonomy: %v", err)
	}
	gotTax, err := repo.FindTaxonomy(db, "TESTFD")
	if err != nil || gotTax == nil || gotTax.Taxonomy != tax.Taxonomy {
		t.Fatalf("taxonomy = %+v, err %v", gotTax, err)
	}
	run := &entity.FinancialDepthRun{
		RunMonth: month, Ticker: "TESTFD", Status: financials.DepthStatusObserved,
		RowsWritten: 1, Attempts: 1,
	}
	if err := repo.UpsertDepthRun(db, run); err != nil {
		t.Fatalf("UpsertDepthRun: %v", err)
	}
	if err := repo.UpsertDepthRun(db, run); err != nil {
		t.Fatalf("UpsertDepthRun retry: %v", err)
	}
	gotRun, err := repo.FindDepthRun(db, month, "TESTFD")
	if err != nil || gotRun == nil || gotRun.Attempts != 2 {
		t.Fatalf("run = %+v, err %v; want attempts 2", gotRun, err)
	}
}
