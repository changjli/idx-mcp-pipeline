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

func finI64(v int64) *int64     { return &v }
func finF64(v float64) *float64 { return &v }

func finTestMonth(y int, m time.Month) time.Time {
	return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
}

func finTestStatement(ticker string, assets int64) financials.Statement {
	period := time.Date(2090, time.June, 30, 0, 0, 0, 0, time.UTC)
	return financials.Statement{
		Ticker:          ticker,
		PeriodEnd:       period,
		FiscalYearEnd:   time.Date(2090, time.December, 31, 0, 0, 0, 0, time.UTC),
		DurationMonths:  6,
		Assets:          finI64(assets),
		Liabilities:     finI64(40000000000),
		Equity:          finI64(assets - 40000000000),
		Revenue:         finI64(45581520000000),
		EBT:             finI64(11896820000000),
		NetProfit:       finI64(9386810000000),
		NetProfitParent: finI64(8479920000000),
		EPS:             finF64(1833.76),
		BookValue:       finF64(9043.36),
		ROA:             finF64(12.62),
		ROE:             finF64(20.2774),
		NPM:             finF64(31.327),
		DER:             finF64(0.61),
		AuditStatus:     finStr("LR"),
		Source:          financials.SourceRatioSnapshot,
		ValueBasis:      financials.ValueBasisIDX,
		SourceFSDate:    period,
	}
}

func finStr(s string) *string { return &s }

func finSnapshot(month time.Time, st financials.Statement) *financials.Snapshot {
	share := 1.0
	return &financials.Snapshot{
		Month:           month,
		Status:          financials.StatusOK,
		RowCount:        1,
		DistinctPeriods: 1,
		QuarterEndShare: &share,
		ContentHash:     "payload-" + month.Format("2006-01"),
		Statements:      []financials.Statement{st},
	}
}

func finRun(month time.Time) *entity.FinancialSnapshotRun {
	hash := "payload-" + month.Format("2006-01")
	share := 1.0
	return &entity.FinancialSnapshotRun{
		SnapshotMonth:   month,
		Source:          financials.SourceRatioSnapshot,
		Status:          financials.StatusOK,
		RowCount:        1,
		DistinctPeriods: 1,
		QuarterEndShare: &share,
		ContentHash:     &hash,
	}
}

// TestFinancialsRepository_Ingest exercises the idempotency contract against a
// real Postgres: one row per statement, version rows only on first sight and
// value changes, and an older snapshot never overwriting a newer observation.
// Skipped unless IDX_MCP_DB_DSN is set; all test months are in 2090 so
// production rows are never touched.
func TestFinancialsRepository_Ingest(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	repo := NewFinancialsRepository(log)

	cleanup := func() {
		db.MustExec(`DELETE FROM financial_statement_versions WHERE ticker = 'TESTFA'`)
		db.MustExec(`DELETE FROM financial_statements WHERE ticker = 'TESTFA'`)
		db.MustExec(`DELETE FROM financial_snapshot_runs WHERE snapshot_month >= '2090-01-01' AND snapshot_month < '2091-01-01'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	month := finTestMonth(2090, time.October)
	period := time.Date(2090, time.June, 30, 0, 0, 0, 0, time.UTC)

	// First sight: one statement, one version, run ledger ok.
	st := finTestStatement("TESTFA", 113148130000000)
	stats, err := repo.IngestSnapshot(db, finSnapshot(month, st), finRun(month))
	if err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if stats.Inserted != 1 || stats.Versions != 1 {
		t.Fatalf("first ingest stats = %+v, want inserted 1 / versions 1", stats)
	}

	rows, err := repo.FindByTicker(db, "TESTFA")
	if err != nil {
		t.Fatalf("FindByTicker: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	got := rows[0]
	if got.Assets == nil || *got.Assets != 113148130000000 {
		t.Errorf("assets = %v, want exact rupiah 113148130000000", got.Assets)
	}
	if got.AuditStatus == nil || *got.AuditStatus != "LR" {
		t.Errorf("audit = %v, want LR", got.AuditStatus)
	}
	if !got.FirstSeenMonth.Equal(month) || !got.LastSeenMonth.Equal(month) {
		t.Errorf("seen = %s..%s, want %s", got.FirstSeenMonth, got.LastSeenMonth, month)
	}
	if got.RevisionCount != 0 {
		t.Errorf("revision_count = %d, want 0", got.RevisionCount)
	}
	if got.ContentHash != st.ContentHash() {
		t.Errorf("content hash mismatch")
	}
	firstUpdatedAt := got.UpdatedAt

	versions, err := repo.FindVersions(db, "TESTFA", period)
	if err != nil {
		t.Fatalf("FindVersions: %v", err)
	}
	if len(versions) != 1 || !versions[0].SeenMonth.Equal(month) {
		t.Fatalf("versions = %+v, want one seen in %s", versions, month)
	}

	run, err := repo.FindRun(db, month)
	if err != nil {
		t.Fatalf("FindRun: %v", err)
	}
	if run == nil || run.Status != financials.StatusOK || run.RowCount != 1 || run.DistinctPeriods != 1 {
		t.Fatalf("run = %+v, want ok/1/1", run)
	}
	if run.QuarterEndShare == nil || *run.QuarterEndShare != 1 {
		t.Errorf("run share = %v, want 1", run.QuarterEndShare)
	}

	// Unchanged re-run: no duplicate row, no version, no revision bump.
	stats, err = repo.IngestSnapshot(db, finSnapshot(month, st), finRun(month))
	if err != nil {
		t.Fatalf("idempotent re-run: %v", err)
	}
	if stats.Unchanged != 1 || stats.Versions != 0 {
		t.Errorf("re-run stats = %+v, want unchanged 1 / versions 0", stats)
	}
	versions, _ = repo.FindVersions(db, "TESTFA", period)
	if len(versions) != 1 {
		t.Errorf("versions after re-run = %d, want 1", len(versions))
	}
	rows, _ = repo.FindByTicker(db, "TESTFA")
	if rows[0].RevisionCount != 0 {
		t.Errorf("revision after re-run = %d, want 0", rows[0].RevisionCount)
	}
	if !rows[0].UpdatedAt.Equal(firstUpdatedAt) {
		t.Errorf("updated_at moved on an unchanged re-run: %s -> %s", firstUpdatedAt, rows[0].UpdatedAt)
	}

	// An older snapshot with a different value must be skipped entirely.
	older := finTestMonth(2090, time.September)
	olderSt := finTestStatement("TESTFA", 1)
	stats, err = repo.IngestSnapshot(db, finSnapshot(older, olderSt), finRun(older))
	if err != nil {
		t.Fatalf("older ingest: %v", err)
	}
	if stats.Skipped != 1 || stats.Versions != 0 {
		t.Errorf("older stats = %+v, want skipped 1 / versions 0", stats)
	}
	rows, _ = repo.FindByTicker(db, "TESTFA")
	if *rows[0].Assets != 113148130000000 {
		t.Errorf("older snapshot overwrote a newer value: %d", *rows[0].Assets)
	}
	if !rows[0].LastSeenMonth.Equal(month) {
		t.Errorf("last_seen = %s, want the newer month %s", rows[0].LastSeenMonth, month)
	}

	// A newer snapshot with a real correction updates and appends a version.
	newer := finTestMonth(2090, time.November)
	newerSt := finTestStatement("TESTFA", 999)
	stats, err = repo.IngestSnapshot(db, finSnapshot(newer, newerSt), finRun(newer))
	if err != nil {
		t.Fatalf("newer ingest: %v", err)
	}
	if stats.Updated != 1 || stats.Versions != 1 {
		t.Errorf("newer stats = %+v, want updated 1 / versions 1", stats)
	}
	rows, _ = repo.FindByTicker(db, "TESTFA")
	if *rows[0].Assets != 999 || rows[0].RevisionCount != 1 || !rows[0].LastSeenMonth.Equal(newer) {
		t.Errorf("after correction: assets=%d revision=%d last_seen=%s, want 999/1/%s",
			*rows[0].Assets, rows[0].RevisionCount, rows[0].LastSeenMonth, newer)
	}
	versions, _ = repo.FindVersions(db, "TESTFA", period)
	if len(versions) != 2 {
		t.Fatalf("versions after correction = %d, want 2", len(versions))
	}
	if !versions[1].SeenMonth.Equal(newer) || *versions[1].Assets != 999 {
		t.Errorf("newest version = %s/%d, want %s/999", versions[1].SeenMonth, *versions[1].Assets, newer)
	}

	// Re-running the same new month with the same value is a no-op.
	stats, err = repo.IngestSnapshot(db, finSnapshot(newer, newerSt), finRun(newer))
	if err != nil {
		t.Fatalf("same-month re-run: %v", err)
	}
	if stats.Unchanged != 1 || stats.Versions != 0 {
		t.Errorf("same-month re-run stats = %+v, want unchanged 1 / versions 0", stats)
	}
	rows, _ = repo.FindByTicker(db, "TESTFA")
	if rows[0].RevisionCount != 1 {
		t.Errorf("revision after same-month re-run = %d, want 1", rows[0].RevisionCount)
	}
}
