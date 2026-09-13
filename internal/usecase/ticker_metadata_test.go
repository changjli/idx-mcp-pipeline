package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestTickerMetadata_PureValidation verifies the ticker guard runs before any
// DB read, with no DB needed.
func TestTickerMetadata_PureValidation(t *testing.T) {
	uc := NewTickerMetadataUseCase(nil, logrus.New(), nil, nil)

	bad := "test!"
	if _, err := uc.GetTickerMetadata(context.Background(), &bad, nil); !errors.Is(err, ErrInvalidTicker) {
		t.Errorf("expected ErrInvalidTicker, got %v", err)
	}
}

// TestTickerMetadata_Read verifies the pure DB read against a real Postgres:
// per-ticker sector + point-in-time membership (latest, a pinned snapshot, and
// a pre-history date), and the full-universe dump. Skipped unless
// IDX_MCP_DB_DSN is set. Cleanup is scoped to the TESTMA/TESTMB tickers.
func TestTickerMetadata_Read(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM ticker_indices WHERE ticker_code IN ('TESTMA', 'TESTMB')")
		db.MustExec("DELETE FROM tickers WHERE code IN ('TESTMA', 'TESTMB')")
	}
	cleanup()
	t.Cleanup(cleanup)

	tickerRepo := repository.NewTickerRepository(log)
	indexRepo := repository.NewTickerIndexRepository(log)

	day1 := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2027, 2, 6, 0, 0, 0, 0, time.UTC)
	if err := tickerRepo.UpsertSectorIndex(db, []entity.Ticker{
		{Code: "TESTMA", Sektor: strPtr("Energy"), Industri: strPtr("Coal"), SubSektor: strPtr("Oil, Gas & Coal"), SubIndustry: strPtr("Coal Production"), SubIndustryCode: strPtr("A121")},
		{Code: "TESTMB", Sektor: strPtr("Financials"), Industri: strPtr("Banks")},
	}); err != nil {
		t.Fatalf("UpsertSectorIndex: %v", err)
	}
	// TESTMA drops IDX30 between snapshots; TESTMB joins LQ45.
	if err := indexRepo.ReplaceMembership(db, day1, []entity.TickerIndex{
		{TickerCode: "TESTMA", IndexCode: "LQ45", EffectiveDate: day1},
		{TickerCode: "TESTMA", IndexCode: "IDX30", EffectiveDate: day1},
		{TickerCode: "TESTMB", IndexCode: "IDX30", EffectiveDate: day1},
	}); err != nil {
		t.Fatalf("ReplaceMembership day1: %v", err)
	}
	if err := indexRepo.ReplaceMembership(db, day2, []entity.TickerIndex{
		{TickerCode: "TESTMA", IndexCode: "LQ45", EffectiveDate: day2},
		{TickerCode: "TESTMB", IndexCode: "LQ45", EffectiveDate: day2},
	}); err != nil {
		t.Fatalf("ReplaceMembership day2: %v", err)
	}

	uc := NewTickerMetadataUseCase(db, log, tickerRepo, indexRepo)

	// Single ticker, latest snapshot: sector + day2 membership (TESTMA dropped
	// IDX30 in day2 — it must not leak through).
	ma := "TESTMA"
	got, err := uc.GetTickerMetadata(context.Background(), &ma, nil)
	if err != nil {
		t.Fatalf("GetTickerMetadata(TESTMA, latest): %v", err)
	}
	if got.Count != 1 || len(got.Tickers) != 1 {
		t.Fatalf("count = %d tickers = %d, want 1", got.Count, len(got.Tickers))
	}
	m := got.Tickers[0]
	if m.Sector == nil || *m.Sector != "Energy" || m.Industry == nil || *m.Industry != "Coal" {
		t.Errorf("sector/industry = %v/%v, want Energy/Coal", m.Sector, m.Industry)
	}
	if got.EffectiveDate != "2027-02-06" {
		t.Errorf("effective_date = %q, want 2027-02-06 (latest snapshot)", got.EffectiveDate)
	}
	if len(m.Indices) != 1 || m.Indices[0] != "LQ45" {
		t.Errorf("indices = %v, want [LQ45] (IDX30 dropped in day2)", m.Indices)
	}

	// Pinned historical snapshot: day1 membership (TESTMA still in IDX30).
	got, err = uc.GetTickerMetadata(context.Background(), &ma, &day1)
	if err != nil {
		t.Fatalf("GetTickerMetadata(TESTMA, day1): %v", err)
	}
	if got.EffectiveDate != "2026-09-09" {
		t.Errorf("effective_date = %q, want 2026-09-09 (pinned snapshot)", got.EffectiveDate)
	}
	if len(got.Tickers[0].Indices) != 2 || got.Tickers[0].Indices[0] != "IDX30" || got.Tickers[0].Indices[1] != "LQ45" {
		t.Errorf("indices = %v, want [IDX30 LQ45] ascending", got.Tickers[0].Indices)
	}

	// Pre-history date: no snapshot at or before it — empty membership, empty
	// effective_date, sector still returned.
	early := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err = uc.GetTickerMetadata(context.Background(), &ma, &early)
	if err != nil {
		t.Fatalf("GetTickerMetadata(TESTMA, early): %v", err)
	}
	if got.EffectiveDate != "" || len(got.Tickers[0].Indices) != 0 {
		t.Errorf("pre-history read = effective_date %q indices %v, want empty", got.EffectiveDate, got.Tickers[0].Indices)
	}
	if got.Tickers[0].Sector == nil || *got.Tickers[0].Sector != "Energy" {
		t.Errorf("sector = %v, want Energy (sector is current state, not snapshot-dated)", got.Tickers[0].Sector)
	}

	// Full universe: both seeded tickers come back, each with its effective
	// membership (TESTMB has no day2 TESTMA-style drop: LQ45).
	got, err = uc.GetTickerMetadata(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("GetTickerMetadata(universe): %v", err)
	}
	byCode := make(map[string]TickerMetadata, got.Count)
	for _, m := range got.Tickers {
		byCode[m.Ticker] = m
	}
	mb, ok := byCode["TESTMB"]
	if !ok {
		t.Fatalf("universe missing TESTMB (%d rows)", got.Count)
	}
	if mb.Sector == nil || *mb.Sector != "Financials" {
		t.Errorf("TESTMB sector = %v, want Financials", mb.Sector)
	}
	if len(mb.Indices) != 1 || mb.Indices[0] != "LQ45" {
		t.Errorf("TESTMB indices = %v, want [LQ45]", mb.Indices)
	}
	if m, ok := byCode["TESTMA"]; !ok || len(m.Indices) != 1 || m.Indices[0] != "LQ45" {
		t.Errorf("TESTMA universe entry = %+v, want LQ45 membership", m)
	}
}
