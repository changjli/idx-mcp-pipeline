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

// TestCorporateActions_PureValidation verifies input guards run before any DB
// read, with no DB needed.
func TestCorporateActions_PureValidation(t *testing.T) {
	uc := NewCorporateActionsUseCase(nil, logrus.New(), nil)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)

	badTicker := "test!"
	if _, err := uc.GetCorporateActions(context.Background(), from, to, &badTicker); !errors.Is(err, ErrInvalidTicker) {
		t.Errorf("expected ErrInvalidTicker, got %v", err)
	}

	backwards := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	if _, err := uc.GetCorporateActions(context.Background(), to, backwards, nil); !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange, got %v", err)
	}
}

// TestCorporateActions_Read verifies the pure DB read against a real Postgres:
// range + ticker filter, ascending order, and empty range. Skipped unless
// IDX_MCP_DB_DSN is set.
func TestCorporateActions_Read(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM corporate_actions WHERE ticker IN ('TESTCA', 'TESTCB')")
	}
	cleanup()
	t.Cleanup(cleanup)

	repo := repository.NewCorporateActionRepository(log)

	rows := []entity.CorporateAction{
		{Id: 9_200_001, Ticker: "TESTCA", EventDate: time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC), Type: "Waran", JumlahSaham: 1000, JumlahSahamSetelahTindakan: 2000},
		{Id: 9_200_002, Ticker: "TESTCA", EventDate: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), Type: "Stock Split", JumlahSaham: 2000, JumlahSahamSetelahTindakan: 4000},
		{Id: 9_200_003, Ticker: "TESTCB", EventDate: time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC), Type: "Rights Issue", JumlahSaham: 3000, JumlahSahamSetelahTindakan: 6000},
	}
	if err := repo.Upsert(db, rows); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}

	uc := NewCorporateActionsUseCase(db, log, repo)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)

	// Full range: 3 events, ascending by date.
	resp, err := uc.GetCorporateActions(context.Background(), from, to, nil)
	if err != nil {
		t.Fatalf("GetCorporateActions: %v", err)
	}
	if resp.Count != 3 || len(resp.Events) != 3 {
		t.Fatalf("expected 3 events, got count=%d len=%d", resp.Count, len(resp.Events))
	}
	if resp.Events[0].Ticker != "TESTCA" || resp.Events[0].Type != "Waran" {
		t.Errorf("events[0] = %+v, want TESTCA/Waran (earliest first)", resp.Events[0])
	}
	if resp.Events[2].Ticker != "TESTCB" || resp.Events[2].Type != "Rights Issue" {
		t.Errorf("events[2] = %+v, want TESTCB/Rights Issue", resp.Events[2])
	}
	if resp.Events[0].Detail.JumlahSaham != 1000 || resp.Events[0].Detail.JumlahSahamSetelahTindakan != 2000 {
		t.Errorf("events[0].detail = %+v, want {1000, 2000}", resp.Events[0].Detail)
	}
	if resp.From != "2026-09-01" || resp.To != "2026-09-30" {
		t.Errorf("range = %s..%s, want 2026-09-01..2026-09-30", resp.From, resp.To)
	}

	// Ticker filter.
	ticker := "TESTCA"
	respCA, err := uc.GetCorporateActions(context.Background(), from, to, &ticker)
	if err != nil {
		t.Fatalf("GetCorporateActions tickered: %v", err)
	}
	if respCA.Count != 2 {
		t.Errorf("expected 2 TESTCA events, got %d", respCA.Count)
	}
	for _, e := range respCA.Events {
		if e.Ticker != "TESTCA" {
			t.Errorf("unexpected ticker %q in filtered response", e.Ticker)
		}
	}

	// Empty range → empty non-nil events.
	outFrom := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	outTo := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	respEmpty, err := uc.GetCorporateActions(context.Background(), outFrom, outTo, nil)
	if err != nil {
		t.Fatalf("GetCorporateActions empty: %v", err)
	}
	if respEmpty.Count != 0 || respEmpty.Events == nil || len(respEmpty.Events) != 0 {
		t.Errorf("expected empty non-nil Events, got count=%d events=%v", respEmpty.Count, respEmpty.Events)
	}
}
