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

// TestSuspensions_PureValidation verifies input guards run before any DB read,
// with no DB needed.
func TestSuspensions_PureValidation(t *testing.T) {
	uc := NewSuspensionsUseCase(nil, logrus.New(), nil)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)

	badTicker := "test!"
	if _, err := uc.GetSuspensions(context.Background(), from, to, &badTicker); !errors.Is(err, ErrInvalidTicker) {
		t.Errorf("expected ErrInvalidTicker, got %v", err)
	}

	backwards := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	if _, err := uc.GetSuspensions(context.Background(), to, backwards, nil); !errors.Is(err, ErrInvalidRange) {
		t.Errorf("expected ErrInvalidRange, got %v", err)
	}
}

// TestSuspensions_Read verifies the pure DB read against a real Postgres:
// range + ticker filter, ascending order, and empty range. Skipped unless
// IDX_MCP_DB_DSN is set.
func TestSuspensions_Read(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM suspensions WHERE ticker IN ('TESTSA', 'TESTSB')")
	}
	cleanup()
	t.Cleanup(cleanup)

	repo := repository.NewSuspensionRepository(log)

	rows := []entity.Suspension{
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), Type: "SPT", Reason: "Penghentian Sementara TESTSA"},
		{Ticker: "TESTSA", EventDate: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC), Type: "UPT", Reason: "Pembukaan Kembali TESTSA"},
		{Ticker: "TESTSB", EventDate: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), Type: "UMA", Reason: "UMA atas Saham TESTSB"},
	}
	if err := repo.Upsert(db, rows); err != nil {
		t.Fatalf("seed Upsert: %v", err)
	}

	uc := NewSuspensionsUseCase(db, log, repo)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)

	// Full range: 3 events, ascending by date.
	resp, err := uc.GetSuspensions(context.Background(), from, to, nil)
	if err != nil {
		t.Fatalf("GetSuspensions: %v", err)
	}
	if resp.Count != 3 || len(resp.Events) != 3 {
		t.Fatalf("expected 3 events, got count=%d len=%d", resp.Count, len(resp.Events))
	}
	if resp.Events[0].Ticker != "TESTSA" || resp.Events[0].Type != "SPT" {
		t.Errorf("events[0] = %+v, want TESTSA/SPT (earliest first)", resp.Events[0])
	}
	if resp.Events[1].Type != "UMA" {
		t.Errorf("events[1] = %+v, want UMA", resp.Events[1])
	}
	if resp.Events[2].Type != "UPT" {
		t.Errorf("events[2] = %+v, want UPT", resp.Events[2])
	}
	if resp.Events[0].Reason == "" {
		t.Error("events[0].reason must not be empty")
	}
	if resp.From != "2026-09-01" || resp.To != "2026-09-30" {
		t.Errorf("range = %s..%s, want 2026-09-01..2026-09-30", resp.From, resp.To)
	}

	// Ticker filter.
	ticker := "TESTSA"
	respSA, err := uc.GetSuspensions(context.Background(), from, to, &ticker)
	if err != nil {
		t.Fatalf("GetSuspensions tickered: %v", err)
	}
	if respSA.Count != 2 {
		t.Errorf("expected 2 TESTSA events, got %d", respSA.Count)
	}
	for _, e := range respSA.Events {
		if e.Ticker != "TESTSA" {
			t.Errorf("unexpected ticker %q in filtered response", e.Ticker)
		}
	}

	// Empty range → empty non-nil events.
	outFrom := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	outTo := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	respEmpty, err := uc.GetSuspensions(context.Background(), outFrom, outTo, nil)
	if err != nil {
		t.Fatalf("GetSuspensions empty: %v", err)
	}
	if respEmpty.Count != 0 || respEmpty.Events == nil || len(respEmpty.Events) != 0 {
		t.Errorf("expected empty non-nil Events, got count=%d events=%v", respEmpty.Count, respEmpty.Events)
	}
}
