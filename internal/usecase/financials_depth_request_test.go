package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

// fakeDepthRequestStore implements FinancialDepthRequestStore in memory.
type fakeDepthRequestStore struct {
	taxonomies map[string]entity.TickerFinancialTaxonomy
	requests   map[string]entity.FinancialDepthRequest
	taxErr     error
	reqErr     error
	stampErr   error
}

func newFakeDepthRequestStore() *fakeDepthRequestStore {
	return &fakeDepthRequestStore{
		taxonomies: map[string]entity.TickerFinancialTaxonomy{},
		requests:   map[string]entity.FinancialDepthRequest{},
	}
}

func (f *fakeDepthRequestStore) FindTaxonomy(_ *sqlx.DB, ticker string) (*entity.TickerFinancialTaxonomy, error) {
	if f.taxErr != nil {
		return nil, f.taxErr
	}
	row, ok := f.taxonomies[ticker]
	if !ok {
		return nil, nil
	}
	return &row, nil
}

func (f *fakeDepthRequestStore) FindDepthRequest(_ *sqlx.DB, ticker string) (*entity.FinancialDepthRequest, error) {
	if f.reqErr != nil {
		return nil, f.reqErr
	}
	row, ok := f.requests[ticker]
	if !ok {
		return nil, nil
	}
	return &row, nil
}

func (f *fakeDepthRequestStore) UpsertDepthRequest(_ *sqlx.DB, ticker string, at time.Time) error {
	if f.stampErr != nil {
		return f.stampErr
	}
	f.requests[ticker] = entity.FinancialDepthRequest{Ticker: ticker, RequestedAt: at}
	return nil
}

// fakeDepthEnqueuer records on-demand enqueues and can force an error.
type fakeDepthEnqueuer struct {
	calls []string
	err   error
}

func (f *fakeDepthEnqueuer) EnqueueFinancialDepth(ticker string) error {
	f.calls = append(f.calls, ticker)
	return f.err
}

func newDepthRequestTestUseCase(store *fakeDepthRequestStore, enq *fakeDepthEnqueuer, now *time.Time) *FinancialDepthRequestUseCase {
	return NewFinancialDepthRequestUseCase(
		nil, logrus.New(), store, enq,
		WithFinancialDepthCooldown(6*time.Hour),
		WithFinancialDepthRequestClock(func() time.Time { return *now }),
	)
}

// TestRequireDepth_EnqueuesOncePerCooldown pins the core trigger contract: one
// job per cooldown window, deduped by ticker, and no enqueue at all once depth
// is present (the store-hit path).
func TestRequireDepth_EnqueuesOncePerCooldown(t *testing.T) {
	store := newFakeDepthRequestStore()
	enq := &fakeDepthEnqueuer{}
	now := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	uc := newDepthRequestTestUseCase(store, enq, &now)

	status, err := uc.RequireDepth(context.Background(), "RAJA", true)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	if status != FinancialDepthPending {
		t.Errorf("status = %q, want pending", status)
	}
	if len(enq.calls) != 1 || enq.calls[0] != "RAJA" {
		t.Fatalf("enqueues = %v, want one for RAJA", enq.calls)
	}
	if got := store.requests["RAJA"].RequestedAt; !got.Equal(now) {
		t.Errorf("request stamp = %v, want %v", got, now)
	}

	// A repeated poll inside the cooldown must not enqueue again.
	now = now.Add(5 * time.Hour)
	status, err = uc.RequireDepth(context.Background(), "RAJA", true)
	if err != nil {
		t.Fatalf("repeat request: %v", err)
	}
	if status != FinancialDepthPending {
		t.Errorf("status = %q, want pending", status)
	}
	if len(enq.calls) != 1 {
		t.Errorf("enqueues = %v, want the cooldown to suppress the repeat", enq.calls)
	}

	// After the cooldown a new attempt is allowed.
	now = now.Add(2 * time.Hour)
	if _, err := uc.RequireDepth(context.Background(), "RAJA", true); err != nil {
		t.Fatalf("post-cooldown request: %v", err)
	}
	if len(enq.calls) != 2 {
		t.Errorf("enqueues = %v, want 2 after the cooldown", enq.calls)
	}

	// Depth present: the read is a store hit, no job and no upstream call.
	if _, err := uc.RequireDepth(context.Background(), "RAJA", false); err != nil {
		t.Fatalf("observed request: %v", err)
	}
	if len(enq.calls) != 2 {
		t.Errorf("enqueues = %v, want no enqueue when depth is present", enq.calls)
	}
}

// TestRequireDepth_NotCoveredIsTerminal pins the vendor-gap contract: a
// not_covered ticker is reported and never retried automatically.
func TestRequireDepth_NotCoveredIsTerminal(t *testing.T) {
	store := newFakeDepthRequestStore()
	store.taxonomies["NOS"] = entity.TickerFinancialTaxonomy{
		Ticker: "NOS", Taxonomy: string(financials.TaxonomyOther),
		DepthStatus: financials.DepthStatusNotCovered,
	}
	enq := &fakeDepthEnqueuer{}
	now := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	uc := newDepthRequestTestUseCase(store, enq, &now)

	status, err := uc.RequireDepth(context.Background(), "NOS", true)
	if err != nil {
		t.Fatalf("not_covered: %v", err)
	}
	if status != FinancialDepthNotCovered {
		t.Errorf("status = %q, want not_covered", status)
	}
	if len(enq.calls) != 0 {
		t.Errorf("enqueues = %v, want none for a not_covered ticker", enq.calls)
	}
}

// TestRequireDepth_FailedRespectsCooldown pins the failure path: the failure
// is reported while the cooldown is open, then a fresh attempt is made.
func TestRequireDepth_FailedRespectsCooldown(t *testing.T) {
	store := newFakeDepthRequestStore()
	store.taxonomies["BAD"] = entity.TickerFinancialTaxonomy{
		Ticker: "BAD", Taxonomy: string(financials.TaxonomyGeneral154),
		DepthStatus: financials.DepthStatusFailed,
	}
	// The last attempt was four hours ago: still cooling down.
	now := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	store.requests["BAD"] = entity.FinancialDepthRequest{Ticker: "BAD", RequestedAt: now.Add(-4 * time.Hour)}
	enq := &fakeDepthEnqueuer{}
	uc := newDepthRequestTestUseCase(store, enq, &now)

	status, err := uc.RequireDepth(context.Background(), "BAD", true)
	if err != nil {
		t.Fatalf("failed request: %v", err)
	}
	if status != FinancialDepthFailed {
		t.Errorf("status = %q, want failed", status)
	}
	if len(enq.calls) != 0 {
		t.Errorf("enqueues = %v, want none inside the cooldown", enq.calls)
	}

	// Eight hours later the cooldown has expired: retry and report pending.
	now = now.Add(4 * time.Hour)
	status, err = uc.RequireDepth(context.Background(), "BAD", true)
	if err != nil {
		t.Fatalf("retry request: %v", err)
	}
	if status != FinancialDepthPending {
		t.Errorf("status = %q, want pending after a retry", status)
	}
	if len(enq.calls) != 1 {
		t.Errorf("enqueues = %v, want one retry", enq.calls)
	}
}

// TestRequireDepth_TaskIDConflictIsPending pins idempotence: asynq's TaskID
// dedup means a second request for the same ticker/month is already queued,
// which is the same pending answer, not an error.
func TestRequireDepth_TaskIDConflictIsPending(t *testing.T) {
	store := newFakeDepthRequestStore()
	enq := &fakeDepthEnqueuer{err: asynq.ErrTaskIDConflict}
	now := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	uc := newDepthRequestTestUseCase(store, enq, &now)

	status, err := uc.RequireDepth(context.Background(), "RAJA", true)
	if err != nil {
		t.Fatalf("conflict must not error: %v", err)
	}
	if status != FinancialDepthPending {
		t.Errorf("status = %q, want pending", status)
	}
	if _, ok := store.requests["RAJA"]; !ok {
		t.Error("request was not stamped after a TaskID conflict")
	}
}

// TestRequireDepth_EnqueueErrorSurfacesWithoutStamp pins the best-effort
// contract: an enqueue failure is returned for the caller to log, the status
// stays conservative, and the ticker is not stamped as pending.
func TestRequireDepth_EnqueueErrorSurfacesWithoutStamp(t *testing.T) {
	store := newFakeDepthRequestStore()
	enq := &fakeDepthEnqueuer{err: errors.New("redis down")}
	now := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	uc := newDepthRequestTestUseCase(store, enq, &now)

	status, err := uc.RequireDepth(context.Background(), "RAJA", true)
	if err == nil {
		t.Fatal("expected the enqueue error to surface")
	}
	if status != FinancialDepthPending {
		t.Errorf("status = %q, want pending", status)
	}
	if _, ok := store.requests["RAJA"]; ok {
		t.Error("request must not be stamped when the enqueue failed")
	}
}

// TestRequireDepth_StoreErrorsAreReported pins that a broken store degrades to
// a status plus an error instead of panicking the read path.
func TestRequireDepth_StoreErrorsAreReported(t *testing.T) {
	store := newFakeDepthRequestStore()
	store.taxErr = errors.New("db down")
	enq := &fakeDepthEnqueuer{}
	now := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	uc := newDepthRequestTestUseCase(store, enq, &now)

	if _, err := uc.RequireDepth(context.Background(), "RAJA", true); err == nil {
		t.Fatal("expected the store error to surface")
	}
	if len(enq.calls) != 0 {
		t.Errorf("enqueues = %v, want none after a taxonomy read failure", enq.calls)
	}
}
