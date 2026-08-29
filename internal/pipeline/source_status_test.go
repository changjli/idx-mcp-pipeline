package pipeline

import (
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// fakeSourceStatusStore records upserts; findErr surfaces read failures.
type fakeSourceStatusStore struct {
	current     *entity.SourceStatus
	upserted    []*entity.SourceStatus
	findErr     error
	upsertFails bool
}

func (f *fakeSourceStatusStore) FindBySource(source string) (*entity.SourceStatus, error) {
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.current, nil
}

func (f *fakeSourceStatusStore) Upsert(status *entity.SourceStatus) error {
	if f.upsertFails {
		return errFakeInsert
	}
	f.upserted = append(f.upserted, status)
	f.current = status
	return nil
}

type fakeAlertStore struct{ alerts []*entity.Alert }

func (f *fakeAlertStore) Insert(alert *entity.Alert) error {
	f.alerts = append(f.alerts, alert)
	return nil
}

func fixedClock(now time.Time) func() time.Time {
	return func() time.Time { return now }
}

func newTestRecorder() (*SourceStatusRecorder, *fakeSourceStatusStore, *fakeAlertStore) {
	store := &fakeSourceStatusStore{}
	alerts := &fakeAlertStore{}
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	rec := NewSourceStatusRecorder(store, alerts, log)
	return rec, store, alerts
}

func TestRecorder_SuccessClearsFailureState(t *testing.T) {
	now := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
	rec, store, _ := newTestRecorder()
	rec.now = fixedClock(now)
	store.current = &entity.SourceStatus{
		Source: "test_source", ConsecutiveFailures: 5, Stale: true,
		HighWaterMark: ptrTime(time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)),
	}

	hwm := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	rec.Success("test_source", 86400, &hwm)

	if len(store.upserted) != 1 {
		t.Fatalf("expected 1 upsert, got %d", len(store.upserted))
	}
	got := store.upserted[0]
	if got.LastSuccessAt == nil || !got.LastSuccessAt.Equal(now) {
		t.Errorf("expected LastSuccessAt=%v, got %v", now, got.LastSuccessAt)
	}
	if got.ConsecutiveFailures != 0 || got.Stale {
		t.Errorf("expected failures reset, got %+v", got)
	}
	if got.HighWaterMark == nil || !got.HighWaterMark.Equal(hwm) {
		t.Errorf("expected explicit watermark kept, got %v", got.HighWaterMark)
	}
}

func TestRecorder_SuccessNilWatermarkCarriesForward(t *testing.T) {
	prev := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	rec, store, _ := newTestRecorder()
	store.current = &entity.SourceStatus{Source: "s", HighWaterMark: &prev}

	rec.Success("s", 86400, nil)

	got := store.upserted[len(store.upserted)-1]
	if got.HighWaterMark == nil || !got.HighWaterMark.Equal(prev) {
		t.Errorf("expected nil watermark to carry forward %v, got %v", prev, got.HighWaterMark)
	}
}

func TestRecorder_FailureIncrementsAndMarksStale(t *testing.T) {
	rec, store, alerts := newTestRecorder()
	store.current = &entity.SourceStatus{Source: "s", ConsecutiveFailures: 2}

	rec.Failure("s", 86400, "2026-08-29", errors.New("boom"))

	got := store.upserted[len(store.upserted)-1]
	if got.ConsecutiveFailures != 3 {
		t.Errorf("expected consecutive=3, got %d", got.ConsecutiveFailures)
	}
	if !got.Stale {
		t.Error("expected stale at 3 consecutive failures")
	}
	if len(alerts.alerts) != 1 || alerts.alerts[0].AlertType != "ingestion_error" {
		t.Errorf("expected ingestion_error alert, got %+v", alerts.alerts)
	}
}

func TestRecorder_FailureKeepsCountingOnceStale(t *testing.T) {
	rec, store, alerts := newTestRecorder()
	store.current = &entity.SourceStatus{Source: "s", ConsecutiveFailures: 3, Stale: true}

	rec.Failure("s", 86400, "2026-08-29", errors.New("again"))
	rec.Failure("s", 86400, "2026-08-29", errors.New("again"))

	// source_stale_raised is the raise-transition log only (verified via its
	// once-only payload in the review fix); the status keeps counting and
	// stale stays set.
	got := store.upserted[len(store.upserted)-1]
	if got.ConsecutiveFailures != 5 || !got.Stale {
		t.Errorf("expected consecutive=5 stale, got %+v", got)
	}
	if len(alerts.alerts) != 2 {
		t.Errorf("expected one ingestion_error alert per failure, got %d", len(alerts.alerts))
	}
}

func TestRecorder_SuccessMonotonicNeverRegresses(t *testing.T) {
	current := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	rec, store, _ := newTestRecorder()
	store.current = &entity.SourceStatus{Source: "s", HighWaterMark: &current}

	older := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	rec.SuccessMonotonic("s", 86400, &older)

	got := store.upserted[len(store.upserted)-1]
	if got.HighWaterMark == nil || !got.HighWaterMark.Equal(current) {
		t.Errorf("expected watermark %v kept, got %v", current, got.HighWaterMark)
	}

	// nil candidate carries the current watermark forward.
	rec.SuccessMonotonic("s", 86400, nil)
	got = store.upserted[len(store.upserted)-1]
	if got.HighWaterMark == nil || !got.HighWaterMark.Equal(current) {
		t.Errorf("expected nil candidate to carry forward %v, got %v", current, got.HighWaterMark)
	}

	// newer candidate advances.
	newer := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	rec.SuccessMonotonic("s", 86400, &newer)
	got = store.upserted[len(store.upserted)-1]
	if got.HighWaterMark == nil || !got.HighWaterMark.Equal(newer) {
		t.Errorf("expected watermark %v, got %v", newer, got.HighWaterMark)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
