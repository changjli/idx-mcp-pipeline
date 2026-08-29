package pipeline

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// staleConsecutiveFailures is the failure count at which the recorder marks a
// source stale. Preserved wart: this is a failure-count definition, while
// CONTEXT.md / get_pipeline_status read staleness time-based (see follow-up
// 05-staleness-semantics.md).
const staleConsecutiveFailures = 3

// SourceStatusStore persists per-source freshness state. Consumer-side
// interface (ADR-0006): satisfied by the sqlx-backed SourceStatusRepository
// via NewSQLSourceStatusStore; tests provide the second adapter.
type SourceStatusStore interface {
	FindBySource(source string) (*entity.SourceStatus, error)
	Upsert(status *entity.SourceStatus) error
}

// AlertStore persists alert rows. Satisfied by AlertRepository via
// NewSQLAlertStore.
type AlertStore interface {
	Insert(alert *entity.Alert) error
}

// SQLSourceStatusStore adapts SourceStatusRepository to SourceStatusStore.
type SQLSourceStatusStore struct {
	repo *repository.SourceStatusRepository
	db   *sqlx.DB
}

// NewSQLSourceStatusStore binds a source-status repository to its database.
func NewSQLSourceStatusStore(repo *repository.SourceStatusRepository, db *sqlx.DB) *SQLSourceStatusStore {
	return &SQLSourceStatusStore{repo: repo, db: db}
}

// FindBySource returns the row for one source, or nil when absent.
func (s *SQLSourceStatusStore) FindBySource(source string) (*entity.SourceStatus, error) {
	return s.repo.FindBySource(s.db, source)
}

// Upsert writes the status row.
func (s *SQLSourceStatusStore) Upsert(status *entity.SourceStatus) error {
	return s.repo.Upsert(s.db, status)
}

// SQLAlertStore adapts AlertRepository to AlertStore.
type SQLAlertStore struct {
	repo *repository.AlertRepository
	db   *sqlx.DB
}

// NewSQLAlertStore binds an alert repository to its database.
func NewSQLAlertStore(repo *repository.AlertRepository, db *sqlx.DB) *SQLAlertStore {
	return &SQLAlertStore{repo: repo, db: db}
}

// Insert persists one alert row.
func (s *SQLAlertStore) Insert(alert *entity.Alert) error {
	return s.repo.Insert(s.db, alert)
}

// SourceStatusRecorder updates source_status after a source fetch succeeds or
// fails, and raises the source_stale_raised alert on the transition into
// staleness. One recorder is shared by every ingest stage; each call names
// its source. Staleness: `stale = consecutive_failures >= 3` (preserved
// behavior — see staleConsecutiveFailures).
type SourceStatusRecorder struct {
	statuses SourceStatusStore
	alerts   AlertStore
	log      *logrus.Logger
	now      func() time.Time
}

// NewSourceStatusRecorder wires a recorder over its stores. now defaults to
// time.Now; tests inject a fixed clock.
func NewSourceStatusRecorder(statuses SourceStatusStore, alerts AlertStore, log *logrus.Logger) *SourceStatusRecorder {
	return &SourceStatusRecorder{statuses: statuses, alerts: alerts, log: log, now: time.Now}
}

// Success records a successful fetch for a source. Clears LastError and resets
// consecutive_failures. A nil highWaterMark preserves any existing watermark —
// only a non-nil value advances it, so a zero-reply run can't clobber the
// incremental cursor (the same carry-forward as SuccessMonotonic, without the
// candidate).
func (r *SourceStatusRecorder) Success(source string, maxAgeSeconds int32, highWaterMark *time.Time) {
	now := r.now()

	// Carry forward the existing watermark when the caller didn't advance it.
	if highWaterMark == nil {
		if current, err := r.statuses.FindBySource(source); err == nil {
			highWaterMark = current.HighWaterMark
		}
	}

	status := &entity.SourceStatus{
		Source:              source,
		LastSuccessAt:       &now,
		LastAttemptAt:       &now,
		LastError:           nil,
		ConsecutiveFailures: 0,
		Stale:               false,
		MaxAgeSeconds:       maxAgeSeconds,
		HighWaterMark:       highWaterMark,
	}
	if err := r.statuses.Upsert(status); err != nil {
		r.log.Errorf("%s: failed to update source_status: %v", source, err)
	}
}

// CurrentWatermark returns the source's high_water_mark, or nil when unset or
// the read fails (callers fall back to their default window — errors from this
// read are non-fatal by original semantics).
func (r *SourceStatusRecorder) CurrentWatermark(source string) (*time.Time, error) {
	status, err := r.statuses.FindBySource(source)
	if err != nil {
		return nil, err
	}
	return status.HighWaterMark, nil
}

// SuccessMonotonic records a successful fetch while never regressing the
// high-water mark below its current value: the announcements endpoint's result
// ordering is unstable (server-side index lag), so a run may return older
// items — the incremental cursor stays put. When no candidate exists
// (maxAnnouncementDate returned nil), the current watermark is carried
// forward.
func (r *SourceStatusRecorder) SuccessMonotonic(source string, maxAgeSeconds int32, candidate *time.Time) {
	if current, err := r.statuses.FindBySource(source); err == nil && current.HighWaterMark != nil {
		if candidate == nil || current.HighWaterMark.After(*candidate) {
			candidate = current.HighWaterMark
		}
	}
	r.Success(source, maxAgeSeconds, candidate)
}

// Failure records a failed fetch: the error, incremented consecutive_failures,
// the staleness flag, and an ingestion_error alert row. source_stale_raised
// fires only on the transition into staleness — later failures of an
// already-stale source stay quiet; recovery is signalled by the next Success.
func (r *SourceStatusRecorder) Failure(source string, maxAgeSeconds int32, date string, fetchErr error) {
	now := r.now()
	errStr := fetchErr.Error()

	// Get current status to increment consecutive_failures.
	current, _ := r.statuses.FindBySource(source)
	consecutive := int32(1)
	if current != nil {
		consecutive = current.ConsecutiveFailures + 1
	}

	stale := consecutive >= staleConsecutiveFailures
	status := &entity.SourceStatus{
		Source:              source,
		LastAttemptAt:       &now,
		LastError:           &errStr,
		ConsecutiveFailures: consecutive,
		Stale:               stale,
		MaxAgeSeconds:       maxAgeSeconds,
	}
	if err := r.statuses.Upsert(status); err != nil {
		r.log.Errorf("%s: failed to update source_status (failure): %v", source, err)
	}

	if stale && current != nil && current.ConsecutiveFailures < staleConsecutiveFailures {
		r.log.WithFields(logrus.Fields{"event": "source_stale_raised", "source": source, "consecutive_failures": consecutive, "error": errStr}).
			Warn("source marked stale after consecutive failures")
	}

	alert := &entity.Alert{
		Source:    source,
		AlertType: "ingestion_error",
		Message:   fmt.Sprintf("%s fetch failed for %s (attempt %d): %s", source, date, consecutive, errStr),
	}
	// The bulk CLI wires a nil AlertStore (alerts are a worker concern there);
	// nil is tolerated explicitly instead of panicking if Failure ever joins it.
	if r.alerts != nil {
		if err := r.alerts.Insert(alert); err != nil {
			r.log.Errorf("%s: failed to insert alert: %v", source, err)
		}
	}
}
