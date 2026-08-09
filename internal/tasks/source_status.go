package tasks

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// recordSourceSuccess updates source_status after a successful fetch for a
// source. Clears LastError and resets consecutive_failures. A nil highWaterMark
// preserves any existing watermark — only a non-nil value advances it, so a
// zero-reply run can't clobber the incremental cursor.
func recordSourceSuccess(db *sqlx.DB, repo *repository.SourceStatusRepository, source string, maxAgeSeconds int32, highWaterMark *time.Time, log *logrus.Logger) {
	now := time.Now()

	// Carry forward the existing watermark when the caller didn't advance it.
	if highWaterMark == nil {
		if current, err := repo.FindBySource(db, source); err == nil {
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
	if err := repo.Upsert(db, status); err != nil {
		log.Errorf("%s: failed to update source_status: %v", source, err)
	}
}

// recordSourceFailure updates source_status (last_error, consecutive_failures,
// stale) and inserts an alert row. Called when a source fetch fails.
func recordSourceFailure(db *sqlx.DB, repo *repository.SourceStatusRepository, alertRepo *repository.AlertRepository, source string, maxAgeSeconds int32, date string, fetchErr error, log *logrus.Logger) {
	now := time.Now()
	errStr := fetchErr.Error()

	// Get current status to increment consecutive_failures.
	current, _ := repo.FindBySource(db, source)
	consecutive := int32(1)
	if current != nil {
		consecutive = current.ConsecutiveFailures + 1
	}

	status := &entity.SourceStatus{
		Source:              source,
		LastAttemptAt:       &now,
		LastError:           &errStr,
		ConsecutiveFailures: consecutive,
		Stale:               consecutive >= 3,
		MaxAgeSeconds:       maxAgeSeconds,
	}
	if err := repo.Upsert(db, status); err != nil {
		log.Errorf("%s: failed to update source_status (failure): %v", source, err)
	}

	// Insert alert.
	alert := &entity.Alert{
		Source:    source,
		AlertType: "ingestion_error",
		Message:   fmt.Sprintf("%s fetch failed for %s (attempt %d): %s", source, date, consecutive, errStr),
	}
	if err := alertRepo.Insert(db, alert); err != nil {
		log.Errorf("%s: failed to insert alert: %v", source, err)
	}
}
