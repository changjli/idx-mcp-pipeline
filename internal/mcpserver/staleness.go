package mcpserver

import (
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// stalenessFor computes the staleness metadata for one source: stale when
// now - last_success_at > max_age (per-source, ~1 trading day) — the same
// time-based rule as entity.SourceStatus.IsStale. A missing source_status row
// or missing last_success_at means stale with no last-good date. DB errors
// also report stale — the tool still returns its data, just flagged, rather
// than failing the whole call.
func stalenessFor(db *sqlx.DB, repo *repository.SourceStatusRepository, source string, now time.Time) mcp.StalenessMetadata {
	if db == nil || repo == nil {
		// Test wiring or an unconfigured source: report stale rather than
		// panic on a nil DB.
		return mcp.StalenessMetadata{DataStale: true}
	}
	status, err := repo.FindBySource(db, source)
	if err != nil || status.LastSuccessAt == nil {
		return mcp.StalenessMetadata{DataStale: true}
	}
	return mcp.StalenessMetadata{
		DataStale:    status.IsStale(now),
		LastGoodDate: status.LastSuccessAt.Format("2006-01-02"),
	}
}

// pipelineStaleness computes the overall staleness metadata for
// get_pipeline_status: stale when any source is stale (time-based, computed
// live), last_good_date is the most recent last_success_at across sources.
func pipelineStaleness(db *sqlx.DB, repo *repository.SourceStatusRepository, now time.Time) mcp.StalenessMetadata {
	statuses, err := repo.FindAll(db)
	if err != nil {
		return mcp.StalenessMetadata{DataStale: true}
	}
	meta := mcp.StalenessMetadata{DataStale: false}
	var newest *time.Time
	for _, s := range statuses {
		if s.IsStale(now) {
			meta.DataStale = true
		}
		if s.LastSuccessAt != nil && (newest == nil || s.LastSuccessAt.After(*newest)) {
			newest = s.LastSuccessAt
		}
	}
	if newest != nil {
		meta.LastGoodDate = newest.Format("2006-01-02")
	}
	return meta
}
