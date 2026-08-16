package mcpserver

import (
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// defaultMaxAge is the freshness window used when a source_status row has no
// max_age_seconds set (~1 trading day).
const defaultMaxAge = 24 * time.Hour

// stalenessFor computes the staleness metadata for one source: stale when
// now - last_success_at > max_age (per-source, ~1 trading day). A missing
// source_status row or missing last_success_at means stale with no last-good
// date. DB errors also report stale — the tool still returns its data, just
// flagged, rather than failing the whole call.
func stalenessFor(db *sqlx.DB, repo *repository.SourceStatusRepository, source string, now time.Time) mcp.StalenessMetadata {
	status, err := repo.FindBySource(db, source)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			// DB hiccup — flag stale rather than fail the tool call.
			return mcp.StalenessMetadata{DataStale: true}
		}
		return mcp.StalenessMetadata{DataStale: true}
	}
	if status.LastSuccessAt == nil {
		return mcp.StalenessMetadata{DataStale: true}
	}
	maxAge := time.Duration(status.MaxAgeSeconds) * time.Second
	if maxAge <= 0 {
		maxAge = defaultMaxAge
	}
	return mcp.StalenessMetadata{
		DataStale:    now.Sub(*status.LastSuccessAt) > maxAge,
		LastGoodDate: status.LastSuccessAt.Format("2006-01-02"),
	}
}

// pipelineStaleness computes the overall staleness metadata for
// get_pipeline_status: stale when any source is stale, last_good_date is the
// most recent last_success_at across sources.
func pipelineStaleness(db *sqlx.DB, repo *repository.SourceStatusRepository, now time.Time) mcp.StalenessMetadata {
	statuses, err := repo.FindAll(db)
	if err != nil {
		return mcp.StalenessMetadata{DataStale: true}
	}
	meta := mcp.StalenessMetadata{DataStale: false}
	var newest *time.Time
	for _, s := range statuses {
		if s.Stale {
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
