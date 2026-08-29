package tasks

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// BulkAnnouncementsResult summarizes a bulk disclosure-metadata backfill run.
type BulkAnnouncementsResult struct {
	Total           int    // dates in the requested range
	Succeeded       int    // dates processed without error (incl. empty/no-data)
	Failed          int    // dates whose fetch failed
	Empty           int    // dates with 0 announcement rows (weekends/holidays)
	LastSuccessDate string // YYYY-MM-DD of the most recent date that produced data
}

// RunBulkAnnouncements fetches IDX announcement metadata for every date in
// [start, end] sequentially, upserting disclosure rows per date. Mirrors
// RunBulkBackfill but for the announcements source: synchronous, local nodriver
// egress via the shared idxClient, direct DB upsert — no asynq, no worker.
//
// Each date is fetched as a single-day window (from==to==date) so the run is
// deterministic and not driven by the announcements high_water_mark (which the
// live handler advances incrementally). Upserts are idempotent via pdf_url, so
// overlapping with prior ingestion is safe. Source status is updated once at
// the end with the most recent announcement date seen.
func RunBulkAnnouncements(
	log *logrus.Logger,
	idxClient *client.Client,
	db *sqlx.DB,
	tickerRepo *repository.TickerRepository,
	disclosureRepo *repository.DisclosureRepository,
	recorder *pipeline.SourceStatusRecorder,
	start, end time.Time,
) BulkAnnouncementsResult {
	var result BulkAnnouncementsResult
	var lastSuccess *time.Time
	stage := pipeline.NewIngestStage(TypeAnnouncements, log, nil, 3)

	for _, d := range datesInRange(start, end) {
		dateKey := d.Format("2006-01-02")
		result.Total++

		path := announcementsPath(d, d, 0)
		f := stage.StartFetch("", "bulk announcements fetch",
			logrus.Fields{"date": dateKey, "fetch_url": path})
		replies, err := fetchAnnouncements(idxClient, d, d, log)
		if err != nil {
			f.Fail("bulk announcements fetch failed", err, logrus.Fields{"date": dateKey})
			result.Failed++
			continue
		}
		f.Ok("bulk announcements fetched", logrus.Fields{"date": dateKey, "rows": len(replies)})

		if len(replies) == 0 {
			log.Infof("bulk announcements: no data for %s (weekend/holiday)", dateKey)
			result.Empty++
			result.Succeeded++
			continue
		}

		upserted, upsertErr := upsertDisclosureRows(db, tickerRepo, disclosureRepo, replies, log)
		if upsertErr != nil {
			log.Errorf("bulk announcements: upsert failed for %s: %v", dateKey, upsertErr)
			result.Failed++
			continue
		}
		log.Infof("bulk announcements: upserted %d disclosure row(s) for %s", upserted, dateKey)
		result.Succeeded++
		if upserted > 0 {
			t := d
			lastSuccess = &t
		}
	}

	if lastSuccess != nil {
		result.LastSuccessDate = lastSuccess.Format("2006-01-02")
		recorder.Success(TypeAnnouncements, announcementsMaxAgeSeconds, lastSuccess)
	}

	return result
}
