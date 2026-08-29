package tasks

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// BulkBackfillResult summarizes a bulk historical backfill run.
type BulkBackfillResult struct {
	Total           int    // dates in the requested range
	Succeeded       int    // dates processed without error (incl. empty/no-data)
	Failed          int    // dates whose fetch failed
	Empty           int    // dates with 0 rows (weekends/holidays)
	LastSuccessDate string // YYYY-MM-DD of the most recent date that produced data
}

// RunBulkBackfill fetches GetStockSummary for every date in [start, end]
// sequentially, upserting rows into daily_prices. The shared IDX client's
// rate limiter paces requests at ~1 req/sec. Individual date failures are
// logged at ERROR and skipped; the loop continues. Empty responses
// (weekends/holidays) are logged at INFO and are not errors. Source status
// is updated once at the end with the most recent date that produced data.
func RunBulkBackfill(
	log *logrus.Logger,
	idxClient *client.Client,
	db *sqlx.DB,
	tickerRepo *repository.TickerRepository,
	dailyPriceRepo *repository.DailyPriceRepository,
	recorder *pipeline.SourceStatusRecorder,
	start, end time.Time,
) BulkBackfillResult {
	var result BulkBackfillResult
	var lastSuccess *time.Time
	stage := pipeline.NewIngestStage(TypeStockSummary, log, nil, 3)

	for _, d := range datesInRange(start, end) {
		dateKey := d.Format("2006-01-02")
		result.Total++

		path := stockSummaryPath(d)
		f := stage.StartFetch("", "bulk backfill fetch",
			logrus.Fields{"date": dateKey, "fetch_url": path})
		resp, err := fetchStockSummary(idxClient, path, log)
		if err != nil {
			f.Fail("bulk backfill fetch failed", err, logrus.Fields{"date": dateKey})
			result.Failed++
			continue
		}
		f.Ok("bulk backfill fetched", logrus.Fields{"date": dateKey, "rows": len(resp.Data)})

		rows := resp.Data
		if len(rows) == 0 {
			log.Infof("bulk: no data for %s (weekend/holiday)", dateKey)
			result.Empty++
			result.Succeeded++
			continue
		}

		upserted := upsertStockSummaryRows(db, tickerRepo, dailyPriceRepo, rows, dateKey, log)
		log.Infof("bulk: upserted %d/%d rows for %s", upserted, len(rows), dateKey)
		result.Succeeded++
		if upserted > 0 {
			t := d
			lastSuccess = &t
		}
	}

	if lastSuccess != nil {
		result.LastSuccessDate = lastSuccess.Format("2006-01-02")
		recorder.Success(TypeStockSummary, stockSummaryMaxAgeSeconds, lastSuccess)
	}

	return result
}

// datesInRange returns every date in [start, end] inclusive.
// Returns an empty slice when start is after end.
func datesInRange(start, end time.Time) []time.Time {
	var dates []time.Time
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d)
	}
	return dates
}
