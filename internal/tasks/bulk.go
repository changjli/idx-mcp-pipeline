package tasks

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
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
	sourceStatusRepo *repository.SourceStatusRepository,
	start, end time.Time,
) BulkBackfillResult {
	var result BulkBackfillResult
	var lastSuccess *time.Time

	for _, d := range datesInRange(start, end) {
		dateKey := d.Format("2006-01-02")
		result.Total++

		path := stockSummaryPath(d)
		startTime := time.Now()
		logEvent(log, logrus.InfoLevel, "fetch_start", "bulk backfill fetch",
			logrus.Fields{"source": TypeStockSummary, "date": dateKey, "fetch_url": path})
		resp, err := fetchStockSummary(idxClient, path, log)
		latency := time.Since(startTime).Milliseconds()
		if err != nil {
			logEvent(log, logrus.ErrorLevel, "fetch_failure", "bulk backfill fetch failed",
				logrus.Fields{"source": TypeStockSummary, "date": dateKey, "error": err.Error(), "latency_ms": latency})
			result.Failed++
			continue
		}
		logEvent(log, logrus.InfoLevel, "fetch_success", "bulk backfill fetched",
			logrus.Fields{"source": TypeStockSummary, "date": dateKey, "rows": len(resp.Data), "latency_ms": latency})

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
		recordSourceSuccess(db, sourceStatusRepo, TypeStockSummary, stockSummaryMaxAgeSeconds, lastSuccess, log)
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
