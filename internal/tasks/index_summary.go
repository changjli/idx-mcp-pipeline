package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// indexSummaryMaxAgeSeconds is the source_status max age for
	// idx:index_summary (24 hours). The index/sector summary is a daily
	// snapshot; 24h keeps the envelope fresh while tolerating a daily cadence.
	indexSummaryMaxAgeSeconds int32 = 86400
)

// IndexSummaryPayload is the payload for an idx:index_summary task.
type IndexSummaryPayload struct {
	Date string `json:"date"` // YYYY-MM-DD run date
}

// IndexSummaryFetcher fetches the IDX index/sector summary — all 45 indices in
// one GetIndexSummary call. *client.Client satisfies this; tests use a fake.
type IndexSummaryFetcher interface {
	FetchIndexSummary(ctx context.Context) ([]client.IndexSummary, error)
}

// NewIndexSummaryHandler returns an asynq handler for the idx:index_summary
// task type. It fetches the IDX GetIndexSummary snapshot (one request — all 45
// indices incl. the 11 sector indices), upserts every row keyed by
// (index_code, wire trading date), and records source_status. The run date is
// carried in the payload for dedup/logging only — the row date is the wire
// Date, so a holiday fetch landing Friday's rows keys them as Friday and a
// same-day re-run is idempotent. Fires daily in the pipeline:daily Wave
// (issue 18); feeds the screening flow's Stage-0 sector-rotation trend.
func NewIndexSummaryHandler(
	log *logrus.Logger,
	fetcher IndexSummaryFetcher,
	db *sqlx.DB,
	repo *repository.IndexSummaryRepository,
	recorder *pipeline.SourceStatusRecorder,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[IndexSummaryPayload](t)
		if err != nil {
			return err
		}
		if _, err := pipeline.ParseTaskDay(p.Date); err != nil {
			return err
		}

		log.Infof("index summary: fetching GetIndexSummary, run date=%s", p.Date)

		rows, fetchErr := fetcher.FetchIndexSummary(ctx)
		if fetchErr != nil {
			recorder.Failure(TypeIndexSummary, indexSummaryMaxAgeSeconds, p.Date, fetchErr)
			return fetchErr
		}

		entities := indexSummariesToEntities(rows)
		if err := repo.Upsert(db, entities); err != nil {
			recorder.Failure(TypeIndexSummary, indexSummaryMaxAgeSeconds, p.Date, err)
			return err
		}

		recorder.Success(TypeIndexSummary, indexSummaryMaxAgeSeconds, indexSummariesMaxDate(rows))
		log.Infof("index summary: upserted %d row(s)", len(entities))
		return nil
	}
}

// indexSummariesToEntities converts fetched index summaries into entity rows
// for the upsert. The wire amounts are floats (IDX serializes integral counts
// as 918.0); int truncation is safe — counts are integral, and MarketCap
// (~1.2e16 IDR) still fits int64.
func indexSummariesToEntities(rows []client.IndexSummary) []entity.IndexSummary {
	entities := make([]entity.IndexSummary, 0, len(rows))
	for _, s := range rows {
		high := s.Highest
		low := s.Lowest
		stock := int32(s.NumberOfStock)
		vol := int64(s.Volume)
		val := int64(s.Value)
		freq := int64(s.Frequency)
		mcap := int64(s.MarketCap)
		entities = append(entities, entity.IndexSummary{
			IndexCode:     s.IndexCode,
			Date:          s.Date,
			Previous:      &s.Previous,
			High:          &high,
			Low:           &low,
			Close:         &s.Close,
			Change:        &s.Change,
			NumberOfStock: &stock,
			Volume:        &vol,
			Value:         &val,
			Frequency:     &freq,
			MarketCap:     &mcap,
		})
	}
	return entities
}

// indexSummariesMaxDate returns the newest trading date in the fetched set, or
// nil when empty (the recorder then carries the watermark forward).
func indexSummariesMaxDate(rows []client.IndexSummary) *time.Time {
	var max time.Time
	for _, s := range rows {
		if s.Date.After(max) {
			max = s.Date
		}
	}
	if max.IsZero() {
		return nil
	}
	return &max
}
