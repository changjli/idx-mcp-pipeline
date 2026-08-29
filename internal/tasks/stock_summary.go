package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
)

// Default source_status max age for stock summary (24 hours).
const stockSummaryMaxAgeSeconds int32 = 86400

// StockSummaryPayload is the payload for an idx:stock_summary task.
type StockSummaryPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// StockSummaryResponse is the DataTables-style wrapper returned by the API.
type StockSummaryResponse struct {
	Draw            int                         `json:"draw"`
	RecordsTotal    int                         `json:"recordsTotal"`
	RecordsFiltered int                         `json:"recordsFiltered"`
	Data            []pipeline.StockSummaryItem `json:"data"`
}

// EnqueueStockSummary enqueues an idx:stock_summary task for the given date.
// Uses a date-keyed TaskID for dedup. Returns ErrTaskIDConflict if already enqueued.
// Extra opts (e.g. asynq.ProcessIn) are appended to the default options.
func EnqueueStockSummary(enq pipeline.Enqueuer, date time.Time, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	stage := pipeline.NewIngestStage(TypeStockSummary, nil, enq, 3)
	return stage.EnqueueWithOpts(TaskKey(TypeStockSummary, dateKey), StockSummaryPayload{Date: dateKey}, opts...)
}

// NewStockSummaryHandler returns an asynq handler for the idx:stock_summary task type.
// Fetches GetStockSummary from IDX, parses OHLCV, upserts into daily_prices,
// updates source_status / alerts, and chains a detect:anomalies task on success.
func NewStockSummaryHandler(
	log *logrus.Logger,
	idxClient *client.Client,
	db *sqlx.DB,
	enq pipeline.Enqueuer,
	recorder *pipeline.SourceStatusRecorder,
	ingest *pipeline.StockSummaryIngest,
) asynq.HandlerFunc {
	stage := pipeline.NewIngestStage(TypeStockSummary, log, nil, 3)
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[StockSummaryPayload](t)
		if err != nil {
			return err
		}
		date, err := pipeline.ParseTaskDay(p.Date)
		if err != nil {
			return err
		}

		taskID := pipeline.TaskID(ctx)
		path := stockSummaryPath(date)
		f := stage.StartFetch(taskID, "fetching stock summary",
			logrus.Fields{"date": p.Date, "fetch_url": path})

		// Fetch from IDX API.
		resp, fetchErr := fetchStockSummary(idxClient, path, log)
		if fetchErr != nil {
			f.Fail("stock summary fetch failed", fetchErr, logrus.Fields{"date": p.Date})
			recorder.Failure(TypeStockSummary, stockSummaryMaxAgeSeconds, p.Date, fetchErr)
			return fetchErr
		}

		rows := resp.Data
		f.Ok("stock summary fetched", logrus.Fields{"date": p.Date, "rows": len(rows)})

		// Upsert each row into daily_prices.
		// Ticker must exist first (FK constraint) — auto-discover from response.
		upserted := ingest.UpsertRows(rows, p.Date)
		log.Infof("stock_summary: upserted %d/%d rows for date=%s", upserted, len(rows), p.Date)

		// Update source_status on success.
		recorder.Success(TypeStockSummary, stockSummaryMaxAgeSeconds, nil)

		// Chain detect:anomalies on success so anomaly detection runs after
		// ingestion. Date-keyed TaskID dedups against concurrent chains.
		if _, err := EnqueueDetectAnomalies(enq, date); err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) {
			log.Warnf("stock_summary: failed to enqueue detect:anomalies: %v", err)
		} else {
			log.Infof("stock_summary: chained detect:anomalies for %s", p.Date)
		}

		return nil
	}
}

// stockSummaryPath builds the IDX GetStockSummary endpoint path for a date.
// Shared by the handler (for the fetch_url log field) and the fetch itself.
func stockSummaryPath(date time.Time) string {
	dateIDX := date.Format("20060102") // YYYYMMDD
	return fmt.Sprintf("/primary/TradingSummary/GetStockSummary?length=9999&start=0&date=%s", dateIDX)
}

// fetchStockSummary calls the IDX GetStockSummary API and parses the response.
func fetchStockSummary(idxClient *client.Client, path string, log *logrus.Logger) (StockSummaryResponse, error) {
	headers := map[string]string{
		"Referer": "https://www.idx.co.id/en/market-data/trading-summary/stock-summary/",
	}
	resp, err := idxClient.GetWithHeaders(path, headers)
	if err != nil {
		return StockSummaryResponse{}, fmt.Errorf("idx get: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return StockSummaryResponse{}, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return StockSummaryResponse{}, fmt.Errorf("idx api error: status=%d body=%s", resp.StatusCode, pipeline.Truncate(string(body), 200))
	}

	var items StockSummaryResponse
	if err := json.Unmarshal(body, &items); err != nil {
		return StockSummaryResponse{}, fmt.Errorf("parse response: %w (body=%s)", err, pipeline.Truncate(string(body), 200))
	}

	return items, nil
}
