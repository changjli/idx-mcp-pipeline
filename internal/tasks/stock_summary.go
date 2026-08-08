package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Default source_status max age for stock summary (24 hours).
const stockSummaryMaxAgeSeconds int32 = 86400

// StockSummaryPayload is the payload for an idx:stock_summary task.
type StockSummaryPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// StockSummaryItem is a single row from the IDX GetStockSummary API.
type StockSummaryItem struct {
	StockCode   string   `json:"StockCode"`
	StockName   string   `json:"StockName"`
	OpenPrice   *float64 `json:"OpenPrice"`
	High        *float64 `json:"High"`
	Low         *float64 `json:"Low"`
	Close       *float64 `json:"Close"`
	Volume      *float64 `json:"Volume"`
	Value       *float64 `json:"Value"`
	Frequency   *float64 `json:"Frequency"`
	ListedShares *float64 `json:"ListedShares"`
}

// StockSummaryResponse is the DataTables-style wrapper returned by the API.
type StockSummaryResponse struct {
	Draw            int                `json:"draw"`
	RecordsTotal    int                `json:"recordsTotal"`
	RecordsFiltered int                `json:"recordsFiltered"`
	Data            []StockSummaryItem `json:"data"`
}

// EnqueueStockSummary enqueues an idx:stock_summary task for the given date.
// Uses a date-keyed TaskID for dedup. Returns ErrTaskIDConflict if already enqueued.
// Extra opts (e.g. asynq.ProcessIn) are appended to the default options.
func EnqueueStockSummary(client *asynq.Client, date time.Time, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeStockSummary, dateKey)
	payload := StockSummaryPayload{Date: dateKey}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal stock_summary payload: %w", err)
	}

	task := asynq.NewTask(TypeStockSummary, raw)
	options := []asynq.Option{
		asynq.TaskID(taskKey),
		asynq.Queue("ingest"),
		asynq.MaxRetry(3),
		asynq.Retention(24 * time.Hour),
	}
	options = append(options, opts...)
	return client.Enqueue(task, options...)
}

// NewStockSummaryHandler returns an asynq handler for the idx:stock_summary task type.
// Fetches GetStockSummary from IDX, parses OHLCV, upserts into daily_prices,
// and updates source_status / alerts.
func NewStockSummaryHandler(
	log *logrus.Logger,
	idxClient *client.Client,
	db *sqlx.DB,
	tickerRepo *repository.TickerRepository,
	dailyPriceRepo *repository.DailyPriceRepository,
	sourceStatusRepo *repository.SourceStatusRepository,
	alertRepo *repository.AlertRepository,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var p StockSummaryPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		date, err := time.Parse("2006-01-02", p.Date)
		if err != nil {
			return fmt.Errorf("invalid date %q: %w", p.Date, err)
		}

		log.Infof("stock_summary: fetching for date=%s", p.Date)

		// Fetch from IDX API.
		resp, fetchErr := fetchStockSummary(idxClient, date, log)
		if fetchErr != nil {
			log.Errorf("stock_summary: fetch failed: %v", fetchErr)
			recordFailure(db, sourceStatusRepo, alertRepo, p.Date, fetchErr, log)
			return fetchErr
		}

		rows := resp.Data
		log.Infof("stock_summary: fetched %d rows for date=%s", len(rows), p.Date)

		// Upsert each row into daily_prices.
		// Ticker must exist first (FK constraint) — auto-discover from response.
		upserted := 0
		for _, item := range rows {
			if err := upsertTicker(db, tickerRepo, item); err != nil {
				log.Warnf("stock_summary: ticker upsert failed for %s: %v", item.StockCode, err)
				continue
			}

			price := itemToDailyPrice(item, p.Date)
			if err := dailyPriceRepo.Upsert(db, price); err != nil {
				log.Warnf("stock_summary: upsert failed for %s: %v", item.StockCode, err)
				continue
			}
			upserted++
		}

		log.Infof("stock_summary: upserted %d/%d rows for date=%s", upserted, len(rows), p.Date)

		// Update source_status on success.
		recordSuccess(db, sourceStatusRepo, p.Date, log)

		return nil
	}
}

// fetchStockSummary calls the IDX GetStockSummary API and parses the response.
func fetchStockSummary(idxClient *client.Client, date time.Time, log *logrus.Logger) (StockSummaryResponse, error) {
	dateIDX := date.Format("20060102") // YYYYMMDD
	path := fmt.Sprintf("/primary/TradingSummary/GetStockSummary?length=9999&start=0&date=%s", dateIDX)

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
		return StockSummaryResponse{}, fmt.Errorf("idx api error: status=%d body=%s", resp.StatusCode, truncate(string(body), 200))
	}

	log.Println(body)

	var items StockSummaryResponse
	if err := json.Unmarshal(body, &items); err != nil {
		return StockSummaryResponse{}, fmt.Errorf("parse response: %w (body=%s)", err, truncate(string(body), 200))
	}

	return items, nil
}

// itemToDailyPrice converts a StockSummaryItem to a DailyPrice entity.
func itemToDailyPrice(item StockSummaryItem, dateStr string) *entity.DailyPrice {
	tradingDay, _ := time.Parse("2006-01-02", dateStr)

	// IDX API returns all numerics as float64; DB stores volume/value as int64
	// and frequency as int32. IDX values are whole units so truncation is safe.
	var volume *int64
	if item.Volume != nil {
		v := int64(*item.Volume)
		volume = &v
	}

	var value *int64
	if item.Value != nil {
		v := int64(*item.Value)
		value = &v
	}

	var frequency *int32
	if item.Frequency != nil {
		f := int32(*item.Frequency)
		frequency = &f
	}

	return &entity.DailyPrice{
		Ticker:     item.StockCode,
		TradingDay: tradingDay,
		Open:       item.OpenPrice,
		High:       item.High,
		Low:        item.Low,
		Close:      item.Close,
		Volume:     volume,
		Value:      value,
		Frequency:  frequency,
		Source:     "idx",
		FetchedAt:  time.Now(),
	}
}

// upsertTicker ensures the ticker exists in the tickers table (FK dependency
// for daily_prices). Auto-discovers new listings from the IDX response.
func upsertTicker(db *sqlx.DB, repo *repository.TickerRepository, item StockSummaryItem) error {
	var shares *int64
	if item.ListedShares != nil {
		s := int64(*item.ListedShares)
		shares = &s
	}

	ticker := &entity.Ticker{
		Code:   item.StockCode,
		Name:   item.StockName,
		Shares: shares,
		Active: true,
	}
	return repo.Upsert(db, ticker)
}

// recordSuccess updates source_status after a successful fetch.
// Clears LastError and resets consecutive_failures.
func recordSuccess(db *sqlx.DB, repo *repository.SourceStatusRepository, date string, log *logrus.Logger) {
	now := time.Now()
	status := &entity.SourceStatus{
		Source:              TypeStockSummary,
		LastSuccessAt:       &now,
		LastAttemptAt:       &now,
		LastError:           nil,
		ConsecutiveFailures: 0,
		Stale:               false,
		MaxAgeSeconds:       stockSummaryMaxAgeSeconds,
	}
	if err := repo.Upsert(db, status); err != nil {
		log.Errorf("stock_summary: failed to update source_status: %v", err)
	}
}

// recordFailure updates source_status (last_error, consecutive_failures, stale)
// and inserts an alert row. Called when the stock summary fetch fails.
func recordFailure(db *sqlx.DB, repo *repository.SourceStatusRepository, alertRepo *repository.AlertRepository, date string, fetchErr error, log *logrus.Logger) {
	now := time.Now()
	errStr := fetchErr.Error()

	// Get current status to increment consecutive_failures.
	current, _ := repo.FindBySource(db, TypeStockSummary)
	consecutive := int32(1)
	if current != nil {
		consecutive = current.ConsecutiveFailures + 1
	}

	status := &entity.SourceStatus{
		Source:              TypeStockSummary,
		LastAttemptAt:       &now,
		LastError:           &errStr,
		ConsecutiveFailures: consecutive,
		Stale:               consecutive >= 3,
		MaxAgeSeconds:       stockSummaryMaxAgeSeconds,
	}
	if err := repo.Upsert(db, status); err != nil {
		log.Errorf("stock_summary: failed to update source_status (failure): %v", err)
	}

	// Insert alert.
	alert := &entity.Alert{
		Source:    TypeStockSummary,
		AlertType: "ingestion_error",
		Message:   fmt.Sprintf("stock_summary fetch failed for %s (attempt %d): %s", date, consecutive, errStr),
	}
	if err := alertRepo.Insert(db, alert); err != nil {
		log.Errorf("stock_summary: failed to insert alert: %v", err)
	}
}

// truncate truncates a string to maxLen chars, appending "..." if truncated.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
