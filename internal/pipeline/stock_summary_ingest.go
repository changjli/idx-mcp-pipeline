package pipeline

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// StockSummaryItem is a single row from the IDX GetStockSummary API. Owned by
// the pipeline (the ingest usecase's input shape); the task layer unparses
// the HTTP payload into it.
type StockSummaryItem struct {
	StockCode    string   `json:"StockCode"`
	StockName    string   `json:"StockName"`
	OpenPrice    *float64 `json:"OpenPrice"`
	High         *float64 `json:"High"`
	Low          *float64 `json:"Low"`
	Close        *float64 `json:"Close"`
	Volume       *float64 `json:"Volume"`
	Value        *float64 `json:"Value"`
	Frequency    *float64 `json:"Frequency"`
	ListedShares *float64 `json:"ListedShares"`
}

// DailyPriceStore persists a day's daily_prices rows in one batch call.
// Consumer-side interface (ADR-0006): satisfied by the sqlx-backed
// DailyPriceRepository; tests provide the second adapter. The batch shape is
// the point — the adapter chunks it into multi-row upserts, so the ingest never
// pays a round trip per ticker (issue 21).
type DailyPriceStore interface {
	UpsertBatch(prices []entity.DailyPrice) (int, error)
}

// StockSummaryIngest upserts one day's stock summary rows into daily_prices,
// auto-discovering new listings (ticker FK dependency). Batch error policy:
// fail-fast per the package storage-error policy (see policy.go) — any ticker
// or price upsert failure aborts the batch and returns the error so asynq
// retries and the day's rows land together.
type StockSummaryIngest struct {
	prices  DailyPriceStore
	tickers TickerRegistrar
	log     *logrus.Logger
}

// NewStockSummaryIngest wires the stock summary ingest usecase over its
// stores.
func NewStockSummaryIngest(prices DailyPriceStore, tickers TickerRegistrar, log *logrus.Logger) *StockSummaryIngest {
	return &StockSummaryIngest{prices: prices, tickers: tickers, log: log}
}

// UpsertRows persists all rows for one date into daily_prices. Returns rows
// upserted and the first error — fail-fast per the package storage-error
// policy (see policy.go).
//
// Two passes. Tickers first: a new listing must exist before any price row
// referencing it can land (FK), so every registration completes before the
// price batch is issued — a ticker failure therefore writes no prices at all.
// Prices second, as one batch call the store chunks into multi-row upserts;
// a chunk failure aborts the day and returns the rows written by the chunks
// that preceded it.
func (n *StockSummaryIngest) UpsertRows(rows []StockSummaryItem, dateKey string) (int, error) {
	for _, item := range rows {
		if err := n.tickers.Upsert(tickerFromSummaryItem(item)); err != nil {
			n.log.Warnf("stock_summary: ticker upsert failed for %s: %v", item.StockCode, err)
			return 0, fmt.Errorf("ticker upsert %s: %w", item.StockCode, err)
		}
	}

	prices := make([]entity.DailyPrice, 0, len(rows))
	for _, item := range rows {
		prices = append(prices, *itemToDailyPrice(item, dateKey))
	}

	upserted, err := n.prices.UpsertBatch(prices)
	if err != nil {
		n.log.Warnf("stock_summary: daily_price batch upsert failed after %d rows: %v", upserted, err)
		return upserted, fmt.Errorf("daily_price upsert: %w", err)
	}
	return upserted, nil
}

// tickerFromSummaryItem adapts a summary row to the tickers-table row; new
// listings are auto-discovered from the IDX response name/shares.
func tickerFromSummaryItem(item StockSummaryItem) *entity.Ticker {
	var shares *int64
	if item.ListedShares != nil {
		s := int64(*item.ListedShares)
		shares = &s
	}
	return &entity.Ticker{
		Code:   item.StockCode,
		Name:   item.StockName,
		Shares: shares,
		Active: true,
	}
}

// itemToDailyPrice converts a summary row to a DailyPrice entity for the
// given date.
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

// SQLDailyPriceStore adapts DailyPriceRepository to DailyPriceStore.
type SQLDailyPriceStore struct {
	repo *repository.DailyPriceRepository
	db   *sqlx.DB
}

// NewSQLDailyPriceStore binds a daily-price repository to its database.
func NewSQLDailyPriceStore(repo *repository.DailyPriceRepository, db *sqlx.DB) *SQLDailyPriceStore {
	return &SQLDailyPriceStore{repo: repo, db: db}
}

// UpsertBatch writes a day's daily_prices rows, chunked into multi-row
// statements by the repository. Returns the rows written.
func (s *SQLDailyPriceStore) UpsertBatch(prices []entity.DailyPrice) (int, error) {
	return s.repo.UpsertBatch(s.db, prices)
}
