package pipeline

import (
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

// DailyPriceStore upserts daily_prices rows. Consumer-side interface
// (ADR-0006): satisfied by the sqlx-backed DailyPriceRepository; tests provide
// the second adapter.
type DailyPriceStore interface {
	Upsert(price *entity.DailyPrice) error
}

// StockSummaryIngest upserts one day's stock summary rows into daily_prices,
// auto-discovering new listings (ticker FK dependency). Batch error policy:
// log-and-skip — an individual row's failure is logged and skipped so one bad
// row can't drop a whole trading day from the DB; healthy rows still land
// (the row itself is retried on the next run's upsert). Declared policy;
// unchanged from the task-layer loop (follow-up 07 tracks the whole policy).
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

// UpsertRows upserts all rows for one date into daily_prices, returning rows
// upserted (failed rows logged and skipped, per the declared policy).
func (n *StockSummaryIngest) UpsertRows(rows []StockSummaryItem, dateKey string) int {
	upserted := 0
	for _, item := range rows {
		if err := n.tickers.Upsert(tickerFromSummaryItem(item)); err != nil {
			n.log.Warnf("stock_summary: ticker upsert failed for %s: %v", item.StockCode, err)
			continue
		}

		price := itemToDailyPrice(item, dateKey)
		if err := n.prices.Upsert(price); err != nil {
			n.log.Warnf("stock_summary: upsert failed for %s: %v", item.StockCode, err)
			continue
		}
		upserted++
	}
	return upserted
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

// Upsert writes one daily_prices row.
func (s *SQLDailyPriceStore) Upsert(price *entity.DailyPrice) error {
	return s.repo.Upsert(s.db, price)
}
