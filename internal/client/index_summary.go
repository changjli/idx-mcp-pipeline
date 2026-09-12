package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	// indexSummaryPath is the IDX GetIndexSummary endpoint: one GET, no
	// pagination, length=9999 returns all 45 indices (34 main + 11 sector) in
	// one page (findings-sector-index-membership.md, probe 2026-09-09).
	indexSummaryPath = "/primary/TradingSummary/GetIndexSummary?length=9999&start=0"
	// indexSummaryReferer matches the IDX page hosting the trading-summary data
	// (the Cloudflare JS-gate expects a same-site referrer). Confirmed by live
	// probe for the corporate-action page; this one still needs a live check.
	indexSummaryReferer = "https://www.idx.co.id/en/market-data/trading-summary/"
)

// IndexSummary is one index's daily summary as fetched from IDX. Date is the
// trading day the summary describes. All value fields stay faithful to the
// wire floats — the entity conversion (tasks layer) applies the int truncation.
type IndexSummary struct {
	Date          time.Time
	IndexCode     string
	Previous      float64
	Highest       float64
	Lowest        float64
	Close         float64
	Change        float64
	NumberOfStock float64
	Volume        float64
	Value         float64
	Frequency     float64
	MarketCap     float64
}

// indexSummaryRow is the wire row shape (IDX PascalCase keys, confirmed by a
// live probe 2026-09-09).
type indexSummaryRow struct {
	No             int64   `json:"No"`
	IndexSummaryID int64   `json:"IndexSummaryID"`
	Date           string  `json:"Date"`
	IndexCode      string  `json:"IndexCode"`
	Previous       float64 `json:"Previous"`
	Highest        float64 `json:"Highest"`
	Lowest         float64 `json:"Lowest"`
	Close          float64 `json:"Close"`
	Change         float64 `json:"Change"`
	NumberOfStock  float64 `json:"NumberOfStock"`
	Volume         float64 `json:"Volume"`
	Value          float64 `json:"Value"`
	Frequency      float64 `json:"Frequency"`
	MarketCapital  float64 `json:"MarketCapital"`
}

// indexSummaryPage is the DataTables wrapper the endpoint returns.
type indexSummaryPage struct {
	RecordsTotal int               `json:"recordsTotal"`
	Data         []indexSummaryRow `json:"data"`
}

// FetchIndexSummary fetches the IDX index/sector summary — all 45 indices in
// one call. Rows with an empty IndexCode or an unparseable Date are skipped.
// ctx is accepted for interface uniformity with the usecase fetcher seam; the
// transport itself is not context-aware.
func (c *Client) FetchIndexSummary(ctx context.Context) ([]IndexSummary, error) {
	headers := map[string]string{"Referer": indexSummaryReferer}
	resp, err := c.GetWithHeaders(indexSummaryPath, headers)
	if err != nil {
		return nil, fmt.Errorf("idx get: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("idx api error: status=%d body=%s", resp.StatusCode, truncate(string(body), 200))
	}

	var pageResp indexSummaryPage
	if err := json.Unmarshal(body, &pageResp); err != nil {
		return nil, fmt.Errorf("parse response: %w (body=%s)", err, truncate(string(body), 200))
	}

	rows := make([]IndexSummary, 0, len(pageResp.Data))
	for _, r := range pageResp.Data {
		if s, ok := rowToIndexSummary(r); ok {
			rows = append(rows, s)
		}
	}
	if pageResp.RecordsTotal > 0 && len(rows) < pageResp.RecordsTotal {
		c.log.Warnf("index summary: truncated fetch — got %d of RecordsTotal=%d", len(rows), pageResp.RecordsTotal)
	}
	return rows, nil
}

// rowToIndexSummary converts one wire row, skipping rows without an index code
// or with an unparseable date.
func rowToIndexSummary(r indexSummaryRow) (IndexSummary, bool) {
	code := strings.TrimSpace(r.IndexCode)
	if code == "" {
		return IndexSummary{}, false
	}
	t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSpace(r.Date))
	if err != nil {
		return IndexSummary{}, false
	}
	return IndexSummary{
		Date:          t,
		IndexCode:     code,
		Previous:      r.Previous,
		Highest:       r.Highest,
		Lowest:        r.Lowest,
		Close:         r.Close,
		Change:        r.Change,
		NumberOfStock: r.NumberOfStock,
		Volume:        r.Volume,
		Value:         r.Value,
		Frequency:     r.Frequency,
		MarketCap:     r.MarketCapital,
	}, true
}
