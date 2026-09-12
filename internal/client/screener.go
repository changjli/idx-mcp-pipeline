package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	// screenerPath is the IDX stock-screener endpoint: one GET, no pagination,
	// ~960 rows (~1MB JSON) covering the full sector taxonomy (4 levels) plus
	// comma-separated index membership for all 45 indices (issue 15b).
	screenerPath = "/support/stock-screener/api/v1/stock-screener/get"
	// screenerReferer matches the IDX page hosting the stock screener (the
	// Cloudflare JS-gate expects a same-site referrer). Confirmed by live probe
	// for the corporate-action page; this one still needs a live check.
	screenerReferer = "https://www.idx.co.id/en/listed-companies/stock-screener/"
)

// ScreenerRow is one stock-screener row as fetched from IDX. Only the
// sector-taxonomy and index-membership fields are kept — the fundamentals
// snapshot (PER/PBV/ROE/...) and price windows are the screener tool's domain,
// out of scope here (findings-sector-index-membership.md). Industry,
// SubIndustry, SubIndustryCode and IndexCode are nil when the wire value is
// null (dirty rows: KETR/VICI lack industry levels; 8 suspended rows lack
// indexCode). IndexCode is a comma-separated list when present.
type ScreenerRow struct {
	StockCode       string
	Sector          string
	SubSector       string
	Industry        *string
	SubIndustry     *string
	SubIndustryCode *string
	IndexCode       *string
}

// screenerRow is the wire row shape (IDX camelCase keys, confirmed by a live
// probe 2026-09-09).
type screenerRow struct {
	StockCode       string  `json:"stockCode"`
	Sector          string  `json:"sector"`
	SubSector       string  `json:"subSector"`
	Industry        *string `json:"industry"`
	SubIndustry     *string `json:"subIndustry"`
	SubIndustryCode *string `json:"subIndustryCode"`
	IndexCode       *string `json:"indexCode"`
}

// screenerResponse is the top-level wrapper the endpoint returns.
type screenerResponse struct {
	Results     []screenerRow `json:"results"`
	ResultCount int           `json:"resultCount"`
}

// FetchScreener fetches the full stock-screener list (one call, no pagination)
// and decodes the sector taxonomy + index membership per ticker. Rows with an
// empty stockCode are skipped. ctx is accepted for interface uniformity with
// the usecase fetcher seam; the transport itself is not context-aware.
func (c *Client) FetchScreener(ctx context.Context) ([]ScreenerRow, error) {
	headers := map[string]string{"Referer": screenerReferer}
	resp, err := c.GetWithHeaders(screenerPath, headers)
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

	var pageResp screenerResponse
	if err := json.Unmarshal(body, &pageResp); err != nil {
		return nil, fmt.Errorf("parse response: %w (body=%s)", err, truncate(string(body), 200))
	}

	rows := make([]ScreenerRow, 0, len(pageResp.Results))
	for _, r := range pageResp.Results {
		if row, ok := rowToScreener(r); ok {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// rowToScreener converts one wire row, skipping rows without a stock code. All
// string fields are trimmed (whitespace on the wire must not land in the DB);
// nullable wire fields stay nil — the ingest-side cleanup (usecase) owns the
// dirty-row normalization.
func rowToScreener(r screenerRow) (ScreenerRow, bool) {
	code := strings.TrimSpace(r.StockCode)
	if code == "" {
		return ScreenerRow{}, false
	}
	return ScreenerRow{
		StockCode:       code,
		Sector:          strings.TrimSpace(r.Sector),
		SubSector:       strings.TrimSpace(r.SubSector),
		Industry:        trimPtr(r.Industry),
		SubIndustry:     trimPtr(r.SubIndustry),
		SubIndustryCode: trimPtr(r.SubIndustryCode),
		IndexCode:       trimPtr(r.IndexCode),
	}, true
}

// trimPtr trims a nullable wire string, returning nil for a nil input.
func trimPtr(s *string) *string {
	if s == nil {
		return nil
	}
	t := strings.TrimSpace(*s)
	return &t
}
