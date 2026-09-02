package ipot

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// FinancialView selects which fundamental columns the tool returns.
type FinancialView string

const (
	// ViewRecent returns the latest ~2 years as IPOT reports them — one
	// column per report type (3M/6M/9M/12M cumulative) plus the
	// analyst-forecast and latest interim columns (quarter=5).
	ViewRecent FinancialView = "recent"
	// ViewQuarterly returns uniform reported 3-month (Q1) columns across
	// ~6 years (quarter=1).
	ViewQuarterly FinancialView = "quarterly"
	// ViewAnnual returns uniform reported 12-month columns across ~6 years
	// (quarter=4).
	ViewAnnual FinancialView = "annual"
)

// ipotQuarter maps a view to the fundamental.php quarter parameter:
// 5 = all report types (recent window), 1 = 3M columns only, 4 = 12M columns.
func (v FinancialView) ipotQuarter() string {
	switch v {
	case ViewAnnual:
		return "4"
	case ViewQuarterly:
		return "1"
	default:
		return "5"
	}
}

// durationMonths returns the uniform column duration the view keeps, or
// false for the unfiltered recent view.
func (v FinancialView) durationMonths() (int, bool) {
	switch v {
	case ViewAnnual:
		return 12, true
	case ViewQuarterly:
		return 3, true
	}
	return 0, false
}

// finCacheEntry holds a parsed fundamental result with its creation time.
type finCacheEntry struct {
	result  *Financials
	created time.Time
}

// FetchFinancial returns the normalized financial statements for one ticker in
// the requested view. Results are cached per ticker+view for CacheTTL (the
// broker summary cache is separate — different result types). All columns are
// returned: analyst-forecast ("Anlz") and interim (bracketed) columns are
// tagged (IsForecast/IsInterim), not dropped; the uniform views then keep only
// the reported columns matching their duration.
func (c *Client) FetchFinancial(ctx context.Context, ticker string, view FinancialView) (*Financials, error) {
	switch view {
	case ViewRecent, ViewQuarterly, ViewAnnual:
	default:
		return nil, fmt.Errorf("ipot: invalid financial view %q", view)
	}
	key := "fin:" + ticker + ":" + string(view)

	if res, ok := c.finCacheGet(key); ok {
		c.log.Debugf("ipot: cache HIT %s", key)
		return cloneFinancials(res), nil
	}

	c.waitPacing()

	u := c.buildFundamentalURL(ticker, view)
	c.log.Debugf("ipot: fetching %s", u)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("ipot: build request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ipot: get %s: %w", u, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ipot: read body: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("%w: status=%d", ErrUpstream429, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ipot: upstream error: status=%d body=%s", resp.StatusCode, truncate(string(body), 200))
	}

	fin, err := ParseFundamental(body)
	if err != nil {
		return nil, fmt.Errorf("ipot: parse %s: %w", u, err)
	}
	fin.Ticker = strings.ToUpper(ticker)
	filtered := filterView(fin, view)

	c.finCacheSet(key, filtered)
	return cloneFinancials(filtered), nil
}

// buildFundamentalURL constructs the module/saham/include/fundamental.php URL
// with the ticker and quarter view parameter.
func (c *Client) buildFundamentalURL(ticker string, view FinancialView) string {
	return fmt.Sprintf("%s/module/saham/include/fundamental.php?code=%s&quarter=%s",
		c.baseURL, strings.ToUpper(ticker), view.ipotQuarter())
}

// filterView keeps the view's reported period columns; the recent view keeps
// everything. Forecast and interim columns pass through tagged — the caller
// decides how to weigh them. The slice is copied so the cached value stays
// independent of later callers.
func filterView(fin *Financials, view FinancialView) *Financials {
	out := &Financials{
		Ticker:    fin.Ticker,
		Currency:  fin.Currency,
		LastPrice: fin.LastPrice,
		Periods:   []FinancialStatement{},
	}
	duration, filtered := view.durationMonths()
	for _, p := range fin.Periods {
		if filtered && p.DurationMonths != duration {
			continue
		}
		out.Periods = append(out.Periods, p)
	}
	return out
}

func (c *Client) finCacheGet(key string) (*Financials, bool) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	entry, ok := c.finCache[key]
	if !ok {
		return nil, false
	}
	if time.Since(entry.created) >= c.cacheTTL {
		delete(c.finCache, key)
		return nil, false
	}
	return entry.result, true
}

func (c *Client) finCacheSet(key string, res *Financials) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	if len(c.finCache) >= c.maxCache {
		// Evict the oldest entry to bound memory.
		var oldestKey string
		var oldest time.Time
		for k, e := range c.finCache {
			if oldestKey == "" || e.created.Before(oldest) {
				oldestKey, oldest = k, e.created
			}
		}
		delete(c.finCache, oldestKey)
	}
	c.finCache[key] = finCacheEntry{result: res, created: time.Now()}
}

// cloneFinancials returns a deep copy so callers can't mutate the cache.
// Period Extra maps are copied too — they're lazily-built maps, and a shallow
// copy would share them between the cache and callers.
func cloneFinancials(fin *Financials) *Financials {
	if fin == nil {
		return nil
	}
	cp := *fin
	cp.Periods = append([]FinancialStatement(nil), fin.Periods...)
	for i := range cp.Periods {
		if cp.Periods[i].Extra != nil {
			extra := make(map[string]float64, len(cp.Periods[i].Extra))
			for k, v := range cp.Periods[i].Extra {
				extra[k] = v
			}
			cp.Periods[i].Extra = extra
		}
	}
	return &cp
}
