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
	// suspensionsPathFormat is the IDX GetSuspension endpoint: per-ticker
	// suspend (SPT) / unsuspend (UPT) announcements. dateFrom/dateTo (YYYYMMDD)
	// bound the announcement date server-side; pageSize=9999 returns the whole
	// window in one call.
	suspensionsPathFormat = "/primary/NewsAnnouncement/GetSuspension?indexFrom=1&dateFrom=%s&dateTo=%s&pageSize=9999&lang=id"
	// umaPathFormat is the IDX GetUma endpoint: per-ticker unusual-market-
	// activity (UMA) warnings, same window params.
	umaPathFormat = "/primary/NewsAnnouncement/GetUma?indexFrom=1&dateFrom=%s&dateTo=%s&pageSize=9999&lang=id"
	// suspensionsReferer matches the IDX page hosting the suspension/UMA list
	// (the Cloudflare JS-gate expects a same-site referrer). Confirmed by live
	// probe for the corporate-action page; this one still needs a live check.
	suspensionsReferer = "https://www.idx.co.id/en/listed-companies/news-announcement/"
	// aggregateKode is the Kode of the ">1 Kode" batch announcements whose
	// per-ticker detail lives only in the PDF (not extracted) — rows carrying it
	// have no usable per-ticker data and are skipped.
	aggregateKode = ">1 Kode"
)

// Suspension is one per-ticker suspension event as fetched from IDX. Type is
// the raw Info_Type: SPT (suspend) or UPT (unsuspend). Reason is the Judul
// (announcement title).
type Suspension struct {
	Ticker    string
	EventDate time.Time
	Type      string
	Reason    string
}

// Uma is one per-ticker UMA warning as fetched from IDX. AnnouncementNo is the
// BEI letter number (Peng-UMA-....); Reason is the Judul.
type Uma struct {
	Ticker         string
	EventDate      time.Time
	AnnouncementNo string
	Reason         string
}

// suspensionRow is the wire row shape (IDX PascalCase keys, confirmed by a
// live probe 2026-09-06).
type suspensionRow struct {
	Judul string `json:"Judul"`
	Kode  string `json:"Kode"`
	Date  string `json:"Date"`
	Type  string `json:"Info_Type"`
}

// suspensionPage is the wrapper the GetSuspension endpoint returns.
type suspensionPage struct {
	ResultCount int             `json:"ResultCount"`
	Results     []suspensionRow `json:"Results"`
}

// umaRow is the wire row shape (IDX PascalCase keys, confirmed by a live probe
// 2026-09-06).
type umaRow struct {
	UMAID          string `json:"UMAID"`
	UMADate        string `json:"UMADate"`
	AnnouncementNo string `json:"AnnouncementNo"`
	CompanyID      string `json:"CompanyID"`
	CompanyName    string `json:"CompanyName"`
	Status         string `json:"Status"`
	Judul          string `json:"Judul"`
}

// umaPage is the wrapper the GetUma endpoint returns.
type umaPage struct {
	ResultCount int      `json:"ResultCount"`
	Results     []umaRow `json:"Results"`
}

// FetchSuspensions fetches every per-ticker suspension event with an
// announcement date in [from, to] (inclusive). Rows with an empty Kode, the
// ">1 Kode" aggregate, or an unparseable Date are skipped. ctx is accepted for
// interface uniformity with the usecase fetcher seam; the transport itself is
// not context-aware.
func (c *Client) FetchSuspensions(ctx context.Context, from, to time.Time) ([]Suspension, error) {
	body, err := c.fetchSuspensionsJSON(suspensionsPathFormat, from, to)
	if err != nil {
		return nil, err
	}
	var page suspensionPage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("parse response: %w (body=%s)", err, truncate(string(body), 200))
	}

	var all []Suspension
	for _, row := range page.Results {
		if s, ok := rowToSuspension(row); ok {
			all = append(all, s)
		}
	}
	return all, nil
}

// FetchUma fetches every per-ticker UMA warning with an announcement date in
// [from, to] (inclusive). Rows with an empty CompanyID or an unparseable
// UMADate are skipped. ctx is accepted for interface uniformity with the
// usecase fetcher seam; the transport itself is not context-aware.
func (c *Client) FetchUma(ctx context.Context, from, to time.Time) ([]Uma, error) {
	body, err := c.fetchSuspensionsJSON(umaPathFormat, from, to)
	if err != nil {
		return nil, err
	}
	var page umaPage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("parse response: %w (body=%s)", err, truncate(string(body), 200))
	}

	var all []Uma
	for _, row := range page.Results {
		if u, ok := rowToUma(row); ok {
			all = append(all, u)
		}
	}
	return all, nil
}

// fetchSuspensionsJSON performs one browser-routed GET of the given path format
// over [from, to] and returns the raw response body.
func (c *Client) fetchSuspensionsJSON(pathFormat string, from, to time.Time) ([]byte, error) {
	path := fmt.Sprintf(pathFormat, from.Format("20060102"), to.Format("20060102"))
	headers := map[string]string{"Referer": suspensionsReferer}
	resp, err := c.GetWithHeaders(path, headers)
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
	return body, nil
}

// rowToSuspension converts one wire row, skipping the ">1 Kode" aggregate rows
// (their per-ticker detail lives only in the PDF) and rows without a ticker or
// with an unparseable date.
func rowToSuspension(r suspensionRow) (Suspension, bool) {
	ticker := strings.TrimSpace(r.Kode)
	if ticker == "" || ticker == aggregateKode {
		return Suspension{}, false
	}
	t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSpace(r.Date))
	if err != nil {
		return Suspension{}, false
	}
	return Suspension{
		Ticker:    ticker,
		EventDate: t,
		Type:      strings.TrimSpace(r.Type),
		Reason:    strings.TrimSpace(r.Judul),
	}, true
}

// rowToUma converts one wire row, skipping rows without a CompanyID or with an
// unparseable UMADate.
func rowToUma(r umaRow) (Uma, bool) {
	ticker := strings.TrimSpace(r.CompanyID)
	if ticker == "" {
		return Uma{}, false
	}
	t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSpace(r.UMADate))
	if err != nil {
		return Uma{}, false
	}
	return Uma{
		Ticker:         ticker,
		EventDate:      t,
		AnnouncementNo: strings.TrimSpace(r.AnnouncementNo),
		Reason:         strings.TrimSpace(r.Judul),
	}, true
}
