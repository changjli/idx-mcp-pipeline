// Package ksei is the client for KSEI's public monthly "Kepemilikan Efek
// (Lokal-Asing)" balance-position archive (issue 08). One zip per month-end —
// BalanceposEfekYYYYMMDD.zip — contains a single pipe-delimited txt covering
// the whole market: per security, scripless holdings split Local/Foreign
// across nine KSEI investor-type codes (IS=Insurance, CP=Corporate,
// PF=Pension Fund, IB=Financial Institution, ID=Individu, MF=Mutual Fund,
// SC=Securities Company, FD=Foundation, OT=Others — per KSEI's official
// investor-type guide). Plain HTTP, no auth and no Cloudflare gate — the
// nodriver sidecar is not involved.
package ksei

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// DefaultBaseURL is the KSEI web host serving the download archive. The
	// www. host 404s the /Download/ path — verified 2026-09-06.
	DefaultBaseURL = "https://web.ksei.co.id"
	// balanceposReferer matches the archive page the download links live on;
	// KSEI rejects the direct download without it (verified 2026-09-06).
	balanceposReferer = "https://web.ksei.co.id/archive_download/holding_composition"
	// balanceposPathFormat is the monthly zip: BalanceposEfekYYYYMMDD.zip.
	balanceposPathFormat = "/Download/BalanceposEfek%s.zip"
	// fetchTimeout bounds one download — the zip is ~130KB, well under a
	// second in practice; a generous timeout tolerates KSEI latency.
	fetchTimeout = 60 * time.Second
	// userAgent — the download was verified with curl's UA; a plain Go
	// default is unverified, so send a common browser string defensively.
	userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
)

// Holding is one parsed row of the balance-position file. The 18 per-type
// columns keep KSEI's own codes verbatim (IS/CP/PF/IB/ID/MF/SC/FD/OT — see
// the package comment for the expansions). Totals are the file's own Total
// columns, not recomputed sums: the file is the source of truth.
type Holding struct {
	Date         time.Time
	Code         string
	Type         string
	SecNum       int64
	Price        int64
	LocalIS      int64
	LocalCP      int64
	LocalPF      int64
	LocalIB      int64
	LocalID      int64
	LocalMF      int64
	LocalSC      int64
	LocalFD      int64
	LocalOT      int64
	LocalTotal   int64
	ForeignIS    int64
	ForeignCP    int64
	ForeignPF    int64
	ForeignIB    int64
	ForeignID    int64
	ForeignMF    int64
	ForeignSC    int64
	ForeignFD    int64
	ForeignOT    int64
	ForeignTotal int64
}

// Client downloads and parses KSEI balance-position archives.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient returns a Client over the given base URL (DefaultBaseURL when
// empty). httpClient may be nil — a default with a fetch timeout is used.
func NewClient(baseURL string, httpClient *http.Client) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: fetchTimeout}
	}
	return &Client{baseURL: baseURL, http: httpClient}
}

// LatestFileDate returns the month-end file date the daily task should
// ingest on the given run date: the last day of the preceding month. KSEI
// publishes month M's file at the start of M+1 (the August 2026 file
// appeared Sep 1), so the previous month-end is always the newest candidate.
func LatestFileDate(runDate time.Time) time.Time {
	firstOfMonth := time.Date(runDate.Year(), runDate.Month(), 1, 0, 0, 0, 0, runDate.Location())
	return firstOfMonth.AddDate(0, 0, -1)
}

// FetchBalancepos downloads and parses the balance-position archive for the
// given month-end file date. Every EQUITY row is returned (other security
// types — bonds, warrants, etc. — are skipped by the parser).
func (c *Client) FetchBalancepos(ctx context.Context, fileDate time.Time) ([]Holding, error) {
	path := fmt.Sprintf(balanceposPathFormat, fileDate.Format("20060102"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Referer", balanceposReferer)
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ksei get %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ksei api error: status=%d url=%s", resp.StatusCode, path)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("open zip %s: %w", path, err)
	}
	for _, f := range zipReader.File {
		if f.FileInfo().IsDir() || !strings.Contains(strings.ToUpper(f.Name), "BALANCEPOS") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("open zip entry %s: %w", f.Name, err)
		}
		defer rc.Close()
		return ParseBalancepos(rc)
	}
	return nil, fmt.Errorf("zip %s contains no Balancepos entry", path)
}
