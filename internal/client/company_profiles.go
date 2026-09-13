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
	// companyProfilesPathFormat is the IDX GetCompanyProfiles endpoint: the
	// listed-companies (perusahaan-tercatut) data. emitenType=s filters to
	// saham (equities); start/length are the DataTables page window. The
	// endpoint accepts 100 rows/page (re-probed 2026-09-12 — an earlier probe
	// 2026-09-09 measured a 10-row cap, no longer true), so a full fetch is
	// ~10 pages.
	companyProfilesPathFormat = "/primary/ListedCompany/GetCompanyProfiles?emitenType=s&start=%d&length=%d"
	// companyProfilesReferer matches the IDX page hosting the listed-companies
	// list (the Cloudflare JS-gate expects a same-site referrer). Confirmed by
	// live probe for the corporate-action page; this one still needs a live check.
	companyProfilesReferer = "https://www.idx.co.id/en/listed-companies/"
	// companyProfilesLength is the row window per request; 100 rows/page keeps
	// a full fetch at ~10 pages (each page is a flaky Cloudflare-gated fetch,
	// so fewer pages is directly more reliable).
	companyProfilesLength = 100
	// companyProfilesMaxPages caps the pagination loop defensively (962 rows /
	// 100 per page = 10 pages; 200 leaves headroom for IPO growth).
	companyProfilesMaxPages = 200
)

// CompanyProfile is one listed company's profile as fetched from IDX. Only the
// trading-relevant fields are kept — cosmetic contact info (address, phone,
// email, website, NPWP) is out of scope (issue 11b). ListingDate is nil when
// the wire date is empty or unparseable (the row is still kept — the date is
// secondary to name/board/status). Active maps Status 0 (tercatat) to true.
type CompanyProfile struct {
	Ticker       string
	Name         string
	ListingBoard string
	ListingDate  *time.Time
	Active       bool
}

// companyProfileRow is the wire row shape (IDX PascalCase keys, confirmed by a
// live probe 2026-09-09).
type companyProfileRow struct {
	KodeEmiten        string `json:"KodeEmiten"`
	NamaEmiten        string `json:"NamaEmiten"`
	PapanPencatatan   string `json:"PapanPencatatan"`
	TanggalPencatatan string `json:"TanggalPencatatan"`
	Status            int    `json:"Status"`
}

// companyProfilesPage is the DataTables wrapper the endpoint returns.
type companyProfilesPage struct {
	RecordsTotal int                 `json:"recordsTotal"`
	Data         []companyProfileRow `json:"data"`
}

// FetchCompanyProfiles fetches every listed company's profile, paginating the
// DataTables start/length window (10 rows/page). Rows with an empty KodeEmiten
// are skipped; an unparseable TanggalPencatatan leaves ListingDate nil rather
// than dropping the row. ctx is accepted for interface uniformity with the
// usecase fetcher seam; the transport itself is not context-aware.
func (c *Client) FetchCompanyProfiles(ctx context.Context) ([]CompanyProfile, error) {
	var all []CompanyProfile
	total := 0
	received := 0
	start := 0

	for page := 0; page < companyProfilesMaxPages; page++ {
		path := fmt.Sprintf(companyProfilesPathFormat, start, companyProfilesLength)
		headers := map[string]string{"Referer": companyProfilesReferer}
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

		var pageResp companyProfilesPage
		if err := json.Unmarshal(body, &pageResp); err != nil {
			return nil, fmt.Errorf("parse response: %w (body=%s)", err, truncate(string(body), 200))
		}
		if total == 0 {
			total = pageResp.RecordsTotal
		}
		received += len(pageResp.Data)

		for _, row := range pageResp.Data {
			if profile, ok := rowToCompanyProfile(row); ok {
				all = append(all, profile)
			}
		}

		// Termination: empty page, or the advertised count is received. received
		// (not len(all)) drives this — rows with an empty ticker are skipped, so
		// all can legitimately undercount.
		if len(pageResp.Data) == 0 || total > 0 && received >= total {
			break
		}
		start += companyProfilesLength
	}

	if total > 0 && received < total {
		c.log.Warnf("company profiles: truncated fetch — got %d of RecordsTotal=%d", received, total)
	}
	return all, nil
}

// rowToCompanyProfile converts one wire row, skipping rows without a ticker.
// An unparseable listing date yields a nil ListingDate — the profile is still
// kept (name/board/status are the primary fields).
func rowToCompanyProfile(r companyProfileRow) (CompanyProfile, bool) {
	ticker := strings.TrimSpace(r.KodeEmiten)
	if ticker == "" {
		return CompanyProfile{}, false
	}
	var listingDate *time.Time
	if t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSpace(r.TanggalPencatatan)); err == nil {
		listingDate = &t
	}
	return CompanyProfile{
		Ticker:       ticker,
		Name:         strings.TrimSpace(r.NamaEmiten),
		ListingBoard: strings.TrimSpace(r.PapanPencatatan),
		ListingDate:  listingDate,
		Active:       r.Status == 0,
	}, true
}
