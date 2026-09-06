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
	// corporateActionsPathFormat is the IDX GetIssuedHistory endpoint. caType
	// stays empty — no filter returns every corporate-action type in one call.
	// dateFrom/dateTo (YYYYMMDD) bound the event date (TanggalPencatatan)
	// server-side; start/length are the DataTables page window.
	corporateActionsPathFormat = "/primary/ListingActivity/GetIssuedHistory?caType=&dateFrom=%s&dateTo=%s&start=%d&length=%d"
	// corporateActionsReferer matches the IDX page hosting the corporate-action
	// list (the Cloudflare JS-gate expects a same-site referrer).
	corporateActionsReferer = "https://www.idx.co.id/en/listed-companies/corporate-action/"
	// corporateActionsLength is the row window per request; the endpoint is
	// DataTables-style, so start is an offset.
	corporateActionsLength = 9999
	// corporateActionsMaxPages caps the pagination loop defensively.
	corporateActionsMaxPages = 20
)

// CorporateActionDetail is the per-event amount pair from GetIssuedHistory.
// Id is the stable IDX surrogate (the DataTables row id), kept off the wire
// response but used as the persistence key in the usecase.
type CorporateActionDetail struct {
	JumlahSaham                float64 `json:"jumlah_saham"`
	JumlahSahamSetelahTindakan float64 `json:"jumlah_saham_setelah_tindakan"`
}

// CorporateAction is one corporate-action event as fetched from IDX.
type CorporateAction struct {
	ID        int64                 `json:"id"`
	Ticker    string                `json:"ticker"`
	EventDate time.Time             `json:"event_date"`
	Type      string                `json:"type"`
	Detail    CorporateActionDetail `json:"detail"`
}

// corpActionRow is the wire row shape (IDX PascalCase keys, confirmed by a
// live probe 2026-09-06).
type corpActionRow struct {
	ID                         int64   `json:"id"`
	KodeEmiten                 string  `json:"KodeEmiten"`
	TanggalPencatatan          string  `json:"TanggalPencatatan"`
	JenisTindakan              string  `json:"JenisTindakan"`
	JumlahSaham                float64 `json:"JumlahSaham"`
	JumlahSahamSetelahTindakan float64 `json:"JumlahSahamSetelahTindakan"`
}

// corpActionsPage is the DataTables wrapper the endpoint returns.
type corpActionsPage struct {
	RecordsTotal int             `json:"recordsTotal"`
	Data         []corpActionRow `json:"data"`
}

// corporateActionsPath builds the GetIssuedHistory path for one page of the
// [from, to] event-date window.
func corporateActionsPath(from, to time.Time, start int) string {
	return fmt.Sprintf(
		corporateActionsPathFormat, from.Format("20060102"), to.Format("20060102"), start, corporateActionsLength,
	)
}

// FetchCorporateActions fetches every corporate-action event with an event date
// in [from, to] (inclusive), paginating the DataTables start/length window.
// Rows with an empty KodeEmiten or an unparseable TanggalPencatatan are
// skipped. ctx is accepted for interface uniformity with the usecase fetcher
// seam; the transport itself is not context-aware.
func (c *Client) FetchCorporateActions(ctx context.Context, from, to time.Time) ([]CorporateAction, error) {
	var all []CorporateAction
	total := 0
	received := 0
	start := 0

	for page := 0; page < corporateActionsMaxPages; page++ {
		path := corporateActionsPath(from, to, start)
		headers := map[string]string{"Referer": corporateActionsReferer}
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

		var pageResp corpActionsPage
		if err := json.Unmarshal(body, &pageResp); err != nil {
			return nil, fmt.Errorf("parse response: %w (body=%s)", err, truncate(string(body), 200))
		}
		if total == 0 {
			total = pageResp.RecordsTotal
		}
		received += len(pageResp.Data)

		for _, row := range pageResp.Data {
			if action, ok := rowToAction(row); ok {
				all = append(all, action)
			}
		}

		// Termination: empty page, or the advertised count is received. received
		// (not len(all)) drives this — rows with an empty ticker or bad date are
		// skipped, so all can legitimately undercount.
		if len(pageResp.Data) == 0 || total > 0 && received >= total {
			break
		}
		start += corporateActionsLength
	}

	if total > 0 && received < total {
		c.log.Warnf("corporate actions: truncated fetch — got %d of RecordsTotal=%d", received, total)
	}
	return all, nil
}

// rowToAction converts one wire row, skipping rows without a ticker or with an
// unparseable event date. Amounts of 0 are valid (e.g. Obligasi Wajib Konversi).
func rowToAction(r corpActionRow) (CorporateAction, bool) {
	ticker := strings.TrimSpace(r.KodeEmiten)
	if ticker == "" {
		return CorporateAction{}, false
	}
	t, err := time.Parse("2006-01-02T15:04:05", strings.TrimSpace(r.TanggalPencatatan))
	if err != nil {
		return CorporateAction{}, false
	}
	return CorporateAction{
		ID:        r.ID,
		Ticker:    ticker,
		EventDate: t,
		Type:      strings.TrimSpace(r.JenisTindakan),
		Detail: CorporateActionDetail{
			JumlahSaham:                r.JumlahSaham,
			JumlahSahamSetelahTindakan: r.JumlahSahamSetelahTindakan,
		},
	}, true
}

// truncate shortens a string for error messages, mirroring the task layer's
// helper without importing pipeline (the client package must not depend on it).
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
