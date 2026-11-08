package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

const (
	// financialSnapshotPath is IDX's paginated digital-statistic endpoint. The
	// urlName selector is required and case-sensitive; periodMonth=M returns
	// the market at the close of month M (point-in-time, immutable).
	financialSnapshotPath = "/primary/DigitalStatistic/GetApiDataPaginated"

	// financialSnapshotDataset selects the market-wide ratio snapshot (one row
	// per listed company, values in billions of IDR).
	financialSnapshotDataset = "LINK_FINANCIAL_DATA_RATIO"

	// financialSnapshotPageSize must return the whole market in one response.
	// The server's default is 25 — the exact trap that produced the
	// committed 25-row evidence file — and it honours any larger value without
	// an observed cap. One request per month is the whole cost of Tier 1.
	financialSnapshotPageSize = "100000"
)

// ErrMalformedSnapshot reports a payload that parsed as JSON but failed the
// wire-integrity contract: a truncated body (len(data) != totalItems), a
// duplicate ticker, or an inconsistent empty response. It is deliberately
// distinct from a transport error so the ingest can record the month as
// `failed` in the run ledger instead of treating it as a transient blip.
var ErrMalformedSnapshot = errors.New("financials: malformed snapshot payload")

// financialSnapshotResponse is the endpoint's wrapper. The footer/columns
// blocks are display metadata and deliberately not decoded: units are
// hard-coded (money in billions, ratios in percent/multiples) because the
// decimal-place metadata is unstable across identical pulls.
type financialSnapshotResponse struct {
	Data []financialSnapshotRow `json:"data"`
	Meta struct {
		PageNumber int `json:"pageNumber"`
		PageSize   int `json:"pageSize"`
		TotalItems int `json:"totalItems"`
	} `json:"meta"`
}

// financialSnapshotRow is one wire row. Only the fields the store persists are
// decoded. The dirty string fields accept a quoted value, a JSON number or
// null — IDX's audit/opinion encodings vary by year (null, "0", "0.0",
// "(nul)", "A", ...).
type financialSnapshotRow struct {
	Code          string      `json:"code"`
	FsDate        dirtyString `json:"fsDate"`
	FiscalYearEnd dirtyString `json:"fiscalYearEnd"`

	Assets          *float64 `json:"assets"`
	Liabilities     *float64 `json:"liabilities"`
	Equity          *float64 `json:"equity"`
	Sales           *float64 `json:"sales"`
	EBT             *float64 `json:"ebt"`
	ProfitPeriod    *float64 `json:"profitPeriod"`
	ProfitAttrOwner *float64 `json:"profitAttrOwner"`

	EPS       *float64 `json:"eps"`
	BookValue *float64 `json:"bookValue"`
	ROA       *float64 `json:"roa"`
	ROE       *float64 `json:"roe"`
	NPM       *float64 `json:"npm"`
	DeRatio   *float64 `json:"deRatio"`

	Audit dirtyString `json:"audit"`
	Opini dirtyString `json:"opini"`
}

// dirtyString accepts a JSON string, number or null. It exists because the
// source encodes "no value" several ways and occasionally as a bare number.
type dirtyString struct {
	Value string
	Valid bool
}

func (d *dirtyString) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		d.Value, d.Valid = "", false
		return nil
	}
	var str string
	if err := json.Unmarshal(b, &str); err == nil {
		d.Value, d.Valid = str, true
		return nil
	}
	// A bare JSON number/bool: keep its text so the normalisers can classify it
	// (e.g. 0 -> "0" -> nil).
	d.Value, d.Valid = strings.Trim(s, `"`), true
	return nil
}

// financialSnapshotQuery builds the query string for one snapshot month.
func financialSnapshotQuery(year, month int) string {
	q := make([]string, 0, 8)
	add := func(k, v string) { q = append(q, k+"="+v) }
	add("urlName", financialSnapshotDataset)
	add("periodYear", strconv.Itoa(year))
	add("periodMonth", strconv.Itoa(month))
	add("periodType", "monthly")
	add("isPrint", "False")
	add("cumulative", "false")
	add("pageSize", financialSnapshotPageSize)
	add("pageNumber", "1")
	return strings.Join(q, "&")
}

// FetchFinancialSnapshot fetches one month's market-wide ratio snapshot and
// returns it after both store guards. The payload is validated before anything
// is persisted: JSON parses, totalItems is consistent with len(data), and no
// ticker is duplicated. A month that is not published yet returns an empty
// snapshot with StatusEmpty — not an error. Row-level rejections are carried in
// Snapshot.Rejected, never written.
//
// ctx is accepted for interface uniformity with the usecase fetcher seam; the
// transport itself is not context-aware.
func (c *Client) FetchFinancialSnapshot(ctx context.Context, year, month int) (*financials.Snapshot, error) {
	if year < 1900 || year > 9999 {
		return nil, fmt.Errorf("financial snapshot: invalid year %d", year)
	}
	if month < 1 || month > 12 {
		return nil, fmt.Errorf("financial snapshot: invalid month %d", month)
	}

	path := financialSnapshotPath + "?" + financialSnapshotQuery(year, month)
	// An explicit same-site Referer makes the sidecar use its in-page fetch()
	// path rather than navigating to the URL and reading body.innerText, which
	// has returned truncated JSON bodies for this endpoint.
	headers := map[string]string{"Referer": c.config.BaseURL}
	resp, err := c.GetWithHeaders(path, headers)
	if err != nil {
		return nil, fmt.Errorf("financial snapshot: idx get: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("financial snapshot: read body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("financial snapshot: idx api error: status=%d body=%s", resp.StatusCode, truncate(string(body), 200))
	}

	var page financialSnapshotResponse
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("%w: parse response: %v (body=%s)", ErrMalformedSnapshot, err, truncate(string(body), 200))
	}

	// Empty is the normal "month not published yet" state: HTTP 200, data [],
	// totalItems 0, full column metadata.
	if page.Meta.TotalItems == 0 && len(page.Data) == 0 {
		return financials.ValidateSnapshot(nil, financials.FirstOfMonth(time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)), bodyHash(body)), nil
	}
	if page.Meta.TotalItems <= 0 {
		return nil, fmt.Errorf("%w: totalItems=%d with %d rows", ErrMalformedSnapshot, page.Meta.TotalItems, len(page.Data))
	}
	if len(page.Data) != page.Meta.TotalItems {
		return nil, fmt.Errorf("%w: len(data)=%d does not match totalItems=%d (truncated body?)",
			ErrMalformedSnapshot, len(page.Data), page.Meta.TotalItems)
	}

	rows := make([]financials.SnapshotRow, 0, len(page.Data))
	seen := make(map[string]struct{}, len(page.Data))
	for _, r := range page.Data {
		code := strings.ToUpper(strings.TrimSpace(r.Code))
		if code != "" {
			if _, dup := seen[code]; dup {
				return nil, fmt.Errorf("%w: duplicate ticker %q", ErrMalformedSnapshot, code)
			}
			seen[code] = struct{}{}
		}
		rows = append(rows, snapshotRowFromWire(r))
	}

	monthStart := financials.FirstOfMonth(time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC))
	return financials.ValidateSnapshot(rows, monthStart, bodyHash(body)), nil
}

// snapshotRowFromWire converts one wire row to the source-neutral shape.
// Billions become exact rupiah here, at the adapter boundary; the neutral
// package never sees a float billion.
func snapshotRowFromWire(r financialSnapshotRow) financials.SnapshotRow {
	return financials.SnapshotRow{
		Ticker:          strings.ToUpper(strings.TrimSpace(r.Code)),
		FsDate:          r.FsDate.Value,
		FiscalYearEnd:   r.FiscalYearEnd.Value,
		Assets:          financials.RupiahFromBillions(r.Assets),
		Liabilities:     financials.RupiahFromBillions(r.Liabilities),
		Equity:          financials.RupiahFromBillions(r.Equity),
		Revenue:         financials.RupiahFromBillions(r.Sales),
		EBT:             financials.RupiahFromBillions(r.EBT),
		NetProfit:       financials.RupiahFromBillions(r.ProfitPeriod),
		NetProfitParent: financials.RupiahFromBillions(r.ProfitAttrOwner),
		EPS:             r.EPS,
		BookValue:       r.BookValue,
		ROA:             r.ROA,
		ROE:             r.ROE,
		NPM:             r.NPM,
		DER:             r.DeRatio,
		Audit:           r.Audit.Value,
		Opinion:         r.Opini.Value,
	}
}

// bodyHash is the run ledger's payload provenance: the exact bytes fetched.
func bodyHash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
