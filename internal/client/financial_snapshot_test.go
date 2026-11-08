package client

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

const snapshotFixture = `{
  "TableName": "",
  "data": [
    {
      "code": "AADI", "fsDate": "2026-09-30", "fiscalYearEnd": "Dec",
      "assets": 113148.13, "liabilities": 42728.47, "equity": 70419.66,
      "sales": 45581.52, "ebt": 11896.82, "profitPeriod": 9386.81,
      "profitAttrOwner": 8479.92, "eps": 1833.76, "audit": "LR", "opini": "-",
      "bookValue": 9043.36, "per": 5.97, "priceBV": 1.21, "deRatio": 0.61,
      "roa": 12.62, "roe": 20.2774, "npm": 31.327
    },
    {
      "code": "CANI", "fsDate": "2026-06-30", "fiscalYearEnd": "Jun",
      "assets": 100.0, "sales": 50.5, "profitPeriod": 2.0,
      "profitAttrOwner": 1.5, "audit": 0, "opini": "Wajar Dengan Pengecualian / Qualified"
    }
  ],
  "footer": [],
  "notes": null,
  "columns": [],
  "FlattenColumns": [],
  "NameRow": null,
  "meta": {"search":"","orderBy":"","pageNumber":1,"pageSize":100000,"s":{"columns":[],"terms":""},"totalItems":2}
}`

func TestFetchFinancialSnapshot_ParsesAndValidates(t *testing.T) {
	stub := &stubBrowser{body: []byte(snapshotFixture), status: http.StatusOK}
	c := newTestClient(stub)

	snap, err := c.FetchFinancialSnapshot(context.Background(), 2026, 10)
	if err != nil {
		t.Fatalf("FetchFinancialSnapshot: %v", err)
	}
	if snap.Status != financials.StatusOK {
		t.Fatalf("status = %q, want ok", snap.Status)
	}
	if snap.RowCount != 2 || snap.DistinctPeriods != 2 {
		t.Errorf("rows/periods = %d/%d, want 2/2", snap.RowCount, snap.DistinctPeriods)
	}
	if snap.QuarterEndShare == nil || *snap.QuarterEndShare != 1 {
		t.Errorf("quarter-end share = %v, want 1", snap.QuarterEndShare)
	}
	if len(snap.Statements) != 2 {
		t.Fatalf("statements = %d, want 2", len(snap.Statements))
	}

	aadi := snap.Statements[0]
	if aadi.Ticker != "AADI" || aadi.DurationMonths != 9 {
		t.Errorf("first statement = %s/%dM, want AADI/9M", aadi.Ticker, aadi.DurationMonths)
	}
	if aadi.Assets == nil || *aadi.Assets != 113148130000000 {
		t.Errorf("assets = %v, want 113148130000000 rupiah", aadi.Assets)
	}
	if aadi.AuditStatus == nil || *aadi.AuditStatus != "LR" {
		t.Errorf("audit = %v, want LR", aadi.AuditStatus)
	}
	if aadi.Opinion != nil {
		t.Errorf("opinion = %v, want nil for '-'", *aadi.Opinion)
	}

	cani := snap.Statements[1]
	if cani.DurationMonths != 12 {
		t.Errorf("CANI duration = %d, want 12 (Jun fiscal year end)", cani.DurationMonths)
	}
	if cani.AuditStatus != nil {
		t.Errorf("CANI audit = %v, want nil for numeric 0", *cani.AuditStatus)
	}
	if cani.Opinion == nil || *cani.Opinion != "WDP" {
		t.Errorf("CANI opinion = %v, want WDP", cani.Opinion)
	}

	// Request contract: explicit year/month, whole-market page size, same-site
	// referer, and the JSON (not binary) transport.
	for _, want := range []string{
		"urlName=" + financialSnapshotDataset,
		"periodYear=2026",
		"periodMonth=10",
		"pageSize=100000",
		"pageNumber=1",
	} {
		if !strings.Contains(stub.gotURL, want) {
			t.Errorf("URL %q missing %q", stub.gotURL, want)
		}
	}
	if stub.gotHeaders["Referer"] != "https://idx.example" {
		t.Errorf("referer = %q, want the IDX base URL", stub.gotHeaders["Referer"])
	}
	if stub.fetchCalls != 1 || stub.fetchBinaryCalls != 0 {
		t.Errorf("transport calls = fetch:%d binary:%d, want 1/0", stub.fetchCalls, stub.fetchBinaryCalls)
	}
}

func TestFetchFinancialSnapshot_EmptyIsNotAnError(t *testing.T) {
	body := `{"data":[],"meta":{"pageNumber":1,"pageSize":100000,"totalItems":0},"columns":[]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	snap, err := c.FetchFinancialSnapshot(context.Background(), 2026, 11)
	if err != nil {
		t.Fatalf("FetchFinancialSnapshot: %v", err)
	}
	if snap.Status != financials.StatusEmpty {
		t.Errorf("status = %q, want empty", snap.Status)
	}
	if snap.Month.Format("2006-01") != "2026-11" {
		t.Errorf("month = %s, want 2026-11", snap.Month.Format("2006-01"))
	}
	if len(snap.Statements) != 0 {
		t.Errorf("statements = %d, want 0", len(snap.Statements))
	}
}

func TestFetchFinancialSnapshot_RejectsMalformedPayloads(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "truncated body (len(data) != totalItems)",
			body: `{"data":[{"code":"AADI","fsDate":"2026-09-30","fiscalYearEnd":"Dec"}],"meta":{"totalItems":2}}`,
		},
		{
			name: "duplicate ticker",
			body: `{"data":[{"code":"AADI","fsDate":"2026-09-30","fiscalYearEnd":"Dec"},{"code":"aadi","fsDate":"2026-09-30","fiscalYearEnd":"Dec"}],"meta":{"totalItems":2}}`,
		},
		{
			name: "invalid JSON",
			body: `{"data":[{"code":"AADI"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(&stubBrowser{body: []byte(tc.body), status: http.StatusOK})
			if _, err := c.FetchFinancialSnapshot(context.Background(), 2026, 10); !errors.Is(err, ErrMalformedSnapshot) {
				t.Errorf("err = %v, want ErrMalformedSnapshot", err)
			}
		})
	}
}

func TestFetchFinancialSnapshot_RejectsInvalidMonth(t *testing.T) {
	c := newTestClient(&stubBrowser{body: []byte(`{}`), status: http.StatusOK})
	if _, err := c.FetchFinancialSnapshot(context.Background(), 2026, 13); err == nil {
		t.Fatal("expected an error for month 13")
	}
}
