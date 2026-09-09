package client

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// TestFetchCompanyProfiles_parsesWrapper verifies a single GetCompanyProfiles
// page parses into typed profiles with the right resolved URL and Referer.
func TestFetchCompanyProfiles_parsesWrapper(t *testing.T) {
	body := `{"draw":0,"recordsTotal":2,"recordsFiltered":2,"data":[
		{"KodeEmiten":"AADI","NamaEmiten":"PT Adaro Andalan Indonesia Tbk","PapanPencatatan":"Utama","TanggalPencatatan":"2024-12-05T00:00:00","Status":0},
		{"KodeEmiten":"BBRI","NamaEmiten":"PT Bank Rakyat Indonesia (Persero) Tbk","PapanPencatatan":"Utama","TanggalPencatatan":"2003-11-10T00:00:00","Status":0}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	profiles, err := c.FetchCompanyProfiles(context.Background())
	if err != nil {
		t.Fatalf("FetchCompanyProfiles: %v", err)
	}

	if len(profiles) != 2 {
		t.Fatalf("expected 2 profiles, got %d", len(profiles))
	}
	p := profiles[0]
	if p.Ticker != "AADI" || p.Name != "PT Adaro Andalan Indonesia Tbk" || p.ListingBoard != "Utama" {
		t.Errorf("unexpected first profile: %+v", p)
	}
	if p.ListingDate == nil || p.ListingDate.Format("2006-01-02") != "2024-12-05" {
		t.Errorf("expected listing date 2024-12-05, got %v", p.ListingDate)
	}
	if !p.Active {
		t.Errorf("Status 0 should map to active=true, got %+v", p)
	}
	if !strings.Contains(stub.gotURL, "emitenType=s") || !strings.Contains(stub.gotURL, "start=0") || !strings.Contains(stub.gotURL, "length=10") {
		t.Errorf("expected emitenType=s and start=0/length=10 in URL, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != companyProfilesReferer {
		t.Errorf("expected referer %q, got %q", companyProfilesReferer, stub.gotHeaders["Referer"])
	}
	if stub.fetchCalls != 1 {
		t.Errorf("expected 1 fetch call, got %d", stub.fetchCalls)
	}
}

// TestFetchCompanyProfiles_statusMapping verifies Status 0 → active and any
// non-zero Status → inactive.
func TestFetchCompanyProfiles_statusMapping(t *testing.T) {
	body := `{"recordsTotal":2,"data":[
		{"KodeEmiten":"ACTIVE","NamaEmiten":"Active Co","PapanPencatatan":"Utama","TanggalPencatatan":"2020-01-01T00:00:00","Status":0},
		{"KodeEmiten":"INACTV","NamaEmiten":"Inactive Co","PapanPencatatan":"Pengembangan","TanggalPencatatan":"2020-01-01T00:00:00","Status":1}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	profiles, err := c.FetchCompanyProfiles(context.Background())
	if err != nil {
		t.Fatalf("FetchCompanyProfiles: %v", err)
	}
	if len(profiles) != 2 {
		t.Fatalf("expected 2 profiles, got %d", len(profiles))
	}
	if !profiles[0].Active {
		t.Errorf("Status 0 should be active, got %+v", profiles[0])
	}
	if profiles[1].Active {
		t.Errorf("Status 1 should be inactive, got %+v", profiles[1])
	}
}

// TestFetchCompanyProfiles_skipsUnparseable verifies rows with an empty ticker
// are skipped, and an unparseable listing date yields a nil ListingDate while
// the row is kept.
func TestFetchCompanyProfiles_skipsUnparseable(t *testing.T) {
	body := `{"recordsTotal":3,"data":[
		{"KodeEmiten":"","NamaEmiten":"No Ticker","PapanPencatatan":"Utama","TanggalPencatatan":"2020-01-01T00:00:00","Status":0},
		{"KodeEmiten":"NODATE","NamaEmiten":"No Date Co","PapanPencatatan":"Utama","TanggalPencatatan":"not-a-date","Status":0},
		{"KodeEmiten":"OKAY","NamaEmiten":"Okay Co","PapanPencatatan":"Utama","TanggalPencatatan":"2020-01-01T00:00:00","Status":0}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	profiles, err := c.FetchCompanyProfiles(context.Background())
	if err != nil {
		t.Fatalf("FetchCompanyProfiles: %v", err)
	}
	if len(profiles) != 2 {
		t.Fatalf("expected 2 valid profiles, got %d", len(profiles))
	}
	if profiles[0].Ticker != "NODATE" || profiles[0].ListingDate != nil {
		t.Errorf("NODATE should be kept with nil date, got %+v", profiles[0])
	}
	if profiles[1].Ticker != "OKAY" || profiles[1].ListingDate == nil {
		t.Errorf("OKAY should have a date, got %+v", profiles[1])
	}
}

// TestFetchCompanyProfiles_paginates verifies the DataTables start/length loop:
// two pages, start advances by the page length, all rows collected.
func TestFetchCompanyProfiles_paginates(t *testing.T) {
	page0 := `{"recordsTotal":2,"data":[
		{"KodeEmiten":"AAA1","NamaEmiten":"First Co","PapanPencatatan":"Utama","TanggalPencatatan":"2020-01-01T00:00:00","Status":0}
	]}`
	page1 := `{"recordsTotal":2,"data":[
		{"KodeEmiten":"BBB2","NamaEmiten":"Second Co","PapanPencatatan":"Pengembangan","TanggalPencatatan":"2020-02-01T00:00:00","Status":0}
	]}`
	stub := &pageStub{bodies: [][]byte{[]byte(page0), []byte(page1)}, status: http.StatusOK}
	c := newTestClient(stub)

	profiles, err := c.FetchCompanyProfiles(context.Background())
	if err != nil {
		t.Fatalf("FetchCompanyProfiles: %v", err)
	}
	if len(profiles) != 2 {
		t.Fatalf("expected 2 profiles across pages, got %d", len(profiles))
	}
	if len(stub.gotURLs) != 2 {
		t.Fatalf("expected 2 fetch calls, got %d", len(stub.gotURLs))
	}
	if !strings.Contains(stub.gotURLs[0], "start=0") {
		t.Errorf("first call should start at 0, got %q", stub.gotURLs[0])
	}
	if !strings.Contains(stub.gotURLs[1], "start=10") {
		t.Errorf("second call should advance start by 10, got %q", stub.gotURLs[1])
	}
	if profiles[0].Ticker != "AAA1" || profiles[1].Ticker != "BBB2" {
		t.Errorf("expected page-ordered profiles, got %+v", profiles)
	}
}

// TestFetchCompanyProfiles_httpError verifies a 4xx/5xx status surfaces as an
// api error, not a parse error.
func TestFetchCompanyProfiles_httpError(t *testing.T) {
	stub := &stubBrowser{body: []byte("busted"), status: http.StatusInternalServerError}
	c := newTestClient(stub)

	_, err := c.FetchCompanyProfiles(context.Background())
	if err == nil {
		t.Fatal("expected error on 500")
	}
	if !strings.Contains(err.Error(), "idx api error: status=500") {
		t.Errorf("expected api error line, got %v", err)
	}
}

// TestFetchCompanyProfiles_transportError verifies a browser fetcher failure
// surfaces wrapped through the client's idx get path.
func TestFetchCompanyProfiles_transportError(t *testing.T) {
	stub := &stubBrowser{err: errBrowserFetch}
	c := newTestClient(stub)

	_, err := c.FetchCompanyProfiles(context.Background())
	if err == nil {
		t.Fatal("expected transport error")
	}
	if !strings.Contains(err.Error(), "idx get:") {
		t.Errorf("expected idx get wrap, got %v", err)
	}
}
