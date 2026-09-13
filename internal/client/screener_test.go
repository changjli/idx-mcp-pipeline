package client

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// TestFetchScreener_parsesWrapper verifies a single stock-screener/get response
// parses into typed rows with the right resolved URL and Referer.
func TestFetchScreener_parsesWrapper(t *testing.T) {
	body := `{"results":[
		{"companyName":"Adaro Andalan Indonesia Tbk.","stockCode":"AADI","subIndustryCode":"A121","sector":"Energy","subSector":"Oil, Gas & Coal","industry":"Coal","subIndustry":"Coal Production","indexCode":"COMPOSITE, IDX30, LQ45"},
		{"companyName":"Bank Rakyat Indonesia (Persero) Tbk.","stockCode":"BBRI","subIndustryCode":"G111","sector":"Financials","subSector":"Banks","industry":"Banks","subIndustry":"Banks","indexCode":"COMPOSITE, LQ45"}
	],"searchCriteria":{},"resultCount":2}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	rows, err := c.FetchScreener(context.Background())
	if err != nil {
		t.Fatalf("FetchScreener: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	r := rows[0]
	if r.StockCode != "AADI" || r.Sector != "Energy" || r.SubSector != "Oil, Gas & Coal" {
		t.Errorf("unexpected first row: %+v", r)
	}
	if r.Industry == nil || *r.Industry != "Coal" {
		t.Errorf("industry = %v, want Coal", r.Industry)
	}
	if r.SubIndustry == nil || *r.SubIndustry != "Coal Production" {
		t.Errorf("subIndustry = %v, want Coal Production", r.SubIndustry)
	}
	if r.SubIndustryCode == nil || *r.SubIndustryCode != "A121" {
		t.Errorf("subIndustryCode = %v, want A121", r.SubIndustryCode)
	}
	if r.IndexCode == nil || *r.IndexCode != "COMPOSITE, IDX30, LQ45" {
		t.Errorf("indexCode = %v, want COMPOSITE, IDX30, LQ45", r.IndexCode)
	}
	if !strings.Contains(stub.gotURL, "stock-screener/get") {
		t.Errorf("expected stock-screener/get in URL, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != screenerReferer {
		t.Errorf("expected referer %q, got %q", screenerReferer, stub.gotHeaders["Referer"])
	}
	if stub.fetchCalls != 1 {
		t.Errorf("expected 1 fetch call, got %d", stub.fetchCalls)
	}
}

// TestFetchScreener_nullableFields verifies null industry/subIndustry/
// subIndustryCode/indexCode decode to nil pointers (the dirty rows the ingest
// cleanup handles: KETR/VICI sector rows, 8 null-indexCode rows).
func TestFetchScreener_nullableFields(t *testing.T) {
	body := `{"results":[
		{"companyName":"KETR","stockCode":"KETR","subIndustryCode":null,"sector":"No Sector","subSector":"No Subsector","industry":null,"subIndustry":null,"indexCode":"COMPOSITE, IDXINFRA"},
		{"companyName":"FIMP","stockCode":"FIMP","subIndustryCode":"A121","sector":"Energy","subSector":"Oil, Gas & Coal","industry":"Coal","subIndustry":"Coal Production","indexCode":null}
	],"resultCount":2}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	rows, err := c.FetchScreener(context.Background())
	if err != nil {
		t.Fatalf("FetchScreener: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Industry != nil || rows[0].SubIndustry != nil || rows[0].SubIndustryCode != nil {
		t.Errorf("KETR nullable fields = %+v, want nil", rows[0])
	}
	if rows[0].Sector != "No Sector" {
		t.Errorf("KETR sector = %q, want raw No Sector (cleanup is ingest-side)", rows[0].Sector)
	}
	if rows[1].IndexCode != nil {
		t.Errorf("FIMP indexCode = %v, want nil", rows[1].IndexCode)
	}
}

// TestFetchScreener_skipsEmptyTicker verifies rows without a stockCode are
// dropped.
func TestFetchScreener_skipsEmptyTicker(t *testing.T) {
	body := `{"results":[
		{"companyName":"No Code","stockCode":"","sector":"Energy","subSector":"Oil, Gas & Coal","industry":"Coal","subIndustry":"Coal Production","indexCode":"LQ45"},
		{"companyName":"Okay Co","stockCode":"OKAY","sector":"Energy","subSector":"Oil, Gas & Coal","industry":"Coal","subIndustry":"Coal Production","indexCode":"LQ45"}
	],"resultCount":2}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	rows, err := c.FetchScreener(context.Background())
	if err != nil {
		t.Fatalf("FetchScreener: %v", err)
	}
	if len(rows) != 1 || rows[0].StockCode != "OKAY" {
		t.Fatalf("expected 1 row (OKAY), got %+v", rows)
	}
}

// TestFetchScreener_httpError verifies a 4xx/5xx status surfaces as an api
// error, not a parse error.
func TestFetchScreener_httpError(t *testing.T) {
	stub := &stubBrowser{body: []byte("busted"), status: http.StatusInternalServerError}
	c := newTestClient(stub)

	_, err := c.FetchScreener(context.Background())
	if err == nil {
		t.Fatal("expected error on 500")
	}
	if !strings.Contains(err.Error(), "idx api error: status=500") {
		t.Errorf("expected api error line, got %v", err)
	}
}

// TestFetchScreener_transportError verifies a browser fetcher failure surfaces
// wrapped through the client's idx get path.
func TestFetchScreener_transportError(t *testing.T) {
	stub := &stubBrowser{err: errBrowserFetch}
	c := newTestClient(stub)

	_, err := c.FetchScreener(context.Background())
	if err == nil {
		t.Fatal("expected transport error")
	}
	if !strings.Contains(err.Error(), "idx get:") {
		t.Errorf("expected idx get wrap, got %v", err)
	}
}
