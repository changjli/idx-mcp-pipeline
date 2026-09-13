package client

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// TestFetchIndexSummary_parsesWrapper verifies a single GetIndexSummary page
// parses into typed summaries with the right resolved URL and Referer.
func TestFetchIndexSummary_parsesWrapper(t *testing.T) {
	// Sample rows mirror the 2026-09-09 probe (findings-sector-index-membership
	// example 3): one full wire row plus a sector-index row.
	body := `{"draw":0,"recordsTotal":45,"recordsFiltered":45,"data":[
		{"No":1,"IndexSummaryID":195705,"Date":"2026-09-09T00:00:00","IndexCode":"COMPOSITE","Previous":6686.442,"Highest":6707.305,"Lowest":6642.153,"Close":6678.201,"NumberOfStock":918.0,"Change":-8.241,"Volume":32530207402.0,"Value":16134522240878.0,"Frequency":2147005.0,"MarketCapital":1.16749775143559E+16},
		{"No":2,"IndexSummaryID":195706,"Date":"2026-09-09T00:00:00","IndexCode":"IDXENERGY","Previous":2501.1,"Highest":2530.0,"Lowest":2488.8,"Close":2512.3,"NumberOfStock":90.0,"Change":11.2,"Volume":123456.0,"Value":987654321.0,"Frequency":12345.0,"MarketCapital":12345678901234.0}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	rows, err := c.FetchIndexSummary(context.Background())
	if err != nil {
		t.Fatalf("FetchIndexSummary: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 summaries, got %d", len(rows))
	}

	comp := rows[0]
	if comp.IndexCode != "COMPOSITE" {
		t.Errorf("expected COMPOSITE, got %q", comp.IndexCode)
	}
	if comp.Date.Format("2006-01-02") != "2026-09-09" {
		t.Errorf("expected 2026-09-09, got %v", comp.Date)
	}
	if comp.Previous != 6686.442 || comp.Highest != 6707.305 || comp.Lowest != 6642.153 || comp.Close != 6678.201 {
		t.Errorf("unexpected OHLC: %+v", comp)
	}
	if comp.NumberOfStock != 918.0 || comp.Change != -8.241 {
		t.Errorf("unexpected stock count/change: %+v", comp)
	}
	if comp.Volume != 32530207402.0 || comp.Value != 16134522240878.0 || comp.Frequency != 2147005.0 {
		t.Errorf("unexpected aggregates: %+v", comp)
	}
	if comp.MarketCap != 1.16749775143559e+16 {
		t.Errorf("unexpected market cap: %v", comp.MarketCap)
	}

	// Sector index present and typed the same way (Highest → High field).
	if rows[1].IndexCode != "IDXENERGY" || rows[1].Highest != 2530.0 {
		t.Errorf("unexpected sector row: %+v", rows[1])
	}

	if !strings.Contains(stub.gotURL, "length=9999") || !strings.Contains(stub.gotURL, "start=0") {
		t.Errorf("expected length=9999&start=0 in URL, got %q", stub.gotURL)
	}
	if !strings.Contains(stub.gotURL, "/primary/TradingSummary/GetIndexSummary") {
		t.Errorf("expected GetIndexSummary path, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != indexSummaryReferer {
		t.Errorf("expected referer %q, got %q", indexSummaryReferer, stub.gotHeaders["Referer"])
	}
	if stub.fetchCalls != 1 {
		t.Errorf("expected 1 fetch call, got %d", stub.fetchCalls)
	}
}

// TestFetchIndexSummary_SkipsBadRows verifies rows without an index code or
// with an unparseable date are dropped, not fatal.
func TestFetchIndexSummary_SkipsBadRows(t *testing.T) {
	body := `{"draw":0,"recordsTotal":3,"recordsFiltered":3,"data":[
		{"No":1,"IndexSummaryID":1,"Date":"2026-09-09T00:00:00","IndexCode":"COMPOSITE","Previous":1.0,"Highest":2.0,"Lowest":0.5,"Close":1.5,"NumberOfStock":918.0,"Change":0.5,"Volume":1.0,"Value":2.0,"Frequency":3.0,"MarketCapital":4.0},
		{"No":2,"IndexSummaryID":2,"Date":"2026-09-09T00:00:00","IndexCode":"","Previous":1.0,"Highest":2.0,"Lowest":1.0,"Close":1.5,"NumberOfStock":1.0,"Change":0.5,"Volume":1.0,"Value":2.0,"Frequency":3.0,"MarketCapital":4.0},
		{"No":3,"IndexSummaryID":3,"Date":"not-a-date","IndexCode":"LQ45","Previous":1.0,"Highest":2.0,"Lowest":1.0,"Close":1.5,"NumberOfStock":45.0,"Change":0.5,"Volume":1.0,"Value":2.0,"Frequency":3.0,"MarketCapital":4.0}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	rows, err := c.FetchIndexSummary(context.Background())
	if err != nil {
		t.Fatalf("FetchIndexSummary: %v", err)
	}
	if len(rows) != 1 || rows[0].IndexCode != "COMPOSITE" {
		t.Fatalf("expected only the COMPOSITE row, got %+v", rows)
	}
}

// TestFetchIndexSummary_TruncatedFetchLogs verifies a short page warning path:
// rows still return, the fetch is not treated as an error.
func TestFetchIndexSummary_TruncatedFetchLogs(t *testing.T) {
	body := `{"draw":0,"recordsTotal":45,"recordsFiltered":45,"data":[
		{"No":1,"IndexSummaryID":1,"Date":"2026-09-09T00:00:00","IndexCode":"COMPOSITE","Previous":1.0,"Highest":2.0,"Lowest":0.5,"Close":1.5,"NumberOfStock":918.0,"Change":0.5,"Volume":1.0,"Value":2.0,"Frequency":3.0,"MarketCapital":4.0}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)
	rows, err := c.FetchIndexSummary(context.Background())
	if err != nil {
		t.Fatalf("FetchIndexSummary: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}
