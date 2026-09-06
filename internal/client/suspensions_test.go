package client

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestFetchSuspensions_parsesPage verifies a GetSuspension page parses into
// per-ticker suspension events with the right resolved URL and Referer.
func TestFetchSuspensions_parsesPage(t *testing.T) {
	body := `{"SearchCriteria":{"indexfrom":1,"pagesize":9999},"ResultCount":2,"Results":[
		{"Judul":"Penghentian Sementara Perdagangan (Suspend) Saham PT Charnic Capital Tbk. (NICK)","Kode":"NICK","Date":"2026-09-03T01:30:00","Info_Type":"SPT","Data_Download":"/Portals/0/x.pdf"},
		{"Judul":"Pembukaan Kembali Perdagangan (Unsuspend) Saham PT MSIG Life Insurance Indonesia Tbk. (LIFE)","Kode":"LIFE","Date":"2026-09-02T01:00:00","Info_Type":"UPT","Data_Download":"/Portals/0/y.pdf"}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	rows, err := c.FetchSuspensions(context.Background(), from, to)
	if err != nil {
		t.Fatalf("FetchSuspensions: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 events, got %d", len(rows))
	}
	if rows[0].Ticker != "NICK" || rows[0].Type != "SPT" {
		t.Errorf("rows[0] = %+v, want NICK/SPT", rows[0])
	}
	if rows[0].EventDate.Year() != 2026 || rows[0].EventDate.Day() != 3 {
		t.Errorf("rows[0] event date = %v, want 2026-09-03", rows[0].EventDate)
	}
	if !strings.Contains(rows[0].Reason, "Charnic Capital") {
		t.Errorf("rows[0].reason = %q, want title", rows[0].Reason)
	}
	if rows[1].Type != "UPT" {
		t.Errorf("rows[1].type = %q, want UPT", rows[1].Type)
	}

	if !strings.Contains(stub.gotURL, "GetSuspension") {
		t.Errorf("expected GetSuspension in URL, got %q", stub.gotURL)
	}
	if !strings.Contains(stub.gotURL, "dateFrom=20260901") || !strings.Contains(stub.gotURL, "dateTo=20260903") {
		t.Errorf("expected date params in URL, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != suspensionsReferer {
		t.Errorf("expected referer %q, got %q", suspensionsReferer, stub.gotHeaders["Referer"])
	}
	if stub.fetchCalls != 1 {
		t.Errorf("expected 1 fetch call, got %d", stub.fetchCalls)
	}
}

// TestFetchSuspensions_skipsAggregateAndUnparseable verifies the ">1 Kode"
// aggregate rows, rows with an empty Kode, and rows with an unparseable Date
// are skipped, not errors.
func TestFetchSuspensions_skipsAggregateAndUnparseable(t *testing.T) {
	body := `{"ResultCount":3,"Results":[
		{"Judul":"Pembukaan Penghentian Sementara Perdagangan Efek (>1 Kode)","Kode":">1 Kode","Date":"2026-09-02T09:18:42","Info_Type":"UPT","Data_Download":"/x.pdf"},
		{"Judul":"Penghentian Sementara Perdagangan (Suspend) Saham PT Steady Safe Tbk (SAFE)","Kode":"","Date":"2026-09-01T00:00:00","Info_Type":"SPT","Data_Download":"/y.pdf"},
		{"Judul":"Penghentian Sementara Perdagangan (Suspend) Saham PT Guna Timur Raya Tbk. (TRUK)","Kode":"TRUK","Date":"not-a-date","Info_Type":"SPT","Data_Download":"/z.pdf"},
		{"Judul":"Penghentian Sementara Perdagangan (Suspend) Saham PT Asri Karya Lestari Tbk. (ASLI)","Kode":"ASLI","Date":"2026-09-03T01:00:00","Info_Type":"SPT","Data_Download":"/w.pdf"}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	rows, err := c.FetchSuspensions(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FetchSuspensions: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 valid event, got %d", len(rows))
	}
	if rows[0].Ticker != "ASLI" || rows[0].Type != "SPT" {
		t.Errorf("unexpected event: %+v", rows[0])
	}
}

// TestFetchUma_parsesPage verifies a GetUma page parses into UMA events with
// the expected fields (ticker, date, announcement number, reason).
func TestFetchUma_parsesPage(t *testing.T) {
	body := `{"SearchCriteria":{"indexfrom":1,"pagesize":9999},"ResultCount":2,"Results":[
		{"UMAID":"20260904001107","UMADate":"2026-09-03T00:00:00","AnnouncementNo":"Peng-UMA-00278/BEI.WAS/09-2026","CompanyID":"PTSP","CompanyName":"Pioneerindo Gourmet International Tbk.","Attachment":"/Portals/0/20260903-UMA_PTSP.pdf","Status":"A","Judul":"UMA atas Saham PT Pioneerindo Gourmet International Tbk. (PTSP)"},
		{"UMAID":"20260904000928","UMADate":"2026-09-02T00:00:00","AnnouncementNo":"Peng-UMA-00277/BEI.WAS/09-2026","CompanyID":"GRPH","CompanyName":"Griptha Putra Persada Tbk.","Attachment":"/Portals/0/20260903-UMA_GRPH.pdf","Status":"A","Judul":"UMA atas Saham PT Griptha Putra Persada Tbk. (GRPH)"}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	rows, err := c.FetchUma(context.Background(), from, to)
	if err != nil {
		t.Fatalf("FetchUma: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 UMA events, got %d", len(rows))
	}
	if rows[0].Ticker != "PTSP" || rows[0].AnnouncementNo != "Peng-UMA-00278/BEI.WAS/09-2026" {
		t.Errorf("rows[0] = %+v, want PTSP/Peng-UMA-00278", rows[0])
	}
	if rows[0].EventDate.Year() != 2026 || rows[0].EventDate.Month() != 9 || rows[0].EventDate.Day() != 3 {
		t.Errorf("rows[0] event date = %v, want 2026-09-03", rows[0].EventDate)
	}
	if !strings.Contains(rows[0].Reason, "Pioneerindo") {
		t.Errorf("rows[0].reason = %q, want title", rows[0].Reason)
	}
	if rows[1].Ticker != "GRPH" {
		t.Errorf("rows[1].ticker = %q, want GRPH", rows[1].Ticker)
	}

	if !strings.Contains(stub.gotURL, "GetUma") {
		t.Errorf("expected GetUma in URL, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != suspensionsReferer {
		t.Errorf("expected referer %q, got %q", suspensionsReferer, stub.gotHeaders["Referer"])
	}
	if stub.fetchCalls != 1 {
		t.Errorf("expected 1 fetch call, got %d", stub.fetchCalls)
	}
}

// TestFetchSuspensions_httpError verifies a 4xx/5xx status surfaces as an api
// error, not a parse error.
func TestFetchSuspensions_httpError(t *testing.T) {
	stub := &stubBrowser{body: []byte("busted"), status: http.StatusInternalServerError}
	c := newTestClient(stub)

	_, err := c.FetchSuspensions(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected error on 500")
	}
	if !strings.Contains(err.Error(), "idx api error: status=500") {
		t.Errorf("expected api error line, got %v", err)
	}
}

// TestFetchUma_transportError verifies a browser fetcher failure surfaces
// wrapped through the client's idx get path.
func TestFetchUma_transportError(t *testing.T) {
	stub := &stubBrowser{err: errBrowserFetch}
	c := newTestClient(stub)

	_, err := c.FetchUma(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected transport error")
	}
	if !strings.Contains(err.Error(), "idx get:") {
		t.Errorf("expected idx get wrap, got %v", err)
	}
}
