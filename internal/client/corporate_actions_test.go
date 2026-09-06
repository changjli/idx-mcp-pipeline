package client

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// pageStub is a browserFetcher stub that serves one body per Fetch call, in
// order — the corporate-actions pagination test needs per-page responses.
type pageStub struct {
	bodies    [][]byte
	status    int
	gotURLs   []string
	gotHeader map[string]string
}

func (s *pageStub) Fetch(url string, headers map[string]string) ([]byte, int, error) {
	s.gotURLs = append(s.gotURLs, url)
	s.gotHeader = headers
	if len(s.bodies) == 0 {
		return nil, s.status, nil
	}
	body := s.bodies[0]
	s.bodies = s.bodies[1:]
	return body, s.status, nil
}

func (s *pageStub) FetchBinary(url string, headers map[string]string) ([]byte, int, error) {
	return nil, s.status, nil
}

func (s *pageStub) Close() {}

// TestFetchCorporateActions_parsesWrapper verifies a single GetIssuedHistory
// page parses into typed events with the right resolved URL and Referer.
func TestFetchCorporateActions_parsesWrapper(t *testing.T) {
	body := `{"draw":0,"recordsTotal":2,"recordsFiltered":2,"data":[
		{"id":82998,"KodeEmiten":"PJHB","TanggalPencatatan":"2026-09-04T00:00:00","JenisTindakan":"Waran","JumlahSaham":11661.0,"JumlahSahamSetelahTindakan":1920917693.0},
		{"id":82999,"KodeEmiten":"KOCI","TanggalPencatatan":"2026-09-04T00:00:00","JenisTindakan":"Obligasi Wajib Konversi","JumlahSaham":0.0,"JumlahSahamSetelahTindakan":0.0}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	actions, err := c.FetchCorporateActions(context.Background(), from, to)
	if err != nil {
		t.Fatalf("FetchCorporateActions: %v", err)
	}

	if len(actions) != 2 {
		t.Fatalf("expected 2 events, got %d", len(actions))
	}
	a := actions[0]
	if a.ID != 82998 || a.Ticker != "PJHB" || a.Type != "Waran" {
		t.Errorf("unexpected first event: %+v", a)
	}
	if a.EventDate.Year() != 2026 || a.EventDate.Month() != 9 || a.EventDate.Day() != 4 {
		t.Errorf("expected 2026-09-04, got %v", a.EventDate)
	}
	if a.Detail.JumlahSaham != 11661.0 || a.Detail.JumlahSahamSetelahTindakan != 1920917693.0 {
		t.Errorf("unexpected detail: %+v", a.Detail)
	}
	if !strings.Contains(stub.gotURL, "dateFrom=20260901") || !strings.Contains(stub.gotURL, "dateTo=20260906") {
		t.Errorf("expected date params in URL, got %q", stub.gotURL)
	}
	if !strings.Contains(stub.gotURL, "start=0") || !strings.Contains(stub.gotURL, "caType=") {
		t.Errorf("expected empty caType and start=0 in URL, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != corporateActionsReferer {
		t.Errorf("expected referer %q, got %q", corporateActionsReferer, stub.gotHeaders["Referer"])
	}
	if stub.fetchCalls != 1 {
		t.Errorf("expected 1 fetch call, got %d", stub.fetchCalls)
	}
}

// TestFetchCorporateActions_zeroNumbers verifies zero amounts are kept — a real
// value (Obligasi Wajib Konversi), never dropped as empty.
func TestFetchCorporateActions_zeroNumbers(t *testing.T) {
	body := `{"recordsTotal":1,"data":[
		{"id":83000,"KodeEmiten":"PACK","TanggalPencatatan":"2026-09-04T00:00:00","JenisTindakan":"Obligasi Wajib Konversi","JumlahSaham":0.0,"JumlahSahamSetelahTindakan":0.0}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	actions, err := c.FetchCorporateActions(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FetchCorporateActions: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected 1 event (zero amounts kept), got %d", len(actions))
	}
	if actions[0].Detail.JumlahSaham != 0.0 || actions[0].Detail.JumlahSahamSetelahTindakan != 0.0 {
		t.Errorf("expected zero amounts preserved, got %+v", actions[0].Detail)
	}
}

// TestFetchCorporateActions_skipsUnparseable verifies rows with an empty ticker
// or an unparseable event date are skipped, not errors.
func TestFetchCorporateActions_skipsUnparseable(t *testing.T) {
	body := `{"recordsTotal":3,"data":[
		{"id":1,"KodeEmiten":"","TanggalPencatatan":"2026-09-04T00:00:00","JenisTindakan":"Waran"},
		{"id":2,"KodeEmiten":"TEST","TanggalPencatatan":"not-a-date","JenisTindakan":"Waran"},
		{"id":3,"KodeEmiten":"TEST","TanggalPencatatan":"2026-09-04T00:00:00","JenisTindakan":"Stock Split","JumlahSaham":5.0,"JumlahSahamSetelahTindakan":10.0}
	]}`
	stub := &stubBrowser{body: []byte(body), status: http.StatusOK}
	c := newTestClient(stub)

	actions, err := c.FetchCorporateActions(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FetchCorporateActions: %v", err)
	}
	if len(actions) != 1 {
		t.Fatalf("expected 1 valid event, got %d", len(actions))
	}
	if actions[0].ID != 3 || actions[0].Type != "Stock Split" {
		t.Errorf("unexpected event: %+v", actions[0])
	}
}

// TestFetchCorporateActions_paginates verifies the DataTables start/length loop:
// two pages, start advances by the length, all rows collected.
func TestFetchCorporateActions_paginates(t *testing.T) {
	page0 := `{"recordsTotal":2,"data":[
		{"id":1,"KodeEmiten":"AAA1","TanggalPencatatan":"2026-09-04T00:00:00","JenisTindakan":"Waran","JumlahSaham":1.0}
	]}`
	page1 := `{"recordsTotal":2,"data":[
		{"id":2,"KodeEmiten":"BBB2","TanggalPencatatan":"2026-09-05T00:00:00","JenisTindakan":"Stock Split","JumlahSaham":2.0}
	]}`
	stub := &pageStub{bodies: [][]byte{[]byte(page0), []byte(page1)}, status: http.StatusOK}
	c := newTestClient(stub)

	actions, err := c.FetchCorporateActions(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FetchCorporateActions: %v", err)
	}
	if len(actions) != 2 {
		t.Fatalf("expected 2 events across pages, got %d", len(actions))
	}
	if len(stub.gotURLs) != 2 {
		t.Fatalf("expected 2 fetch calls, got %d", len(stub.gotURLs))
	}
	if !strings.Contains(stub.gotURLs[0], "start=0") {
		t.Errorf("first call should start at 0, got %q", stub.gotURLs[0])
	}
	if !strings.Contains(stub.gotURLs[1], "start=9999") {
		t.Errorf("second call should advance start, got %q", stub.gotURLs[1])
	}
	if actions[0].Ticker != "AAA1" || actions[1].Ticker != "BBB2" {
		t.Errorf("expected page-ordered events, got %+v", actions)
	}
}

// TestFetchCorporateActions_httpError verifies a 4xx/5xx status surfaces as an
// api error, not a parse error.
func TestFetchCorporateActions_httpError(t *testing.T) {
	stub := &stubBrowser{body: []byte("busted"), status: http.StatusInternalServerError}
	c := newTestClient(stub)

	_, err := c.FetchCorporateActions(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected error on 500")
	}
	if !strings.Contains(err.Error(), "idx api error: status=500") {
		t.Errorf("expected api error line, got %v", err)
	}
}

// TestFetchCorporateActions_transportError verifies a browser fetcher failure
// surfaces wrapped through the client's idx get path.
func TestFetchCorporateActions_transportError(t *testing.T) {
	stub := &stubBrowser{err: errBrowserFetch}
	c := newTestClient(stub)

	_, err := c.FetchCorporateActions(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected transport error")
	}
	if !strings.Contains(err.Error(), "idx get:") {
		t.Errorf("expected idx get wrap, got %v", err)
	}
}
