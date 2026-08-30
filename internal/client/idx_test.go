package client

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

// stubBrowser is a hermetic browserFetcher for the IDX client's browser modes.
type stubBrowser struct {
	body             []byte
	status           int
	err              error
	gotURL           string
	gotHeaders       map[string]string
	fetchCalls       int
	fetchBinaryCalls int
}

func (s *stubBrowser) Fetch(url string, headers map[string]string) ([]byte, int, error) {
	s.fetchCalls++
	s.gotURL = url
	s.gotHeaders = headers
	return s.body, s.status, s.err
}

func (s *stubBrowser) FetchBinary(url string, headers map[string]string) ([]byte, int, error) {
	s.fetchBinaryCalls++
	s.gotURL = url
	s.gotHeaders = headers
	return s.body, s.status, s.err
}

func (s *stubBrowser) Close() {}

// newTestClient builds a Client wired to a stub browser (browser mode is the
// only production transport).
func newTestClient(stub *stubBrowser) *Client {
	return &Client{
		config:  Config{BaseURL: "https://idx.example"},
		fetcher: &browserFetcherAdapter{browser: stub},
		browser: stub,
		log:     logrus.New(),
	}
}

// TestClient_GetWithHeaders_BrowserMode verifies GetWithHeaders delegates to
// the browser path and surfaces the extracted JSON as the response body with
// the expected status.
func TestClient_GetWithHeaders_BrowserMode(t *testing.T) {
	stub := &stubBrowser{
		body:   []byte(`{"data":[{"StockCode":"BBCA"}]}`),
		status: http.StatusOK,
	}
	c := newTestClient(stub)

	headers := map[string]string{"Referer": "https://idx.example/en/market-data/"}
	resp, err := c.GetWithHeaders("/primary/TradingSummary/GetStockSummary", headers)
	if err != nil {
		t.Fatalf("GetWithHeaders: %v", err)
	}
	defer resp.Body.Close()

	if stub.gotURL != "https://idx.example/primary/TradingSummary/GetStockSummary" {
		t.Errorf("expected resolved URL, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != headers["Referer"] {
		t.Errorf("expected Referer passthrough, got %v", stub.gotHeaders)
	}
	if stub.fetchCalls != 1 {
		t.Errorf("expected 1 Fetch call, got %d", stub.fetchCalls)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != string(stub.body) {
		t.Errorf("expected body %q, got %q", stub.body, body)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

// TestClient_GetWithHeaders_BrowserModeError verifies a browser fetcher error
// surfaces wrapped and no synthetic response is returned.
func TestClient_GetWithHeaders_BrowserModeError(t *testing.T) {
	stub := &stubBrowser{err: errBrowserFetch}
	c := newTestClient(stub)
	if _, err := c.GetWithHeaders("/x", nil); err == nil {
		t.Fatal("expected error from browser fetcher")
	}
}

var errBrowserFetch = fmt.Errorf("all proxies exhausted")

// TestClient_GetStream_RoutesBrowserInBrowserMode verifies GetStream delegates
// to the browser's binary transport (FetchBinary), defaulting the Referer to
// the IDX base URL when the caller sends none (the sidecar loads it first to
// clear Cloudflare) and forwarding any extra headers (e.g. the Range size
// probe) unchanged.
func TestClient_GetStream_RoutesBrowserInBrowserMode(t *testing.T) {
	pdf := []byte("%PDF-1.6 fake disclosure bytes")
	stub := &stubBrowser{body: pdf, status: http.StatusOK}
	c := newTestClient(stub)

	resp, err := c.GetStream("/StaticData/x.pdf", map[string]string{"Range": "bytes=0-0"})
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	defer resp.Body.Close()

	if stub.gotURL != "https://idx.example/StaticData/x.pdf" {
		t.Errorf("expected resolved PDF URL, got %q", stub.gotURL)
	}
	if stub.gotHeaders["Referer"] != "https://idx.example" {
		t.Errorf("expected defaulted Referer %q, got %q", "https://idx.example", stub.gotHeaders["Referer"])
	}
	if stub.gotHeaders["Range"] != "bytes=0-0" {
		t.Errorf("expected Range probe forwarded, got %q", stub.gotHeaders["Range"])
	}
	if stub.fetchBinaryCalls != 1 {
		t.Errorf("expected 1 FetchBinary call, got %d", stub.fetchBinaryCalls)
	}
	if stub.fetchCalls != 0 {
		t.Errorf("GetStream must not use Fetch, got %d calls", stub.fetchCalls)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != string(pdf) {
		t.Errorf("expected body %q, got %q", pdf, body)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

// TestClient_GetStream_PreservesExplicitReferer verifies a caller-supplied
// Referer is not overwritten by the base-URL default.
func TestClient_GetStream_PreservesExplicitReferer(t *testing.T) {
	stub := &stubBrowser{body: []byte("pdf"), status: http.StatusOK}
	c := newTestClient(stub)

	referer := "https://another.example/hotlink"
	if _, err := c.GetStream("https://idx.example/StaticData/x.pdf", map[string]string{"Referer": referer}); err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	if stub.gotHeaders["Referer"] != referer {
		t.Errorf("expected explicit Referer %q preserved, got %q", referer, stub.gotHeaders["Referer"])
	}
	if stub.gotURL != "https://idx.example/StaticData/x.pdf" {
		t.Errorf("expected absolute URL passed through, got %q", stub.gotURL)
	}
}

// fakeFetcher records calls for routing tests.
type fakeFetcher struct {
	getCalls            int
	getWithHeadersCalls int
	getStreamCalls      int
	gotPath             string
	gwhHeaders          map[string]string
	resp                *http.Response
	err                 error
}

func (f *fakeFetcher) Get(path string) (*http.Response, error) {
	f.getCalls++
	f.gotPath = path
	return f.resp, f.err
}

func (f *fakeFetcher) GetWithHeaders(path string, h map[string]string) (*http.Response, error) {
	f.getWithHeadersCalls++
	f.gotPath = path
	f.gwhHeaders = h
	return f.resp, f.err
}

func (f *fakeFetcher) GetStream(path string, h map[string]string) (*http.Response, error) {
	f.getStreamCalls++
	f.gotPath = path
	return f.resp, f.err
}

// TestClient_Routing_DelegatesToFetcher verifies Client is a thin router: all
// three methods resolve the path and delegate to the active Fetcher. Get is
// sugar over GetWithHeaders (preserved behavior), so it never reaches the
// fetcher's Get directly.
func TestClient_Routing_DelegatesToFetcher(t *testing.T) {
	fake := &fakeFetcher{resp: &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}}
	c := &Client{config: Config{BaseURL: "https://idx.example"}, fetcher: fake, log: logrus.New()}

	if _, err := c.Get("/a"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.GetWithHeaders("/b", map[string]string{"Referer": "r"}); err != nil {
		t.Fatalf("GetWithHeaders: %v", err)
	}
	if _, err := c.GetStream("/c", nil); err != nil {
		t.Fatalf("GetStream: %v", err)
	}

	if fake.getCalls != 0 || fake.getWithHeadersCalls != 2 || fake.getStreamCalls != 1 {
		t.Errorf("expected get=0 gwh=2 stream=1, got get=%d gwh=%d stream=%d", fake.getCalls, fake.getWithHeadersCalls, fake.getStreamCalls)
	}
	if fake.gotPath != "https://idx.example/c" {
		t.Errorf("expected resolved URL, got %q", fake.gotPath)
	}
	if fake.gwhHeaders["Referer"] != "r" {
		t.Errorf("expected Referer passthrough, got %v", fake.gwhHeaders)
	}
}

// TestClient_Routing_ResponseContract verifies the *http.Response from the
// active Fetcher passes through with status, headers, and a closable body
// intact.
func TestClient_Routing_ResponseContract(t *testing.T) {
	fake := &fakeFetcher{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
	}}
	c := &Client{config: Config{BaseURL: "https://idx.example"}, fetcher: fake, log: logrus.New()}

	resp, err := c.GetWithHeaders("/x", nil)
	if err != nil {
		t.Fatalf("GetWithHeaders: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type header, got %v", resp.Header)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != `{"ok":true}` {
		t.Errorf("expected body %q, got %q", `{"ok":true}`, body)
	}
	if err := resp.Body.Close(); err != nil {
		t.Errorf("body close: %v", err)
	}
}
