package tasks

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// httpClientFetcher adapts a plain *http.Client to the pdfFetcher seam so
// tests can reuse httptest servers without a real upstream.
type httpClientFetcher struct{ c *http.Client }

func (f httpClientFetcher) GetStream(url string, extraHeaders map[string]string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return f.c.Do(req)
}

func TestDownloadBounded_Ok(t *testing.T) {
	body := strings.Repeat("x", 100)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()

	data, err := downloadBounded(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if string(data) != body {
		t.Errorf("expected body %q, got %q", body, data)
	}
}

func TestDownloadBounded_ContentLengthTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10485761") // 10MB + 1
		w.Write([]byte("x"))
	}))
	defer srv.Close()

	_, err := downloadBounded(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if !errors.Is(err, errPDFTooLarge) {
		t.Fatalf("expected errPDFTooLarge, got %v", err)
	}
}

func TestDownloadBounded_ChunkedExceedsCap(t *testing.T) {
	// No Content-Length (chunked): the bounded reader must abort at cap+1.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", 10*1024*1024+1)))
	}))
	defer srv.Close()

	_, err := downloadBounded(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if !errors.Is(err, errPDFTooLarge) {
		t.Fatalf("expected errPDFTooLarge, got %v", err)
	}
}

func TestDownloadBounded_ExactlyAtCap(t *testing.T) {
	body := strings.Repeat("x", 10*1024*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()

	data, err := downloadBounded(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if len(data) != 10*1024*1024 {
		t.Errorf("expected %d bytes, got %d", 10*1024*1024, len(data))
	}
}

func TestDownloadBounded_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := downloadBounded(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err == nil {
		t.Fatal("expected error for 404")
	}
}

// TestFetchPDF_SendsRangedGETProbe verifies the size probe is a ranged GET
// (Range: bytes=0-0) — Cloudflare 403s a bare HEAD — and the download request
// carries no Range.
func TestFetchPDF_SendsRangedGETProbe(t *testing.T) {
	var probes []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			probes = append(probes, r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", "bytes 0-0/1234")
		w.Write([]byte("%PDF-1.4 fake"))
	}))
	defer srv.Close()

	data, err := fetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err != nil {
		t.Fatalf("fetchPDF: %v", err)
	}
	if len(probes) != 1 || probes[0] != "bytes=0-0" {
		t.Errorf("expected one ranged probe bytes=0-0, got %v", probes)
	}
	if string(data) != "%PDF-1.4 fake" {
		t.Errorf("expected body %q, got %q", "%PDF-1.4 fake", data)
	}
}

// TestFetchPDF_ProbeTooLarge verifies an oversized PDF is rejected at the size
// probe and the body is never downloaded.
func TestFetchPDF_ProbeTooLarge(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Range", "bytes 0-0/10485761") // 10MB + 1
		w.Write([]byte("x"))
	}))
	defer srv.Close()

	_, err := fetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if !errors.Is(err, errPDFTooLarge) {
		t.Fatalf("expected errPDFTooLarge, got %v", err)
	}
	if requests != 1 {
		t.Errorf("expected only the probe request, got %d", requests)
	}
}

// TestFetchPDF_ProbeHTTPError verifies a blocked probe (e.g. Cloudflare 403)
// surfaces as a fetch error, never as errPDFTooLarge.
func TestFetchPDF_ProbeHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "blocked", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := fetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err == nil {
		t.Fatal("expected error for 403 probe")
	}
	if errors.Is(err, errPDFTooLarge) {
		t.Fatal("403 must not be reported as too_large")
	}
}

// TestFetchPDF_Probe403WithLargeSize verifies the status check wins over the
// size check: a Cloudflare 403 whose body happens to declare a size over the
// cap must surface as a retryable fetch error, not errPDFTooLarge.
func TestFetchPDF_Probe403WithLargeSize(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/10485761") // over the 10MB cap
		http.Error(w, "blocked", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := fetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err == nil {
		t.Fatal("expected error for 403 probe")
	}
	if errors.Is(err, errPDFTooLarge) {
		t.Fatal("403 with large declared size must not be reported as too_large")
	}
}

// TestFetchPDF_DownloadTooLarge verifies the bounded reader catches a body
// that grows past the cap even when the probe reported a small total.
func TestFetchPDF_DownloadTooLarge(t *testing.T) {
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 1 {
			w.Header().Set("Content-Range", "bytes 0-0/100")
			w.Write([]byte("x"))
			return
		}
		// Chunked (no Content-Length): the LimitedReader must abort at cap+1.
		w.Write([]byte(strings.Repeat("x", 10*1024*1024+1)))
	}))
	defer srv.Close()

	_, err := fetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if !errors.Is(err, errPDFTooLarge) {
		t.Fatalf("expected errPDFTooLarge, got %v", err)
	}
}

func TestProbeSize(t *testing.T) {
	cases := []struct {
		name         string
		contentRange string
		contentLen   int64
		want         int64
	}{
		{"content-range total", "bytes 0-0/1234", -1, 1234},
		{"content-range partial", "bytes 100-199/5000", -1, 5000},
		{"content-range unknown total", "bytes 0-0/*", -1, -1},
		{"content-range malformed", "bytes 0-0/abc", 77, 77},
		{"content-length fallback", "", 5678, 5678},
		{"neither", "", -1, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := make(http.Header)
			if tc.contentRange != "" {
				h.Set("Content-Range", tc.contentRange)
			}
			resp := &http.Response{Header: h, ContentLength: tc.contentLen}
			if got := probeSize(resp); got != tc.want {
				t.Errorf("probeSize = %d, want %d", got, tc.want)
			}
		})
	}
}
