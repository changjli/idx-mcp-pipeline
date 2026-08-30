package client

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Fetcher is the IDX fetch seam. Implementations: *browserFetcherAdapter
// (headless browser via the nodriver sidecar — the only production transport).
// Client resolves relative paths to absolute URLs before delegating, so
// implementations receive full URLs.
type Fetcher interface {
	Get(path string) (*http.Response, error)
	GetWithHeaders(path string, extraHeaders map[string]string) (*http.Response, error)
	GetStream(path string, extraHeaders map[string]string) (*http.Response, error)
}

// browserFetcher is the headless-browser fetch seam. Implemented by
// *NodriverClient; stubbed in tests to verify the IDX client's browser modes
// without a network. Fetch carries text (JSON API bodies); FetchBinary carries
// binary content (disclosure PDFs) through the sidecar's base64 transport.
type browserFetcher interface {
	Fetch(url string, headers map[string]string) ([]byte, int, error)
	FetchBinary(url string, headers map[string]string) ([]byte, int, error)
	Close()
}

// browserFetcherAdapter adapts a browserFetcher to the Fetcher seam. Get and
// GetWithHeaders route JSON fetches through Fetch; GetStream routes disclosure
// PDFs through FetchBinary (the sidecar's base64 transport) — the Cloudflare
// JS-execution gate blocks direct GETs on the StaticData host (issue 01).
type browserFetcherAdapter struct {
	browser browserFetcher
}

// Get performs a browser-fetched GET.
func (b *browserFetcherAdapter) Get(path string) (*http.Response, error) {
	return b.GetWithHeaders(path, nil)
}

// GetWithHeaders fetches through the configured browser fetcher and surfaces
// the extracted payload as a synthetic response, reusing the caller-side
// contract (status >= 400 -> error, json.Unmarshal in the task) unchanged.
func (b *browserFetcherAdapter) GetWithHeaders(path string, extraHeaders map[string]string) (*http.Response, error) {
	body, status, err := b.browser.Fetch(path, extraHeaders)
	if err != nil {
		return nil, fmt.Errorf("browser fetch: %w", err)
	}
	return syntheticResponse(status, make(http.Header), body, path), nil
}

// GetStream fetches a binary body (disclosure PDF) through the browser
// fetcher's binary transport. The caller owns the response and must close Body.
func (b *browserFetcherAdapter) GetStream(path string, extraHeaders map[string]string) (*http.Response, error) {
	body, status, err := b.browser.FetchBinary(path, extraHeaders)
	if err != nil {
		return nil, fmt.Errorf("browser fetch: %w", err)
	}
	return syntheticResponse(status, make(http.Header), body, path), nil
}

// syntheticResponse builds an http.Response from fetched bytes.
func syntheticResponse(status int, headers http.Header, body []byte, rawURL string) *http.Response {
	u, _ := url.Parse(rawURL)
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       ReplayBody(body),
		Request:    &http.Request{URL: u},
	}
}

// ReplayBody returns a reader for an in-memory body.
func ReplayBody(body []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(body))
}
