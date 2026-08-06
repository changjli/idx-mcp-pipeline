package client

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"
)

// browserCipherSuites is a Go subset of a typical Chrome cipher suite list.
// Used as fallback when the default Go cipher order is rejected.
var browserCipherSuites = []uint16{
	tls.TLS_AES_128_GCM_SHA256,
	tls.TLS_AES_256_GCM_SHA384,
	tls.TLS_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
	tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	tls.TLS_RSA_WITH_AES_256_CBC_SHA,
}

// browserCurveIDs is a typical Chrome curve preference list.
var browserCurveIDs = []tls.CurveID{
	tls.X25519,
	tls.CurveP256,
	tls.CurveP384,
}

// fallbackTransport wraps http.Transport and falls back to browser-compatible
// TLS settings if the standard Go TLS handshake fails.
type fallbackTransport struct {
	primary *http.Transport
	browser *http.Transport
}

// newFallbackTransport creates a transport with standard Go TLS settings
// and a pre-built browser-compatible fallback.
func newFallbackTransport(timeout time.Duration) *fallbackTransport {
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}

	primary := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	browser := primary.Clone()
	browser.TLSClientConfig = &tls.Config{
		CipherSuites:             browserCipherSuites,
		CurvePreferences:         browserCurveIDs,
		MinVersion:               tls.VersionTLS12,
		InsecureSkipVerify:       false,
		SessionTicketsDisabled:   false,
		PreferServerCipherSuites: false,
	}

	return &fallbackTransport{primary: primary, browser: browser}
}

// RoundTrip implements http.RoundTripper.
// On TLS error, retries with browser-compatible cipher suites and curve IDs.
func (ft *fallbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := ft.primary.RoundTrip(req)
	if err != nil && isTLSError(err) {
		return ft.browser.RoundTrip(req)
	}
	return resp, err
}

// isTLSError returns true if the error is related to TLS handshake failure.
func isTLSError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "tls:") ||
		strings.Contains(msg, "handshake failure") ||
		strings.Contains(msg, "certificate") ||
		strings.Contains(msg, "protocol version")
}
