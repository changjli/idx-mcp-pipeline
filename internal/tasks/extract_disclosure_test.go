package tasks

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDownloadBounded_Ok(t *testing.T) {
	body := strings.Repeat("x", 100)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()

	data, err := downloadBounded(srv.Client(), srv.URL, 10*1024*1024)
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

	_, err := downloadBounded(srv.Client(), srv.URL, 10*1024*1024)
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

	_, err := downloadBounded(srv.Client(), srv.URL, 10*1024*1024)
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

	data, err := downloadBounded(srv.Client(), srv.URL, 10*1024*1024)
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

	_, err := downloadBounded(srv.Client(), srv.URL, 10*1024*1024)
	if err == nil {
		t.Fatal("expected error for 404")
	}
}
