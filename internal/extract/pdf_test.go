package extract

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// httpClientFetcher adapts a plain *http.Client to the PDFFetcher seam so
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
	if !errors.Is(err, ErrPDFTooLarge) {
		t.Fatalf("expected ErrPDFTooLarge, got %v", err)
	}
}

func TestDownloadBounded_ChunkedExceedsCap(t *testing.T) {
	// No Content-Length (chunked): the bounded reader must abort at cap+1.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", 10*1024*1024+1)))
	}))
	defer srv.Close()

	_, err := downloadBounded(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if !errors.Is(err, ErrPDFTooLarge) {
		t.Fatalf("expected ErrPDFTooLarge, got %v", err)
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

	data, err := FetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err != nil {
		t.Fatalf("FetchPDF: %v", err)
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

	_, err := FetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if !errors.Is(err, ErrPDFTooLarge) {
		t.Fatalf("expected ErrPDFTooLarge, got %v", err)
	}
	if requests != 1 {
		t.Errorf("expected only the probe request, got %d", requests)
	}
}

// TestFetchPDF_ProbeHTTPError verifies a blocked probe (e.g. Cloudflare 403)
// surfaces as a fetch error, never as ErrPDFTooLarge.
func TestFetchPDF_ProbeHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "blocked", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := FetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err == nil {
		t.Fatal("expected error for 403 probe")
	}
	if errors.Is(err, ErrPDFTooLarge) {
		t.Fatal("403 must not be reported as too_large")
	}
}

// TestFetchPDF_Probe403WithLargeSize verifies the status check wins over the
// size check: a Cloudflare 403 whose body happens to declare a size over the
// cap must surface as a retryable fetch error, not ErrPDFTooLarge.
func TestFetchPDF_Probe403WithLargeSize(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-0/10485761") // over the 10MB cap
		http.Error(w, "blocked", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := FetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if err == nil {
		t.Fatal("expected error for 403 probe")
	}
	if errors.Is(err, ErrPDFTooLarge) {
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

	_, err := FetchPDF(httpClientFetcher{srv.Client()}, srv.URL, 10*1024*1024)
	if !errors.Is(err, ErrPDFTooLarge) {
		t.Fatalf("expected ErrPDFTooLarge, got %v", err)
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

// memStore is an in-memory ObjectStore for the persist test.
type memStore struct {
	objects map[string][]byte
}

func (m *memStore) PutObject(_ context.Context, key string, data []byte) error {
	m.objects[key] = append([]byte(nil), data...)
	return nil
}

func (m *memStore) GetObject(_ context.Context, key string) ([]byte, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, storage.ErrObjectNotFound
	}
	return append([]byte(nil), data...), nil
}

func (m *memStore) DeleteObject(_ context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

// TestDisclosureTextPersister verifies the ADR-0004 contract against a real
// Postgres: R2 object stored, raw_files claim-check row inserted, and the
// disclosure's extraction_status moved to ok with text_r2_key set. Skipped
// unless IDX_MCP_DB_DSN is set.
func TestDisclosureTextPersister(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup below
	log := logrus.New()
	disclosureRepo := repository.NewDisclosureRepository(log)
	rawFileRepo := repository.NewRawFileRepository(log)

	pdfURL := "https://example.com/persist-test.pdf"
	db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
	db.MustExec("DELETE FROM raw_files WHERE storage_key LIKE 'disclosure_text/PERT/%'")
	db.MustExec("DELETE FROM tickers WHERE code = 'PERT'")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
		db.MustExec("DELETE FROM raw_files WHERE storage_key LIKE 'disclosure_text/PERT/%'")
		db.MustExec("DELETE FROM tickers WHERE code = 'PERT'")
	})

	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('PERT', 'Persist Test Tbk.', true)")
	db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status)
		VALUES ('PERT', '2026-08-05', 'Pemanggilan RUPS Tahunan', $1, true, 'pending')`, pdfURL)

	var id int64
	if err := db.Get(&id, "SELECT id FROM disclosures WHERE pdf_url = $1", pdfURL); err != nil {
		t.Fatalf("fetch disclosure id: %v", err)
	}
	d, err := disclosureRepo.FindByID(db, id)
	if err != nil {
		t.Fatalf("find disclosure: %v", err)
	}

	store := &memStore{objects: map[string][]byte{}}
	p := &DisclosureTextPersister{
		Store:       store,
		RawFiles:    rawFileRepo,
		Disclosures: disclosureRepo,
		DB:          db,
	}
	key, err := p.Persist(context.Background(), d, "extracted disclosure text")
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if key != DisclosureTextKey(d) {
		t.Errorf("key = %q, want %q", key, DisclosureTextKey(d))
	}

	// R2 object stored.
	if got, ok := store.objects[key]; !ok || string(got) != "extracted disclosure text" {
		t.Errorf("R2 object %s = %q, want extracted disclosure text", key, got)
	}

	// raw_files claim-check row.
	var rf struct {
		Kind          string `db:"kind"`
		SourceRef     string `db:"source_ref"`
		RetentionDays int32  `db:"retention_days"`
	}
	if err := db.Get(&rf, "SELECT kind, source_ref, retention_days FROM raw_files WHERE storage_key = $1", key); err != nil {
		t.Fatalf("fetch raw_files row: %v", err)
	}
	if rf.Kind != "disclosure_text" || rf.SourceRef != pdfURL || rf.RetentionDays != 90 {
		t.Errorf("raw_files row = %+v, want kind=disclosure_text source_ref=%s retention=90", rf, pdfURL)
	}

	// extraction_status moved to ok with text_r2_key set.
	var status string
	var r2Key *string
	if err := db.Get(&status, "SELECT extraction_status FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch status: %v", err)
	}
	if status != "ok" {
		t.Errorf("extraction_status = %q, want ok", status)
	}
	if err := db.Get(&r2Key, "SELECT text_r2_key FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch r2 key: %v", err)
	}
	if r2Key == nil || *r2Key != key {
		t.Errorf("text_r2_key = %v, want %q", r2Key, key)
	}
}

var _ storage.ObjectStore = (*memStore)(nil)
var _ PDFFetcher = httpClientFetcher{}
