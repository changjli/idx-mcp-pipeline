package usecase

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/extract"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// fakePDFFetcher serves a fixed body/status for both the ranged-GET size probe
// and the bounded download, capturing the URLs and headers it was called with.
type fakePDFFetcher struct {
	body          []byte
	status        int
	contentLength int64
	err           error
	calls         int
	gotURL        string
	gotHeaders    []map[string]string
}

func (f *fakePDFFetcher) GetStream(url string, extraHeaders map[string]string) (*http.Response, error) {
	f.calls++
	f.gotURL = url
	f.gotHeaders = append(f.gotHeaders, extraHeaders)
	if f.err != nil {
		return nil, f.err
	}
	h := make(http.Header)
	if f.contentLength > 0 {
		h.Set("Content-Length", strconv.FormatInt(f.contentLength, 10))
	}
	return &http.Response{
		StatusCode:    f.status,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(f.body)),
		ContentLength: f.contentLength,
	}, nil
}

// fakeExtractor returns a fixed text/error, isolating the usecase from PDF
// parsing (covered in internal/extract).
type fakeExtractor struct {
	text string
	err  error
}

func (f fakeExtractor) Extract(context.Context, []byte) (string, error) { return f.text, f.err }

// fakeObjectStore is an in-memory storage.ObjectStore for tests.
type fakeObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	err     error
}

func (f *fakeObjectStore) PutObject(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.objects[key] = append([]byte(nil), data...)
	return nil
}

func (f *fakeObjectStore) GetObject(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	data, ok := f.objects[key]
	if !ok {
		return nil, storage.ErrObjectNotFound
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeObjectStore) DeleteObject(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	return nil
}

func newFetchDisclosureTestUC(t *testing.T, db *sqlx.DB, fetcher extract.PDFFetcher, store storage.ObjectStore, extractor extract.Extractor) *FetchDisclosureUseCase {
	return newFetchDisclosureTestUCWithEnqueuer(t, db, fetcher, store, extractor, nil)
}

// newFetchDisclosureTestUCWithEnqueuer builds the usecase with an explicit
// enqueuer seam — nil selects the sync fallback (inline live path).
func newFetchDisclosureTestUCWithEnqueuer(t *testing.T, db *sqlx.DB, fetcher extract.PDFFetcher, store storage.ObjectStore, extractor extract.Extractor, enqueuer ExtractDisclosureEnqueuer) *FetchDisclosureUseCase {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return NewFetchDisclosureUseCase(
		db, log, validator.New(),
		repository.NewDisclosureRepository(log),
		repository.NewRawFileRepository(log),
		fetcher, store, extractor, enqueuer,
	)
}

// fakeExtractEnqueuer records enqueue calls and returns a fixed error.
type fakeExtractEnqueuer struct {
	err error
	ids []int64
}

func (f *fakeExtractEnqueuer) EnqueueExtractDisclosure(id int64) error {
	f.ids = append(f.ids, id)
	return f.err
}

// seedDisclosure inserts a ticker + disclosure and returns the disclosure id.
func seedDisclosure(t *testing.T, db *sqlx.DB, ticker, pdfURL, status string) int64 {
	t.Helper()
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true) ON CONFLICT (code) DO NOTHING", ticker, "Fetch Disclosure Test Tbk.")
	var id int64
	db.Get(&id, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status)
		VALUES ($1, '2026-08-05', 'Pemanggilan RUPS Tahunan', $2, true, $3)
		RETURNING id`, ticker, pdfURL, status)
	return id
}

func cleanupDisclosure(t *testing.T, db *sqlx.DB, ticker, pdfURL string) {
	t.Helper()
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
		db.MustExec("DELETE FROM raw_files WHERE storage_key LIKE 'disclosure_text/" + ticker + "/%'")
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})
}

// TestFetchDisclosurePDF_FastPath verifies an already-extracted disclosure is
// served from R2 without any upstream fetch.
func TestFetchDisclosurePDF_FastPath(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdpa.pdf"
	cleanupDisclosure(t, db, "FDPA", pdfURL)

	key := "disclosure_text/FDPA/fast.txt"
	store := &fakeObjectStore{objects: map[string][]byte{key: []byte("already extracted")}}
	fetcher := &fakePDFFetcher{}
	uc := newFetchDisclosureTestUC(t, db, fetcher, store, fakeExtractor{})

	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('FDPA', 'Fetch Disclosure Test Tbk.', true) ON CONFLICT (code) DO NOTHING")
	var id int64
	db.Get(&id, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, text_r2_key)
		VALUES ('FDPA', '2026-08-05', 'Pemanggilan RUPS Tahunan', $1, true, 'ok', $2)
		RETURNING id`, pdfURL, key)

	resp, err := uc.FetchDisclosurePDF(context.Background(), id)
	if err != nil {
		t.Fatalf("FetchDisclosurePDF: %v", err)
	}
	if resp.Status != "ok" || resp.Text == nil || *resp.Text != "already extracted" {
		t.Fatalf("resp = %+v, want ok + already extracted", resp)
	}
	if fetcher.calls != 0 {
		t.Errorf("fast path must not fetch upstream, got %d calls", fetcher.calls)
	}
}

// TestFetchDisclosurePDF_LiveSuccess verifies the full on-demand flow: fetch
// (probe + download), extract, persist to R2 + raw_files, status -> ok.
func TestFetchDisclosurePDF_LiveSuccess(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdpb.pdf"
	cleanupDisclosure(t, db, "FDPB", pdfURL)

	store := &fakeObjectStore{objects: map[string][]byte{}}
	fetcher := &fakePDFFetcher{body: []byte("%PDF-1.4 fake"), status: 200, contentLength: 14}
	uc := newFetchDisclosureTestUC(t, db, fetcher, store, fakeExtractor{text: "extracted on demand"})

	id := seedDisclosure(t, db, "FDPB", pdfURL, "pending")

	resp, err := uc.FetchDisclosurePDF(context.Background(), id)
	if err != nil {
		t.Fatalf("FetchDisclosurePDF: %v", err)
	}
	if resp.Status != "ok" || resp.Text == nil || *resp.Text != "extracted on demand" {
		t.Fatalf("resp = %+v, want ok + extracted on demand", resp)
	}

	// Fetcher seam: probe (ranged GET) then download, same URL.
	if fetcher.calls != 2 {
		t.Errorf("expected probe + download (2 calls), got %d", fetcher.calls)
	}
	if fetcher.gotURL != pdfURL {
		t.Errorf("fetched %q, want %q", fetcher.gotURL, pdfURL)
	}
	if len(fetcher.gotHeaders) != 2 || fetcher.gotHeaders[0]["Range"] != "bytes=0-0" {
		t.Errorf("expected ranged-GET probe first, got headers %v", fetcher.gotHeaders)
	}

	// Persisted: status ok + text_r2_key set, raw_files row, R2 object.
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
	if r2Key == nil || *r2Key == "" {
		t.Fatal("expected text_r2_key set")
	}
	if _, ok := store.objects[*r2Key]; !ok {
		t.Errorf("expected R2 object %s", *r2Key)
	}
	var kind string
	if err := db.Get(&kind, "SELECT kind FROM raw_files WHERE storage_key = $1", *r2Key); err != nil {
		t.Fatalf("fetch raw_files row: %v", err)
	}
	if kind != "disclosure_text" {
		t.Errorf("raw_files kind = %q, want disclosure_text", kind)
	}
}

// TestFetchDisclosurePDF_DownloadFailed verifies a fetch failure marks the
// disclosure failed and returns the failed envelope.
func TestFetchDisclosurePDF_DownloadFailed(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdpc.pdf"
	cleanupDisclosure(t, db, "FDPC", pdfURL)

	store := &fakeObjectStore{objects: map[string][]byte{}}
	fetcher := &fakePDFFetcher{err: errors.New("cloudflare block")}
	uc := newFetchDisclosureTestUC(t, db, fetcher, store, fakeExtractor{})

	id := seedDisclosure(t, db, "FDPC", pdfURL, "pending")

	resp, err := uc.FetchDisclosurePDF(context.Background(), id)
	if err != nil {
		t.Fatalf("FetchDisclosurePDF: %v", err)
	}
	if resp.Status != "failed" || resp.Error == nil || !strings.Contains(*resp.Error, "download_failed") {
		t.Fatalf("resp = %+v, want failed + download_failed", resp)
	}
	var status, errMsg string
	if err := db.Get(&status, "SELECT extraction_status FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch status: %v", err)
	}
	if status != "failed" {
		t.Errorf("extraction_status = %q, want failed", status)
	}
	if err := db.Get(&errMsg, "SELECT extraction_error FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch error: %v", err)
	}
	if !strings.Contains(errMsg, "download_failed") {
		t.Errorf("extraction_error = %q, want download_failed", errMsg)
	}
}

// TestFetchDisclosurePDF_EmptyText verifies a scanned (text-less) PDF is marked
// failed with empty_text and nothing is stored.
func TestFetchDisclosurePDF_EmptyText(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdpd.pdf"
	cleanupDisclosure(t, db, "FDPD", pdfURL)

	store := &fakeObjectStore{objects: map[string][]byte{}}
	fetcher := &fakePDFFetcher{body: []byte("%PDF-1.4 fake"), status: 200, contentLength: 14}
	uc := newFetchDisclosureTestUC(t, db, fetcher, store, fakeExtractor{text: ""})

	id := seedDisclosure(t, db, "FDPD", pdfURL, "pending")

	resp, err := uc.FetchDisclosurePDF(context.Background(), id)
	if err != nil {
		t.Fatalf("FetchDisclosurePDF: %v", err)
	}
	if resp.Status != "failed" || resp.Error == nil || !strings.Contains(*resp.Error, "empty_text") {
		t.Fatalf("resp = %+v, want failed + empty_text", resp)
	}
	if len(store.objects) != 0 {
		t.Errorf("expected no R2 objects for empty text, got %d", len(store.objects))
	}
}

// TestFetchDisclosurePDF_TooLarge verifies a PDF over the 10MB cap is rejected
// at the size probe and marked failed.
func TestFetchDisclosurePDF_TooLarge(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdpe.pdf"
	cleanupDisclosure(t, db, "FDPE", pdfURL)

	store := &fakeObjectStore{objects: map[string][]byte{}}
	fetcher := &fakePDFFetcher{status: 200, contentLength: extract.MaxPDFBytes + 1}
	uc := newFetchDisclosureTestUC(t, db, fetcher, store, fakeExtractor{})

	id := seedDisclosure(t, db, "FDPE", pdfURL, "pending")

	resp, err := uc.FetchDisclosurePDF(context.Background(), id)
	if err != nil {
		t.Fatalf("FetchDisclosurePDF: %v", err)
	}
	if resp.Status != "failed" || resp.Error == nil || !strings.Contains(*resp.Error, "too_large") {
		t.Fatalf("resp = %+v, want failed + too_large", resp)
	}
	if fetcher.calls != 1 {
		t.Errorf("expected only the probe request, got %d", fetcher.calls)
	}
}

// TestFetchDisclosurePDF_EvictedRefetch verifies an ok row whose text was
// evicted falls through to a live re-fetch instead of serving stale metadata.
func TestFetchDisclosurePDF_EvictedRefetch(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdpf.pdf"
	cleanupDisclosure(t, db, "FDPF", pdfURL)

	// status ok + text_r2_key set, but the R2 object is gone (evicted).
	store := &fakeObjectStore{objects: map[string][]byte{}}
	fetcher := &fakePDFFetcher{body: []byte("%PDF-1.4 fake"), status: 200, contentLength: 14}
	uc := newFetchDisclosureTestUC(t, db, fetcher, store, fakeExtractor{text: "re-extracted"})

	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('FDPF', 'Fetch Disclosure Test Tbk.', true) ON CONFLICT (code) DO NOTHING")
	var id int64
	db.Get(&id, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, text_r2_key)
		VALUES ('FDPF', '2026-08-05', 'Pemanggilan RUPS Tahunan', $1, true, 'ok', 'disclosure_text/FDPF/evicted.txt')
		RETURNING id`, pdfURL)

	resp, err := uc.FetchDisclosurePDF(context.Background(), id)
	if err != nil {
		t.Fatalf("FetchDisclosurePDF: %v", err)
	}
	if resp.Status != "ok" || resp.Text == nil || *resp.Text != "re-extracted" {
		t.Fatalf("resp = %+v, want ok + re-extracted", resp)
	}
	if fetcher.calls != 2 {
		t.Errorf("expected live re-fetch (2 calls), got %d", fetcher.calls)
	}
}

// TestFetchDisclosurePDF_NotFound maps an unknown id to ErrNotFound.
func TestFetchDisclosurePDF_NotFound(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	uc := newFetchDisclosureTestUC(t, db, &fakePDFFetcher{}, &fakeObjectStore{objects: map[string][]byte{}}, fakeExtractor{})

	if _, err := uc.FetchDisclosurePDF(context.Background(), 999999999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id: err = %v, want ErrNotFound", err)
	}
}

// TestFetchDisclosurePDF_AsyncEnqueued verifies the async live path: with an
// enqueuer wired, the tool enqueues extract:disclosure and returns the pending
// envelope immediately — no upstream fetch, no persist, status stays pending
// (the worker owns the transition to ok|failed).
func TestFetchDisclosurePDF_AsyncEnqueued(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdpg.pdf"
	cleanupDisclosure(t, db, "FDPG", pdfURL)

	store := &fakeObjectStore{objects: map[string][]byte{}}
	fetcher := &fakePDFFetcher{}
	enq := &fakeExtractEnqueuer{}
	uc := newFetchDisclosureTestUCWithEnqueuer(t, db, fetcher, store, fakeExtractor{}, enq)

	id := seedDisclosure(t, db, "FDPG", pdfURL, "pending")

	resp, err := uc.FetchDisclosurePDF(context.Background(), id)
	if err != nil {
		t.Fatalf("FetchDisclosurePDF: %v", err)
	}
	if resp.Status != "pending" || resp.Text != nil {
		t.Fatalf("resp = %+v, want pending envelope with no text", resp)
	}
	if len(enq.ids) != 1 || enq.ids[0] != id {
		t.Errorf("enqueued ids = %v, want [%d]", enq.ids, id)
	}
	if fetcher.calls != 0 {
		t.Errorf("async path must not fetch upstream, got %d calls", fetcher.calls)
	}
	var status string
	if err := db.Get(&status, "SELECT extraction_status FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch status: %v", err)
	}
	if status != "pending" {
		t.Errorf("extraction_status = %q, want pending (worker owns the transition)", status)
	}
}

// TestFetchDisclosurePDF_AsyncDedupe verifies a duplicate enqueue
// (asynq.ErrTaskIDConflict) returns the same pending envelope — idempotent,
// matching the daily filter's conflict-swallow precedent.
func TestFetchDisclosurePDF_AsyncDedupe(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdph.pdf"
	cleanupDisclosure(t, db, "FDPH", pdfURL)

	store := &fakeObjectStore{objects: map[string][]byte{}}
	fetcher := &fakePDFFetcher{}
	enq := &fakeExtractEnqueuer{err: asynq.ErrTaskIDConflict}
	uc := newFetchDisclosureTestUCWithEnqueuer(t, db, fetcher, store, fakeExtractor{}, enq)

	id := seedDisclosure(t, db, "FDPH", pdfURL, "pending")

	resp, err := uc.FetchDisclosurePDF(context.Background(), id)
	if err != nil {
		t.Fatalf("FetchDisclosurePDF: %v", err)
	}
	if resp.Status != "pending" || resp.Text != nil {
		t.Fatalf("resp = %+v, want pending envelope", resp)
	}
	if len(enq.ids) != 1 {
		t.Errorf("enqueued ids = %v, want 1 call", enq.ids)
	}
	if fetcher.calls != 0 {
		t.Errorf("dedupe path must not fetch upstream, got %d calls", fetcher.calls)
	}
}

// TestFetchDisclosurePDF_AsyncEnqueueError verifies an enqueue failure (Redis
// down) is a Go error — the tool raises, not a failed envelope.
func TestFetchDisclosurePDF_AsyncEnqueueError(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	t.Cleanup(func() { db.Close() }) // LIFO: runs after the data cleanup
	pdfURL := "https://example.com/fdpi.pdf"
	cleanupDisclosure(t, db, "FDPI", pdfURL)

	store := &fakeObjectStore{objects: map[string][]byte{}}
	fetcher := &fakePDFFetcher{}
	enq := &fakeExtractEnqueuer{err: errors.New("redis down")}
	uc := newFetchDisclosureTestUCWithEnqueuer(t, db, fetcher, store, fakeExtractor{}, enq)

	id := seedDisclosure(t, db, "FDPI", pdfURL, "pending")

	if _, err := uc.FetchDisclosurePDF(context.Background(), id); err == nil {
		t.Fatal("expected Go error on enqueue failure")
	}
	if fetcher.calls != 0 {
		t.Errorf("enqueue-error path must not fetch upstream, got %d calls", fetcher.calls)
	}
}
