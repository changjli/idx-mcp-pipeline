package tasks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// memStore is an in-memory ObjectStore for tests.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) PutObject(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), data...)
	return nil
}

func (m *memStore) GetObject(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, storage.ErrObjectNotFound
	}
	return append([]byte(nil), data...), nil
}

// fakeExtractor returns a fixed text/error, isolating the runner from PDF
// parsing (covered in internal/extract).
type fakeExtractor struct {
	text string
	err  error
}

func (f fakeExtractor) Extract(context.Context, []byte) (string, error) { return f.text, f.err }

// TestExtractDisclosure_EndToEnd verifies the extract runner against a real
// Postgres: download, extraction, R2 store, raw_files claim-check row, and
// extraction_status='ok'. Skipped unless IDX_MCP_DB_DSN is set.
func TestExtractDisclosure_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	disclosureRepo := repository.NewDisclosureRepository(log)
	rawFileRepo := repository.NewRawFileRepository(log)

	// PDF server: HEAD + GET both serve the body.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("%PDF-1.4 fake body"))
	}))
	defer srv.Close()

	pdfURL := srv.URL + "/extract-test.pdf"
	db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
	db.MustExec("DELETE FROM raw_files WHERE storage_key LIKE 'disclosure_text/EXTA/%'")
	db.MustExec("DELETE FROM tickers WHERE code = 'EXTA'")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
		db.MustExec("DELETE FROM raw_files WHERE storage_key LIKE 'disclosure_text/EXTA/%'")
		db.MustExec("DELETE FROM tickers WHERE code = 'EXTA'")
	})

	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('EXTA', 'Extract Test A Tbk.', true)")
	db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status)
		VALUES ('EXTA', '2026-08-05', 'Pemanggilan RUPS Tahunan', $1, true, 'pending')`, pdfURL)

	store := newMemStore()
	runner := &extractDisclosureRunner{
		log:            log,
		httpClient:     srv.Client(),
		r2Store:        store,
		db:             db,
		disclosureRepo: disclosureRepo,
		rawFileRepo:    rawFileRepo,
		extractor:      fakeExtractor{text: "extracted disclosure text"},
		reenqueue:      func(int, time.Duration) error { return nil },
	}

	var id int64
	if err := db.Get(&id, "SELECT id FROM disclosures WHERE pdf_url = $1", pdfURL); err != nil {
		t.Fatalf("fetch disclosure id: %v", err)
	}
	d, err := disclosureRepo.FindByID(db, id)
	if err != nil {
		t.Fatalf("find disclosure: %v", err)
	}

	if err := runner.run(context.Background(), d, 0); err != nil {
		t.Fatalf("run: %v", err)
	}

	// extraction_status='ok' + text_r2_key set.
	var status string
	var r2Key *string
	if err := db.Get(&status, "SELECT extraction_status FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch status: %v", err)
	}
	if status != "ok" {
		t.Errorf("expected extraction_status ok, got %s", status)
	}
	if err := db.Get(&r2Key, "SELECT text_r2_key FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch r2 key: %v", err)
	}
	if r2Key == nil || *r2Key == "" {
		t.Fatal("expected text_r2_key set")
	}

	// Text readable on R2.
	store.mu.Lock()
	text, ok := store.objects[*r2Key]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("expected R2 object %s", *r2Key)
	}
	if string(text) != "extracted disclosure text" {
		t.Errorf("unexpected R2 text %q", text)
	}

	// raw_files claim-check row.
	var rf struct {
		Kind          string `db:"kind"`
		SourceRef     string `db:"source_ref"`
		RetentionDays int32  `db:"retention_days"`
	}
	if err := db.Get(&rf, "SELECT kind, source_ref, retention_days FROM raw_files WHERE storage_key = $1", *r2Key); err != nil {
		t.Fatalf("fetch raw_files row: %v", err)
	}
	if rf.Kind != "disclosure_text" {
		t.Errorf("expected kind disclosure_text, got %s", rf.Kind)
	}
	if rf.SourceRef != pdfURL {
		t.Errorf("expected source_ref %s, got %s", pdfURL, rf.SourceRef)
	}
	if rf.RetentionDays != 90 {
		t.Errorf("expected retention 90, got %d", rf.RetentionDays)
	}
}

// TestExtractDisclosure_EmptyText verifies a scanned (text-less) PDF is marked
// failed with extraction_error='empty_text' and nothing is stored.
func TestExtractDisclosure_EmptyText(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	disclosureRepo := repository.NewDisclosureRepository(log)
	rawFileRepo := repository.NewRawFileRepository(log)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("%PDF-1.4 fake body"))
	}))
	defer srv.Close()

	pdfURL := srv.URL + "/extract-test-empty.pdf"
	db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
	db.MustExec("DELETE FROM tickers WHERE code = 'EXTB'")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
		db.MustExec("DELETE FROM tickers WHERE code = 'EXTB'")
	})

	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('EXTB', 'Extract Test B Tbk.', true)")
	db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status)
		VALUES ('EXTB', '2026-08-05', 'Pemanggilan RUPS Tahunan', $1, true, 'pending')`, pdfURL)

	store := newMemStore()
	runner := &extractDisclosureRunner{
		log:            log,
		httpClient:     srv.Client(),
		r2Store:        store,
		db:             db,
		disclosureRepo: disclosureRepo,
		rawFileRepo:    rawFileRepo,
		extractor:      fakeExtractor{text: ""},
		reenqueue:      func(int, time.Duration) error { return nil },
	}

	var id int64
	if err := db.Get(&id, "SELECT id FROM disclosures WHERE pdf_url = $1", pdfURL); err != nil {
		t.Fatalf("fetch disclosure id: %v", err)
	}
	d, err := disclosureRepo.FindByID(db, id)
	if err != nil {
		t.Fatalf("find disclosure: %v", err)
	}

	if err := runner.run(context.Background(), d, 0); err != nil {
		t.Fatalf("run: %v", err)
	}

	var status, errMsg string
	if err := db.Get(&status, "SELECT extraction_status FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch status: %v", err)
	}
	if status != "failed" {
		t.Errorf("expected extraction_status failed, got %s", status)
	}
	if err := db.Get(&errMsg, "SELECT extraction_error FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch error: %v", err)
	}
	if !strings.Contains(errMsg, "empty_text") {
		t.Errorf("expected extraction_error containing empty_text, got %q", errMsg)
	}
	if len(store.objects) != 0 {
		t.Errorf("expected no R2 objects for empty text, got %d", len(store.objects))
	}
}

// TestExtractDisclosure_TooLarge verifies a PDF over the 10MB cap is rejected
// at the HEAD check and marked failed.
func TestExtractDisclosure_TooLarge(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	disclosureRepo := repository.NewDisclosureRepository(log)
	rawFileRepo := repository.NewRawFileRepository(log)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10485761") // 10MB + 1
	}))
	defer srv.Close()

	pdfURL := srv.URL + "/extract-test-large.pdf"
	db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
	db.MustExec("DELETE FROM tickers WHERE code = 'EXTC'")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE pdf_url = $1", pdfURL)
		db.MustExec("DELETE FROM tickers WHERE code = 'EXTC'")
	})

	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('EXTC', 'Extract Test C Tbk.', true)")
	db.MustExec(`INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status)
		VALUES ('EXTC', '2026-08-05', 'Pemanggilan RUPS Tahunan', $1, true, 'pending')`, pdfURL)

	store := newMemStore()
	runner := &extractDisclosureRunner{
		log:            log,
		httpClient:     srv.Client(),
		r2Store:        store,
		db:             db,
		disclosureRepo: disclosureRepo,
		rawFileRepo:    rawFileRepo,
		extractor:      fakeExtractor{text: "should not be reached"},
		reenqueue:      func(int, time.Duration) error { return nil },
	}

	var id int64
	if err := db.Get(&id, "SELECT id FROM disclosures WHERE pdf_url = $1", pdfURL); err != nil {
		t.Fatalf("fetch disclosure id: %v", err)
	}
	d, err := disclosureRepo.FindByID(db, id)
	if err != nil {
		t.Fatalf("find disclosure: %v", err)
	}

	if err := runner.run(context.Background(), d, 0); err != nil {
		t.Fatalf("run: %v", err)
	}

	var status, errMsg string
	if err := db.Get(&status, "SELECT extraction_status FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch status: %v", err)
	}
	if status != "failed" {
		t.Errorf("expected extraction_status failed, got %s", status)
	}
	if err := db.Get(&errMsg, "SELECT extraction_error FROM disclosures WHERE id = $1", id); err != nil {
		t.Fatalf("fetch error: %v", err)
	}
	if !strings.Contains(errMsg, "too_large") {
		t.Errorf("expected extraction_error containing too_large, got %q", errMsg)
	}
	if len(store.objects) != 0 {
		t.Errorf("expected no R2 objects for oversized PDF, got %d", len(store.objects))
	}
}

var _ storage.ObjectStore = (*memStore)(nil)
