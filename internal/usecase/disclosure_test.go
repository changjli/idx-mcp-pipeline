package usecase

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/go-playground/validator/v10"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// fakeTextStore is an in-memory DisclosureTextStore for tests.
type fakeTextStore struct {
	objects map[string][]byte
	err     error
}

func (f *fakeTextStore) GetObject(_ context.Context, key string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	data, ok := f.objects[key]
	if !ok {
		return nil, storage.ErrObjectNotFound
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeTextStore) DeleteObject(_ context.Context, key string) error {
	delete(f.objects, key)
	return nil
}

func newDisclosureTestUC(t *testing.T, db *sqlx.DB, store DisclosureTextStore) *DisclosureUseCase {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return NewDisclosureUseCase(
		db, log, validator.New(),
		repository.NewDisclosureRepository(log),
		store,
		repository.NewRawFileRepository(log),
	)
}

func TestTruncateDisclosureText(t *testing.T) {
	short := "short text"
	if got := truncateDisclosureText(short); got != short {
		t.Fatalf("short text mangled: got %q", got)
	}

	big := strings.Repeat("a", maxDisclosureTextBytes+10)
	got := truncateDisclosureText(big)
	if len(got) != maxDisclosureTextBytes {
		t.Fatalf("truncated length = %d, want %d", len(got), maxDisclosureTextBytes)
	}

	// Multibyte rune straddling the cut: the truncated output must stay valid
	// UTF-8, so the cut backs off to a rune boundary.
	prefix := strings.Repeat("a", maxDisclosureTextBytes-1)
	straddler := "é" // 2 bytes; starts at byte maxBytes-1, ends past it
	got = truncateDisclosureText(prefix + straddler + strings.Repeat("b", 100))
	if !utf8.ValidString(got) {
		t.Fatalf("truncation produced invalid UTF-8")
	}
	if strings.Contains(got, "é") {
		t.Fatalf("truncation split a multibyte rune")
	}
}

func TestReadIdxDisclosure_Statuses(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	store := &fakeTextStore{objects: map[string][]byte{}}
	uc := newDisclosureTestUC(t, db, store)

	db.MustExec("DELETE FROM disclosures WHERE ticker = 'REDA'")
	db.MustExec("DELETE FROM raw_files WHERE storage_key LIKE 'disclosure_text/REDA/%'")
	db.MustExec("DELETE FROM tickers WHERE code = 'REDA'")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE ticker = 'REDA'")
		db.MustExec("DELETE FROM raw_files WHERE storage_key LIKE 'disclosure_text/REDA/%'")
		db.MustExec("DELETE FROM tickers WHERE code = 'REDA'")
	})

	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('REDA', 'Read Disclosure Test A Tbk.', true)")

	// ok: extraction succeeded, text present in the store.
	okKey := "disclosure_text/REDA/ok.txt"
	store.objects[okKey] = []byte("extracted text")
	var okID int64
	db.Get(&okID, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, text_r2_key)
		VALUES ('REDA', '2026-08-05', 'Rapat Umum', 'https://example.com/ok.pdf', true, 'ok', $1)
		RETURNING id`, okKey)

	// pending: not yet processed.
	var pendingID int64
	db.Get(&pendingID, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status)
		VALUES ('REDA', '2026-08-05', 'Belum Diproses', 'https://example.com/pending.pdf', true, 'pending')
		RETURNING id`)

	// failed: extraction errored.
	errMsg := "too_large"
	var failedID int64
	db.Get(&failedID, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, extraction_error)
		VALUES ('REDA', '2026-08-05', 'Gagal', 'https://example.com/failed.pdf', true, 'failed', $1)
		RETURNING id`, errMsg)

	// evicted via raw_files.deleted_at: metadata says ok, claim-check deleted.
	evictedKey := "disclosure_text/REDA/evicted.txt"
	var evictedID int64
	db.Get(&evictedID, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, text_r2_key)
		VALUES ('REDA', '2026-08-05', 'Terhapus', 'https://example.com/evicted.pdf', true, 'ok', $1)
		RETURNING id`, evictedKey)
	db.MustExec(`INSERT INTO raw_files (storage_key, kind, source_ref, size_bytes, stored_at, deleted_at, retention_days)
		VALUES ($1, 'disclosure_text', 'https://example.com/evicted.pdf', 10, NOW(), NOW(), 90)`, evictedKey)

	// gone: status ok but the R2 object no longer exists (no raw_files row).
	goneKey := "disclosure_text/REDA/gone.txt"
	var goneID int64
	db.Get(&goneID, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, text_r2_key)
		VALUES ('REDA', '2026-08-05', 'Hilang', 'https://example.com/gone.pdf', true, 'ok', $1)
		RETURNING id`, goneKey)

	cases := []struct {
		name       string
		id         int64
		wantStatus string
		wantText   string // "" = expect null
		wantErr    string // "" = expect null
	}{
		{"ok", okID, "ok", "extracted text", ""},
		{"pending", pendingID, "pending", "", ""},
		{"failed", failedID, "failed", "", errMsg},
		{"evicted via raw_files", evictedID, "evicted", "", ""},
		{"evicted via missing object", goneID, "evicted", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := uc.ReadIdxDisclosure(context.Background(), tc.id)
			if err != nil {
				t.Fatalf("ReadIdxDisclosure(%d): %v", tc.id, err)
			}
			if resp.Status != tc.wantStatus {
				t.Fatalf("status = %q, want %q", resp.Status, tc.wantStatus)
			}
			if tc.wantText == "" {
				if resp.Text != nil {
					t.Fatalf("text = %q, want null", *resp.Text)
				}
			} else if resp.Text == nil || *resp.Text != tc.wantText {
				t.Fatalf("text = %v, want %q", resp.Text, tc.wantText)
			}
			if tc.wantErr == "" {
				if resp.Error != nil {
					t.Fatalf("error = %q, want null", *resp.Error)
				}
			} else if resp.Error == nil || *resp.Error != tc.wantErr {
				t.Fatalf("error = %v, want %q", resp.Error, tc.wantErr)
			}
			if resp.Ticker == nil || *resp.Ticker != "REDA" {
				t.Fatalf("ticker = %v, want REDA", resp.Ticker)
			}
			if resp.Title == "" || resp.PdfURL == "" || resp.Date == "" {
				t.Fatalf("metadata incomplete: %+v", resp)
			}
		})
	}

	// Truncation: a >64KB object is capped and stays valid UTF-8.
	bigKey := "disclosure_text/REDA/big.txt"
	big := strings.Repeat("x", maxDisclosureTextBytes+500)
	store.objects[bigKey] = []byte(big)
	var bigID int64
	db.Get(&bigID, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, text_r2_key)
		VALUES ('REDA', '2026-08-05', 'Besar', 'https://example.com/big.pdf', true, 'ok', $1)
		RETURNING id`, bigKey)
	resp, err := uc.ReadIdxDisclosure(context.Background(), bigID)
	if err != nil {
		t.Fatalf("ReadIdxDisclosure(big): %v", err)
	}
	if resp.Status != "ok" || resp.Text == nil {
		t.Fatalf("big: status = %q, text nil", resp.Status)
	}
	if len(*resp.Text) != maxDisclosureTextBytes {
		t.Fatalf("big: truncated length = %d, want %d", len(*resp.Text), maxDisclosureTextBytes)
	}

	// Unknown id → ErrNotFound.
	if _, err := uc.ReadIdxDisclosure(context.Background(), 999999999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id: err = %v, want ErrNotFound", err)
	}
}

// TestReadIdxDisclosure_NilStore downgrades an ok row to pending when no text
// store is configured, rather than claiming ok with text unavailable.
func TestReadIdxDisclosure_NilStore(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	uc := newDisclosureTestUC(t, db, nil)

	db.MustExec("DELETE FROM disclosures WHERE ticker = 'REDC'")
	db.MustExec("DELETE FROM tickers WHERE code = 'REDC'")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE ticker = 'REDC'")
		db.MustExec("DELETE FROM tickers WHERE code = 'REDC'")
	})
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('REDC', 'Read Disclosure Test C Tbk.', true)")
	key := "disclosure_text/REDC/nil-store.txt"
	var id int64
	db.Get(&id, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, text_r2_key)
		VALUES ('REDC', '2026-08-05', 'Nil Store', 'https://example.com/nil-store.pdf', true, 'ok', $1)
		RETURNING id`, key)

	resp, err := uc.ReadIdxDisclosure(context.Background(), id)
	if err != nil {
		t.Fatalf("ReadIdxDisclosure: %v", err)
	}
	if resp.Status != "pending" {
		t.Fatalf("status = %q, want pending (nil store)", resp.Status)
	}
	if resp.Text != nil {
		t.Fatalf("text = %q, want null", *resp.Text)
	}
}

// TestReadIdxDisclosure_StoreError surfaces a store failure as a real error,
// not a status value.
func TestReadIdxDisclosure_StoreError(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	store := &fakeTextStore{err: errors.New("r2 down")}
	uc := newDisclosureTestUC(t, db, store)

	db.MustExec("DELETE FROM disclosures WHERE ticker = 'REDB'")
	db.MustExec("DELETE FROM tickers WHERE code = 'REDB'")
	t.Cleanup(func() {
		db.MustExec("DELETE FROM disclosures WHERE ticker = 'REDB'")
		db.MustExec("DELETE FROM tickers WHERE code = 'REDB'")
	})
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ('REDB', 'Read Disclosure Test B Tbk.', true)")
	key := "disclosure_text/REDB/down.txt"
	var id int64
	db.Get(&id, `INSERT INTO disclosures (ticker, announcement_date, title, pdf_url, passed_filter, extraction_status, text_r2_key)
		VALUES ('REDB', '2026-08-05', 'Store Down', 'https://example.com/down.pdf', true, 'ok', $1)
		RETURNING id`, key)

	if _, err := uc.ReadIdxDisclosure(context.Background(), id); err == nil {
		t.Fatal("store error swallowed, want error")
	}
}
