package extract

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// PDFFetcher is the seam for disclosure PDF downloads. The caller owns the
// returned response body and must close it — no buffering, no caching.
// client.Client's GetStream satisfies it with the full Cloudflare session
// (cookies, browser headers, pacing); a fake backed by httptest keeps unit
// tests hermetic (no real upstream, per CLAUDE.md).
type PDFFetcher interface {
	GetStream(url string, extraHeaders map[string]string) (*http.Response, error)
}

// MaxPDFBytes caps a disclosure PDF at 10MB — probed via a ranged GET before
// download and enforced again by the bounded read buffer.
const MaxPDFBytes = 10 * 1024 * 1024

// ExtractTimeout caps text extraction per disclosure (30s). OCR (ticket 16)
// will need its own budget — scans are slower than text layers.
const ExtractTimeout = 30 * time.Second

// DisclosureTextRetentionDays is the raw_files retention for extracted
// disclosure text (90 days; metadata survives eviction).
const DisclosureTextRetentionDays = 90

// ErrPDFTooLarge is returned when a download exceeds the size cap.
var ErrPDFTooLarge = errors.New("pdf exceeds size cap")

// FetchPDF downloads a disclosure PDF through the session-aware fetcher,
// bounded to maxBytes. The size probe is a ranged GET (Range: bytes=0-0) —
// Cloudflare 403s a bare HEAD on the StaticData path — and the probe body is
// closed unread. The total comes from Content-Range, or Content-Length when
// the server ignores the range.
func FetchPDF(f PDFFetcher, url string, maxBytes int64) ([]byte, error) {
	probe, err := f.GetStream(url, map[string]string{"Range": "bytes=0-0"})
	if err != nil {
		return nil, fmt.Errorf("size probe: %w", err)
	}
	// Status before size: a 403 (Cloudflare block) must surface as a retryable
	// fetch error, never as ErrPDFTooLarge — the CDN's 403 body could carry a
	// Content-Length/Content-Range that only coincidentally exceeds the cap.
	probe.Body.Close()
	if probe.StatusCode >= 400 {
		return nil, fmt.Errorf("size probe: http status %d", probe.StatusCode)
	}
	if probeSize(probe) > maxBytes {
		return nil, ErrPDFTooLarge
	}
	return downloadBounded(f, url, maxBytes)
}

// probeSize extracts the full resource size from a ranged-GET response,
// preferring Content-Range ("bytes 0-0/TOTAL") and falling back to
// Content-Length. -1 means unknown.
func probeSize(resp *http.Response) int64 {
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if _, after, ok := strings.Cut(cr, "/"); ok {
			if total, err := strconv.ParseInt(strings.TrimSpace(after), 10, 64); err == nil {
				return total
			}
		}
	}
	return resp.ContentLength
}

// downloadBounded fetches a URL through the session-aware fetcher into memory,
// aborting once the body exceeds maxBytes. The Content-Length check is a fast
// path; the LimitedReader is the enforcement for chunked/unknown-size responses.
func downloadBounded(f PDFFetcher, url string, maxBytes int64) ([]byte, error) {
	resp, err := f.GetStream(url, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	if resp.ContentLength > maxBytes {
		return nil, ErrPDFTooLarge
	}
	lr := &io.LimitedReader{R: resp.Body, N: maxBytes + 1}
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, ErrPDFTooLarge
	}
	return data, nil
}

// DisclosureTextKey builds the R2 key for extracted disclosure text, following
// the rss_xml content-hash scheme: kind/ticker/sha256(pdf_url)[:16].txt. The
// content-addressed key makes re-extraction idempotent.
func DisclosureTextKey(d *entity.Disclosure) string {
	sum := sha256.Sum256([]byte(d.PdfURL))
	ticker := "unknown"
	if d.Ticker != nil {
		ticker = *d.Ticker
	}
	return fmt.Sprintf("disclosure_text/%s/%x.txt", ticker, sum[:16])
}

// DisclosureTextPersister owns the ADR-0004 Raw File / Extraction Status
// contract: store extracted text on R2 (claim-checked via raw_files) and move
// the disclosure's Extraction Status to ok. Shared by the extract:disclosure
// task and the fetch_disclosure_pdf tool so the contract has a single owner.
type DisclosureTextPersister struct {
	Store       storage.ObjectStore
	RawFiles    *repository.RawFileRepository
	Disclosures *repository.DisclosureRepository
	DB          *sqlx.DB
}

// Persist stores text for a disclosure and returns the R2 key. The disclosure's
// status is only moved to ok on full success; any step failing returns an error
// and leaves the caller to decide retry vs. permanent failure.
func (p *DisclosureTextPersister) Persist(ctx context.Context, d *entity.Disclosure, text string) (string, error) {
	key := DisclosureTextKey(d)
	if err := p.Store.PutObject(ctx, key, []byte(text)); err != nil {
		return "", fmt.Errorf("r2 put: %w", err)
	}
	size := int64(len(text))
	sourceRef := d.PdfURL
	rf := &entity.RawFile{
		StorageKey:    key,
		Kind:          "disclosure_text",
		SourceRef:     &sourceRef,
		SizeBytes:     &size,
		RetentionDays: DisclosureTextRetentionDays,
	}
	if err := p.RawFiles.Insert(p.DB, rf); err != nil {
		return "", fmt.Errorf("raw_files insert: %w", err)
	}
	if err := p.Disclosures.UpdateExtractionStatus(p.DB, d.ID, "ok", &key, nil); err != nil {
		return "", fmt.Errorf("status update: %w", err)
	}
	return key, nil
}
