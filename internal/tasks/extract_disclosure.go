package tasks

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"database/sql"
	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/extract"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

const (
	// extractMaxPDFBytes caps a disclosure PDF at 10MB — probed via a ranged
	// GET before download and enforced again by the bounded read buffer.
	extractMaxPDFBytes = 10 * 1024 * 1024
	// extractTimeout caps text extraction per disclosure (30s). OCR (ticket
	// 16) will need its own budget — scans are slower than text layers.
	extractTimeout = 30 * time.Second
	// extractConcurrency caps concurrent extract tasks (RAM: 3 x 10MB PDFs +
	// overhead stays well under 512MB). asynq has no per-type concurrency, so
	// the handler gates itself with a semaphore.
	extractConcurrency = 3
	// disclosureTextRetentionDays is the raw_files retention for extracted
	// disclosure text (90 days; metadata survives eviction).
	disclosureTextRetentionDays = 90
)

// pdfFetcher is the injected seam for disclosure PDF downloads. The caller
// owns the returned response body and must close it — no buffering, no
// caching. client.Client's GetStream satisfies it with the full Cloudflare
// session (cookies, browser headers, pacing); a fake backed by httptest keeps
// unit tests hermetic (no real upstream, per CLAUDE.md).
type pdfFetcher interface {
	GetStream(url string, extraHeaders map[string]string) (*http.Response, error)
}

// extractRetryDelays is the self-managed retry backoff for transient failures
// (network, timeout, 5xx). Longer than asynq's default so a bad PDF or a
// Cloudflare block doesn't hammer idx.co.id. Index = attempt number.
var extractRetryDelays = []time.Duration{30 * time.Second, 2 * time.Minute}

// errPDFTooLarge is returned when a download exceeds the size cap.
var errPDFTooLarge = errors.New("pdf exceeds size cap")

// ExtractDisclosurePayload is the payload for an extract:disclosure task.
type ExtractDisclosurePayload struct {
	DisclosureID int64 `json:"disclosure_id"`
	Attempt      int   `json:"attempt"` // self-managed retry counter
}

// EnqueueExtractDisclosure enqueues an extract:disclosure task for one
// disclosure. Uses a per-disclosure TaskID for dedup. Returns
// ErrTaskIDConflict if already enqueued. Chained from filter:disclosures.
func EnqueueExtractDisclosure(client *asynq.Client, id int64) (*asynq.TaskInfo, error) {
	taskKey := fmt.Sprintf("%s:%d", TypeExtractDisclosure, id)
	payload := ExtractDisclosurePayload{DisclosureID: id}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal extract:disclosure payload: %w", err)
	}
	task := asynq.NewTask(TypeExtractDisclosure, raw)
	return client.Enqueue(task,
		asynq.TaskID(taskKey),
		asynq.Queue("ingest"),
		asynq.MaxRetry(2),
		asynq.Retention(24*time.Hour),
	)
}

// reenqueueExtractDisclosure re-enqueues an extract:disclosure task with a
// delay. Uses a unique TaskID (no dedup key) because the current task still
// holds the per-disclosure ID while active.
func reenqueueExtractDisclosure(client *asynq.Client, id int64, attempt int, delay time.Duration) error {
	payload := ExtractDisclosurePayload{DisclosureID: id, Attempt: attempt}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	task := asynq.NewTask(TypeExtractDisclosure, raw)
	_, err = client.Enqueue(task,
		asynq.Queue("ingest"),
		asynq.ProcessIn(delay),
		asynq.MaxRetry(2),
		asynq.Retention(24*time.Hour),
	)
	return err
}

// NewExtractDisclosureHandler returns an asynq handler for the
// extract:disclosure task type. It downloads one disclosure PDF (bounded to
// 10MB), extracts text in-memory, and stores the text on R2 with a raw_files
// claim-check row. Per-disclosure isolation: each task is its own asynq task,
// so one malformed PDF fails alone. Concurrency is capped by a semaphore.
func NewExtractDisclosureHandler(
	log *logrus.Logger,
	client *asynq.Client,
	fetcher pdfFetcher,
	r2Store storage.ObjectStore,
	db *sqlx.DB,
	disclosureRepo *repository.DisclosureRepository,
	rawFileRepo *repository.RawFileRepository,
	extractor extract.Extractor,
) asynq.HandlerFunc {
	sem := make(chan struct{}, extractConcurrency)
	return func(ctx context.Context, t *asynq.Task) error {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		case <-ctx.Done():
			return ctx.Err()
		}

		var p ExtractDisclosurePayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		d, err := disclosureRepo.FindByID(db, p.DisclosureID)
		if errors.Is(err, sql.ErrNoRows) {
			log.Warnf("extract:disclosure: disclosure %d not found, skipping", p.DisclosureID)
			return nil
		}
		if err != nil {
			return fmt.Errorf("find disclosure %d: %w", p.DisclosureID, err)
		}

		if r2Store == nil {
			// Environment state, not a disclosure failure: leave the row
			// pending so a later run (R2 configured) re-enqueues it via the
			// filter's pending-extraction clause.
			log.Warnf("extract:disclosure: r2 not configured, leaving disclosure %d pending", d.ID)
			return nil
		}

		taskID, _ := asynq.GetTaskID(ctx)
		h := &extractDisclosureRunner{
			log:            log,
			taskID:         taskID,
			fetcher:        fetcher,
			r2Store:        r2Store,
			db:             db,
			disclosureRepo: disclosureRepo,
			rawFileRepo:    rawFileRepo,
			extractor:      extractor,
			reenqueue: func(attempt int, delay time.Duration) error {
				return reenqueueExtractDisclosure(client, d.ID, attempt, delay)
			},
		}
		return h.run(ctx, d, p.Attempt)
	}
}

// extractDisclosureRunner holds the extract task's dependencies so the core
// run() is testable without Redis (reenqueue is injected).
type extractDisclosureRunner struct {
	log            *logrus.Logger
	taskID         string
	fetcher        pdfFetcher
	r2Store        storage.ObjectStore
	db             *sqlx.DB
	disclosureRepo *repository.DisclosureRepository
	rawFileRepo    *repository.RawFileRepository
	extractor      extract.Extractor
	reenqueue      func(attempt int, delay time.Duration) error
}

// run executes one disclosure extraction. Transient failures mark the row
// failed and re-enqueue a delayed retry while the attempt budget remains;
// permanent failures (too_large, empty_text) mark failed and stop.
func (h *extractDisclosureRunner) run(ctx context.Context, d *entity.Disclosure, attempt int) error {
	// Session-aware fetch: ranged-GET size probe (Cloudflare 403s a bare HEAD)
	// then bounded download. Fetch failures are retryable; too_large is
	// permanent — a re-download won't shrink the PDF. Per-attempt download
	// budget is the client's idx.timeout (default 30s).
	data, err := fetchPDF(h.fetcher, d.PdfURL, extractMaxPDFBytes)
	if err != nil {
		if errors.Is(err, errPDFTooLarge) {
			return h.failPermanent(d.ID, "too_large", nil)
		}
		return h.retryOrGiveUp(d.ID, attempt, "download_failed", err)
	}

	// Extract text in-memory under a hard timeout.
	ectx, cancel := context.WithTimeout(ctx, extractTimeout)
	defer cancel()
	text, err := h.extractor.Extract(ectx, data)
	if err != nil {
		return h.retryOrGiveUp(d.ID, attempt, "extract_failed", err)
	}
	if strings.TrimSpace(text) == "" {
		// Image-scanned PDF: no text layer. No retry — a scan won't grow text.
		return h.failPermanent(d.ID, "empty_text", nil)
	}

	// Store extracted text on R2 (claim-checked via raw_files).
	key := disclosureTextKey(d)
	if err := h.r2Store.PutObject(ctx, key, []byte(text)); err != nil {
		return h.retryOrGiveUp(d.ID, attempt, "r2_put_failed", err)
	}

	size := int64(len(text))
	sourceRef := d.PdfURL
	rf := &entity.RawFile{
		StorageKey:    key,
		Kind:          "disclosure_text",
		SourceRef:     &sourceRef,
		SizeBytes:     &size,
		RetentionDays: disclosureTextRetentionDays,
	}
	if err := h.rawFileRepo.Insert(h.db, rf); err != nil {
		return h.retryOrGiveUp(d.ID, attempt, "raw_files_failed", err)
	}

	if err := h.disclosureRepo.UpdateExtractionStatus(h.db, d.ID, "ok", &key, nil); err != nil {
		return h.retryOrGiveUp(d.ID, attempt, "status_update_failed", err)
	}
	logEvent(h.log, logrus.InfoLevel, "extraction_success", "disclosure text extracted and stored",
		logrus.Fields{
			"task_id":       h.taskID,
			"source":        TypeExtractDisclosure,
			"disclosure_id": d.ID,
			"ticker":        disclosureTicker(d),
			"size_bytes":    len(text),
			"r2_key":        key,
		})
	return nil
}

// retryOrGiveUp marks the disclosure failed and re-enqueues a delayed retry
// while the attempt budget remains; otherwise the failure is terminal.
func (h *extractDisclosureRunner) retryOrGiveUp(id int64, attempt int, reason string, err error) error {
	msg := reason
	if err != nil {
		msg = fmt.Sprintf("%s: %v", reason, err)
	}
	if err := h.disclosureRepo.UpdateExtractionStatus(h.db, id, "failed", nil, &msg); err != nil {
		return err
	}
	if attempt < len(extractRetryDelays) {
		delay := extractRetryDelays[attempt]
		if err := h.reenqueue(attempt+1, delay); err != nil {
			return err
		}
		logEvent(h.log, logrus.WarnLevel, "extraction_failed", "extraction failed, retrying",
			logrus.Fields{"task_id": h.taskID, "source": TypeExtractDisclosure, "disclosure_id": id, "reason": reason, "error": errMsg(err), "attempt": attempt, "retry_in_ms": delay.Milliseconds()})
		return nil
	}
	logEvent(h.log, logrus.WarnLevel, "extraction_failed", "extraction failed, giving up",
		logrus.Fields{"task_id": h.taskID, "source": TypeExtractDisclosure, "disclosure_id": id, "reason": reason, "error": errMsg(err), "attempt": attempt})
	return nil
}

// failPermanent marks the disclosure failed without retrying.
func (h *extractDisclosureRunner) failPermanent(id int64, reason string, err error) error {
	msg := reason
	if err != nil {
		msg = fmt.Sprintf("%s: %v", reason, err)
	}
	if err := h.disclosureRepo.UpdateExtractionStatus(h.db, id, "failed", nil, &msg); err != nil {
		return err
	}
	logEvent(h.log, logrus.WarnLevel, "extraction_failed", "extraction failed permanently",
		logrus.Fields{"task_id": h.taskID, "source": TypeExtractDisclosure, "disclosure_id": id, "reason": reason, "error": errMsg(err)})
	return nil
}

// errMsg flattens an error for the log field, or "" when nil.
func errMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// disclosureTicker returns the disclosure's ticker, or "unknown" when nil.
func disclosureTicker(d *entity.Disclosure) string {
	if d.Ticker == nil {
		return "unknown"
	}
	return *d.Ticker
}

// fetchPDF downloads a disclosure PDF through the session-aware fetcher,
// bounded to maxBytes. The size probe is a ranged GET (Range: bytes=0-0) —
// Cloudflare 403s a bare HEAD on the StaticData path — and the probe body is
// closed unread. The total comes from Content-Range, or Content-Length when
// the server ignores the range.
func fetchPDF(f pdfFetcher, url string, maxBytes int64) ([]byte, error) {
	probe, err := f.GetStream(url, map[string]string{"Range": "bytes=0-0"})
	if err != nil {
		return nil, fmt.Errorf("size probe: %w", err)
	}
	// Status before size: a 403 (Cloudflare block) must surface as a retryable
	// fetch error, never as errPDFTooLarge — the CDN's 403 body could carry a
	// Content-Length/Content-Range that only coincidentally exceeds the cap.
	probe.Body.Close()
	if probe.StatusCode >= 400 {
		return nil, fmt.Errorf("size probe: http status %d", probe.StatusCode)
	}
	if probeSize(probe) > maxBytes {
		return nil, errPDFTooLarge
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
func downloadBounded(f pdfFetcher, url string, maxBytes int64) ([]byte, error) {
	resp, err := f.GetStream(url, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http status %d", resp.StatusCode)
	}
	if resp.ContentLength > maxBytes {
		return nil, errPDFTooLarge
	}
	lr := &io.LimitedReader{R: resp.Body, N: maxBytes + 1}
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, errPDFTooLarge
	}
	return data, nil
}

// disclosureTextKey builds the R2 key for extracted disclosure text, following
// the rss_xml content-hash scheme: kind/ticker/sha256(pdf_url)[:16].txt. The
// content-addressed key makes re-extraction idempotent.
func disclosureTextKey(d *entity.Disclosure) string {
	sum := sha256.Sum256([]byte(d.PdfURL))
	ticker := "unknown"
	if d.Ticker != nil {
		ticker = *d.Ticker
	}
	return fmt.Sprintf("disclosure_text/%s/%x.txt", ticker, sum[:16])
}
