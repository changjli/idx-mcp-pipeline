package tasks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/extract"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

const (
	// extractMaxRetry disables asynq's retry clock for extract:disclosure: the
	// self-managed extractRetryDelays ladder is the single owner of the retry
	// policy (issue 06). Two clocks on one task risked a retry storm.
	extractMaxRetry = 0
	// extractConcurrency caps concurrent extract tasks (RAM: 3 x 10MB PDFs +
	// overhead stays well under 512MB). asynq has no per-type concurrency, so
	// the handler gates itself with a semaphore.
	extractConcurrency = 3
)

// extractRetryDelays is the single retry budget for extract:disclosure
// transient failures (network, timeout, 5xx). asynq retry is disabled
// (extractMaxRetry = 0) so the ladder owns the policy alone. Delays are longer
// than asynq's default so a bad PDF or a Cloudflare block doesn't hammer
// idx.co.id. Index = attempt number.
var extractRetryDelays = []time.Duration{30 * time.Second, 2 * time.Minute}

// ExtractDisclosurePayload is the payload for an extract:disclosure task.
type ExtractDisclosurePayload struct {
	DisclosureID int64 `json:"disclosure_id"`
	Attempt      int   `json:"attempt"` // self-managed retry counter
}

// EnqueueExtractDisclosure enqueues an extract:disclosure task for one
// disclosure. Uses a per-disclosure TaskID for dedup. Returns
// ErrTaskIDConflict if already enqueued. Chained from filter:disclosures.
func EnqueueExtractDisclosure(enq pipeline.Enqueuer, id int64) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeExtractDisclosure).Enqueue(enq, time.Time{}, []string{"id=" + strconv.FormatInt(id, 10)})
}

// extractDisclosureEnqueuer adapts EnqueueExtractDisclosure to the usecase's
// async seam (issue 05b): enqueue an extract:disclosure task for one
// disclosure id. The per-disclosure TaskID dedup surfaces as
// asynq.ErrTaskIDConflict, which the usecase maps to the pending envelope.
type extractDisclosureEnqueuer struct{ enq pipeline.Enqueuer }

func (e extractDisclosureEnqueuer) EnqueueExtractDisclosure(id int64) error {
	_, err := EnqueueExtractDisclosure(e.enq, id)
	return err
}

// NewExtractDisclosureEnqueuer returns the usecase's async enqueue seam over
// the given pipeline.Enqueuer (the asynq client in production).
func NewExtractDisclosureEnqueuer(enq pipeline.Enqueuer) usecase.ExtractDisclosureEnqueuer {
	return extractDisclosureEnqueuer{enq: enq}
}

// reenqueueExtractDisclosure re-enqueues an extract:disclosure task with a
// delay. Uses a unique TaskID (no dedup key) because the current task still
// holds the per-disclosure ID while active.
func reenqueueExtractDisclosure(enq pipeline.Enqueuer, id int64, attempt int, delay time.Duration) error {
	stage := pipeline.NewIngestStage(TypeExtractDisclosure, nil, enq, extractMaxRetry)
	return stage.Reenqueue(ExtractDisclosurePayload{DisclosureID: id, Attempt: attempt}, delay)
}

// NewExtractDisclosureHandler returns an asynq handler for the
// extract:disclosure task type. It downloads one disclosure PDF (bounded to
// 10MB), extracts text in-memory, and stores the text on R2 with a raw_files
// claim-check row. Per-disclosure isolation: each task is its own asynq task,
// so one malformed PDF fails alone. Concurrency is capped by a semaphore.
func NewExtractDisclosureHandler(
	log *logrus.Logger,
	enq pipeline.Enqueuer,
	fetcher extract.PDFFetcher,
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

		p, err := pipeline.DecodeTask[ExtractDisclosurePayload](t)
		if err != nil {
			return err
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

		taskID := pipeline.TaskID(ctx)
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
				return reenqueueExtractDisclosure(enq, d.ID, attempt, delay)
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
	fetcher        extract.PDFFetcher
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
	data, err := extract.FetchPDF(h.fetcher, d.PdfURL, extract.MaxPDFBytes)
	if err != nil {
		if errors.Is(err, extract.ErrPDFTooLarge) {
			return h.failPermanent(d.ID, "too_large", nil)
		}
		return h.retryOrGiveUp(d.ID, attempt, "download_failed", err)
	}

	// Extract text in-memory under a hard timeout.
	ectx, cancel := context.WithTimeout(ctx, extract.ExtractTimeout)
	defer cancel()
	text, err := h.extractor.Extract(ectx, data)
	if err != nil {
		return h.retryOrGiveUp(d.ID, attempt, "extract_failed", err)
	}
	if strings.TrimSpace(text) == "" {
		// Image-scanned PDF: no text layer. No retry — a scan won't grow text.
		return h.failPermanent(d.ID, "empty_text", nil)
	}

	// Store extracted text on R2 (claim-checked via raw_files) and move the
	// Extraction Status to ok — the shared ADR-0004 contract.
	persister := &extract.DisclosureTextPersister{
		Store:       h.r2Store,
		RawFiles:    h.rawFileRepo,
		Disclosures: h.disclosureRepo,
		DB:          h.db,
	}
	key, err := persister.Persist(ctx, d, text)
	if err != nil {
		return h.retryOrGiveUp(d.ID, attempt, "persist_failed", err)
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
