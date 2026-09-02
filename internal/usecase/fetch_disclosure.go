package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/extract"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// FetchDisclosureUseCase fetches and extracts a single Disclosure's PDF on
// demand, persisting the text under the ADR-0004 Raw File / Extraction Status
// contract and returning it. It is the live counterpart to the read-only
// DisclosureUseCase: where read_idx_disclosure serves pre-extracted text,
// fetch_disclosure_pdf covers the pending/failed/evicted cases the daily
// extract never produced text for (e.g. disclosure 27883).
type FetchDisclosureUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	Validate       *validator.Validate
	DisclosureRepo *repository.DisclosureRepository
	RawFileRepo    *repository.RawFileRepository
	Fetcher        extract.PDFFetcher
	Store          storage.ObjectStore
	Extractor      extract.Extractor
}

func NewFetchDisclosureUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	disclosureRepo *repository.DisclosureRepository,
	rawFileRepo *repository.RawFileRepository,
	fetcher extract.PDFFetcher,
	store storage.ObjectStore,
	extractor extract.Extractor,
) *FetchDisclosureUseCase {
	return &FetchDisclosureUseCase{
		DB:             db,
		Log:            log,
		Validate:       validate,
		DisclosureRepo: disclosureRepo,
		RawFileRepo:    rawFileRepo,
		Fetcher:        fetcher,
		Store:          store,
		Extractor:      extractor,
	}
}

// FetchDisclosurePDFData is a fetch_disclosure_pdf response. Mirrors
// ReadIdxDisclosureData: Text is non-null only when Status is "ok"; Error is
// populated only when Status is "failed".
type FetchDisclosurePDFData struct {
	Ticker *string `json:"ticker"`
	Title  string  `json:"title"`
	Date   string  `json:"date"`
	PdfURL string  `json:"pdf_url"`
	Text   *string `json:"text"`
	Status string  `json:"status"`
	Error  *string `json:"error,omitempty"`
}

// FetchDisclosurePDF fetches one disclosure's PDF on demand, extracts its
// text, persists it to R2 + raw_files, and moves the Extraction Status to ok
// (or failed). When the disclosure already has retrievable text (status ok),
// the stored text is returned without an upstream call — the tool is
// idempotent and never burns Cloudflare quota re-fetching what the daily
// extract already produced.
func (uc *FetchDisclosureUseCase) FetchDisclosurePDF(ctx context.Context, id int64) (*FetchDisclosurePDFData, error) {
	d, err := uc.DisclosureRepo.FindByID(uc.DB, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find disclosure %d: %w", id, err)
	}

	// Fast path: text already extracted and retrievable — serve it, no fetch.
	if d.ExtractionStatus == "ok" && d.TextR2Key != nil && uc.Store != nil {
		data, evicted, err := fetchDisclosureText(ctx, uc.DB, uc.RawFileRepo, uc.Store, *d.TextR2Key)
		if err != nil {
			return nil, err
		}
		if !evicted {
			text := truncateDisclosureText(string(data))
			return &FetchDisclosurePDFData{
				Ticker: d.Ticker,
				Title:  d.Title,
				Date:   d.AnnouncementDate.Format("2006-01-02"),
				PdfURL: d.PdfURL,
				Text:   &text,
				Status: "ok",
			}, nil
		}
		// Evicted: fall through to a live re-fetch.
	}

	if uc.Store == nil {
		// Environment state, not a disclosure failure: the extract task leaves
		// rows pending in this state. The tool cannot fulfill its persist
		// contract without R2.
		return nil, fmt.Errorf("r2 not configured")
	}

	// Live path: session-aware fetch (ranged-GET size probe, then bounded
	// download), extract in-memory under a hard timeout, persist.
	data, err := extract.FetchPDF(uc.Fetcher, d.PdfURL, extract.MaxPDFBytes)
	if err != nil {
		if errors.Is(err, extract.ErrPDFTooLarge) {
			return uc.fail(d, "too_large", nil)
		}
		return uc.fail(d, "download_failed", err)
	}

	ectx, cancel := context.WithTimeout(ctx, extract.ExtractTimeout)
	defer cancel()
	text, err := uc.Extractor.Extract(ectx, data)
	if err != nil {
		return uc.fail(d, "extract_failed", err)
	}
	if strings.TrimSpace(text) == "" {
		// Image-scanned PDF: no text layer. No retry — a scan won't grow text.
		return uc.fail(d, "empty_text", nil)
	}

	persister := &extract.DisclosureTextPersister{
		Store:       uc.Store,
		RawFiles:    uc.RawFileRepo,
		Disclosures: uc.DisclosureRepo,
		DB:          uc.DB,
	}
	if _, err := persister.Persist(ctx, d, text); err != nil {
		return uc.fail(d, "persist_failed", err)
	}

	truncated := truncateDisclosureText(text)
	return &FetchDisclosurePDFData{
		Ticker: d.Ticker,
		Title:  d.Title,
		Date:   d.AnnouncementDate.Format("2006-01-02"),
		PdfURL: d.PdfURL,
		Text:   &truncated,
		Status: "ok",
	}, nil
}

// fail marks the disclosure failed and returns the failed envelope — a success
// body the consumer reads via Status, not an MCP protocol error.
func (uc *FetchDisclosureUseCase) fail(d *entity.Disclosure, reason string, err error) (*FetchDisclosurePDFData, error) {
	msg := reason
	if err != nil {
		msg = fmt.Sprintf("%s: %v", reason, err)
	}
	if uerr := uc.DisclosureRepo.UpdateExtractionStatus(uc.DB, d.ID, "failed", nil, &msg); uerr != nil {
		uc.Log.Warnf("fetch_disclosure_pdf: mark disclosure %d failed: %v", d.ID, uerr)
	}
	return &FetchDisclosurePDFData{
		Ticker: d.Ticker,
		Title:  d.Title,
		Date:   d.AnnouncementDate.Format("2006-01-02"),
		PdfURL: d.PdfURL,
		Status: "failed",
		Error:  &msg,
	}, nil
}
