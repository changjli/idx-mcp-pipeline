package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// DisclosureTextStore fetches extracted disclosure text from object storage.
// *storage.R2Store satisfies this; tests use a fake. Only the read path is
// needed — writing lives in the extract task.
type DisclosureTextStore interface {
	GetObject(ctx context.Context, key string) ([]byte, error)
}

// DisclosureReader is the disclosure read surface the MCP server depends on.
// *DisclosureUseCase satisfies it; handler tests use a fake.
type DisclosureReader interface {
	ListIdxDisclosures(ctx context.Context, ticker string, date *string, limit int) (*DisclosureListData, error)
	ReadIdxDisclosure(ctx context.Context, id int64) (*ReadIdxDisclosureData, error)
	SearchDisclosures(ctx context.Context, category string, from, to *time.Time, limit int) (*DisclosureSearchData, error)
}

// maxDisclosureTextBytes bounds read_idx_disclosure's text payload to 64KB so
// one disclosure can't blow up the MCP response.
const maxDisclosureTextBytes = 64 * 1024

type DisclosureUseCase struct {
	DB             *sqlx.DB
	Log            *logrus.Logger
	Validate       *validator.Validate
	DisclosureRepo *repository.DisclosureRepository
	TextStore      DisclosureTextStore
	RawFileRepo    *repository.RawFileRepository
}

func NewDisclosureUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	disclosureRepo *repository.DisclosureRepository,
	textStore DisclosureTextStore,
	rawFileRepo *repository.RawFileRepository,
) *DisclosureUseCase {
	return &DisclosureUseCase{
		DB:             db,
		Log:            log,
		Validate:       validate,
		DisclosureRepo: disclosureRepo,
		TextStore:      textStore,
		RawFileRepo:    rawFileRepo,
	}
}

// DisclosureListItem is one disclosure row in a list_idx_disclosures response.
// Metadata only — no extracted text.
type DisclosureListItem struct {
	DisclosureID     int64    `json:"disclosure_id"`
	Title            string   `json:"title"`
	Date             string   `json:"date"`
	Categories       []string `json:"categories"`
	PassedFilter     *bool    `json:"passed_filter"`
	ExtractionStatus string   `json:"extraction_status"`
}

// DisclosureListData is the data payload of a list_idx_disclosures response.
type DisclosureListData struct {
	Ticker      string               `json:"ticker"`
	Disclosures []DisclosureListItem `json:"disclosures"`
}

// ListIdxDisclosures returns a ticker's disclosure metadata, newest first,
// optionally filtered to one announcement date ("YYYY-MM-DD"). limit caps the
// result. No extracted text is returned — metadata only.
func (uc *DisclosureUseCase) ListIdxDisclosures(ctx context.Context, ticker string, date *string, limit int) (*DisclosureListData, error) {
	if date != nil && *date != "" {
		if _, err := time.Parse("2006-01-02", *date); err != nil {
			return nil, ErrInvalidArgument
		}
	}
	rows, err := uc.DisclosureRepo.FindByTickerWithDate(uc.DB, ticker, date, limit)
	if err != nil {
		return nil, fmt.Errorf("query disclosures: %w", err)
	}

	resp := &DisclosureListData{Ticker: ticker, Disclosures: []DisclosureListItem{}}
	for _, r := range rows {
		resp.Disclosures = append(resp.Disclosures, DisclosureListItem{
			DisclosureID:     r.ID,
			Title:            r.Title,
			Date:             r.AnnouncementDate.Format("2006-01-02"),
			Categories:       []string(r.Categories),
			PassedFilter:     r.PassedFilter,
			ExtractionStatus: r.ExtractionStatus,
		})
	}
	return resp, nil
}

// DisclosureSearchItem is one disclosure row in a search_disclosures response.
// Cross-ticker, so each row carries its own ticker.
type DisclosureSearchItem struct {
	DisclosureID     int64    `json:"disclosure_id"`
	Ticker           string   `json:"ticker"`
	Title            string   `json:"title"`
	Date             string   `json:"date"`
	Categories       []string `json:"categories"`
	PassedFilter     *bool    `json:"passed_filter"`
	ExtractionStatus string   `json:"extraction_status"`
}

// DisclosureSearchData is the data payload of a search_disclosures response.
type DisclosureSearchData struct {
	Query       string                 `json:"query"`
	Disclosures []DisclosureSearchItem `json:"disclosures"`
}

// SearchDisclosures returns disclosures across tickers whose title or
// categories match the keyword (case-insensitive substring), within an
// optional announcement date range, newest first. from/to are inclusive; nil
// means unbounded. limit caps the result. No passed_filter gate — the filter
// whitelist is narrow, so title matching is how the AI discovers disclosures
// that were never categorized. No extracted text is returned — metadata only.
func (uc *DisclosureUseCase) SearchDisclosures(ctx context.Context, query string, from, to *time.Time, limit int) (*DisclosureSearchData, error) {
	if strings.TrimSpace(query) == "" {
		return nil, ErrInvalidArgument
	}
	if from != nil && to != nil && from.After(*to) {
		return nil, ErrInvalidRange
	}

	rows, err := uc.DisclosureRepo.SearchByKeyword(uc.DB, query, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("search disclosures: %w", err)
	}

	resp := &DisclosureSearchData{Query: query, Disclosures: []DisclosureSearchItem{}}
	for _, r := range rows {
		ticker := ""
		if r.Ticker != nil {
			ticker = *r.Ticker
		}
		resp.Disclosures = append(resp.Disclosures, DisclosureSearchItem{
			DisclosureID:     r.ID,
			Ticker:           ticker,
			Title:            r.Title,
			Date:             r.AnnouncementDate.Format("2006-01-02"),
			Categories:       []string(r.Categories),
			PassedFilter:     r.PassedFilter,
			ExtractionStatus: r.ExtractionStatus,
		})
	}
	return resp, nil
}

// ReadIdxDisclosureData is a read_idx_disclosure response. Text is non-null
// only when status is "ok"; Error is populated only when status is "failed".
// A pending/failed/evicted disclosure is a success body — the consumer reads
// status instead of parsing an error.
type ReadIdxDisclosureData struct {
	Ticker *string `json:"ticker"`
	Title  string  `json:"title"`
	Date   string  `json:"date"`
	PdfURL string  `json:"pdf_url"`
	Text   *string `json:"text"`
	Status string  `json:"status"`
	Error  *string `json:"error,omitempty"`
}

// ReadIdxDisclosure returns one disclosure's metadata plus its pre-extracted
// text when available. Pure storage reader — no upstream network fetch.
//
// status mirrors extraction_status: "pending" (not yet processed by today's
// job), "ok" (text fetched and truncated to 64KB), "failed" (extraction errored
// or exceeded caps, error field populated), or "evicted" (text R2 object past
// the 90-day retention — metadata still served).
func (uc *DisclosureUseCase) ReadIdxDisclosure(ctx context.Context, id int64) (*ReadIdxDisclosureData, error) {
	d, err := uc.DisclosureRepo.FindByID(uc.DB, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("find disclosure %d: %w", id, err)
	}

	resp := &ReadIdxDisclosureData{
		Ticker: d.Ticker,
		Title:  d.Title,
		Date:   d.AnnouncementDate.Format("2006-01-02"),
		PdfURL: d.PdfURL,
		Status: d.ExtractionStatus,
	}
	if d.ExtractionStatus == "failed" {
		resp.Error = d.ExtractionError
	}
	if d.ExtractionStatus != "ok" {
		return resp, nil
	}
	if d.TextR2Key == nil {
		// Corrupt row: extract sets text_r2_key atomically with status 'ok'.
		// Downgrade rather than claim ok with no way to serve text.
		uc.Log.Warnf("read_idx_disclosure: disclosure %d ok without text key", d.ID)
		resp.Status = "pending"
		return resp, nil
	}
	if uc.TextStore == nil {
		// R2 not configured — the extract task leaves rows pending in this
		// state, so an ok row here is corrupt; downgrade to pending rather
		// than claim ok with text unavailable.
		uc.Log.Warnf("read_idx_disclosure: text store not configured, disclosure %d downgraded to pending", d.ID)
		resp.Status = "pending"
		return resp, nil
	}

	data, evicted, err := fetchDisclosureText(ctx, uc.DB, uc.RawFileRepo, uc.TextStore, *d.TextR2Key)
	if err != nil {
		return nil, err
	}
	if evicted {
		resp.Status = "evicted"
		return resp, nil
	}
	text := truncateDisclosureText(string(data))
	resp.Text = &text
	return resp, nil
}

// fetchDisclosureText reads a disclosure's extracted text, reporting eviction
// through the bool return: either the claim-check row is marked deleted or the
// R2 object no longer exists. A missing raw_files row is fine — the row is only
// required for eviction signaling, not for serving. Shared by the read and
// live-fetch disclosure usecases.
func fetchDisclosureText(ctx context.Context, db *sqlx.DB, rawFileRepo *repository.RawFileRepository, store DisclosureTextStore, key string) (data []byte, evicted bool, err error) {
	// The claim-check row is optional — it only signals eviction. A nil repo
	// (alternate wiring) skips the check and relies on the R2 404 path.
	if rawFileRepo != nil {
		rf, rerr := rawFileRepo.FindByStorageKey(db, key)
		if rerr != nil && !errors.Is(rerr, sql.ErrNoRows) {
			return nil, false, rerr
		}
		if rf != nil && rf.DeletedAt != nil {
			return nil, true, nil
		}
	}

	data, err = store.GetObject(ctx, key)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("fetch disclosure text %s: %w", key, err)
	}
	return data, false, nil
}

// truncateDisclosureText caps text at maxDisclosureTextBytes bytes, backing
// off to a UTF-8 rune boundary so the cut never splits a multibyte character.
func truncateDisclosureText(s string) string {
	if len(s) <= maxDisclosureTextBytes {
		return s
	}
	cut := maxDisclosureTextBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
