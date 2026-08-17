package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// announcementsMaxAgeSeconds is the source_status max age for
	// idx:announcements (24 hours).
	announcementsMaxAgeSeconds int32 = 86400
	// DefaultAnnouncementLookbackDays is the initial fetch window (days) when
	// no high_water_mark exists yet. Configurable via
	// idx.disclosure_lookback_days in config.json.
	DefaultAnnouncementLookbackDays = 7
	// announcementsPageSize is the page size for the GetAnnouncement endpoint.
	// indexFrom is a 0-based page number (not a row offset): page 0 = rows
	// [0, pageSize), page 1 = rows [pageSize, 2*pageSize), etc. ResultCount is
	// the true filtered total. 500 maximizes single-request coverage.
	announcementsPageSize = 500
	// announcementsMaxPages caps pagination defensively against a runaway loop.
	announcementsMaxPages = 100
	// announcementsReferer matches the IDX page hosting the announcement list.
	announcementsReferer = "https://www.idx.co.id/en/listed-companies/company-announcement/"
)

// AnnouncementsPayload is the payload for an idx:announcements task.
type AnnouncementsPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// AnnouncementsResponse is the offset-paginated wrapper returned by the IDX
// GetAnnouncement endpoint.
type AnnouncementsResponse struct {
	ResultCount int                 `json:"ResultCount"`
	Replies     []AnnouncementReply `json:"Replies"`
}

// AnnouncementReply is one announcement and its PDF attachments.
type AnnouncementReply struct {
	Pengumuman  AnnouncementMeta         `json:"pengumuman"`
	Attachments []AnnouncementAttachment `json:"attachments"`
}

// AnnouncementMeta is the announcement header.
type AnnouncementMeta struct {
	ID2             string `json:"Id2"`
	NoPengumuman    string `json:"NoPengumuman"`
	TglPengumuman   string `json:"TglPengumuman"` // "2026-08-08T17:22:09" (WIB, no zone suffix)
	JudulPengumuman string `json:"JudulPengumuman"`
	KodeEmiten      string `json:"Kode_Emiten"` // space-padded issuer code
}

// AnnouncementAttachment is a single PDF attachment URL.
type AnnouncementAttachment struct {
	PDFFilename  string `json:"PDFFilename"`
	FullSavePath string `json:"FullSavePath"`
}

// EnqueueAnnouncements enqueues an idx:announcements task for the given date.
// Uses a date-keyed TaskID for dedup. Returns ErrTaskIDConflict if already
// enqueued. Extra opts (e.g. asynq.ProcessIn) are appended to the defaults.
func EnqueueAnnouncements(client *asynq.Client, date time.Time, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeAnnouncements, dateKey)
	payload := AnnouncementsPayload{Date: dateKey}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal announcements payload: %w", err)
	}

	task := asynq.NewTask(TypeAnnouncements, raw)
	options := []asynq.Option{
		asynq.TaskID(taskKey),
		asynq.Queue("ingest"),
		asynq.MaxRetry(3),
		asynq.Retention(24 * time.Hour),
	}
	options = append(options, opts...)
	return client.Enqueue(task, options...)
}

// NewAnnouncementsHandler returns an asynq handler for the idx:announcements
// task type. It fetches IDX material announcement metadata over an
// incremental date window, upserts one disclosures row per PDF attachment, and
// advances the source_status high_water_mark. Metadata only — no PDF download
// or text extraction (that's ticket 11).
func NewAnnouncementsHandler(
	log *logrus.Logger,
	idxClient *client.Client,
	db *sqlx.DB,
	tickerRepo *repository.TickerRepository,
	disclosureRepo *repository.DisclosureRepository,
	sourceStatusRepo *repository.SourceStatusRepository,
	alertRepo *repository.AlertRepository,
	lookbackDays int,
) asynq.HandlerFunc {
	if lookbackDays <= 0 {
		lookbackDays = DefaultAnnouncementLookbackDays
	}
	return func(ctx context.Context, t *asynq.Task) error {
		var p AnnouncementsPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		runDate, err := time.Parse("2006-01-02", p.Date)
		if err != nil {
			return fmt.Errorf("invalid date %q: %w", p.Date, err)
		}

		log.Infof("announcements: fetching metadata, run date=%s", p.Date)

		// Incremental window: start from the high_water_mark date when one
		// exists, else a recent lookback window on first run. The window end is
		// the run date. Overlap is safe — upserts are idempotent via pdf_url.
		from := runDate.AddDate(0, 0, -lookbackDays)
		if status, err := sourceStatusRepo.FindBySource(db, TypeAnnouncements); err == nil && status.HighWaterMark != nil {
			from = status.HighWaterMark.Truncate(24 * time.Hour)
		}
		if from.After(runDate) {
			from = runDate
		}

		taskID, _ := asynq.GetTaskID(ctx)
		start := time.Now()
		logEvent(log, logrus.InfoLevel, "fetch_start", "fetching announcement metadata",
			logrus.Fields{"task_id": taskID, "source": TypeAnnouncements, "date": p.Date, "fetch_url": announcementsPath(from, runDate, 0)})
		replies, fetchErr := fetchAnnouncements(idxClient, from, runDate, log)
		latency := time.Since(start).Milliseconds()
		if fetchErr != nil {
			logEvent(log, logrus.ErrorLevel, "fetch_failure", "announcements fetch failed",
				logrus.Fields{"task_id": taskID, "source": TypeAnnouncements, "date": p.Date, "error": fetchErr.Error(), "latency_ms": latency})
			recordSourceFailure(db, sourceStatusRepo, alertRepo, TypeAnnouncements, announcementsMaxAgeSeconds, p.Date, fetchErr, log)
			return fetchErr
		}

		logEvent(log, logrus.InfoLevel, "fetch_success", "announcement metadata fetched",
			logrus.Fields{"task_id": taskID, "source": TypeAnnouncements, "date": p.Date, "rows": len(replies), "latency_ms": latency})

		upserted, upsertErr := upsertDisclosureRows(db, tickerRepo, disclosureRepo, replies, log)
		if upsertErr != nil {
			// Surface so asynq retries and the watermark stays put — otherwise
			// a row dropped mid-upsert would be skipped forever on the next run.
			log.Errorf("announcements: upsert failed: %v", upsertErr)
			recordSourceFailure(db, sourceStatusRepo, alertRepo, TypeAnnouncements, announcementsMaxAgeSeconds, p.Date, upsertErr, log)
			return upsertErr
		}
		log.Infof("announcements: upserted %d disclosure row(s)", upserted)

		// Advance high_water_mark to the most recent announcement date seen.
		// Monotonic: the endpoint's result ordering is unstable (server-side
		// index lag), so a run may return older items — never regress the
		// watermark below its current value or we'd re-scan the same backlog.
		watermark := maxAnnouncementDate(replies)
		if current, err := sourceStatusRepo.FindBySource(db, TypeAnnouncements); err == nil && current.HighWaterMark != nil {
			if watermark == nil || current.HighWaterMark.After(*watermark) {
				watermark = current.HighWaterMark
			}
		}
		recordSourceSuccess(db, sourceStatusRepo, TypeAnnouncements, announcementsMaxAgeSeconds, watermark, log)

		return nil
	}
}

// announcementsPath builds the IDX GetAnnouncement endpoint path for one page
// of the [from, to] date window. Shared by the handler (fetch_url log field)
// and the paginated fetch.
func announcementsPath(from, to time.Time, indexFrom int) string {
	return fmt.Sprintf(
		"/primary/ListedCompany/GetAnnouncement?kodeEmiten=&emitenType=*&indexFrom=%d&pageSize=%d&dateFrom=%s&dateTo=%s&lang=id&keyword=",
		indexFrom, announcementsPageSize, from.Format("20060102"), to.Format("20060102"),
	)
}

// fetchAnnouncements calls the IDX GetAnnouncement API with offset pagination
// over the [from, to] date window and returns every announcement reply.
func fetchAnnouncements(idxClient *client.Client, from, to time.Time, log *logrus.Logger) ([]AnnouncementReply, error) {
	var all []AnnouncementReply
	total := 0
	indexFrom := 0

	for page := 0; page < announcementsMaxPages; page++ {
		path := announcementsPath(from, to, indexFrom)
		headers := map[string]string{"Referer": announcementsReferer}
		resp, err := idxClient.GetWithHeaders(path, headers)
		if err != nil {
			return nil, fmt.Errorf("idx get: %w", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response body: %w", err)
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("idx api error: status=%d body=%s", resp.StatusCode, truncate(string(body), 200))
		}

		var pageResp AnnouncementsResponse
		if err := json.Unmarshal(body, &pageResp); err != nil {
			return nil, fmt.Errorf("parse response: %w (body=%s)", err, truncate(string(body), 200))
		}
		if total == 0 {
			total = pageResp.ResultCount
		}
		all = append(all, pageResp.Replies...)
		log.Debugf("announcements: page indexFrom=%d got %d reply(s) (total=%d)", indexFrom, len(pageResp.Replies), total)

		// Termination: empty page, or we've collected the advertised count.
		if len(pageResp.Replies) == 0 || total > 0 && len(all) >= total {
			break
		}
		indexFrom++
	}

	// A truncated window is silently partial — surface it so operators know
	// metadata ingestion is incomplete. Can happen when ResultCount is
	// misreported or the server-side index lags (returns fewer rows than the
	// advertised count).
	if total > 0 && len(all) < total {
		log.Warnf("announcements: truncated fetch — got %d of ResultCount=%d", len(all), total)
	}
	return all, nil
}

// replyToDisclosures flattens one announcement reply into one Disclosure row
// per PDF attachment. Rows carry a trimmed ticker (IDX pads Kode_Emiten with
// spaces), the announcement date, and the attachment index. Attachments without
// a PDF URL are skipped. New rows default to extraction_status='pending' and
// passed_filter=NULL (pending — the filter task owns the transition); the
// pdf_url is the idempotency key. Returns nil when the announcement date
// doesn't parse.
func replyToDisclosures(reply AnnouncementReply) []*entity.Disclosure {
	date, err := parseAnnouncementDate(reply.Pengumuman.TglPengumuman)
	if err != nil {
		return nil
	}

	ticker := strings.TrimSpace(reply.Pengumuman.KodeEmiten)
	var tickerPtr *string
	if ticker != "" {
		tickerPtr = &ticker
	}

	title := strings.TrimSpace(reply.Pengumuman.JudulPengumuman)

	var out []*entity.Disclosure
	for i, att := range reply.Attachments {
		url := strings.TrimSpace(att.FullSavePath)
		if url == "" {
			continue
		}
		out = append(out, &entity.Disclosure{
			Ticker:           tickerPtr,
			AnnouncementDate: date,
			Title:            title,
			PdfURL:           url,
			AttachmentIdx:    int32(i),
			FetchedAt:        time.Now(),
		})
	}
	return out
}

// parseAnnouncementDate parses the IDX announcement timestamp format
// ("2006-01-02T15:04:05", WIB, no timezone suffix).
func parseAnnouncementDate(s string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04:05", strings.TrimSpace(s))
}

// maxAnnouncementDate returns the latest announcement timestamp across all
// replies, or nil if none parse. Used to advance the incremental watermark.
func maxAnnouncementDate(replies []AnnouncementReply) *time.Time {
	var max *time.Time
	for _, r := range replies {
		t, err := parseAnnouncementDate(r.Pengumuman.TglPengumuman)
		if err != nil {
			continue
		}
		if max == nil || t.After(*max) {
			tt := t
			max = &tt
		}
	}
	return max
}

// ensureTickerForDisclosure ensures the disclosure's ticker exists in the
// tickers table (FK dependency). Announcements don't carry issuer names, so a
// new ticker is seeded with name=code; the stock_summary upsert enriches it
// (name, shares) once the issuer appears in a trading summary.
func ensureTickerForDisclosure(db *sqlx.DB, repo *repository.TickerRepository, code string) error {
	ticker := &entity.Ticker{
		Code:   code,
		Name:   code,
		Active: true,
	}
	return repo.Upsert(db, ticker)
}

// upsertDisclosureRows flattens every reply into disclosure rows and upserts
// them (idempotent via pdf_url UNIQUE). Tickers are auto-discovered. Returns
// rows upserted and the first error; any failed row fails the whole batch so
// the caller can retry rather than silently advancing past unpersisted rows.
func upsertDisclosureRows(db *sqlx.DB, tickerRepo *repository.TickerRepository, disclosureRepo *repository.DisclosureRepository, replies []AnnouncementReply, log *logrus.Logger) (int, error) {
	upserted := 0
	for _, reply := range replies {
		rows := replyToDisclosures(reply)
		if len(rows) == 0 {
			log.Warnf("announcements: skipping reply %q (unparseable date or no PDF attachments)", reply.Pengumuman.ID2)
			continue
		}
		for _, d := range rows {
			if d.Ticker != nil {
				if err := ensureTickerForDisclosure(db, tickerRepo, *d.Ticker); err != nil {
					log.Warnf("announcements: ticker upsert failed for %s: %v", *d.Ticker, err)
					return upserted, fmt.Errorf("ticker upsert %s: %w", *d.Ticker, err)
				}
			}
			if err := disclosureRepo.Upsert(db, d); err != nil {
				log.Warnf("announcements: disclosure upsert failed for %s: %v", d.PdfURL, err)
				return upserted, fmt.Errorf("disclosure upsert %s: %w", d.PdfURL, err)
			}
			upserted++
		}
	}
	return upserted, nil
}
