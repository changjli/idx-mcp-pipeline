package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Disclosure whitelist categories (ticket 11, layer 2): material-event titles
// kept for extraction. Titles are matched case-insensitively as substrings —
// real IDX titles are longer than the category names ("Pemanggilan RUPS
// Tahunan" contains "Pemanggilan RUPS"). All matched categories are stored.
var disclosureWhitelistKeywords = []string{
	"Informasi dan Fakta Material",
	"Pemanggilan RUPS",
	"Pembelian/Penjualan Efek",
	"Dividen",
	"Right Issue",
	"Stock Split",
	"Bonus Share",
	"Perubahan Papan Pencatatan",
	"Suspensi/Penundaan Pencatatan",
}

// disclosureExclusionKeywords are routine filings excluded even when a
// whitelist keyword also matches (exclusion wins). Laporan Keuangan covers
// routine quarterly financials.
var disclosureExclusionKeywords = []string{
	"Laporan Keuangan",
}

// FilterDisclosuresPayload is the payload for a filter:disclosures task.
type FilterDisclosuresPayload struct {
	Date string `json:"date"` // YYYY-MM-DD — the anomaly trading day that triggered the run
}

// EnqueueFilterDisclosures enqueues a filter:disclosures task for the given
// date. Uses a date-keyed TaskID for dedup. Returns ErrTaskIDConflict if
// already enqueued. Chained from detect:anomalies success (unconditionally,
// even when zero anomalies were flagged — non-anomaly disclosures still need
// marking passed_filter=false).
func EnqueueFilterDisclosures(client *asynq.Client, date time.Time) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeFilterDisclosures, dateKey)
	payload := FilterDisclosuresPayload{Date: dateKey}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal filter:disclosures payload: %w", err)
	}
	task := asynq.NewTask(TypeFilterDisclosures, raw)
	return client.Enqueue(task,
		asynq.TaskID(taskKey),
		asynq.Queue("ingest"),
		asynq.MaxRetry(3),
		asynq.Retention(24*time.Hour),
	)
}

// NewFilterDisclosuresHandler returns an asynq handler for the
// filter:disclosures task type. It applies the 3-layer filter to pending
// disclosures and enqueues one extract:disclosure task per passing row.
func NewFilterDisclosuresHandler(
	log *logrus.Logger,
	client *asynq.Client,
	db *sqlx.DB,
	disclosureRepo *repository.DisclosureRepository,
	anomalyRepo *repository.AnomalyRepository,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var p FilterDisclosuresPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}
		today, err := time.Parse("2006-01-02", p.Date)
		if err != nil {
			return fmt.Errorf("invalid date %q: %w", p.Date, err)
		}

		enqueue := func(id int64) {
			if _, err := EnqueueExtractDisclosure(client, id); err != nil && err != asynq.ErrTaskIDConflict {
				log.Warnf("filter:disclosures: enqueue extract for disclosure %d: %v", id, err)
			}
		}
		return runFilter(log, db, disclosureRepo, anomalyRepo, today, enqueue)
	}
}

// runFilter applies the anomaly-gate and keyword whitelist to every pending
// disclosure and marks the verdict. enqueue is called once per passing
// disclosure (injected so tests can collect IDs without Redis).
func runFilter(
	log *logrus.Logger,
	db *sqlx.DB,
	disclosureRepo *repository.DisclosureRepository,
	anomalyRepo *repository.AnomalyRepository,
	today time.Time,
	enqueue func(id int64),
) error {
	rows, err := disclosureRepo.FindPendingForFilter(db, today)
	if err != nil {
		return fmt.Errorf("query pending disclosures: %w", err)
	}

	passed := 0
	rejected := 0
	reExtracted := 0
	for _, d := range rows {
		// Sticky true: never re-evaluate the gate. Re-enqueue extract only
		// while the row still awaits extraction (self-heal for missed or
		// R2-less runs).
		if d.PassedFilter != nil && *d.PassedFilter {
			if d.ExtractionStatus == "pending" {
				enqueue(d.ID)
				reExtracted++
			}
			continue
		}

		gate, err := anomalyGate(db, anomalyRepo, d)
		if err != nil {
			return fmt.Errorf("anomaly gate for disclosure %d: %w", d.ID, err)
		}
		ok, categories := evaluateDisclosure(d.Title, gate)
		if err := disclosureRepo.MarkFiltered(db, d.ID, ok, categories); err != nil {
			return fmt.Errorf("mark disclosure %d: %w", d.ID, err)
		}
		if ok {
			enqueue(d.ID)
			passed++
		} else {
			rejected++
		}
	}

	log.Infof("filter:disclosures: %d pending processed (%d passed, %d rejected), %d passing re-enqueued for extraction",
		len(rows), passed, rejected, reExtracted)
	return nil
}

// anomalyGate reports whether the disclosure's ticker has an anomaly whose
// trading_day falls within the lookback window after the announcement date.
// A disclosure with no ticker can never match.
func anomalyGate(db *sqlx.DB, anomalyRepo *repository.AnomalyRepository, d entity.Disclosure) (bool, error) {
	if d.Ticker == nil {
		return false, nil
	}
	return anomalyRepo.ExistsForTickerInWindow(db, *d.Ticker, d.AnnouncementDate, repository.DisclosureFilterLookbackDays)
}

// evaluateDisclosure applies the keyword whitelist (layer 2) to a title whose
// anomaly-gate already passed. Exclusion keywords win: a title matching
// "Laporan Keuangan" is rejected even if a whitelist keyword also matches.
// Returns the verdict and every matched whitelist category.
func evaluateDisclosure(title string, gatePasses bool) (bool, []string) {
	if !gatePasses {
		return false, nil
	}
	lower := strings.ToLower(title)
	for _, kw := range disclosureExclusionKeywords {
		if strings.Contains(lower, strings.ToLower(kw)) {
			return false, nil
		}
	}
	var categories []string
	for _, kw := range disclosureWhitelistKeywords {
		if strings.Contains(lower, strings.ToLower(kw)) {
			categories = append(categories, kw)
		}
	}
	return len(categories) > 0, categories
}
