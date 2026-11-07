package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/extract"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

const (
	// cleanupDailyPricesRetentionDays is how long daily_prices rows are kept
	// (2 years). Older rows are deleted outright — the IDX bulk backfill can
	// rebuild them on demand.
	cleanupDailyPricesRetentionDays = 730
	// cleanupShortRetentionDays is the retention for broker_summaries,
	// news_items/news_tickers, alerts, and anomalies (90 days).
	cleanupShortRetentionDays = 90
	// cleanupBrokerWindowRetentionDays is the retention for foreign-lane
	// non-archive windows: day rows and multi-day answer caches alike. Archive
	// and backfill chunks plus new-lane all-investor windows are exempt.
	cleanupBrokerWindowRetentionDays = 60
	// cleanupBatchSize bounds each eviction query so a large backlog (first run
	// after years of accumulation) is processed incrementally instead of
	// materializing the whole set into memory at once.
	cleanupBatchSize = 500
	// cleanupDelay is how long after the daily ingestion wave the cleanup task
	// fires. The wave (stock_summary → anomalies → filter → extract) can run for
	// a while; a fixed delay keeps cleanup from racing extraction. Cleanup is
	// idempotent, so an early fire is harmless.
	cleanupDelay = 3 * time.Hour
)

// CleanupPayload is the payload for a cleanup task. The date only keys the
// asynq TaskID for dedup — retention is wall-clock based, not date-based.
type CleanupPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// EnqueueCleanup enqueues a cleanup task for the given date with a date-keyed
// TaskID for dedup (one run per day). Extra opts (e.g. asynq.ProcessIn) are
// appended to the defaults.
func EnqueueCleanup(enq pipeline.Enqueuer, date time.Time, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	return Graph.Node(TypeCleanup).Enqueue(enq, date, nil, opts...)
}

// NewCleanupHandler returns an asynq handler for the cleanup task type. It
// evicts expired data per the tiered retention policy:
//
//   - daily_prices rows older than 2 years are deleted;
//   - broker_summaries, news_items (+ news_tickers), alerts, and anomalies
//     rows older than 90 days are deleted;
//   - foreign-lane non-archive broker windows older than 60 days are deleted,
//     except a Tier-2 ticker-month whose archive chunk does not exist yet;
//     archive/backfill chunks and new-lane all-investor windows are exempt;
//   - R2 objects past their raw_files.retention_days are deleted and the
//     raw_files row marked deleted_at (row kept for audit);
//   - extracted disclosure text older than 90 days is deleted from R2 and the
//     disclosures row marked extraction_status='evicted' with text_r2_key
//     nulled (metadata kept indefinitely).
//
// source_status is deliberately NOT pruned: its rows carry the incremental
// high_water_mark (announcements) and failure history, and deleting them would
// silently degrade ingestion after a long outage.
//
// Every step is idempotent and safe to re-run: deletes are WHERE-guarded,
// R2 DeleteObject is a no-op on missing keys, and eviction guards on
// extraction_status='ok'.
func NewCleanupHandler(
	log *logrus.Logger,
	db *sqlx.DB,
	dailyPriceRepo *repository.DailyPriceRepository,
	brokerRepo *repository.BrokerRepository,
	brokerStockSummaryRepo *repository.BrokerStockSummaryRepository,
	newsRepo *repository.NewsRepository,
	newsTickerRepo *repository.NewsTickerRepository,
	alertRepo *repository.AlertRepository,
	anomalyRepo *repository.AnomalyRepository,
	rawFileRepo *repository.RawFileRepository,
	disclosureRepo *repository.DisclosureRepository,
	r2Store storage.ObjectStore,
	minADTV int64,
) asynq.HandlerFunc {
	if minADTV <= 0 {
		minADTV = pipeline.DefaultADTVMinValue
	}
	return func(ctx context.Context, t *asynq.Task) error {
		var p CleanupPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}

		r := &cleanupRunner{
			log:                    log,
			db:                     db,
			dailyPriceRepo:         dailyPriceRepo,
			brokerRepo:             brokerRepo,
			brokerStockSummaryRepo: brokerStockSummaryRepo,
			newsRepo:               newsRepo,
			newsTickerRepo:         newsTickerRepo,
			alertRepo:              alertRepo,
			anomalyRepo:            anomalyRepo,
			rawFileRepo:            rawFileRepo,
			disclosureRepo:         disclosureRepo,
			r2Store:                r2Store,
			minADTV:                minADTV,
		}
		taskID, _ := asynq.GetTaskID(ctx)
		return r.run(ctx, taskID)
	}
}

// cleanupRunner holds the cleanup task's dependencies so run() is testable
// without Redis.
type cleanupRunner struct {
	log                    *logrus.Logger
	db                     *sqlx.DB
	dailyPriceRepo         *repository.DailyPriceRepository
	brokerRepo             *repository.BrokerRepository
	brokerStockSummaryRepo *repository.BrokerStockSummaryRepository
	newsRepo               *repository.NewsRepository
	newsTickerRepo         *repository.NewsTickerRepository
	alertRepo              *repository.AlertRepository
	anomalyRepo            *repository.AnomalyRepository
	rawFileRepo            *repository.RawFileRepository
	disclosureRepo         *repository.DisclosureRepository
	r2Store                storage.ObjectStore
	minADTV                int64
}

// cleanupResult summarizes one cleanup run for the cleanup_evicted event.
type cleanupResult struct {
	DailyPrices         int64
	Broker              int64
	BrokerStock         int64
	BrokerStockTotals   int64
	BrokerStockRetained int64
	News                int64
	NewsTickers         int64
	Alerts              int64
	Anomalies           int64
	RawFiles            int
	DisclosureText      int
}

func (r *cleanupRunner) run(ctx context.Context, taskID string) error {
	res := cleanupResult{}
	var err error

	if res.DailyPrices, err = r.dailyPriceRepo.DeleteOlderThan(r.db, cleanupDailyPricesRetentionDays); err != nil {
		return fmt.Errorf("cleanup daily_prices: %w", err)
	}
	if res.Broker, err = r.brokerRepo.DeleteOlderThan(r.db, cleanupShortRetentionDays); err != nil {
		return fmt.Errorf("cleanup broker_summaries: %w", err)
	}
	if res.BrokerStock, res.BrokerStockTotals, res.BrokerStockRetained, err = r.expireBrokerWindows(); err != nil {
		return fmt.Errorf("cleanup broker_stock_summaries: %w", err)
	}
	// news_tickers first — its news_id FK references news_items.
	if res.NewsTickers, err = r.newsTickerRepo.DeleteOlderThan(r.db, cleanupShortRetentionDays); err != nil {
		return fmt.Errorf("cleanup news_tickers: %w", err)
	}
	if res.News, err = r.newsRepo.DeleteOlderThan(r.db, cleanupShortRetentionDays); err != nil {
		return fmt.Errorf("cleanup news_items: %w", err)
	}
	if res.Alerts, err = r.alertRepo.DeleteOlderThan(r.db, cleanupShortRetentionDays); err != nil {
		return fmt.Errorf("cleanup alerts: %w", err)
	}
	if res.Anomalies, err = r.anomalyRepo.DeleteOlderThan(r.db, cleanupShortRetentionDays); err != nil {
		return fmt.Errorf("cleanup anomalies: %w", err)
	}

	if res.RawFiles, err = r.evictExpiredRawFiles(ctx); err != nil {
		return fmt.Errorf("cleanup raw_files: %w", err)
	}
	if res.DisclosureText, err = r.evictExpiredDisclosureText(ctx); err != nil {
		return fmt.Errorf("cleanup disclosure text: %w", err)
	}

	rows := res.DailyPrices + res.Broker + res.BrokerStock + res.BrokerStockTotals +
		res.News + res.NewsTickers + res.Alerts + res.Anomalies

	logEvent(r.log, logrus.InfoLevel, "cleanup_evicted",
		fmt.Sprintf("evicted %d raw file(s) and %d disclosure text(s); deleted %d row(s)", res.RawFiles, res.DisclosureText, rows),
		logrus.Fields{
			"task_id":                taskID,
			"source":                 TypeCleanup,
			"files":                  res.RawFiles,
			"rows":                   rows,
			"daily_prices":           res.DailyPrices,
			"broker_summaries":       res.Broker,
			"broker_stock_summaries": res.BrokerStock,
			"broker_stock_totals":    res.BrokerStockTotals,
			"broker_stock_retained":  res.BrokerStockRetained,
			"news_items":             res.News,
			"news_tickers":           res.NewsTickers,
			"alerts":                 res.Alerts,
			"anomalies":              res.Anomalies,
			"disclosure_text":        res.DisclosureText,
		})
	return nil
}

// cleanupBrokerADTVMinDays mirrors the archiver's ADTV predicate: a ticker is
// Tier 2 for a month when it traded at least this many of the 21 sessions
// ending on the month's last day.
const cleanupBrokerADTVMinDays = 8

// cleanupMonth is the calendar context one expired day row's month needs: the
// month's trading-day range and its Tier-2 membership.
type cleanupMonth struct {
	first time.Time
	last  time.Time
	tier2 map[string]bool
}

// expireBrokerWindows applies the broker retention rule window by window.
//
// The blanket delete is gone because the table now holds three different
// things: short-lived foreign observations/caches, permanent archive chunks,
// and the on-demand all-investor lane. It keeps archive/backfill chunks and
// new-lane all windows; deletes legacy rows and foreign-lane non-archive
// windows older than the cutoff; and, for a foreign day row in a Tier-2
// ticker-month, refuses to delete until an archive chunk covers the month.
// That last rule is the ordering guarantee, enforced here rather than trusted
// to the scheduler.
func (r *cleanupRunner) expireBrokerWindows() (rows, totals, retained int64, err error) {
	cutoff := time.Now().AddDate(0, 0, -cleanupBrokerWindowRetentionDays)
	keys, err := r.brokerStockSummaryRepo.FindExpiredWindowKeys(r.db, cutoff)
	if err != nil {
		return 0, 0, 0, err
	}

	months := make(map[string]*cleanupMonth)
	for _, key := range keys {
		// Permanent partition chunks.
		if key.Source == entity.BrokerSourceArchive || key.Source == entity.BrokerSourceBackfill {
			continue
		}
		// The on-demand all-investor lane is kept; legacy all rows are not.
		if key.Investor == broker.InvestorAll && key.Source != entity.BrokerSourceLegacyIPOT {
			continue
		}

		// Only one-day foreign rows have a compaction dependency. Multi-day
		// caches are disposable answer caches.
		if key.Investor == broker.InvestorForeign && key.Source != entity.BrokerSourceLegacyIPOT &&
			key.WinFrom.Equal(key.TradingDay) {
			monthKey := key.TradingDay.Format("2006-01")
			month, ok := months[monthKey]
			if !ok {
				month, err = r.cleanupMonthContext(monthKey)
				if err != nil {
					return rows, totals, retained, err
				}
				months[monthKey] = month
			}
			if month != nil && month.tier2[key.Ticker] {
				covered, err := r.brokerStockSummaryRepo.HasPartitionChunk(r.db, key.Ticker, month.first, month.last)
				if err != nil {
					return rows, totals, retained, err
				}
				if !covered {
					retained++
					continue
				}
			}
		}

		dRows, dTotals, err := r.brokerStockSummaryRepo.DeleteWindow(r.db, key)
		if err != nil {
			return rows, totals, retained, err
		}
		rows += dRows
		totals += dTotals
	}
	return rows, totals, retained, nil
}

// cleanupMonthContext resolves a month's trading-day range and Tier-2 set.
// An empty month (no stored prices) has no Tier 2, so its day rows are not
// protected.
func (r *cleanupRunner) cleanupMonthContext(monthKey string) (*cleanupMonth, error) {
	monthStart, err := time.Parse("2006-01", monthKey)
	if err != nil {
		return nil, err
	}
	monthEnd := monthStart.AddDate(0, 1, -1)
	days, err := r.dailyPriceRepo.TradingDaysInRangeAll(r.db, monthStart, monthEnd)
	if err != nil {
		return nil, err
	}
	if len(days) == 0 {
		return nil, nil
	}
	month := &cleanupMonth{first: days[0], last: days[len(days)-1], tier2: map[string]bool{}}

	lookback := month.last.AddDate(0, 0, -70)
	trading, err := r.dailyPriceRepo.TradingDaysInRangeAll(r.db, lookback, month.last)
	if err != nil {
		return nil, err
	}
	if len(trading) > 21 {
		trading = trading[len(trading)-21:]
	}
	if len(trading) == 0 {
		return month, nil
	}
	eligible, err := r.dailyPriceRepo.ADTVEligibleTickers(
		r.db, trading[0], month.last, cleanupBrokerADTVMinDays, r.minADTV)
	if err != nil {
		return nil, err
	}
	for _, t := range eligible {
		month.tier2[t.Ticker] = true
	}
	return month, nil
}

// evictExpiredRawFiles deletes R2 objects past their raw_files.retention_days
// and marks the claim-check row deleted_at (row kept for audit). Batched so a
// large backlog is processed incrementally. A failed R2 delete is logged and
// skipped — the row stays undeleted so the next run retries. Returns objects
// evicted.
//
// The measurement and config objects (measurement/reports/… and
// measurement/config/…) are deliberately out of scope: eviction is driven by
// raw_files rows, and no measurement object has one, so they are indefinite by
// construction and can never be evicted here (ADR-0016).
func (r *cleanupRunner) evictExpiredRawFiles(ctx context.Context) (int, error) {
	evicted := 0
	for {
		files, err := r.rawFileRepo.FindExpired(r.db, cleanupBatchSize)
		if err != nil {
			return evicted, err
		}
		if len(files) == 0 {
			break
		}
		for _, f := range files {
			if r.r2Store != nil {
				if err := r.r2Store.DeleteObject(ctx, f.StorageKey); err != nil {
					r.log.WithFields(logrus.Fields{
						"event": "cleanup_evicted", "source": TypeCleanup, "storage_key": f.StorageKey,
					}).Warnf("cleanup: r2 delete failed for %s: %v", f.StorageKey, err)
					continue
				}
			}
			if err := r.rawFileRepo.MarkDeleted(r.db, f.StorageKey); err != nil {
				return evicted, err
			}
			evicted++
		}
	}
	return evicted, nil
}

// evictExpiredDisclosureText deletes extracted disclosure text older than 90
// days from R2 and marks the disclosures row extraction_status='evicted' with
// text_r2_key nulled. The metadata row is kept indefinitely. Batched so a
// large backlog is processed incrementally. A failed R2 delete is logged and
// skipped so the next run retries.
func (r *cleanupRunner) evictExpiredDisclosureText(ctx context.Context) (int, error) {
	evicted := 0
	for {
		rows, err := r.disclosureRepo.FindEvictable(r.db, extract.DisclosureTextRetentionDays, cleanupBatchSize)
		if err != nil {
			return evicted, err
		}
		if len(rows) == 0 {
			break
		}
		for _, d := range rows {
			if d.TextR2Key != nil && r.r2Store != nil {
				if err := r.r2Store.DeleteObject(ctx, *d.TextR2Key); err != nil {
					r.log.WithFields(logrus.Fields{
						"event": "cleanup_evicted", "source": TypeCleanup, "disclosure_id": d.ID, "storage_key": *d.TextR2Key,
					}).Warnf("cleanup: r2 delete failed for disclosure %d: %v", d.ID, err)
					continue
				}
			}
			if err := r.disclosureRepo.UpdateExtractionStatus(r.db, d.ID, "evicted", nil, nil); err != nil {
				return evicted, err
			}
			evicted++
		}
	}
	return evicted, nil
}
