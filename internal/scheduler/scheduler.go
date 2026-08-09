package scheduler

import (
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/tasks"
)

const (
	// DailyCronSpec fires at 4:05 PM WIB (Asia/Jakarta, UTC+7) every day.
	DailyCronSpec = "CRON_TZ=Asia/Jakarta 5 16 * * *"

	// archivedRequeueDelay is how long a recovered archived task waits before
	// firing — gives transient upstream blocks (e.g. Cloudflare 403) time to
	// lift. Shared by stock_summary and announcements self-heal.
	archivedRequeueDelay = 30 * time.Minute
)

// NewScheduler creates an asynq Scheduler configured for WIB timezone.
func NewScheduler(vip *viper.Viper, log *logrus.Logger) *asynq.Scheduler {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		log.Warnf("failed to load Asia/Jakarta timezone, falling back to UTC: %v", err)
		loc = time.UTC
	}

	sched := asynq.NewScheduler(
		config.NewRedisConnOpt(vip),
		&asynq.SchedulerOpts{
			Location: loc,
			PostEnqueueFunc: func(info *asynq.TaskInfo, err error) {
				if err != nil {
					log.Errorf("scheduler enqueue error: %v", err)
				} else {
					log.Infof("scheduler enqueued: type=%s id=%s queue=%s", info.Type, info.ID, info.Queue)
				}
			},
		},
	)

	return sched
}

// RegisterDailyTasks registers the daily pipeline task on the scheduler.
// It logs the next fire time of each registered entry.
func RegisterDailyTasks(sched *asynq.Scheduler, log *logrus.Logger) {
	task := asynq.NewTask(tasks.TypePipelineDaily, nil)
	entryID, err := sched.Register(DailyCronSpec, task)
	if err != nil {
		log.Fatalf("failed to register daily pipeline task: %v", err)
	}

	log.Infof("daily pipeline task registered: entry=%s cron=%s", entryID, DailyCronSpec)
}

// LogNextFireTime logs the next fire time for all scheduler entries.
func LogNextFireTime(sched *asynq.Scheduler, log *logrus.Logger) {
	// asynq Scheduler doesn't expose Entries() publicly.
	// Log the next expected fire based on current time and cron spec.
	now := time.Now()
	loc, _ := time.LoadLocation("Asia/Jakarta")
	if loc == nil {
		loc = time.UTC
	}
	// Approximate next fire: 4:05 PM WIB today, or tomorrow if past
	next := time.Date(now.Year(), now.Month(), now.Day(), 16, 5, 0, 0, loc)
	if now.After(next) {
		next = next.AddDate(0, 0, 1)
	}
	log.Infof("scheduler next fire: %s (%s)", next.Format(time.RFC3339), DailyCronSpec)
}

// SelfHealMissedTick checks if today's noop task was already enqueued.
// If not, it enqueues one immediately to recover from a missed scheduler tick.
func SelfHealMissedTick(client *asynq.Client, log *logrus.Logger) {
	now := time.Now()
	info, err := tasks.EnqueueNoop(client, now)
	if err == asynq.ErrTaskIDConflict {
		log.Infof("self-heal: noop task for %s already enqueued, skipping", now.Format("2006-01-02"))
	} else if err != nil {
		log.Warnf("self-heal: failed to enqueue noop task: %v", err)
	} else {
		log.Infof("self-heal: enqueued missed noop task id=%s", info.ID)
	}
}

// SelfHealArchivedStockSummary recovers archived idx:stock_summary tasks.
func SelfHealArchivedStockSummary(inspector *asynq.Inspector, client *asynq.Client, log *logrus.Logger) {
	selfHealArchived(inspector, client, log,
		tasks.TypeStockSummary, "stock_summary",
		stockSummaryDateFromID,
		func(date time.Time) (*asynq.TaskInfo, error) {
			return tasks.EnqueueStockSummary(client, date, asynq.ProcessIn(archivedRequeueDelay))
		},
	)
}

// SelfHealArchivedAnnouncements recovers archived idx:announcements tasks.
func SelfHealArchivedAnnouncements(inspector *asynq.Inspector, client *asynq.Client, log *logrus.Logger) {
	selfHealArchived(inspector, client, log,
		tasks.TypeAnnouncements, "announcements",
		announcementsDateFromID,
		func(date time.Time) (*asynq.TaskInfo, error) {
			return tasks.EnqueueAnnouncements(client, date, asynq.ProcessIn(archivedRequeueDelay))
		},
	)
}

// selfHealArchived recovers archived tasks of one type. Archived tasks hold
// their date-keyed TaskID, blocking re-enqueue (ErrTaskIDConflict) — a dead-end
// after retries are exhausted. Deletes each archived task to free the ID, then
// re-enqueues with a delay so transient upstream blocks (e.g. Cloudflare 403)
// have time to lift.
func selfHealArchived(
	inspector *asynq.Inspector,
	client *asynq.Client,
	log *logrus.Logger,
	typ, label string,
	dateFromID func(string) (time.Time, error),
	enqueue func(time.Time) (*asynq.TaskInfo, error),
) {
	archived, err := inspector.ListArchivedTasks("ingest")
	if err != nil {
		log.Warnf("self-heal: failed to list archived tasks: %v", err)
		return
	}

	recovered := 0
	for _, t := range archived {
		if t.Type != typ {
			continue
		}

		date, err := dateFromID(t.ID)
		if err != nil {
			log.Warnf("self-heal: invalid archived task id %q: %v", t.ID, err)
			continue
		}

		// Delete to free the TaskID.
		if err := inspector.DeleteTask("ingest", t.ID); err != nil {
			log.Warnf("self-heal: failed to delete archived task %s: %v", t.ID, err)
			continue
		}

		// Re-enqueue with delay so transient blocks can lift.
		info, err := enqueue(date)
		if err == asynq.ErrTaskIDConflict {
			log.Infof("self-heal: task %s already enqueued, skipping", t.ID)
			continue
		}
		if err != nil {
			log.Warnf("self-heal: failed to re-enqueue %s: %v", t.ID, err)
			continue
		}
		log.Infof("self-heal: recovered archived %s task %s -> new id=%s", label, t.ID, info.ID)
		recovered++
	}

	if recovered > 0 {
		log.Infof("self-heal: recovered %d archived %s task(s)", recovered, label)
	}
}

// announcementsDateFromID parses the date from an announcements task ID
// of the form "idx:announcements:2026-08-09".
func announcementsDateFromID(id string) (time.Time, error) {
	dateStr := strings.TrimPrefix(id, tasks.TypeAnnouncements+":")
	return time.Parse("2006-01-02", dateStr)
}

// stockSummaryDateFromID parses the date from a stock_summary task ID
// of the form "idx:stock_summary:2026-08-08".
func stockSummaryDateFromID(id string) (time.Time, error) {
	dateStr := strings.TrimPrefix(id, tasks.TypeStockSummary+":")
	return time.Parse("2006-01-02", dateStr)
}
