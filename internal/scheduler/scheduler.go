package scheduler

import (
	"errors"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/tasks"
)

const (
	// DailyCronSpec fires at 8:05 PM WIB (Asia/Jakarta, UTC+7) every day.
	// IDX publishes the day's TradingSummary after market close (4 PM WIB);
	// 8 PM gives a few hours for the data to land before the pipeline fetches it.
	DailyCronSpec = "CRON_TZ=Asia/Jakarta 5 20 * * *"

	// SweepCronSpec fires the full-market broker-summary sweep at 10:05 PM WIB,
	// an hour after the pipeline wave so the anomaly-gated per-ticker broker
	// summaries have drained first — the sweep then skips them (HasStoredDay)
	// and only fetches the quiet tickers the anomaly gate missed. A fresh
	// sweep takes ~30 min at the IPOT client's 2s pacing; the date-keyed
	// TaskID dedups a same-day re-fire.
	SweepCronSpec = "CRON_TZ=Asia/Jakarta 5 22 * * *"

	// archivedRequeueDelay is how long a recovered archived task waits before
	// firing — gives transient upstream blocks (e.g. Cloudflare 403) time to
	// lift. Shared by every self-heal-eligible node.
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

// RegisterDailyTasks registers the daily pipeline task and the full-market
// broker-summary sweep (issue 14) on the scheduler. It logs the next fire time
// of each registered entry. The sweep task carries no payload — the handler
// derives the sweep date from time.Now() at fire time (same convention as
// pipeline:daily), so the cron fires on the current trading day.
func RegisterDailyTasks(sched *asynq.Scheduler, log *logrus.Logger) {
	task := asynq.NewTask(tasks.TypePipelineDaily, nil)
	entryID, err := sched.Register(DailyCronSpec, task)
	if err != nil {
		log.Fatalf("failed to register daily pipeline task: %v", err)
	}
	log.Infof("daily pipeline task registered: entry=%s cron=%s", entryID, DailyCronSpec)

	sweepTask := asynq.NewTask(tasks.TypeBrokerStockSummarySweep, nil)
	sweepEntryID, err := sched.Register(SweepCronSpec, sweepTask)
	if err != nil {
		log.Fatalf("failed to register broker summary sweep task: %v", err)
	}
	log.Infof("broker summary sweep task registered: entry=%s cron=%s", sweepEntryID, SweepCronSpec)
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
	// Approximate next fire: 8:05 PM WIB today, or tomorrow if past
	next := time.Date(now.Year(), now.Month(), now.Day(), 20, 5, 0, 0, loc)
	if now.After(next) {
		next = next.AddDate(0, 0, 1)
	}
	log.Infof("scheduler next fire: %s (%s)", next.Format(time.RFC3339), DailyCronSpec)
}

// SelfHealMissedTick recovers a missed scheduler tick: if today's
// pipeline:daily task isn't enqueued, enqueue it. TaskID dedup makes a
// double-fire safe — ErrTaskIDConflict means today's fan-out is already queued.
func SelfHealMissedTick(client *asynq.Client, log *logrus.Logger) {
	now := time.Now()
	info, err := tasks.EnqueuePipelineDaily(client, now)
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		log.Infof("self-heal: pipeline:daily task for %s already enqueued, skipping", now.Format("2006-01-02"))
	} else if err != nil {
		log.Warnf("self-heal: failed to enqueue pipeline:daily: %v", err)
	} else {
		log.Infof("self-heal: re-enqueued missed pipeline:daily task id=%s", info.ID)
	}
}

// SelfHealArchived recovers archived tasks of one registry node. Archived
// tasks hold their date-keyed TaskID, blocking re-enqueue (ErrTaskIDConflict)
// — a dead-end after retries are exhausted. Deletes each archived task to free
// the ID, then re-enqueues with a delay so transient upstream blocks (e.g.
// Cloudflare 403) have time to lift. The node's Day parser inverts its TaskKey
// format; only date-keyed, self-heal-eligible nodes (tasks.Graph.SelfHealEligible)
// are passed in.
func SelfHealArchived(inspector *asynq.Inspector, client *asynq.Client, log *logrus.Logger, node *tasks.Node) {
	archived, err := inspector.ListArchivedTasks("ingest")
	if err != nil {
		log.Warnf("self-heal: failed to list archived tasks: %v", err)
		return
	}

	recovered := 0
	for _, t := range archived {
		if t.Type != node.Type {
			continue
		}

		date, err := node.Day(t.ID)
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
		info, err := node.Enqueue(client, date, nil, asynq.ProcessIn(archivedRequeueDelay))
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			log.Infof("self-heal: task %s already enqueued, skipping", t.ID)
			continue
		}
		if err != nil {
			log.Warnf("self-heal: failed to re-enqueue %s: %v", t.ID, err)
			continue
		}
		log.Infof("self-heal: recovered archived %s task %s -> new id=%s", node.Type, t.ID, info.ID)
		recovered++
	}

	if recovered > 0 {
		log.Infof("self-heal: recovered %d archived %s task(s)", recovered, node.Type)
	}
}
