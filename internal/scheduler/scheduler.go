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

	// SweepCronSpec fires the weekly ADTV-gated broker-summary sweep (issue
	// 14b) at 9:00 PM WIB every Saturday. IDX is closed weekends, so Friday's
	// data is fully settled and the weekday pipeline wave + anomaly gate have
	// drained — the sweep skips those days (HasStoredDay) and only fetches the
	// quiet liquid names the anomaly gate missed. The date-keyed TaskID dedups
	// a same-day re-fire; a missed Saturday self-heals on the next run (the
	// trailing-21-day window still covers the gap).
	SweepCronSpec = "CRON_TZ=Asia/Jakarta 0 21 * * 6"

	// SectorIndexCronSpec fires the 6-monthly sector/industry + index-membership
	// seeder (issue 15b) at 10:00 PM WIB on Mar 1 and Aug 1 — the month after
	// each IDX index review (LQ45/Kompas100/IDX80/IDX30 rebalance in Feb+Jul,
	// effective at month-end), so the snapshot captures the post-rebalance
	// constituents, not the stale list. A same-month fire would label the old
	// set with the rebalance month for the next 5 months. The daily pipeline
	// wave (8:05 PM) has drained by then; a missed fire is caught by the next
	// scheduled run (the snapshot is point-in-time, not incremental).
	SectorIndexCronSpec = "CRON_TZ=Asia/Jakarta 0 22 1 3,8 *"

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

// RegisterDailyTasks registers the daily pipeline task and the weekly
// ADTV-gated broker-summary sweep (issue 14b) on the scheduler. It logs the next fire time
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

	// The sweep task must carry the same task-level timeout the graph node
	// applies: a fresh catch-up run exceeds asynq's 30m default at the IPOT
	// client's 2s pacing. The scheduler registers the task directly (not via
	// the node), so the override is applied here.
	sweepTask := asynq.NewTask(tasks.TypeBrokerStockSummarySweep, nil, asynq.Timeout(tasks.SweepTaskTimeout))
	sweepEntryID, err := sched.Register(SweepCronSpec, sweepTask)
	if err != nil {
		log.Fatalf("failed to register broker summary sweep task: %v", err)
	}
	log.Infof("broker summary sweep task registered: entry=%s cron=%s", sweepEntryID, SweepCronSpec)

	// The sector/index seeder (issue 15b) fires every 6 months; the handler
	// derives the run date from time.Now() at fire time (nil payload, same
	// convention as pipeline:daily and the sweep).
	sectorIndexTask := asynq.NewTask(tasks.TypeSectorIndex, nil)
	sectorIndexEntryID, err := sched.Register(SectorIndexCronSpec, sectorIndexTask)
	if err != nil {
		log.Fatalf("failed to register sector/index seeder task: %v", err)
	}
	log.Infof("sector/index seeder task registered: entry=%s cron=%s", sectorIndexEntryID, SectorIndexCronSpec)
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
