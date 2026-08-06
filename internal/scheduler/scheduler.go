package scheduler

import (
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
