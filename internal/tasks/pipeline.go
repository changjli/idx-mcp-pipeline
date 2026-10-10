package tasks

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Wave timing constants: the daily wave fires at 20:05 WIB
// (scheduler.DailyCronSpec), declared here because the scheduler package
// imports this one, not the other way around.
const (
	WaveWIBHour   = 20
	WaveWIBMinute = 5

	// waveWalkbackMaxDays bounds the missing-day walk-back: a deeper hole is
	// the enqueue-daily bulk backfill's job, not a nightly self-heal.
	waveWalkbackMaxDays = 10
)

// WaveTargetDays lists the trading days the pipeline:daily wave should
// ingest: every weekday within the walk-back window whose daily_prices rows
// are not stored, walking back from today (at or after the 20:05 WIB wave
// time) or from yesterday (before it — today has no published summary yet),
// newest first. Weekends are skipped: a weekend's empty fetch completes
// "successfully" and its date-keyed TaskID (IngestRetention: 24h) then blocks
// re-enqueue, which would dead-end the walk one hop above the real gap.
//
// The walk collects ALL missing days rather than stopping at the first —
// a single-target walk starves backlogged holes: the freshest missing day
// wins every nightly fire (at fire time "today" is always missing), so a gap
// behind it is never reached. Full catch-up in one fire drains a backlog
// bounded by waveWalkbackMaxDays; a deeper hole is the enqueue-daily bulk
// backfill's job, not a nightly self-heal. An empty result is a no-op run,
// not an error.
func WaveTargetDays(now time.Time, hasStored func(day string) (bool, error)) ([]time.Time, error) {
	nowLoc := now.In(wibLoc())
	start := nowLoc
	if nowLoc.Hour() < WaveWIBHour || (nowLoc.Hour() == WaveWIBHour && nowLoc.Minute() < WaveWIBMinute) {
		start = nowLoc.AddDate(0, 0, -1)
	}
	var targets []time.Time
	for i := 0; i < waveWalkbackMaxDays; i++ {
		day := start.AddDate(0, 0, -i)
		if day.Weekday() == time.Saturday || day.Weekday() == time.Sunday {
			continue
		}
		stored, err := hasStored(day.Format("2006-01-02"))
		if err != nil {
			return nil, fmt.Errorf("pipeline:daily: stored-day check failed: %w", err)
		}
		if !stored {
			targets = append(targets, day)
		}
	}
	return targets, nil
}

// NewPipelineDailyHandler returns an asynq handler for the pipeline:daily task type.
// It picks the wave's target day (WaveTargetDay) and fires the fixed wave-1
// fan-out declared in the graph registry (ADR-0008): stock_summary,
// announcements, rss, corporate_actions, suspensions, index_summary,
// ksei_balancepos, and cleanup. Date-keyed TaskIDs dedup against a same-day
// re-run. Announcement/rss/cleanup failures are logged but don't fail the
// pipeline; a stock_summary failure fails the fan-out so asynq retries the
// whole pipeline:daily task.
func NewPipelineDailyHandler(
	log *logrus.Logger,
	enq pipeline.Enqueuer,
	db *sqlx.DB,
	dailyPriceRepo *repository.DailyPriceRepository,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		hasStored := func(day string) (bool, error) { return dailyPriceRepo.ExistsForDate(db, day) }
		targets, err := WaveTargetDays(time.Now(), hasStored)
		if err != nil {
			return err // fail so asynq retries — a silently skipped wave is a data hole
		}
		if len(targets) == 0 {
			log.Info("pipeline:daily: no missing daily_prices day within walk-back window, no-op")
			return nil
		}
		days := make([]string, len(targets))
		for i, d := range targets {
			days[i] = d.Format("2006-01-02")
		}
		log.Infof("pipeline:daily: wave target days [%s]", strings.Join(days, ", "))

		for _, target := range targets {
			if err := fireWave(log, enq, target); err != nil {
				return err
			}
		}
		return nil
	}
}

// fireWave enqueues the fixed wave-1 fan-out for one target day. A
// stock_summary enqueue failure fails the caller so asynq retries the whole
// pipeline:daily task; the other nodes' failures are logged and don't fail
// the wave.
func fireWave(log *logrus.Logger, enq pipeline.Enqueuer, target time.Time) error {
	dayKey := target.Format("2006-01-02")
	for _, name := range Graph.Node(TypePipelineDaily).Wave {
		node := Graph.Node(name)
		var opts []asynq.Option
		if node.Delay > 0 {
			opts = append(opts, asynq.ProcessIn(node.Delay))
		}
		info, err := node.Enqueue(enq, target, nil, opts...)
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			log.Infof("pipeline:daily: %s task for %s already enqueued", node.Type, dayKey)
		} else if err != nil {
			log.Errorf("pipeline:daily: failed to enqueue %s: %v", node.Type, err)
			if node.Type == TypeStockSummary {
				return err
			}
		} else {
			log.Infof("pipeline:daily: enqueued %s task id=%s", node.Type, info.ID)
		}
	}
	return nil
}
