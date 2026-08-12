package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/tasks"
)

func main() {
	dateStr := flag.String("date", "", "trading date in YYYY-MM-DD format (default: today)")
	startDateStr := flag.String("start-date", "", "bulk backfill start date in YYYY-MM-DD format")
	endDateStr := flag.String("end-date", "", "bulk backfill end date in YYYY-MM-DD format")
	announcementsFlag := flag.Bool("announcements", false, "enqueue idx:announcements task instead of stock_summary")
	rssFlag := flag.Bool("rss", false, "enqueue rss:ingest task instead of stock_summary")
	flag.Parse()

	vip := config.NewViper()
	log := config.NewLogger(vip)

	// Bulk backfill mode: --start-date and --end-date together.
	if *startDateStr != "" || *endDateStr != "" {
		if *startDateStr == "" || *endDateStr == "" {
			log.Fatalf("--start-date and --end-date must be provided together")
		}
		if *dateStr != "" {
			log.Fatalf("--date is mutually exclusive with --start-date/--end-date")
		}
		if *announcementsFlag || *rssFlag {
			log.Fatalf("--announcements/--rss are mutually exclusive with --start-date/--end-date")
		}
		runBulkBackfill(vip, log, *startDateStr, *endDateStr)
		return
	}

	// Single-date mode.
	client := config.NewAsynqClient(vip, log)

	date := time.Now()
	if *dateStr != "" {
		var err error
		date, err = time.Parse("2006-01-02", *dateStr)
		if err != nil {
			log.Fatalf("invalid date format: %s (use YYYY-MM-DD)", *dateStr)
		}
	}

	if *announcementsFlag && *rssFlag {
		log.Fatalf("--announcements and --rss are mutually exclusive")
	}

	dateKey := date.Format("2006-01-02")

	// Task-mode selection: stock_summary by default, or announcements/rss.
	taskType := tasks.TypeStockSummary
	enqueue := func() (*asynq.TaskInfo, error) { return tasks.EnqueueStockSummary(client, date) }
	if *announcementsFlag {
		taskType = tasks.TypeAnnouncements
		enqueue = func() (*asynq.TaskInfo, error) { return tasks.EnqueueAnnouncements(client, date) }
	} else if *rssFlag {
		taskType = tasks.TypeRSS
		enqueue = func() (*asynq.TaskInfo, error) { return tasks.EnqueueRSS(client, date) }
	}

	log.Infof("enqueuing %s task for %s", taskType, dateKey)
	info, err := enqueue()
	if err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			log.Infof("task %s:%s already enqueued, skipping", taskType, dateKey)
		} else {
			log.Errorf("failed to enqueue %s:%s: %v", taskType, dateKey, err)
			os.Exit(1)
		}
	} else {
		log.Infof("enqueued %s:%s: id=%s queue=%s", taskType, dateKey, info.ID, info.Queue)
	}

	fmt.Println("done")
	os.Exit(0)
}

// runBulkBackfill connects to DB + IDX client directly and loops the date
// range sequentially, upserting stock summary rows per date. No asynq task —
// synchronous loop, manual invocation, one-off script.
func runBulkBackfill(vip *viper.Viper, log *logrus.Logger, startStr, endStr string) {
	start, err := time.Parse("2006-01-02", startStr)
	if err != nil {
		log.Fatalf("invalid --start-date format: %s (use YYYY-MM-DD)", startStr)
	}
	end, err := time.Parse("2006-01-02", endStr)
	if err != nil {
		log.Fatalf("invalid --end-date format: %s (use YYYY-MM-DD)", endStr)
	}
	if start.After(end) {
		log.Fatalf("--start-date (%s) must be on or before --end-date (%s)", startStr, endStr)
	}

	db := config.NewDatabase(vip, log)
	defer db.Close()

	idxClient, err := client.InitDefaultClient(vip, log)
	if err != nil {
		log.Fatalf("failed to init IDX client: %v", err)
	}
	defer idxClient.Close()

	tickerRepo := repository.NewTickerRepository(log)
	dailyPriceRepo := repository.NewDailyPriceRepository(log)
	sourceStatusRepo := repository.NewSourceStatusRepository(log)

	log.Infof("bulk backfill: fetching %s to %s", startStr, endStr)
	result := tasks.RunBulkBackfill(log, idxClient, db, tickerRepo, dailyPriceRepo, sourceStatusRepo, start, end)
	log.Infof("bulk backfill complete: %d/%d dates succeeded, %d failed (%d empty/no-data)",
		result.Succeeded, result.Total, result.Failed, result.Empty)

	// A run where every date failed is indistinguishable from a no-op in
	// automation — exit non-zero so callers can detect total failure.
	if result.Total > 0 && result.Failed == result.Total {
		os.Exit(1)
	}
}
