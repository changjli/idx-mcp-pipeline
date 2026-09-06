package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/tasks"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// argList collects repeatable --arg key=value flags.
type argList []string

func (a *argList) String() string { return strings.Join(*a, ",") }
func (a *argList) Set(v string) error {
	*a = append(*a, v)
	return nil
}

func main() {
	taskName := flag.String("task", "stock-summary", "task to enqueue (registry node name): stock-summary, announcements, rss, cleanup, detect, filter, extract, broker-summary, broker-summary-range, broker-summary-sweep, pipeline (default: stock-summary)")
	dateStr := flag.String("date", "", "trading date in YYYY-MM-DD format (default: today)")
	startDateStr := flag.String("start-date", "", "bulk backfill start date in YYYY-MM-DD format")
	endDateStr := flag.String("end-date", "", "bulk backfill end date in YYYY-MM-DD format")
	var args argList
	flag.Var(&args, "arg", "per-task argument, repeatable: broker-summary requires ticker=<code>; extract requires id=<disclosure id>")
	flag.Parse()

	vip := config.NewViper()
	log := config.NewLogger(vip)

	// Bulk backfill mode: --start-date and --end-date together. Only
	// stock-summary (daily_prices backfill) and announcements (disclosure
	// metadata) have bulk paths — direct DB+client loops, no asynq.
	if *startDateStr != "" || *endDateStr != "" {
		if *startDateStr == "" || *endDateStr == "" {
			log.Fatalf("--start-date and --end-date must be provided together")
		}
		if *dateStr != "" {
			log.Fatalf("--date is mutually exclusive with --start-date/--end-date")
		}
		switch *taskName {
		case "announcements":
			runBulkAnnouncements(vip, log, *startDateStr, *endDateStr)
		case "stock-summary":
			runBulkBackfill(vip, log, *startDateStr, *endDateStr)
		case "broker-summary":
			runBulkBrokerSummary(vip, log, *startDateStr, *endDateStr, args)
		default:
			log.Fatalf("--task %s has no bulk mode; bulk backfill supports stock-summary, announcements, and broker-summary", *taskName)
		}
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

	node, err := tasks.Graph.NodeByName(*taskName)
	if err != nil {
		log.Fatalf("unknown --task %q: %v", *taskName, err)
	}

	log.Infof("enqueuing %s task for %s", node.Type, date.Format("2006-01-02"))
	info, err := node.Enqueue(client, date, args)
	if err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			log.Infof("task %s already enqueued, skipping", node.Type)
		} else {
			log.Errorf("failed to enqueue %s: %v", node.Type, err)
			os.Exit(1)
		}
	} else {
		log.Infof("enqueued %s: id=%s queue=%s", node.Type, info.ID, info.Queue)
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

	idxClient, err := client.NewDefaultClient(vip, log)
	if err != nil {
		log.Fatalf("failed to init IDX client: %v", err)
	}
	defer idxClient.Close()

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db), nil, log,
	)
	ingest := pipeline.NewStockSummaryIngest(
		pipeline.NewSQLDailyPriceStore(repository.NewDailyPriceRepository(log), db),
		pipeline.NewSQLTickerRegistrar(repository.NewTickerRepository(log), db),
		log,
	)

	log.Infof("bulk backfill: fetching %s to %s", startStr, endStr)
	result := tasks.RunBulkBackfill(log, idxClient, db, recorder, ingest, start, end)
	log.Infof("bulk backfill complete: %d/%d dates succeeded, %d failed (%d empty/no-data)",
		result.Succeeded, result.Total, result.Failed, result.Empty)

	// A run where every date failed is indistinguishable from a no-op in
	// automation — exit non-zero so callers can detect total failure.
	if result.Total > 0 && result.Failed == result.Total {
		os.Exit(1)
	}
}

// runBulkAnnouncements connects to DB + local IDX client (nodriver) directly
// and loops the date range, fetching disclosure metadata per date and upserting
// into disclosures. Mirrors runBulkBackfill: synchronous, local egress, no asynq,
// no worker. Run BEFORE enqueuing single-date --detect tasks so the
// detect-chained filter:disclosures has rows to filter (filter:{date} TaskID
// dedup otherwise blocks re-filter).
func runBulkAnnouncements(vip *viper.Viper, log *logrus.Logger, startStr, endStr string) {
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

	idxClient, err := client.NewDefaultClient(vip, log)
	if err != nil {
		log.Fatalf("failed to init IDX client: %v", err)
	}
	defer idxClient.Close()

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db), nil, log,
	)
	ingest := pipeline.NewDisclosureIngest(
		pipeline.NewSQLDisclosureSink(repository.NewDisclosureRepository(log), db),
		pipeline.NewSQLTickerRegistrar(repository.NewTickerRepository(log), db),
		log,
	)

	log.Infof("bulk announcements: fetching %s to %s", startStr, endStr)
	result := tasks.RunBulkAnnouncements(log, idxClient, db, recorder, ingest, start, end)
	log.Infof("bulk announcements complete: %d/%d dates succeeded, %d failed (%d empty/no-data)",
		result.Succeeded, result.Total, result.Failed, result.Empty)

	if result.Total > 0 && result.Failed == result.Total {
		os.Exit(1)
	}
}

// runBulkBrokerSummary backfills per-stock broker summaries for one ticker over
// a date range, calling the shared GetStockBrokerSummaryRange usecase directly
// (the same core the MCP tool and the async task use). Synchronous loop, manual
// invocation, one-off script — mirrors runBulkBackfill. Requires
// --arg ticker=<code>; only trading days (daily_prices presence) are fetched.
func runBulkBrokerSummary(vip *viper.Viper, log *logrus.Logger, startStr, endStr string, args argList) {
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
	ticker := argValue(args, "ticker")
	if ticker == "" {
		log.Fatalf("--task broker-summary requires --arg ticker=<code>")
	}

	db := config.NewDatabase(vip, log)
	defer db.Close()

	ipotClient := ipot.NewClient(ipot.DefaultConfig(), log)
	brokerStockSummaryUC := usecase.NewBrokerStockSummaryUseCase(
		db, log, config.NewValidator(), ipotClient,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)

	log.Infof("bulk broker-summary: backfilling %s %s to %s", ticker, startStr, endStr)
	resp, err := brokerStockSummaryUC.GetStockBrokerSummaryRange(context.Background(), ticker, start, end)
	if err != nil {
		log.Fatalf("bulk broker-summary: %v", err)
	}
	log.Infof("bulk broker-summary complete: %s %s..%s fetched=%d failed=%d empty=%d",
		resp.Ticker, resp.Start, resp.End, resp.Fetched, resp.Failed, resp.Empty)

	// Record source_status so the read tools' staleness metadata reflects the
	// backfill; last good date = the most recent day that produced data.
	if resp.Fetched > 0 {
		recorder := pipeline.NewSourceStatusRecorder(
			pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db), nil, log,
		)
		last, _ := time.Parse("2006-01-02", resp.Days[len(resp.Days)-1].TradingDay)
		recorder.Success(tasks.TypeBrokerStockSummary, tasks.BrokerStockSummaryMaxAgeSeconds, &last)
	}

	// A run where every trading day failed is indistinguishable from a no-op in
	// automation — exit non-zero so callers can detect total failure.
	if resp.Failed > 0 && resp.Fetched == 0 {
		os.Exit(1)
	}
}

// argValue returns the value of an --arg key=value entry, or "" if absent.
func argValue(args argList, key string) string {
	prefix := key + "="
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			return strings.TrimPrefix(a, prefix)
		}
	}
	return ""
}
