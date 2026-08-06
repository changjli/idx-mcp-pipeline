package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hibiken/asynq"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/tasks"
)

func main() {
	dateStr := flag.String("date", "", "trading date in YYYY-MM-DD format (default: today)")
	flag.Parse()

	vip := config.NewViper()
	log := config.NewLogger(vip)
	client := config.NewAsynqClient(vip, log)

	date := time.Now()
	if *dateStr != "" {
		var err error
		date, err = time.Parse("2006-01-02", *dateStr)
		if err != nil {
			log.Fatalf("invalid date format: %s (use YYYY-MM-DD)", *dateStr)
		}
	}

	dateKey := date.Format("2006-01-02")
	log.Infof("enqueuing daily tasks for %s", dateKey)

	info, err := tasks.EnqueueNoop(client, date)
	if err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			log.Infof("task noop:%s already enqueued, skipping", dateKey)
		} else {
			log.Errorf("failed to enqueue noop:%s: %v", dateKey, err)
			os.Exit(1)
		}
	} else {
		log.Infof("enqueued noop:%s: id=%s queue=%s", dateKey, info.ID, info.Queue)
	}

	fmt.Println("done")
	os.Exit(0)
}
