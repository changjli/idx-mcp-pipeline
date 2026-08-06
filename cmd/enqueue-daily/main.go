package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hibiken/asynq"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
)

func main() {
	dateStr := flag.String("date", "", "trading date in YYYY-MM-DD format (default: today)")
	flag.Parse()

	vip := config.NewViper()
	log := config.NewLogger(vip)
	asynqClient := config.NewAsynqClient(vip, log)

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

	tasks := []struct {
		Type    string
		Payload []byte
	}{
		{Type: "idx:stock_summary", Payload: []byte(`{"date":"` + dateKey + `"}`)},
		{Type: "idx:announcements", Payload: []byte(`{"date":"` + dateKey + `"}`)},
		{Type: "idx:broker_summary", Payload: []byte(`{"date":"` + dateKey + `"}`)},
		{Type: "rss:ingest", Payload: []byte(`{"date":"` + dateKey + `"}`)},
		{Type: "cleanup", Payload: []byte(`{"date":"` + dateKey + `"}`)},
	}

	for _, t := range tasks {
		task := asynq.NewTask(t.Type, t.Payload)
		info, err := asynqClient.Enqueue(task, asynq.Queue("ingest"))
		if err != nil {
			log.Errorf("failed to enqueue %s: %v", t.Type, err)
			continue
		}
		log.Infof("enqueued %s: id=%s queue=%s", t.Type, info.ID, info.Queue)
	}

	fmt.Println("done")
	os.Exit(0)
}
