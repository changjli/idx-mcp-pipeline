package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/controller"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/middleware"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/scheduler"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/tasks"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

func main() {
	vip := config.NewViper()
	log := config.NewLogger(vip)
	db := config.NewDatabase(vip, log)
	validate := config.NewValidator()

	log.Info("starting mcp-server")

	// ─── Repository layer ──────────────────────────────────────

	tickerRepo := repository.NewTickerRepository(log)
	dailyPriceRepo := repository.NewDailyPriceRepository(log)
	anomalyRepo := repository.NewAnomalyRepository(log)
	disclosureRepo := repository.NewDisclosureRepository(log)
	brokerRepo := repository.NewBrokerRepository(log)
	newsRepo := repository.NewNewsRepository(log)
	newsTickerRepo := repository.NewNewsTickerRepository(log)
	sourceStatusRepo := repository.NewSourceStatusRepository(log)
	alertRepo := repository.NewAlertRepository(log)

	// ─── asynq task infrastructure ──────────────────────────────

	redisOpt := config.NewRedisConnOpt(vip)
	asynqSrv := config.NewAsynqServer(vip, log)
	asynqClient := config.NewAsynqClient(vip, log)

	// IDX HTTP client (singleton)
	idxClient, err := client.InitDefaultClient(vip, log)
	if err != nil {
		log.Fatalf("failed to init IDX client: %v", err)
	}
	defer idxClient.Close()

	// Task mux: route task types to handlers
	mux := asynq.NewServeMux()
	mux.Handle(tasks.TypeNoop, tasks.NewNoopHandler(log))
	mux.Handle(tasks.TypePipelineDaily, tasks.NewPipelineDailyHandler(log, asynqClient))
	mux.Handle(tasks.TypeStockSummary, tasks.NewStockSummaryHandler(
		log, idxClient, db,
		tickerRepo, dailyPriceRepo, sourceStatusRepo, alertRepo,
	))

	// Start asynq server in background goroutine
	go func() {
		log.Info("asynq server starting")
		if err := asynqSrv.Start(mux); err != nil {
			log.Fatalf("asynq server error: %v", err)
		}
	}()

	// asynq Scheduler: fires daily tasks at 4:05 PM WIB
	sched := scheduler.NewScheduler(vip, log)
	scheduler.RegisterDailyTasks(sched, log)
	scheduler.LogNextFireTime(sched, log)

	go func() {
		log.Info("asynq scheduler starting")
		if err := sched.Start(); err != nil {
			log.Fatalf("asynq scheduler error: %v", err)
		}
	}()

	// Self-heal: enqueue today's noop task if scheduler missed its tick
	scheduler.SelfHealMissedTick(asynqClient, log)

	// Self-heal: recover archived stock_summary tasks (dead-end recovery).
	// Run once at startup, then periodically.
	inspector := asynq.NewInspector(redisOpt)
	scheduler.SelfHealArchivedStockSummary(inspector, asynqClient, log)
	go func() {
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			scheduler.SelfHealArchivedStockSummary(inspector, asynqClient, log)
		}
	}()

	// ─── Use case layer ─────────────────────────────────────────

	anomalyUC := usecase.NewAnomalyUseCase(db, log, validate, dailyPriceRepo, anomalyRepo)
	disclosureUC := usecase.NewDisclosureUseCase(db, log, validate, disclosureRepo)
	brokerUC := usecase.NewBrokerUseCase(db, log, validate, brokerRepo)
	newsUC := usecase.NewNewsUseCase(db, log, validate, newsRepo, newsTickerRepo)
	pipelineUC := usecase.NewPipelineUseCase(db, log, validate, sourceStatusRepo, alertRepo)

	// ─── HTTP router ────────────────────────────────────────────

	router := chi.NewRouter()
	router.Use(chimw.Logger)
	router.Use(chimw.Recoverer)

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// asynq dashboard (dev-only, no auth)
	dashboardPort := vip.GetInt("asynq.dashboard_port")
	if dashboardPort == 0 {
		dashboardPort = 8081
	}
	go startDashboard(dashboardPort, redisOpt, log)

	authMW := middleware.NewAuth(vip.GetString("mcp.token"))
	router.Group(func(r chi.Router) {
		r.Use(authMW.Authenticate)

		mcpCtrl := controller.NewMCPController(log, anomalyUC, disclosureUC, brokerUC, newsUC, pipelineUC)
		mcpCtrl.RegisterRoutes(r)
	})

	port := vip.GetInt("mcp.port")
	if port == 0 {
		port = 8080
	}

	log.Infof("mcp-server listening on :%d", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), router); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// startDashboard starts a dev-only HTTP server with asynq queue status.
func startDashboard(port int, redisOpt asynq.RedisClientOpt, log *logrus.Logger) {
	inspector := asynq.NewInspector(redisOpt)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		queues, err := inspector.Queues()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		type queueInfo struct {
			Queue     string `json:"queue"`
			Pending   int    `json:"pending"`
			Active    int    `json:"active"`
			Scheduled int    `json:"scheduled"`
			Completed int    `json:"completed"`
			Retry     int    `json:"retry"`
			Archived  int    `json:"archived"`
		}

		var infos []queueInfo
		for _, q := range queues {
			info, err := inspector.GetQueueInfo(q)
			if err != nil {
				log.Warnf("dashboard: failed to get queue info for %s: %v", q, err)
				continue
			}
			infos = append(infos, queueInfo{
				Queue:     info.Queue,
				Pending:   info.Pending,
				Active:    info.Active,
				Scheduled: info.Scheduled,
				Completed: info.Completed,
				Retry:     info.Retry,
				Archived:  info.Archived,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(infos)
	})

	log.Infof("asynq dashboard starting on :%d", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		log.Warnf("asynq dashboard stopped: %v", err)
	}
}
