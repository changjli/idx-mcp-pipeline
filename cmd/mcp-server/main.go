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
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/extract"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/mcpserver"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/middleware"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/scheduler"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
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
	rawFileRepo := repository.NewRawFileRepository(log)
	brokerStockSummaryRepo := repository.NewBrokerStockSummaryRepository(log)

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
		log, idxClient, db, asynqClient,
		tickerRepo, dailyPriceRepo, sourceStatusRepo, alertRepo,
	))
	lookback := vip.GetInt("idx.disclosure_lookback_days")
	if lookback <= 0 {
		lookback = tasks.DefaultAnnouncementLookbackDays
	}
	mux.Handle(tasks.TypeAnnouncements, tasks.NewAnnouncementsHandler(
		log, idxClient, db,
		tickerRepo, disclosureRepo, sourceStatusRepo, alertRepo, lookback,
	))
	minADTV := vip.GetInt64("anomaly.min_adtv_value")
	if minADTV <= 0 {
		minADTV = tasks.DefaultADTVMinValue
	}
	mux.Handle(tasks.TypeDetectAnomalies, tasks.NewDetectAnomaliesHandler(
		log, asynqClient, db, dailyPriceRepo, anomalyRepo, minADTV,
	))
	// R2 claim-check is optional: without r2.* credentials the handler skips
	// the raw-XML upload (nil store) instead of hammering a default endpoint.
	var r2Store storage.ObjectStore
	if vip.GetString("r2.access_key") != "" {
		r2Bucket := vip.GetString("r2.bucket")
		if r2Bucket == "" {
			r2Bucket = "idx-mcp"
		}
		r2Store = storage.NewR2Store(config.NewR2Client(vip, log), r2Bucket)
	} else {
		log.Warn("r2 not configured — rss raw-XML claim-check disabled")
	}
	mux.Handle(tasks.TypeRSS, tasks.NewRSSHandler(
		log,
		&http.Client{Timeout: tasks.RSSHTTPTimeout},
		r2Store,
		tasks.DefaultRSSFeeds,
		db,
		tickerRepo, newsRepo, newsTickerRepo, sourceStatusRepo, alertRepo, rawFileRepo,
	))

	// Disclosure filter + extraction (ticket 11): filter:disclosures is
	// chained from detect:anomalies; extract:disclosure is chained per passing
	// disclosure. Extraction needs R2 — a nil store leaves rows pending.
	mux.Handle(tasks.TypeFilterDisclosures, tasks.NewFilterDisclosuresHandler(
		log, asynqClient, db, disclosureRepo, anomalyRepo,
	))
	mux.Handle(tasks.TypeExtractDisclosure, tasks.NewExtractDisclosureHandler(
		log, asynqClient,
		idxClient, // session-aware: Cloudflare cookies, browser headers, pacing
		r2Store,
		db, disclosureRepo, rawFileRepo,
		extract.PDFExtractor{},
	))

	// Retention cleanup (ticket 13): evicts expired data per the tiered
	// retention policy. Chained from pipeline:daily with a delay; idempotent.
	mux.Handle(tasks.TypeCleanup, tasks.NewCleanupHandler(
		log, db,
		dailyPriceRepo, brokerRepo, brokerStockSummaryRepo,
		newsRepo, newsTickerRepo, alertRepo, anomalyRepo,
		rawFileRepo, disclosureRepo, r2Store,
	))

	// Per-stock broker summary: on-demand fetch + persist, auto-triggered by
	// anomaly detection. Shares the same usecase the MCP tool (ticket 10) wires.
	ipotClient := ipot.NewClient(ipot.DefaultConfig(), log)
	brokerStockSummaryUC := usecase.NewBrokerStockSummaryUseCase(
		db, log, validate, ipotClient, brokerStockSummaryRepo, dailyPriceRepo,
	)
	mux.Handle(tasks.TypeBrokerStockSummary, tasks.NewBrokerStockSummaryHandler(
		log, db, brokerStockSummaryUC, sourceStatusRepo, alertRepo,
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

	// Self-heal: recover archived stock_summary, announcements, and rss tasks
	// (dead-end recovery). Run once at startup, then periodically.
	inspector := asynq.NewInspector(redisOpt)
	scheduler.SelfHealArchivedStockSummary(inspector, asynqClient, log)
	scheduler.SelfHealArchivedAnnouncements(inspector, asynqClient, log)
	scheduler.SelfHealArchivedRSS(inspector, asynqClient, log)
	go func() {
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			scheduler.SelfHealArchivedStockSummary(inspector, asynqClient, log)
			scheduler.SelfHealArchivedAnnouncements(inspector, asynqClient, log)
			scheduler.SelfHealArchivedRSS(inspector, asynqClient, log)
		}
	}()

	// ─── Use case layer ─────────────────────────────────────────

	anomalyUC := usecase.NewAnomalyUseCase(db, log, validate, dailyPriceRepo, anomalyRepo)
	// read_idx_disclosure (ticket 12) reads extracted text from R2; a nil
	// store (r2 not configured) serves metadata-only responses.
	disclosureUC := usecase.NewDisclosureUseCase(db, log, validate, disclosureRepo, r2Store, rawFileRepo)
	brokerUC := usecase.NewBrokerUseCase(db, log, validate, brokerRepo, dailyPriceRepo)
	newsUC := usecase.NewNewsUseCase(db, log, validate, newsRepo, newsTickerRepo)
	pipelineUC := usecase.NewPipelineUseCase(db, log, validate, sourceStatusRepo, alertRepo)

	// ─── HTTP router ────────────────────────────────────────────

	router := chi.NewRouter()
	router.Use(chimw.Logger)
	router.Use(chimw.Recoverer)

	// HandleFunc (not Get) so HEAD/POST probes from UptimeRobot and
	// SnapDeploy/Cloudflare health checks don't 405 — liveness is method-agnostic.
	router.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// asynq dashboard (dev-only, no auth)
	dashboardPort := vip.GetInt("asynq.dashboard_port")
	if dashboardPort == 0 {
		dashboardPort = 8081
	}
	go startDashboard(dashboardPort, redisOpt, log)

	// /statusz (ticket 13): bearer-gated source_status dump for quick debugging.
	// /healthz above is unauthenticated for Koyeb uptime checks.
	authMW := middleware.NewAuth(vip.GetString("mcp.token"))
	router.Get("/statusz", authMW.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		statuses, err := sourceStatusRepo.FindAll(db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(statuses)
	})).ServeHTTP)

	// MCP server over streamable HTTP (tickets 10 + 12): 8 tools, bearer-token
	// auth on every request, structured error envelopes, staleness metadata.
	mcpSrv := mcpserver.NewServer(mcpserver.Deps{
		Log:                  log,
		DB:                   db,
		AnomalyUC:            anomalyUC,
		DisclosureUC:         disclosureUC,
		BrokerUC:             brokerUC,
		NewsUC:               newsUC,
		PipelineUC:           pipelineUC,
		BrokerStockSummaryUC: brokerStockSummaryUC,
		SourceStatusRepo:     sourceStatusRepo,
		TickerRepo:           tickerRepo,
	})
	router.Mount("/mcp", authMW.Authenticate(mcpSrv.Handler()))

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
