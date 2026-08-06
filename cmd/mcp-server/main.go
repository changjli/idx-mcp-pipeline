package main

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/config"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/controller"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/middleware"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

func main() {
	vip := config.NewViper()
	log := config.NewLogger(vip)
	db := config.NewDatabase(vip, log)
	validate := config.NewValidator()

	log.Info("starting mcp-server")

	dailyPriceRepo := repository.NewDailyPriceRepository(log)
	anomalyRepo := repository.NewAnomalyRepository(log)
	disclosureRepo := repository.NewDisclosureRepository(log)
	brokerRepo := repository.NewBrokerRepository(log)
	newsRepo := repository.NewNewsRepository(log)
	newsTickerRepo := repository.NewNewsTickerRepository(log)
	sourceStatusRepo := repository.NewSourceStatusRepository(log)
	alertRepo := repository.NewAlertRepository(log)

	anomalyUC := usecase.NewAnomalyUseCase(db, log, validate, dailyPriceRepo, anomalyRepo)
	disclosureUC := usecase.NewDisclosureUseCase(db, log, validate, disclosureRepo)
	brokerUC := usecase.NewBrokerUseCase(db, log, validate, brokerRepo)
	newsUC := usecase.NewNewsUseCase(db, log, validate, newsRepo, newsTickerRepo)
	pipelineUC := usecase.NewPipelineUseCase(db, log, validate, sourceStatusRepo, alertRepo)

	router := chi.NewRouter()
	router.Use(chimw.Logger)
	router.Use(chimw.Recoverer)

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

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
