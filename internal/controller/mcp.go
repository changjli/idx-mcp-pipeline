package controller

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

type MCPController struct {
	Log              *logrus.Logger
	AnomalyUseCase   *usecase.AnomalyUseCase
	DisclosureUseCase *usecase.DisclosureUseCase
	BrokerUseCase    *usecase.BrokerUseCase
	NewsUseCase      *usecase.NewsUseCase
	PipelineUseCase  *usecase.PipelineUseCase
}

func NewMCPController(
	log *logrus.Logger,
	anomalyUC *usecase.AnomalyUseCase,
	disclosureUC *usecase.DisclosureUseCase,
	brokerUC *usecase.BrokerUseCase,
	newsUC *usecase.NewsUseCase,
	pipelineUC *usecase.PipelineUseCase,
) *MCPController {
	return &MCPController{
		Log:              log,
		AnomalyUseCase:   anomalyUC,
		DisclosureUseCase: disclosureUC,
		BrokerUseCase:    brokerUC,
		NewsUseCase:      newsUC,
		PipelineUseCase:  pipelineUC,
	}
}

func (c *MCPController) RegisterRoutes(r chi.Router) {
	r.Post("/mcp/get_market_anomalies", c.GetMarketAnomalies)
	r.Post("/mcp/read_idx_disclosure", c.ReadIdxDisclosure)
	r.Post("/mcp/list_idx_disclosures", c.ListIdxDisclosures)
	r.Post("/mcp/get_ticker_news", c.GetTickerNews)
	r.Post("/mcp/get_broker_summary", c.GetBrokerSummary)
	r.Post("/mcp/get_pipeline_status", c.GetPipelineStatus)
}

func (c *MCPController) GetMarketAnomalies(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (c *MCPController) ReadIdxDisclosure(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (c *MCPController) ListIdxDisclosures(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (c *MCPController) GetTickerNews(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (c *MCPController) GetBrokerSummary(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

func (c *MCPController) GetPipelineStatus(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
