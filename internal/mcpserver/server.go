package mcpserver

import (
	"net/http"

	"github.com/jmoiron/sqlx"
	"github.com/mark3labs/mcp-go/server"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// Deps carries the dependencies the MCP server wires. Handlers stay thin:
// validate → delegate to a usecase → return.
type Deps struct {
	Log                     *logrus.Logger
	DB                      *sqlx.DB
	AnomalyUC               *usecase.AnomalyUseCase
	DisclosureUC            *usecase.DisclosureUseCase
	FetchDisclosureUC       *usecase.FetchDisclosureUseCase
	BrokerUC                *usecase.BrokerUseCase
	NewsUC                  *usecase.NewsUseCase
	PipelineUC              *usecase.PipelineUseCase
	BrokerStockSummaryUC    *usecase.BrokerStockSummaryUseCase
	BrokerSummaryBackfillUC *usecase.BrokerSummaryBackfillUseCase
	DailyPriceUC            *usecase.DailyPriceUseCase
	FinancialsUC            *usecase.FinancialsUseCase
	SourceStatusRepo        *repository.SourceStatusRepository
	TickerRepo              *repository.TickerRepository
}

// Server is the MCP server over streamable HTTP. It owns the tool registry
// and the symbol normalization seam.
type Server struct {
	log                     *logrus.Logger
	db                      *sqlx.DB
	anomalyUC               *usecase.AnomalyUseCase
	disclosureUC            usecase.DisclosureReader
	fetchDisclosureUC       *usecase.FetchDisclosureUseCase
	brokerUC                *usecase.BrokerUseCase
	newsUC                  *usecase.NewsUseCase
	pipelineUC              *usecase.PipelineUseCase
	brokerStockSummaryUC    *usecase.BrokerStockSummaryUseCase
	brokerSummaryBackfillUC *usecase.BrokerSummaryBackfillUseCase
	dailyPriceUC            *usecase.DailyPriceUseCase
	financialsUC            *usecase.FinancialsUseCase
	sourceStatusRepo        *repository.SourceStatusRepository
	tickers                 *TickerValidator
}

func NewServer(deps Deps) *Server {
	return &Server{
		log:                     deps.Log,
		db:                      deps.DB,
		anomalyUC:               deps.AnomalyUC,
		disclosureUC:            deps.DisclosureUC,
		fetchDisclosureUC:       deps.FetchDisclosureUC,
		brokerUC:                deps.BrokerUC,
		newsUC:                  deps.NewsUC,
		pipelineUC:              deps.PipelineUC,
		brokerStockSummaryUC:    deps.BrokerStockSummaryUC,
		brokerSummaryBackfillUC: deps.BrokerSummaryBackfillUC,
		dailyPriceUC:            deps.DailyPriceUC,
		financialsUC:            deps.FinancialsUC,
		sourceStatusRepo:        deps.SourceStatusRepo,
		tickers:                 NewTickerValidator(deps.DB, deps.TickerRepo, deps.Log),
	}
}

// Handler returns the streamable-HTTP MCP handler with all tools registered.
// Mount behind the bearer-token auth middleware.
func (s *Server) Handler() http.Handler {
	srv := server.NewMCPServer("idx-mcp", "1.0.0", server.WithInstructions(instructions))
	srv.AddTool(toolGetMarketAnomalies, s.handleGetMarketAnomalies)
	srv.AddTool(toolGetTickerNews, s.handleGetTickerNews)
	srv.AddTool(toolGetBrokerSummary, s.handleGetBrokerSummary)
	srv.AddTool(toolListIdxDisclosures, s.handleListIdxDisclosures)
	srv.AddTool(toolSearchDisclosures, s.handleSearchDisclosures)
	srv.AddTool(toolReadIdxDisclosure, s.handleReadIdxDisclosure)
	srv.AddTool(toolFetchDisclosurePDF, s.handleFetchDisclosurePDF)
	srv.AddTool(toolGetPipelineStatus, s.handleGetPipelineStatus)
	srv.AddTool(toolGetStockBrokerSummary, s.handleGetStockBrokerSummary)
	srv.AddTool(toolGetStockBrokerSummaryHistory, s.handleGetStockBrokerSummaryHistory)
	srv.AddTool(toolBackfillStockBrokerSummary, s.handleBackfillStockBrokerSummary)
	srv.AddTool(toolGetBrokerNetFlow, s.handleGetBrokerNetFlow)
	srv.AddTool(toolGetDailyPrices, s.handleGetDailyPrices)
	srv.AddTool(toolGetFinancials, s.handleGetFinancials)
	return server.NewStreamableHTTPServer(srv)
}
