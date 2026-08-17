package mcpserver

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// Source names in source_status, per tool. Anomalies and the aggregate broker
// summary derive from daily_prices, which the idx stock_summary task owns.
const (
	sourceIdxStockSummary    = "idx"
	sourceIdxAnnouncements   = "idx:announcements"
	sourceRSS                = "rss"
	sourceBrokerStockSummary = "idx:broker_stock_summary"
)

// defaultLimit is the default row cap for tools with a limit argument.
const defaultLimit = 20

// maxLimit caps a caller-supplied limit so one request can't dump the table.
const maxLimit = 100

// textResult marshals a successful response payload into an MCP text result.
func textResult(v interface{}) *mcpgo.CallToolResult {
	raw, err := json.Marshal(v)
	if err != nil {
		return mcpgo.NewToolResultError(`{"error":{"code":"INTERNAL","message":"marshal response: ` + err.Error() + `","retryable":false}}`)
	}
	return mcpgo.NewToolResultText(string(raw))
}

// envelopeResult returns an MCP error result whose body is the structured
// error envelope. IsError=true signals the protocol-level failure; the body
// carries the envelope the LLM reads.
func envelopeResult(env mcp.ErrorEnvelope) *mcpgo.CallToolResult {
	raw, err := json.Marshal(env)
	if err != nil {
		return mcpgo.NewToolResultError(`{"error":{"code":"INTERNAL","message":"marshal envelope","retryable":false}}`)
	}
	return mcpgo.NewToolResultError(string(raw))
}

// disclosureIDArg extracts disclosure_id in either the documented string form
// or the numeric form get_market_anomalies emits in disclosure_ids, so an LLM
// copying a numeric ID doesn't break the tool chain.
func disclosureIDArg(args map[string]any) (int64, bool) {
	switch v := args["disclosure_id"].(type) {
	case string:
		id, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil || id <= 0 {
			return 0, false
		}
		return id, true
	case float64:
		if v < 1 || v > math.MaxInt64 || v != math.Trunc(v) {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

// argLimit extracts the limit argument, defaulting to defaultLimit and capping
// at maxLimit.
func argLimit(args map[string]any) int {
	v, ok := args["limit"].(float64)
	if !ok {
		return defaultLimit
	}
	n := int(v)
	if n <= 0 {
		return defaultLimit
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

// marketAnomaliesResponse wraps the usecase data with staleness metadata.
type marketAnomaliesResponse struct {
	*usecase.MarketAnomaliesData
	mcp.StalenessMetadata
}

func (s *Server) handleGetMarketAnomalies(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	date, _ := req.GetArguments()["date"].(string)
	ticker, _ := req.GetArguments()["ticker"].(string)

	var datePtr *string
	if date != "" {
		datePtr = &date
	}
	var tickerPtr *string
	if ticker != "" {
		norm, ok := s.tickers.Normalize(ticker)
		if !ok {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
		}
		tickerPtr = &norm
	}

	data, err := s.anomalyUC.GetMarketAnomalies(ctx, datePtr, tickerPtr)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(marketAnomaliesResponse{
		MarketAnomaliesData: data,
		StalenessMetadata:   stalenessFor(s.db, s.sourceStatusRepo, sourceIdxStockSummary, time.Now()),
	}), nil
}

// tickerNewsResponse wraps the usecase data with staleness metadata.
type tickerNewsResponse struct {
	*usecase.TickerNewsData
	mcp.StalenessMetadata
}

func (s *Server) handleGetTickerNews(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	ticker, _ := req.GetArguments()["ticker"].(string)
	since, _ := req.GetArguments()["since"].(string)

	norm, ok := s.tickers.Normalize(ticker)
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
	}
	var sincePtr *string
	if since != "" {
		sincePtr = &since
	}

	data, err := s.newsUC.GetTickerNews(ctx, norm, sincePtr, argLimit(req.GetArguments()))
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(tickerNewsResponse{
		TickerNewsData:    data,
		StalenessMetadata: stalenessFor(s.db, s.sourceStatusRepo, sourceRSS, time.Now()),
	}), nil
}

// brokerSummaryResponse wraps the usecase data with staleness metadata.
type brokerSummaryResponse struct {
	*usecase.BrokerSummaryData
	mcp.StalenessMetadata
}

func (s *Server) handleGetBrokerSummary(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	date, _ := req.GetArguments()["date"].(string)
	var datePtr *string
	if date != "" {
		datePtr = &date
	}

	data, err := s.brokerUC.GetBrokerSummary(ctx, datePtr)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(brokerSummaryResponse{
		BrokerSummaryData: data,
		StalenessMetadata: stalenessFor(s.db, s.sourceStatusRepo, sourceIdxStockSummary, time.Now()),
	}), nil
}

// disclosureListResponse wraps the usecase data with staleness metadata.
type disclosureListResponse struct {
	*usecase.DisclosureListData
	mcp.StalenessMetadata
}

func (s *Server) handleListIdxDisclosures(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	ticker, _ := req.GetArguments()["ticker"].(string)
	date, _ := req.GetArguments()["date"].(string)

	norm, ok := s.tickers.Normalize(ticker)
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
	}
	var datePtr *string
	if date != "" {
		datePtr = &date
	}

	data, err := s.disclosureUC.ListIdxDisclosures(ctx, norm, datePtr, argLimit(req.GetArguments()))
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(disclosureListResponse{
		DisclosureListData: data,
		StalenessMetadata:  stalenessFor(s.db, s.sourceStatusRepo, sourceIdxAnnouncements, time.Now()),
	}), nil
}

// disclosureReadResponse wraps the usecase data with staleness metadata.
type disclosureReadResponse struct {
	*usecase.ReadIdxDisclosureData
	mcp.StalenessMetadata
}

func (s *Server) handleReadIdxDisclosure(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	id, ok := disclosureIDArg(req.GetArguments())
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid disclosure_id", false)), nil
	}

	data, err := s.disclosureUC.ReadIdxDisclosure(ctx, id)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(disclosureReadResponse{
		ReadIdxDisclosureData: data,
		StalenessMetadata:     stalenessFor(s.db, s.sourceStatusRepo, sourceIdxAnnouncements, time.Now()),
	}), nil
}

// pipelineStatusResponse wraps the usecase data with overall staleness.
type pipelineStatusResponse struct {
	*usecase.PipelineStatusData
	mcp.StalenessMetadata
}

func (s *Server) handleGetPipelineStatus(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	data, err := s.pipelineUC.GetPipelineStatus(ctx)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(pipelineStatusResponse{
		PipelineStatusData: data,
		StalenessMetadata:  pipelineStaleness(s.db, s.sourceStatusRepo, time.Now()),
	}), nil
}

// stockBrokerSummaryResponse wraps the usecase data with staleness metadata.
type stockBrokerSummaryResponse struct {
	*usecase.BrokerStockSummaryResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetStockBrokerSummary(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	ticker, _ := req.GetArguments()["ticker"].(string)
	date, _ := req.GetArguments()["date"].(string)

	norm, ok := s.tickers.Normalize(ticker)
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
	}
	var datePtr *time.Time
	if date != "" {
		t, err := time.Parse("2006-01-02", date)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date: "+date, false)), nil
		}
		datePtr = &t
	}

	data, err := s.brokerStockSummaryUC.GetStockBrokerSummary(ctx, norm, datePtr)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(stockBrokerSummaryResponse{
		BrokerStockSummaryResponse: data,
		StalenessMetadata:          stalenessFor(s.db, s.sourceStatusRepo, sourceBrokerStockSummary, time.Now()),
	}), nil
}

// stockBrokerSummaryHistoryResponse wraps the usecase data with staleness.
type stockBrokerSummaryHistoryResponse struct {
	*usecase.BrokerStockSummaryHistoryResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetStockBrokerSummaryHistory(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	ticker, _ := req.GetArguments()["ticker"].(string)
	fromStr, _ := req.GetArguments()["from"].(string)
	toStr, _ := req.GetArguments()["to"].(string)

	norm, ok := s.tickers.Normalize(ticker)
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
	}
	from, err := time.Parse("2006-01-02", fromStr)
	if err != nil {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid from date: "+fromStr, false)), nil
	}
	to, err := time.Parse("2006-01-02", toStr)
	if err != nil {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid to date: "+toStr, false)), nil
	}

	data, err := s.brokerStockSummaryUC.GetStockBrokerSummaryHistory(ctx, norm, from, to)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(stockBrokerSummaryHistoryResponse{
		BrokerStockSummaryHistoryResponse: data,
		StalenessMetadata:                 stalenessFor(s.db, s.sourceStatusRepo, sourceBrokerStockSummary, time.Now()),
	}), nil
}
