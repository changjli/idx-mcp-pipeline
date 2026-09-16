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
	sourceCorporateActions   = "idx:corporate_actions"
	sourceSuspensions        = "idx:suspensions"
	sourceKSEIBalancepos     = "ksei:balancepos"
	sourceSectorIndex        = "idx:sector_index"
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

// argStrings extracts a string-array argument, dropping non-string elements.
// An absent or wrongly-typed argument yields nil, which the usecase rejects as
// a missing required list.
func argStrings(args map[string]any, key string) []string {
	raw, ok := args[key].([]any)
	if !ok {
		return nil
	}
	values := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			values = append(values, s)
		}
	}
	return values
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

// disclosureSearchResponse wraps the usecase data with staleness metadata.
type disclosureSearchResponse struct {
	*usecase.DisclosureSearchData
	mcp.StalenessMetadata
}

func (s *Server) handleSearchDisclosures(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	query, _ := args["query"].(string)
	fromStr, _ := args["date_from"].(string)
	toStr, _ := args["date_to"].(string)

	var fromPtr, toPtr *time.Time
	if fromStr != "" {
		t, err := time.Parse("2006-01-02", fromStr)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date_from: "+fromStr, false)), nil
		}
		fromPtr = &t
	}
	if toStr != "" {
		t, err := time.Parse("2006-01-02", toStr)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date_to: "+toStr, false)), nil
		}
		toPtr = &t
	}

	data, err := s.disclosureUC.SearchDisclosures(ctx, query, fromPtr, toPtr, argLimit(args))
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(disclosureSearchResponse{
		DisclosureSearchData: data,
		StalenessMetadata:    stalenessFor(s.db, s.sourceStatusRepo, sourceIdxAnnouncements, time.Now()),
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

// fetchDisclosurePDFResponse wraps the usecase data with staleness metadata.
type fetchDisclosurePDFResponse struct {
	*usecase.FetchDisclosurePDFData
	mcp.StalenessMetadata
}

func (s *Server) handleFetchDisclosurePDF(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	id, ok := disclosureIDArg(req.GetArguments())
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid disclosure_id", false)), nil
	}

	data, err := s.fetchDisclosureUC.FetchDisclosurePDF(ctx, id)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(fetchDisclosurePDFResponse{
		FetchDisclosurePDFData: data,
		StalenessMetadata:      stalenessFor(s.db, s.sourceStatusRepo, sourceIdxAnnouncements, time.Now()),
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

// handleBackfillStockBrokerSummary validates the range and enqueues the async
// backfill task (issue 12). The response is the pending envelope — the worker
// owns the fetch+persist loop, the client polls get_stock_broker_summary_history.
func (s *Server) handleBackfillStockBrokerSummary(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
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

	data, err := s.brokerSummaryBackfillUC.BackfillStockBrokerSummary(ctx, norm, from, to)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(data), nil
}

// brokerNetFlowResponse wraps the usecase data with staleness metadata.
type brokerNetFlowResponse struct {
	*usecase.BrokerNetFlowResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetBrokerNetFlow(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	ticker, _ := args["ticker"].(string)
	fromStr, _ := args["from"].(string)
	toStr, _ := args["to"].(string)

	var tickerPtr *string
	if ticker != "" {
		norm, ok := s.tickers.Normalize(ticker)
		if !ok {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
		}
		tickerPtr = &norm
	}
	var fromPtr, toPtr *time.Time
	if fromStr != "" {
		t, err := time.Parse("2006-01-02", fromStr)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid from date: "+fromStr, false)), nil
		}
		fromPtr = &t
	}
	if toStr != "" {
		t, err := time.Parse("2006-01-02", toStr)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid to date: "+toStr, false)), nil
		}
		toPtr = &t
	}

	data, err := s.brokerStockSummaryUC.GetBrokerNetFlow(ctx, tickerPtr, fromPtr, toPtr)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(brokerNetFlowResponse{
		BrokerNetFlowResponse: data,
		StalenessMetadata:     stalenessFor(s.db, s.sourceStatusRepo, sourceBrokerStockSummary, time.Now()),
	}), nil
}

// sectorFlowResponse wraps the usecase data with staleness metadata from the
// idx:broker_stock_summary row — the sector read is a pure DB aggregation over
// broker rows, so its freshness is the broker flow's, not the taxonomy's.
type sectorFlowResponse struct {
	*usecase.SectorFlowResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetSectorFlow(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	fromStr, _ := args["from"].(string)
	toStr, _ := args["to"].(string)
	sectorArg, _ := args["sector"].(string)
	groupByArg, _ := args["group_by"].(string)
	breakdownArg, _ := args["breakdown"].(string)

	var fromPtr, toPtr *time.Time
	if fromStr != "" {
		t, err := time.Parse("2006-01-02", fromStr)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid from date: "+fromStr, false)), nil
		}
		fromPtr = &t
	}
	if toStr != "" {
		t, err := time.Parse("2006-01-02", toStr)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid to date: "+toStr, false)), nil
		}
		toPtr = &t
	}
	var sectorPtr *string
	if sectorArg != "" {
		sectorPtr = &sectorArg
	}

	data, err := s.sectorFlowUC.GetSectorFlow(ctx, usecase.SectorFlowRequest{
		From:      fromPtr,
		To:        toPtr,
		Sector:    sectorPtr,
		GroupBy:   groupByArg,
		Breakdown: breakdownArg,
	})
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(sectorFlowResponse{
		SectorFlowResponse: data,
		StalenessMetadata:  stalenessFor(s.db, s.sourceStatusRepo, sourceBrokerStockSummary, time.Now()),
	}), nil
}

// dailyPricesResponse wraps the usecase data with staleness metadata.
type dailyPricesResponse struct {
	*usecase.DailyPricesData
	mcp.StalenessMetadata
}

func (s *Server) handleGetDailyPrices(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
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

	data, err := s.dailyPriceUC.GetDailyPrices(ctx, norm, from, to)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(dailyPricesResponse{
		DailyPricesData:   data,
		StalenessMetadata: stalenessFor(s.db, s.sourceStatusRepo, sourceIdxStockSummary, time.Now()),
	}), nil
}

// computeIndicatorsResponse wraps the registry read with staleness metadata.
type computeIndicatorsResponse struct {
	*usecase.ComputeIndicatorsResponse
	mcp.StalenessMetadata
}

// handleComputeIndicators validates the call shape (mode, ticker list, as_of)
// and delegates the registry/period validation to the usecase, so a bad name
// or an over-cap list returns the same structured error whether it arrives over
// MCP or from another caller.
func (s *Server) handleComputeIndicators(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	mode, _ := args["mode"].(string)
	asOfStr, _ := args["as_of"].(string)
	window, _ := args["window"].(float64)

	tickers := argStrings(args, "tickers")
	if len(tickers) == 0 {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "tickers must list at least one ticker", false)), nil
	}
	normalized := make([]string, 0, len(tickers))
	for _, ticker := range tickers {
		norm, ok := s.tickers.Normalize(ticker)
		if !ok {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
		}
		normalized = append(normalized, norm)
	}

	var asOfPtr *time.Time
	if asOfStr != "" {
		t, err := time.Parse("2006-01-02", asOfStr)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid as_of date: "+asOfStr, false)), nil
		}
		asOfPtr = &t
	}

	data, err := s.computeIndicatorsUC.ComputeIndicators(ctx, usecase.ComputeIndicatorsRequest{
		Mode:       mode,
		Tickers:    normalized,
		Indicators: argStrings(args, "indicators"),
		AsOf:       asOfPtr,
		Window:     int(window),
	})
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(computeIndicatorsResponse{
		ComputeIndicatorsResponse: data,
		StalenessMetadata:         stalenessFor(s.db, s.sourceStatusRepo, sourceIdxStockSummary, time.Now()),
	}), nil
}

// screenStocksResponse wraps the funnel read with staleness metadata.
type screenStocksResponse struct {
	*usecase.ScreenStocksResponse
	mcp.StalenessMetadata
}

// handleScreenStocks parses the funnel parameters and delegates to the usecase,
// which owns every default, bound, and validation rule — so a bad limit, floor,
// sort key, or indicator spec returns the same structured error whether the
// call arrived over MCP or from another caller. The handler adds no ticker
// argument: the universe is the caller's screen, not a list they name.
func (s *Server) handleScreenStocks(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	asOfStr, _ := args["as_of"].(string)
	window, _ := args["window"].(float64)
	limit, _ := args["limit"].(float64)
	suspensionDays, _ := args["suspension_window_days"].(float64)
	sortKey, _ := args["sort"].(string)
	order, _ := args["order"].(string)

	minValue, ok := argMinValue(args)
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument,
			"min_value must be a whole number of rupiah, 0 or more", false)), nil
	}

	var asOfPtr *time.Time
	if asOfStr != "" {
		t, err := time.Parse("2006-01-02", asOfStr)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid as_of date: "+asOfStr, false)), nil
		}
		asOfPtr = &t
	}

	data, err := s.screenStocksUC.ScreenStocks(ctx, usecase.ScreenStocksRequest{
		AsOf:                 asOfPtr,
		Window:               int(window),
		MinValue:             minValue,
		Limit:                int(limit),
		SuspensionWindowDays: int(suspensionDays),
		Indicators:           argStrings(args, "indicators"),
		Sort:                 sortKey,
		Order:                order,
	})
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(screenStocksResponse{
		ScreenStocksResponse: data,
		StalenessMetadata:    stalenessFor(s.db, s.sourceStatusRepo, sourceIdxStockSummary, time.Now()),
	}), nil
}

// argMinValue extracts the optional min_value argument as whole rupiah. Absent
// yields nil, which the usecase resolves to its default floor; a fractional,
// negative, or out-of-range value is rejected rather than silently rounded into
// a different floor than the caller asked for.
func argMinValue(args map[string]any) (*int64, bool) {
	raw, ok := args["min_value"]
	if !ok {
		return nil, true
	}
	v, ok := raw.(float64)
	if !ok || v < 0 || v != math.Trunc(v) || v > math.MaxInt64 {
		return nil, false
	}
	value := int64(v)
	return &value, true
}

// financialsResponse wraps the live financial statements with the staleness
// envelope. The fetch is live, so data_stale is always false (omitted, per
// the shared metadata convention); last_good_date is the newest statement
// period end, not a pipeline timestamp.
type financialsResponse struct {
	*usecase.FinancialsResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetFinancials(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	ticker, _ := req.GetArguments()["ticker"].(string)
	period, _ := req.GetArguments()["period"].(string)

	norm, ok := s.tickers.Normalize(ticker)
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
	}

	data, err := s.financialsUC.GetFinancials(ctx, norm, period)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(financialsResponse{
		FinancialsResponse: data,
		StalenessMetadata: mcp.StalenessMetadata{
			LastGoodDate: data.LatestPeriodEnd,
		},
	}), nil
}

// corporateActionsResponse wraps the usecase data with staleness metadata from
// the idx:corporate_actions source_status row, which the on-demand fetch
// updates on every call.
type corporateActionsResponse struct {
	*usecase.CorporateActionsResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetCorporateActions(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	fromStr, _ := req.GetArguments()["date_from"].(string)
	toStr, _ := req.GetArguments()["date_to"].(string)
	ticker, _ := req.GetArguments()["ticker"].(string)

	from, err := time.Parse("2006-01-02", fromStr)
	if err != nil {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date_from: "+fromStr, false)), nil
	}
	to, err := time.Parse("2006-01-02", toStr)
	if err != nil {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date_to: "+toStr, false)), nil
	}

	var tickerPtr *string
	if ticker != "" {
		norm, ok := s.tickers.Normalize(ticker)
		if !ok {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
		}
		tickerPtr = &norm
	}

	data, err := s.corporateActionsUC.GetCorporateActions(ctx, from, to, tickerPtr)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(corporateActionsResponse{
		CorporateActionsResponse: data,
		StalenessMetadata:        stalenessFor(s.db, s.sourceStatusRepo, sourceCorporateActions, time.Now()),
	}), nil
}

// suspensionsResponse wraps the usecase data with staleness metadata from the
// idx:suspensions source_status row, which the daily task updates on every run.
type suspensionsResponse struct {
	*usecase.SuspensionsResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetSuspensions(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	fromStr, _ := req.GetArguments()["date_from"].(string)
	toStr, _ := req.GetArguments()["date_to"].(string)
	ticker, _ := req.GetArguments()["ticker"].(string)

	from, err := time.Parse("2006-01-02", fromStr)
	if err != nil {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date_from: "+fromStr, false)), nil
	}
	to, err := time.Parse("2006-01-02", toStr)
	if err != nil {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date_to: "+toStr, false)), nil
	}

	var tickerPtr *string
	if ticker != "" {
		norm, ok := s.tickers.Normalize(ticker)
		if !ok {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
		}
		tickerPtr = &norm
	}

	data, err := s.suspensionsUC.GetSuspensions(ctx, from, to, tickerPtr)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(suspensionsResponse{
		SuspensionsResponse: data,
		StalenessMetadata:   stalenessFor(s.db, s.sourceStatusRepo, sourceSuspensions, time.Now()),
	}), nil
}

// shareholderCompositionResponse wraps the usecase data with staleness
// metadata from the ksei:balancepos source_status row, which the monthly task
// updates on every run (including no-ops).
type shareholderCompositionResponse struct {
	*usecase.ShareholderCompositionResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetShareholderComposition(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	ticker, _ := req.GetArguments()["ticker"].(string)
	fromStr, _ := req.GetArguments()["date_from"].(string)
	toStr, _ := req.GetArguments()["date_to"].(string)

	if ticker == "" {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "ticker is required", false)), nil
	}
	norm, ok := s.tickers.Normalize(ticker)
	if !ok {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+ticker, false)), nil
	}
	from, err := time.Parse("2006-01-02", fromStr)
	if err != nil {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date_from: "+fromStr, false)), nil
	}
	to, err := time.Parse("2006-01-02", toStr)
	if err != nil {
		return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date_to: "+toStr, false)), nil
	}

	data, err := s.shareholderCompositionUC.GetShareholderComposition(ctx, norm, from, to)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(shareholderCompositionResponse{
		ShareholderCompositionResponse: data,
		StalenessMetadata:              stalenessFor(s.db, s.sourceStatusRepo, sourceKSEIBalancepos, time.Now()),
	}), nil
}

// tickerMetadataResponse wraps the usecase data with staleness metadata from
// the idx:sector_index source_status row, which the 15b seeder updates on
// every run (6-monthly Feb+Jul rebalance cadence).
type tickerMetadataResponse struct {
	*usecase.TickerMetadataResponse
	mcp.StalenessMetadata
}

func (s *Server) handleGetTickerMetadata(ctx context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	tickerArg, _ := req.GetArguments()["ticker"].(string)
	dateArg, _ := req.GetArguments()["date"].(string)

	var tickerPtr *string
	if tickerArg != "" {
		norm, ok := s.tickers.Normalize(tickerArg)
		if !ok {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidTicker, "invalid ticker: "+tickerArg, false)), nil
		}
		tickerPtr = &norm
	}
	var asOf *time.Time
	if dateArg != "" {
		d, err := time.Parse("2006-01-02", dateArg)
		if err != nil {
			return envelopeResult(mcp.NewError(mcp.ErrorCodeInvalidArgument, "invalid date: "+dateArg, false)), nil
		}
		asOf = &d
	}

	data, err := s.tickerMetadataUC.GetTickerMetadata(ctx, tickerPtr, asOf)
	if err != nil {
		return envelopeResult(exceptionToEnvelope(err)), nil
	}
	return textResult(tickerMetadataResponse{
		TickerMetadataResponse: data,
		StalenessMetadata:      stalenessFor(s.db, s.sourceStatusRepo, sourceSectorIndex, time.Now()),
	}), nil
}
