package mcpserver

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// TestHandleGetBrokerNetFlowInvalidArgs covers the parse path: ticker
// normalization and date parsing return the envelope before the usecase is
// touched (TickerValidator over the bundled list, no DB needed).
func TestHandleGetBrokerNetFlowInvalidArgs(t *testing.T) {
	s := &Server{tickers: NewTickerValidator(nil, nil, logrus.New())}

	cases := []struct {
		name string
		args map[string]any
		want mcp.ErrorCode
	}{
		{"invalid ticker", map[string]any{"ticker": "ZZZZ", "from": "2026-01-01", "to": "2026-01-31"}, mcp.ErrorCodeInvalidTicker},
		{"empty ticker", map[string]any{"ticker": "  ", "from": "2026-01-01", "to": "2026-01-31"}, mcp.ErrorCodeInvalidTicker},
		{"invalid from", map[string]any{"ticker": "BBCA", "from": "abc", "to": "2026-01-31"}, mcp.ErrorCodeInvalidArgument},
		{"invalid to", map[string]any{"ticker": "BBCA", "from": "2026-01-01", "to": "2026-13-40"}, mcp.ErrorCodeInvalidArgument},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
				Name:      "get_broker_net_flow",
				Arguments: tc.args,
			}}
			res, err := s.handleGetBrokerNetFlow(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !res.IsError {
				t.Fatal("result must be an error")
			}
			text, ok := res.Content[0].(mcpgo.TextContent)
			if !ok {
				t.Fatalf("content type = %T, want text", res.Content[0])
			}
			var got mcp.ErrorEnvelope
			if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
				t.Fatalf("unmarshal envelope: %v", err)
			}
			if got.Error.Code != tc.want {
				t.Fatalf("code = %q, want %q", got.Error.Code, tc.want)
			}
		})
	}
}

// netFlowTestServer builds a Server wired to the real broker-stock-summary
// usecase over the test DB.
func netFlowTestServer(t *testing.T, db *sqlx.DB) *Server {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	uc := usecase.NewBrokerStockSummaryUseCase(
		db, log, validator.New(), nil,
		repository.NewBrokerStockSummaryRepository(log),
		repository.NewDailyPriceRepository(log),
	)
	return &Server{
		brokerStockSummaryUC: uc,
		tickers:              NewTickerValidator(db, repository.NewTickerRepository(log), log),
	}
}

// i64ptr mirrors the usecase package's pointer helper for seeding.
func i64ptr(v int64) *int64 { return &v }

func mustCallNetFlow(t *testing.T, s *Server, args map[string]any) map[string]any {
	t.Helper()
	req := mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{
		Name:      "get_broker_net_flow",
		Arguments: args,
	}}
	res, err := s.handleGetBrokerNetFlow(context.Background(), req)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.IsError {
		text, _ := res.Content[0].(mcpgo.TextContent)
		t.Fatalf("handler returned error: %s", text.Text)
	}
	text, ok := res.Content[0].(mcpgo.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want text", res.Content[0])
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return got
}

// TestHandleGetBrokerNetFlowSuccess both modes ride the success envelope with
// the parsed rows. DB-backed (real usecase + validator over the tickers table).
func TestHandleGetBrokerNetFlowSuccess(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	s := netFlowTestServer(t, db)

	ticker := "TESTN"
	d1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summary_totals WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})
	// TESTN must be in the validator's universe (tickers table) for the
	// per-ticker path; the market-wide path skips normalization entirely.
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", ticker, ticker)
	db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)
		VALUES ($1, $2, 100, 101, 99, 100, 1000, 100000, 10, 'idx')`, ticker, d1)
	db.MustExec(`INSERT INTO daily_prices (ticker, trading_day, open, high, low, close, volume, value, frequency, source)
		VALUES ($1, $2, 100, 101, 99, 100, 1000, 100000, 10, 'idx')`, ticker, d2)
	repo := repository.NewBrokerStockSummaryRepository(logrus.New())
	rows := []entity.BrokerStockSummary{
		{Ticker: ticker, BrokerCode: "AK", Side: "buy", TradingDay: d1, Value: i64ptr(10_000_000_000)},
		{Ticker: ticker, BrokerCode: "XL", Side: "sell", TradingDay: d1, Value: i64ptr(6_000_000_000)},
	}
	totals := &entity.BrokerStockSummaryTotals{Ticker: ticker, TradingDay: d1, TVal: i64ptr(0)}
	if err := repo.UpsertDay(db, rows, totals); err != nil {
		t.Fatalf("seed: %v", err)
	}

	d1s, d2s := d1.Format("2006-01-02"), d2.Format("2006-01-02")

	// Per-ticker success.
	got := mustCallNetFlow(t, s, map[string]any{"ticker": ticker, "from": d1s, "to": d2s})
	if got["mode"] != "ticker" {
		t.Errorf("mode = %v, want ticker", got["mode"])
	}
	rowsOut, ok := got["rows"].([]any)
	if !ok || len(rowsOut) != 2 {
		t.Fatalf("rows = %v, want 2 entries", got["rows"])
	}
	first := rowsOut[0].(map[string]any)
	if first["broker_code"] != "AK" || first["net"] != float64(10_000_000_000) {
		t.Errorf("rows[0] = %v, want AK net 10B (top accumulator)", first)
	}

	// Market-wide success (no ticker → no normalization).
	gotM := mustCallNetFlow(t, s, map[string]any{"from": d1s, "to": d2s})
	if gotM["mode"] != "market" {
		t.Errorf("mode = %v, want market", gotM["mode"])
	}
	if gotM["tickers_covered"] != float64(1) {
		t.Errorf("tickers_covered = %v, want 1", gotM["tickers_covered"])
	}
}

// TestHandleGetBrokerNetFlowEmptyWindow — stored rows absent: success envelope
// with an empty rows list (never an error), coverage 0.
func TestHandleGetBrokerNetFlowEmptyWindow(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	s := netFlowTestServer(t, db)

	ticker := "TESTN"
	d1 := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM broker_stock_summaries WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	})
	db.MustExec("INSERT INTO tickers (code, name, active) VALUES ($1, $2, true)", ticker, ticker)

	got := mustCallNetFlow(t, s, map[string]any{
		"ticker": ticker,
		"from":   d1.Format("2006-01-02"),
		"to":     d2.Format("2006-01-02"),
	})
	rows, ok := got["rows"].([]any)
	if !ok || len(rows) != 0 {
		t.Errorf("rows = %v, want empty list", got["rows"])
	}
	if got["covered_days"] != float64(0) {
		t.Errorf("covered_days = %v, want 0", got["covered_days"])
	}
}
