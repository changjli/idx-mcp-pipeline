package tasks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TestStockSummary_FlareSolverr_EndToEnd drives the stock summary handler with a
// fake FlareSolverr returning a canned GetStockSummary payload (wrapped in the
// <pre> envelope) and asserts Daily Price rows land in Postgres. Skipped unless
// IDX_MCP_DB_DSN is set.
func TestStockSummary_FlareSolverr_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()

	const ticker = "FLARX"
	date := "2026-08-21"

	// Clean slate: only rows this test creates.
	db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
	db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
	db.MustExec("DELETE FROM source_status WHERE source = $1", TypeStockSummary)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM daily_prices WHERE ticker = $1", ticker)
		db.MustExec("DELETE FROM tickers WHERE code = $1", ticker)
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeStockSummary)
	})

	// Canned GetStockSummary payload wrapped in FlareSolverr's <pre> envelope.
	open, high, low, closeP, vol, val, freq, shares := 1000.0, 1100.0, 990.0, 1050.0, 100000.0, 105000000.0, 500.0, 1000000000.0
	payload := StockSummaryResponse{
		Draw:            1,
		RecordsTotal:    1,
		RecordsFiltered: 1,
		Data: []StockSummaryItem{
			{StockCode: ticker, StockName: "Flare Test Tbk.", OpenPrice: &open, High: &high, Low: &low, Close: &closeP, Volume: &vol, Value: &val, Frequency: &freq, ListedShares: &shares},
		},
	}
	rawPayload, _ := json.Marshal(payload)
	envelope := "<html><body><pre>" + string(rawPayload) + "</pre></body></html>"

	// Fake FlareSolverr /v1.
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			json.NewEncoder(w).Encode(map[string]string{"msg": "FlareSolverr is ready!"})
			return
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch req["cmd"] {
		case "sessions.create":
			json.NewEncoder(w).Encode(map[string]any{"status": "ok", "session": "test-session"})
		case "sessions.destroy":
			json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		case "request.get":
			json.NewEncoder(w).Encode(map[string]any{
				"status": "ok",
				"solution": map[string]any{
					"status":   http.StatusOK,
					"response": envelope,
				},
			})
		default:
			json.NewEncoder(w).Encode(map[string]any{"status": "error", "message": "unknown cmd"})
		}
	}))
	defer ts.Close()

	// Proxy list file.
	dir := t.TempDir()
	proxyFile := filepath.Join(dir, "proxies.json")
	if err := os.WriteFile(proxyFile, []byte(`["http://127.0.0.1:1"]`), 0o644); err != nil {
		t.Fatalf("write proxy list: %v", err)
	}

	idxClient, err := client.NewClient(client.Config{
		BaseURL:         "https://idx.example",
		Timeout:         5 * time.Second,
		RateLimitPerSec: 1000,
		FetchMode:       "flaresolverr",
		FlareSolverr: client.FlareSolverrConfig{
			BaseURL:           ts.URL,
			Timeout:           time.Second,
			Proxies:           proxyFile,
			ProxiesTTL:        time.Hour,
			DeadRetryAfter:    time.Hour,
			WakeTimeout:       2 * time.Second,
			SessionTTLMinutes: 30,
		},
	}, log)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer idxClient.Close()

	tickerRepo := repository.NewTickerRepository(log)
	dailyPriceRepo := repository.NewDailyPriceRepository(log)
	sourceStatusRepo := repository.NewSourceStatusRepository(log)
	alertRepo := repository.NewAlertRepository(log)

	// asynq client pointed at a dead Redis: the detect:anomalies chain fails
	// fast and is logged, never touching a real queue.
	asynqClient := asynq.NewClient(asynq.RedisClientOpt{Addr: "127.0.0.1:1"})
	defer asynqClient.Close()

	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(sourceStatusRepo, db), pipeline.NewSQLAlertStore(alertRepo, db), log,
	)
	handler := NewStockSummaryHandler(log, idxClient, db, asynqClient, tickerRepo, dailyPriceRepo, recorder)

	payloadBytes, _ := json.Marshal(StockSummaryPayload{Date: date})
	task := asynq.NewTask(TypeStockSummary, payloadBytes)
	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// Assert the Daily Price row landed for the Trading Day.
	var count int
	if err := db.Get(&count, "SELECT COUNT(*) FROM daily_prices WHERE ticker = $1 AND trading_day = $2", ticker, date); err != nil {
		t.Fatalf("query daily_prices: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 daily_price row, got %d", count)
	}
}
