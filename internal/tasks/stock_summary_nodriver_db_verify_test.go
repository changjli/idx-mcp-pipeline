package tasks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// TestStockSummary_Nodriver_EndToEnd drives the stock summary handler with a
// fake nodriver sidecar returning a canned GetStockSummary payload and asserts
// Daily Price rows land in Postgres. Skipped unless IDX_MCP_DB_DSN is set.
func TestStockSummary_Nodriver_EndToEnd(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()

	const ticker = "NODRX"
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

	// Canned GetStockSummary payload returned verbatim by the fake sidecar.
	open, high, low, closeP, vol, val, freq, shares := 1000.0, 1100.0, 990.0, 1050.0, 100000.0, 105000000.0, 500.0, 1000000000.0
	payload := StockSummaryResponse{
		Draw:            1,
		RecordsTotal:    1,
		RecordsFiltered: 1,
		Data: []pipeline.StockSummaryItem{
			{StockCode: ticker, StockName: "Nodriver Test Tbk.", OpenPrice: &open, High: &high, Low: &low, Close: &closeP, Volume: &vol, Value: &val, Frequency: &freq, ListedShares: &shares},
		},
	}
	rawPayload, _ := json.Marshal(payload)

	// Fake nodriver sidecar: /health + /fetch.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		var req struct {
			URL   string `json:"url"`
			Proxy string `json:"proxy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.URL == "" || req.Proxy == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status": http.StatusOK,
			"body":   string(rawPayload),
		})
	}))
	defer ts.Close()

	// Proxy list file.
	dir := t.TempDir()
	proxyFile := filepath.Join(dir, "proxies.json")
	if err := os.WriteFile(proxyFile, []byte(`["http://127.0.0.1:1"]`), 0o644); err != nil {
		t.Fatalf("write proxy list: %v", err)
	}

	idxClient, err := client.NewClient(client.Config{
		BaseURL: "https://idx.example",
		Nodriver: client.NodriverConfig{
			BaseURL:        ts.URL,
			Timeout:        time.Second,
			WakeTimeout:    2 * time.Second,
			Proxies:        proxyFile,
			ProxiesTTL:     time.Hour,
			DeadRetryAfter: time.Hour,
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
	ingest := pipeline.NewStockSummaryIngest(
		pipeline.NewSQLDailyPriceStore(dailyPriceRepo, db),
		pipeline.NewSQLTickerRegistrar(tickerRepo, db),
		log,
	)
	handler := NewStockSummaryHandler(log, idxClient, db, asynqClient, recorder, ingest)

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
