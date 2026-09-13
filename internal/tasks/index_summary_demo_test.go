package tasks

import (
	"context"
	"encoding/json"
	"os"
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

// sectorIndexCodes are the 11 IDX sector indices GetIndexSummary returns
// alongside the 34 main indices (findings-sector-index-membership.md §3).
var sectorIndexCodes = []string{
	"IDXENERGY", "IDXBASIC", "IDXINDUST", "IDXNONCYC", "IDXCYCLIC",
	"IDXHEALTH", "IDXFINANCE", "IDXPROPERT", "IDXTECHNO", "IDXINFRA", "IDXTRANS",
}

// demoSectorIndexCodes maps each sector code to a realistic NumberOfStock for
// the demo snapshot (documented in the 15b findings' validation probe).
var demoSectorIndexCounts = map[string]float64{
	"IDXENERGY": 90, "IDXBASIC": 113, "IDXINDUST": 64, "IDXNONCYC": 124,
	"IDXCYCLIC": 147, "IDXHEALTH": 41, "IDXFINANCE": 106, "IDXPROPERT": 89,
	"IDXTECHNO": 40, "IDXINFRA": 67, "IDXTRANS": 37,
}

// TestIndexSummaryHandler_Demo45Rows is the issue-18 demo: one day's 45-row
// GetIndexSummary snapshot (34 main + 11 sector) persists in full, the 11
// sector indices land, and LQ45's NumberOfStock (45) survives — the
// membership-count validation against ticker_indices (ticket 15b). Skipped
// unless IDX_MCP_DB_DSN is set; the fetch is canned (no live IDX calls).
func TestIndexSummaryHandler_Demo45Rows(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}

	db := sqlx.MustConnect("pgx", dsn)
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	cleanup := func() {
		db.MustExec("DELETE FROM index_summaries WHERE date = $1", demoDay)
		db.MustExec("DELETE FROM source_status WHERE source = $1", TypeIndexSummary)
	}
	cleanup()
	t.Cleanup(cleanup)

	// The 45-index universe: the 11 sector codes verbatim, LQ45 among the
	// main codes, and 16 clearly-marked filler codes to reach the real 45-row
	// width. The demo's contract is persistence width + sector coverage, not
	// enumerating the real 34-main universe (a data question, not an ingestion
	// one) — the filler is labeled so nobody mistakes it for IDX data.
	day := demoDay
	rows := []client.IndexSummary{
		{Date: day, IndexCode: "COMPOSITE", Previous: 6686.442, Highest: 6707.305, Lowest: 6642.153, Close: 6678.201, NumberOfStock: 918, Change: -8.241, Volume: 32530207402, Value: 16134522240878, Frequency: 2147005, MarketCap: 1.16749775143559e+16},
		{Date: day, IndexCode: "LQ45", Previous: 1000.0, Highest: 1100.0, Lowest: 990.0, Close: 1050.5, NumberOfStock: 45, Change: 50.5, Volume: 100000000, Value: 1000000000000, Frequency: 100000, MarketCap: 9000000000000000},
		// 32 filler codes to fill the 34-main universe (2 real main + 32 filler
		// = 34; + the 11 sector = the real 45-row width) — see comment above.
		{Date: day, IndexCode: "MAINFILL01", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL02", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL03", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL04", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL05", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL06", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL07", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL08", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL09", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL10", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL11", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL12", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL13", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL14", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL15", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL16", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL17", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL18", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL19", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL20", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL21", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL22", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL23", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL24", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL25", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL26", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL27", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL28", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL29", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL30", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL31", NumberOfStock: 100},
		{Date: day, IndexCode: "MAINFILL32", NumberOfStock: 100},
	}
	// Append the 11 sector indices with their documented constituent counts.
	for _, code := range sectorIndexCodes {
		rows = append(rows, client.IndexSummary{
			Date: day, IndexCode: code, Previous: 2000.0, Highest: 2100.0,
			Lowest: 1900.0, Close: 2050.0, Change: 50.0,
			NumberOfStock: demoSectorIndexCounts[code],
			Volume:        50000000, Value: 500000000000, Frequency: 50000, MarketCap: 200000000000000,
		})
	}
	if len(rows) != 45 {
		t.Fatalf("demo snapshot = %d rows, want 45", len(rows))
	}

	repo := repository.NewIndexSummaryRepository(log)
	recorder := pipeline.NewSourceStatusRecorder(
		pipeline.NewSQLSourceStatusStore(repository.NewSourceStatusRepository(log), db),
		pipeline.NewSQLAlertStore(repository.NewAlertRepository(log), db),
		log,
	)
	handler := NewIndexSummaryHandler(log, &taskIndexSummaryFetcher{rows: rows}, db, repo, recorder)

	raw, _ := json.Marshal(IndexSummaryPayload{Date: day.Format("2006-01-02")})
	if err := handler(context.Background(), asynq.NewTask(TypeIndexSummary, raw)); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// One day's 45 rows persisted.
	count := 0
	if err := db.Get(&count, "SELECT COUNT(*) FROM index_summaries WHERE date = $1", day); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 45 {
		t.Fatalf("stored %d rows for %s, want 45", count, day.Format("2006-01-02"))
	}

	// All 11 sector indices present.
	var stored []string
	if err := db.Select(&stored, "SELECT index_code FROM index_summaries WHERE date = $1 ORDER BY index_code", day); err != nil {
		t.Fatalf("select codes: %v", err)
	}
	got := map[string]bool{}
	for _, c := range stored {
		got[c] = true
	}
	for _, code := range sectorIndexCodes {
		if !got[code] {
			t.Errorf("sector index %s missing from stored rows", code)
		}
	}

	// NumberOfStock survives the float→INT truncation: LQ45 = 45, the exact
	// match the membership-count validation compares against ticker_indices.
	var lq45Stock int32
	if err := db.Get(&lq45Stock, "SELECT number_of_stock FROM index_summaries WHERE index_code = 'LQ45' AND date = $1", day); err != nil {
		t.Fatalf("LQ45 row: %v", err)
	}
	if lq45Stock != 45 {
		t.Errorf("LQ45 number_of_stock = %d, want 45", lq45Stock)
	}
}

// demoDay is the fixed demo trading date (2026-09-09, matching the findings
// probe sample) shared by the demo test's cleanup, snapshot, and reads.
var demoDay = time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
