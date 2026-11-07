package repository

import (
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

// TestFetchUsage_ReserveCapsAtTheDailyPool verifies the shared daily counter:
// grants are atomic, partial grants never exceed the cap, and a spent pool
// grants nothing. Skipped unless IDX_MCP_DB_DSN is set.
func TestFetchUsage_ReserveCapsAtTheDailyPool(t *testing.T) {
	dsn := os.Getenv("IDX_MCP_DB_DSN")
	if dsn == "" {
		t.Skip("IDX_MCP_DB_DSN not set; skipping DB-backed verification")
	}
	db := sqlx.MustConnect("pgx", dsn)
	repo := NewFetchUsageRepository(logrus.New())

	day := time.Now()
	feature := FetchFeatureScreenerFlow
	db.MustExec("DELETE FROM fetch_usage WHERE usage_day = $1::date AND feature = $2", day, feature)
	t.Cleanup(func() {
		db.MustExec("DELETE FROM fetch_usage WHERE usage_day = $1::date AND feature = $2", day, feature)
	})

	granted, remaining, err := repo.Reserve(db, day, feature, 3, 10)
	if err != nil {
		t.Fatalf("Reserve 3: %v", err)
	}
	if granted != 3 || remaining != 7 {
		t.Fatalf("Reserve 3 = %d/%d, want 3/7", granted, remaining)
	}

	// Asking for more than is left grants exactly the remainder.
	granted, remaining, err = repo.Reserve(db, day, feature, 20, 10)
	if err != nil {
		t.Fatalf("Reserve 20: %v", err)
	}
	if granted != 7 || remaining != 0 {
		t.Fatalf("Reserve 20 = %d/%d, want 7/0", granted, remaining)
	}

	granted, remaining, err = repo.Reserve(db, day, feature, 1, 10)
	if err != nil {
		t.Fatalf("Reserve exhausted: %v", err)
	}
	if granted != 0 || remaining != 0 {
		t.Fatalf("Reserve exhausted = %d/%d, want 0/0", granted, remaining)
	}

	used, err := repo.Units(db, day, feature)
	if err != nil {
		t.Fatalf("Units: %v", err)
	}
	if used != 10 {
		t.Fatalf("Units = %d, want 10", used)
	}

	// A different feature has its own pool.
	granted, _, err = repo.Reserve(db, day, FetchFeatureBrokerRefresh, 30, 30)
	if err != nil {
		t.Fatalf("Reserve other feature: %v", err)
	}
	if granted != 30 {
		t.Fatalf("other feature granted %d, want 30", granted)
	}
	db.MustExec("DELETE FROM fetch_usage WHERE usage_day = $1::date AND feature = $2", day, FetchFeatureBrokerRefresh)
}
