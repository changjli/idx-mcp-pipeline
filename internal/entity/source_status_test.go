package entity

import (
	"testing"
	"time"
)

func TestSourceStatus_IsStale(t *testing.T) {
	now := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)

	t.Run("never succeeded is stale", func(t *testing.T) {
		s := SourceStatus{Source: "s", MaxAgeSeconds: 86400}
		if !s.IsStale(now) {
			t.Error("expected stale with no last_success_at")
		}
	})

	t.Run("within window is fresh", func(t *testing.T) {
		recent := now.Add(-time.Hour)
		s := SourceStatus{Source: "s", LastSuccessAt: &recent, MaxAgeSeconds: 86400}
		if s.IsStale(now) {
			t.Error("expected fresh within max_age window")
		}
	})

	t.Run("beyond window is stale", func(t *testing.T) {
		old := now.Add(-72 * time.Hour)
		s := SourceStatus{Source: "s", LastSuccessAt: &old, MaxAgeSeconds: 86400}
		if !s.IsStale(now) {
			t.Error("expected stale beyond max_age window")
		}
	})

	t.Run("exactly at window boundary is fresh", func(t *testing.T) {
		boundary := now.Add(-24 * time.Hour)
		s := SourceStatus{Source: "s", LastSuccessAt: &boundary, MaxAgeSeconds: 86400}
		if s.IsStale(now) {
			t.Error("expected fresh exactly at max_age boundary (strict >)")
		}
	})

	t.Run("zero max age falls back to one trading day", func(t *testing.T) {
		old := now.Add(-12 * time.Hour)
		s := SourceStatus{Source: "s", LastSuccessAt: &old, MaxAgeSeconds: 0}
		if s.IsStale(now) {
			t.Error("expected fresh: 12h within 24h default")
		}
		older := now.Add(-48 * time.Hour)
		s = SourceStatus{Source: "s", LastSuccessAt: &older, MaxAgeSeconds: 0}
		if !s.IsStale(now) {
			t.Error("expected stale: 48h beyond 24h default")
		}
	})
}
