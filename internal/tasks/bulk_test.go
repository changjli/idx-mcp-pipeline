package tasks

import (
	"testing"
	"time"
)

func TestDatesInRange(t *testing.T) {
	start := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC)

	dates := datesInRange(start, end)
	if len(dates) != 5 {
		t.Fatalf("expected 5 dates, got %d", len(dates))
	}
	for i, d := range dates {
		expected := start.AddDate(0, 0, i)
		if !d.Equal(expected) {
			t.Errorf("dates[%d] = %s, want %s", i, d.Format("2006-01-02"), expected.Format("2006-01-02"))
		}
	}
}

func TestDatesInRange_SingleDate(t *testing.T) {
	d := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	dates := datesInRange(d, d)
	if len(dates) != 1 {
		t.Fatalf("expected 1 date, got %d", len(dates))
	}
	if !dates[0].Equal(d) {
		t.Errorf("expected %s, got %s", d.Format("2006-01-02"), dates[0].Format("2006-01-02"))
	}
}

func TestDatesInRange_Reversed(t *testing.T) {
	start := time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	dates := datesInRange(start, end)
	if len(dates) != 0 {
		t.Fatalf("expected 0 dates for reversed range, got %d", len(dates))
	}
}

func TestRunBulkBackfill_EmptyRange(t *testing.T) {
	// start after end: loop never runs, deps never touched.
	start := time.Date(2026, 1, 9, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	result := RunBulkBackfill(nil, nil, nil, nil, nil, nil, start, end)

	if result.Total != 0 || result.Succeeded != 0 || result.Failed != 0 || result.Empty != 0 {
		t.Errorf("expected zero result, got %+v", result)
	}
	if result.LastSuccessDate != "" {
		t.Errorf("expected empty LastSuccessDate, got %s", result.LastSuccessDate)
	}
}
