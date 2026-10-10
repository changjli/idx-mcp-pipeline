package tasks

import (
	"errors"
	"testing"
	"time"
)

// wibFixed builds a fixed WIB wall-clock time for a test date.
func wibFixed(y, mo, d, h, mi int) time.Time {
	return time.Date(y, time.Month(mo), d, h, mi, 0, 0, wibLoc())
}

// storedExcept builds a hasStored closure: every candidate is stored except
// the listed days.
func storedExcept(missing ...string) func(day string) (bool, error) {
	set := make(map[string]bool, len(missing))
	for _, m := range missing {
		set[m] = true
	}
	return func(day string) (bool, error) { return !set[day], nil }
}

func daysOf(targets []time.Time) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = t.Format("2006-01-02")
	}
	return out
}

// TestWaveTargetDaysBeforeWaveTargetsYesterday: a pre-market boot (before
// 20:05 WIB) walks back from yesterday — today has no published summary yet,
// so today must never be a target before the wave time.
func TestWaveTargetDaysBeforeWaveTargetsYesterday(t *testing.T) {
	// Thu Oct 8, 08:00 WIB — before the wave. Only Oct 7 missing.
	now := wibFixed(2026, 10, 8, 8, 0)
	targets, err := WaveTargetDays(now, storedExcept("2026-10-07"))
	if err != nil {
		t.Fatalf("WaveTargetDays: %v", err)
	}
	if got := daysOf(targets); len(got) != 1 || got[0] != "2026-10-07" {
		t.Fatalf("targets = %v, want [2026-10-07]", got)
	}
}

// TestWaveTargetDaysAfterWaveTargetsToday: a late-evening run targets today.
func TestWaveTargetDaysAfterWaveTargetsToday(t *testing.T) {
	// Thu Oct 8, 21:00 WIB — after the wave. Only Oct 8 missing.
	now := wibFixed(2026, 10, 8, 21, 0)
	targets, err := WaveTargetDays(now, storedExcept("2026-10-08"))
	if err != nil {
		t.Fatalf("WaveTargetDays: %v", err)
	}
	if got := daysOf(targets); len(got) != 1 || got[0] != "2026-10-08" {
		t.Fatalf("targets = %v, want [2026-10-08]", got)
	}
}

// TestWaveTargetDaysWeekendTodaySkipsToLastWeekday: when "today" itself is a
// weekend day it is skipped like any weekend candidate — the wave targets
// Friday, and no fetch fires for a day that has no trading data.
func TestWaveTargetDaysWeekendTodaySkipsToLastWeekday(t *testing.T) {
	// Sat Oct 10, 20:05 — the cron's exact tick, on a weekend. Only Oct 9
	// (Friday) missing.
	now := wibFixed(2026, 10, 10, 20, 5)
	targets, err := WaveTargetDays(now, storedExcept("2026-10-10", "2026-10-09"))
	if err != nil {
		t.Fatalf("WaveTargetDays: %v", err)
	}
	if got := daysOf(targets); len(got) != 1 || got[0] != "2026-10-09" {
		t.Fatalf("targets = %v, want [2026-10-09]", got)
	}
}

// TestWaveTargetDaysSkipsWeekendCandidates: weekends must never become
// targets — an empty weekend fetch claims its date-keyed TaskID under
// IngestRetention and dead-ends the walk above the real gap.
func TestWaveTargetDaysSkipsWeekendCandidates(t *testing.T) {
	// Sun Oct 4, 10:00 WIB — before the wave, so start = Sat Oct 3 (skipped).
	// Fri Oct 2 missing.
	now := wibFixed(2026, 10, 4, 10, 0)
	targets, err := WaveTargetDays(now, storedExcept("2026-10-02"))
	if err != nil {
		t.Fatalf("WaveTargetDays: %v", err)
	}
	if got := daysOf(targets); len(got) != 1 || got[0] != "2026-10-02" {
		t.Fatalf("targets = %v, want [2026-10-02]", got)
	}
}

// TestWaveTargetDaysCatchesUpBacklog: the core catch-up property — a
// multi-day hole drains in ONE fire. Without it the freshest missing day
// wins every nightly fire and older holes starve forever.
func TestWaveTargetDaysCatchesUpBacklog(t *testing.T) {
	// Fri Oct 9, 21:00 WIB — after the wave. Wed, Thu, Fri all missing;
	// Oct 6 and older stored.
	now := wibFixed(2026, 10, 9, 21, 0)
	targets, err := WaveTargetDays(now, storedExcept("2026-10-09", "2026-10-08", "2026-10-07"))
	if err != nil {
		t.Fatalf("WaveTargetDays: %v", err)
	}
	want := []string{"2026-10-09", "2026-10-08", "2026-10-07"}
	if got := daysOf(targets); len(got) != len(want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}
	for i, w := range want {
		if got := daysOf(targets)[i]; got != w {
			t.Fatalf("targets[%d] = %s, want %s", i, got, w)
		}
	}
}

// TestWaveTargetDaysNoOpWhenAllStored: everything covered is an empty
// result, not an error — the evening cron dedup path relies on this being nil.
func TestWaveTargetDaysNoOpWhenAllStored(t *testing.T) {
	now := wibFixed(2026, 10, 8, 20, 10)
	targets, err := WaveTargetDays(now, storedExcept())
	if err != nil {
		t.Fatalf("WaveTargetDays: %v", err)
	}
	if len(targets) != 0 {
		t.Fatalf("targets = %v, want empty (no-op)", daysOf(targets))
	}
}

// TestWaveTargetDaysFailsOnStoredDayError: a DB outage must fail the run (so
// asynq retries) rather than skip the wave — a skipped wave is a data hole.
func TestWaveTargetDaysFailsOnStoredDayError(t *testing.T) {
	now := wibFixed(2026, 10, 8, 8, 0)
	_, err := WaveTargetDays(now, func(day string) (bool, error) { return false, errors.New("db down") })
	if err == nil {
		t.Fatal("want error on stored-day check failure")
	}
}
