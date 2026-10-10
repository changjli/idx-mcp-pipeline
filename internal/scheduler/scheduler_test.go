package scheduler

import (
	"testing"
	"time"
)

func TestDailyCronSpec(t *testing.T) {
	// Pins the registered cron spec. LogNextFireTime hardcodes the same
	// 8:05 PM WIB fire time — a drift in either is caught here.
	const want = "CRON_TZ=Asia/Jakarta 5 20 * * *"
	if DailyCronSpec != want {
		t.Errorf("DailyCronSpec = %q, want %q", DailyCronSpec, want)
	}
}

func TestSectorIndexCronSpec(t *testing.T) {
	// Pins the 6-monthly sector/index seeder cron (issue 15b): 10:00 PM WIB on
	// Mar 1 and Aug 1 — the month AFTER the Feb/Jul IDX index review, so the
	// snapshot captures the post-rebalance constituents. A drift here silently
	// changes the refresh cadence.
	const want = "CRON_TZ=Asia/Jakarta 0 22 1 3,8 *"
	if SectorIndexCronSpec != want {
		t.Errorf("SectorIndexCronSpec = %q, want %q", SectorIndexCronSpec, want)
	}
}

// TestTimeUntilWave pins the 20:05 WIB wave boundary used by the boot
// self-heal: a boot before the wave schedules ahead; at/after the wave time
// it must recover immediately (duration rolls to tomorrow).
func TestTimeUntilWave(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		t.Skip("tz database unavailable")
	}
	cases := []struct {
		name    string
		now     time.Time
		wantMin time.Duration
		wantMax time.Duration
	}{
		{"mid-morning", time.Date(2026, 10, 8, 8, 0, 0, 0, loc), 12*time.Hour + 4*time.Minute, 12*time.Hour + 6*time.Minute},
		{"one second before", time.Date(2026, 10, 8, 20, 4, 59, 0, loc), 900 * time.Millisecond, 1100 * time.Millisecond},
		{"at wave", time.Date(2026, 10, 8, 20, 5, 0, 0, loc), 23*time.Hour + 59*time.Minute + 50*time.Second, 24 * time.Hour},
		{"after wave", time.Date(2026, 10, 8, 22, 0, 0, 0, loc), 22*time.Hour + 4*time.Minute, 22*time.Hour + 6*time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := timeUntilWave(tc.now)
			if d < tc.wantMin || d > tc.wantMax {
				t.Fatalf("timeUntilWave = %s, want within [%s, %s]", d, tc.wantMin, tc.wantMax)
			}
		})
	}
}
