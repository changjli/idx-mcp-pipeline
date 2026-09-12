package scheduler

import "testing"

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
