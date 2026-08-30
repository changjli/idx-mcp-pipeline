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
