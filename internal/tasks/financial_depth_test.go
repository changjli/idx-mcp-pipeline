package tasks

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFinancialDepthPayloadRoundTrip(t *testing.T) {
	payload := FinancialDepthPayload{
		Date:   "2026-11-01",
		Ticker: "RAJA",
		Month:  "2026-10",
		Force:  true,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got FinancialDepthPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != payload {
		t.Errorf("payload = %+v, want %+v", got, payload)
	}
}

// TestEnqueueFinancialDepth_OnDemand pins the on-demand task shape: one task
// per ticker/month (the TaskID dedup) and the force flag carried for the
// manual/quarterly retry path.
func TestEnqueueFinancialDepth_OnDemand(t *testing.T) {
	day := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)

	enq := &fakeEnqueuer{}
	if _, err := EnqueueFinancialDepth(enq, day, "RAJA", "2026-11", false); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if len(enq.tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(enq.tasks))
	}
	task := enq.tasks[0]
	if task.typ != TypeFinancialsDepth {
		t.Errorf("type = %q, want %q", task.typ, TypeFinancialsDepth)
	}
	if opts := optStrings(task.opts); !strings.Contains(opts, "idx:financials_depth:2026-11:RAJA") {
		t.Errorf("options = %q, want the per-ticker/month TaskID", opts)
	}
	var payload FinancialDepthPayload
	if err := json.Unmarshal(task.body, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Ticker != "RAJA" || payload.Month != "2026-11" || payload.Force {
		t.Errorf("payload = %+v", payload)
	}

	// Force is carried through for the not_covered retry path.
	enq = &fakeEnqueuer{}
	if _, err := EnqueueFinancialDepth(enq, day, "NOS", "2026-11", true); err != nil {
		t.Fatalf("force enqueue: %v", err)
	}
	payload = FinancialDepthPayload{}
	if err := json.Unmarshal(enq.tasks[0].body, &payload); err != nil {
		t.Fatalf("decode forced payload: %v", err)
	}
	if !payload.Force {
		t.Errorf("payload = %+v, want force=true", payload)
	}
}

// TestFinancialDepthEnqueuer_OnDemandMonth pins the read-path adapter: the
// task is keyed on the current ledger month, so the on-demand run does not
// collide with the monthly sweep's own month key.
func TestFinancialDepthEnqueuer_OnDemandMonth(t *testing.T) {
	enq := &fakeEnqueuer{}
	adapter := NewFinancialDepthEnqueuer(enq)
	if err := adapter.EnqueueFinancialDepth("raja"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if len(enq.tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(enq.tasks))
	}
	var payload FinancialDepthPayload
	if err := json.Unmarshal(enq.tasks[0].body, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Month != time.Now().Format("2006-01") {
		t.Errorf("month = %q, want the current month", payload.Month)
	}
	if payload.Ticker != "raja" {
		t.Errorf("ticker = %q, want the raw ticker (the handler normalizes)", payload.Ticker)
	}
}

// TestArgBool pins the operator flag parser: only explicit truthy values force
// a re-run, so a typo cannot silently bypass the not_covered guard.
func TestArgBool(t *testing.T) {
	cases := map[string]bool{
		"true": true, "TRUE": true, "1": true, "yes": true, " Yes ": true,
		"false": false, "0": false, "no": false, "tru": false, "": false,
	}
	for raw, want := range cases {
		if got := argBool([]string{"force=" + raw}, "force"); got != want {
			t.Errorf("argBool(force=%q) = %t, want %t", raw, got, want)
		}
	}
	if argBool(nil, "force") {
		t.Error("argBool(absent) = true, want false")
	}
}

func TestDepthRunMonth(t *testing.T) {
	runDate := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)

	// A monthly sweep keys on the previous calendar month (the newest Tier-1
	// snapshot), so a re-run in November is idempotent against the October batch.
	got, err := depthRunMonth("", runDate, false)
	if err != nil {
		t.Fatalf("sweep month: %v", err)
	}
	if want := "2026-10"; got.Format("2006-01") != want {
		t.Errorf("sweep month = %s, want %s", got.Format("2006-01"), want)
	}

	// An on-demand ticker keys on the current month so it does not collide with
	// the monthly batch.
	got, err = depthRunMonth("", runDate, true)
	if err != nil {
		t.Fatalf("on-demand month: %v", err)
	}
	if want := "2026-11"; got.Format("2006-01") != want {
		t.Errorf("on-demand month = %s, want %s", got.Format("2006-01"), want)
	}

	// An explicit month wins.
	got, err = depthRunMonth("2026-08", runDate, true)
	if err != nil {
		t.Fatalf("explicit month: %v", err)
	}
	if want := "2026-08"; got.Format("2006-01") != want {
		t.Errorf("explicit month = %s, want %s", got.Format("2006-01"), want)
	}

	if _, err := depthRunMonth("2026-8", runDate, false); err == nil {
		t.Error("expected an error for a non-YYYY-MM month")
	}
}
