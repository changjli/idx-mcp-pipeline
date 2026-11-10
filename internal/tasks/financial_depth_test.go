package tasks

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFinancialDepthPayloadRoundTrip(t *testing.T) {
	payload := FinancialDepthPayload{
		Date:   "2026-11-01",
		Ticker: "RAJA",
		Month:  "2026-10",
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
