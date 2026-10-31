package indexalpha

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

// usageFixture is the documented response shape for GET /usage.
const usageFixture = `{
  "success": true,
  "data": {
    "api_key": "ia_l...H_Qv",
    "key": {
      "key_id": "uuid-of-your-key",
      "key_name": "Default Key",
      "current_usage": 3,
      "monthly_limit": 25000,
      "remaining": 24997
    },
    "account": {
      "plan": "Starter",
      "current_usage": 42,
      "monthly_limit": 25000,
      "remaining": 24958,
      "period_end": "2026-08-29T00:00:00",
      "is_active": true
    }
  },
  "error": null
}`

func TestParseUsage(t *testing.T) {
	usage, err := ParseUsage([]byte(usageFixture))
	if err != nil {
		t.Fatalf("ParseUsage: %v", err)
	}
	if usage.Account.Plan != "Starter" {
		t.Errorf("plan = %q", usage.Account.Plan)
	}
	if usage.Account.Remaining != 24958 || usage.Key.Remaining != 24997 {
		t.Errorf("remaining key=%d account=%d", usage.Key.Remaining, usage.Account.Remaining)
	}
	if usage.Account.PeriodEnd == nil {
		t.Fatal("period_end is nil")
	}
	// Zone-less vendor timestamps are UTC: the live 2026-10-31 capture's
	// X-Monthly-Reset epoch resolves to the same instant as its period_end.
	wantPeriodEnd := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	if !usage.Account.PeriodEnd.Equal(wantPeriodEnd) {
		t.Errorf("period_end = %s, want %s", usage.Account.PeriodEnd, wantPeriodEnd)
	}
	if usage.Account.IsActive == nil || !*usage.Account.IsActive {
		t.Errorf("is_active = %v, want true", usage.Account.IsActive)
	}
}

func TestParseUsage_Malformed(t *testing.T) {
	cases := []string{
		`not json`,
		`{"success":false,"error":"nope"}`,
		`{"success":true,"data":null}`,
		`{"success":true,"data":{"account":{"period_end":"not-a-date"}}}`,
	}
	for _, body := range cases {
		if _, err := ParseUsage([]byte(body)); !errors.Is(err, ErrMalformedPayload) {
			t.Errorf("ParseUsage(%q) err = %v, want ErrMalformedPayload", body, err)
		}
	}
}

func TestOutcomeFromHeader(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	h := http.Header{}
	h.Set("X-Monthly-Remaining", "0")
	h.Set("X-Monthly-Reset", "2026-11-29T00:00:00")
	h.Set("X-RateLimit-Remaining", "4")
	h.Set("Retry-After", "30")

	out := outcomeFromHeader(h, func() time.Time { return now })
	if out.MonthlyRemaining == nil || *out.MonthlyRemaining != 0 {
		t.Errorf("monthly remaining = %v", out.MonthlyRemaining)
	}
	if out.MonthlyReset == nil || !out.MonthlyReset.Equal(time.Date(2026, 11, 29, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("monthly reset = %v", out.MonthlyReset)
	}
	if out.RateRemaining == nil || *out.RateRemaining != 4 {
		t.Errorf("rate remaining = %v", out.RateRemaining)
	}
	if out.RetryAfter == nil || *out.RetryAfter != 30*time.Second {
		t.Errorf("retry after = %v", out.RetryAfter)
	}
}

func TestParseVendorTime_RFC3339(t *testing.T) {
	got, err := parseVendorTime("2026-11-29T00:00:00+07:00")
	if err != nil {
		t.Fatalf("parseVendorTime: %v", err)
	}
	want := time.Date(2026, 11, 28, 17, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %s, want %s", got, want)
	}
}

// TestParseVendorTime_LiveEpoch pins the observed X-Monthly-Reset shape: Unix
// epoch seconds, which the docs did not mention.
func TestParseVendorTime_LiveEpoch(t *testing.T) {
	got, err := parseVendorTime("1795784282")
	if err != nil {
		t.Fatalf("parseVendorTime: %v", err)
	}
	want := time.Date(2026, 11, 27, 12, 58, 2, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestParseVendorTime_FractionalEpoch(t *testing.T) {
	got, err := parseVendorTime("1793423856.890752")
	if err != nil {
		t.Fatalf("parseVendorTime: %v", err)
	}
	if got.Nanosecond() == 0 {
		t.Errorf("fractional epoch lost its sub-second part: %s", got)
	}
	if !got.Before(time.Date(2026, 10, 31, 5, 18, 0, 0, time.UTC)) {
		t.Errorf("unexpected rate-limit reset: %s", got)
	}
}

// TestParseUsage_LiveShape is the body captured from a real /usage call on
// 2026-10-31: period_end has fractional seconds and no zone.
func TestParseUsage_LiveShape(t *testing.T) {
	body := `{"success":true,"data":{"api_key":"ia_l...W-Ws","key":{"key_id":"id","key_name":"Default Key","current_usage":25,"monthly_limit":150,"remaining":125},"account":{"plan":"Free","current_usage":25,"monthly_limit":150,"remaining":125,"period_end":"2026-11-27T12:58:02.465000","is_active":true}},"error":null}`
	usage, err := ParseUsage([]byte(body))
	if err != nil {
		t.Fatalf("ParseUsage: %v", err)
	}
	want := time.Date(2026, 11, 27, 12, 58, 2, 465000000, time.UTC)
	if usage.Account.PeriodEnd == nil || !usage.Account.PeriodEnd.Equal(want) {
		t.Errorf("period_end = %v, want %s", usage.Account.PeriodEnd, want)
	}
	if usage.Account.Remaining != 125 || usage.Key.Remaining != 125 {
		t.Errorf("remaining key=%d account=%d", usage.Key.Remaining, usage.Account.Remaining)
	}
}

// TestOutcomeFromHeader_LiveHeaders uses the exact headers a successful metered
// call returned on 2026-10-31 (values kept, bearer key not recorded).
func TestOutcomeFromHeader_LiveHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("X-Monthly-Limit", "150")
	h.Set("X-Monthly-Remaining", "124")
	h.Set("X-Monthly-Used", "26")
	h.Set("X-Monthly-Reset", "1795784282")
	h.Set("X-RateLimit-Remaining", "9")
	h.Set("X-RateLimit-Window", "60s")
	h.Set("Retry-After", "60")

	out := outcomeFromHeader(h, func() time.Time {
		return time.Date(2026, 10, 31, 5, 16, 35, 0, time.UTC)
	})
	if out.MonthlyRemaining == nil || *out.MonthlyRemaining != 124 {
		t.Errorf("monthly remaining = %v", out.MonthlyRemaining)
	}
	wantReset := time.Date(2026, 11, 27, 12, 58, 2, 0, time.UTC)
	if out.MonthlyReset == nil || !out.MonthlyReset.Equal(wantReset) {
		t.Errorf("monthly reset = %v, want %s", out.MonthlyReset, wantReset)
	}
	if out.RetryAfter == nil || *out.RetryAfter != 60*time.Second {
		t.Errorf("retry after = %v", out.RetryAfter)
	}
}
