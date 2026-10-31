package indexalpha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
)

// Usage is the free quota report from GET /usage.
//
// The account counters are the authority: the monthly guard is enforced at the
// account level, and a key's own counter is informational. Keys on separate
// accounts therefore add up; keys under one account share a single bucket.
type Usage struct {
	APIKey  string       `json:"api_key"`
	Key     UsageKey     `json:"key"`
	Account UsageAccount `json:"account"`
}

// UsageKey is one API key's informational counters.
type UsageKey struct {
	KeyID        string `json:"key_id"`
	KeyName      string `json:"key_name"`
	CurrentUsage int64  `json:"current_usage"`
	MonthlyLimit int64  `json:"monthly_limit"`
	Remaining    int64  `json:"remaining"`
}

// UsageAccount is the plan-level state that actually gates requests.
type UsageAccount struct {
	Plan         string     `json:"plan"`
	CurrentUsage int64      `json:"current_usage"`
	MonthlyLimit int64      `json:"monthly_limit"`
	Remaining    int64      `json:"remaining"`
	PeriodEnd    *time.Time `json:"period_end"`
	// IsActive is nil when the vendor omitted the field; absent is treated as
	// active so a schema addition never disables a working key.
	IsActive *bool `json:"is_active"`
}

// wireUsage is the response body before period_end is parsed.
type wireUsage struct {
	APIKey  string           `json:"api_key"`
	Key     UsageKey         `json:"key"`
	Account wireUsageAccount `json:"account"`
}

type wireUsageAccount struct {
	Plan         string `json:"plan"`
	CurrentUsage int64  `json:"current_usage"`
	MonthlyLimit int64  `json:"monthly_limit"`
	Remaining    int64  `json:"remaining"`
	PeriodEnd    string `json:"period_end"`
	IsActive     *bool  `json:"is_active"`
}

// ParseUsage decodes GET /usage. The endpoint is free and its shape is
// documented (not guessed): key counters plus account counters and period end.
func ParseUsage(body []byte) (*Usage, error) {
	var env wireEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("%w: decode usage: %v", ErrMalformedPayload, err)
	}
	if !env.Success {
		return nil, fmt.Errorf("%w: usage success=false error=%s", ErrMalformedPayload, rawText(env.Error))
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return nil, fmt.Errorf("%w: usage has no data", ErrMalformedPayload)
	}

	var wire wireUsage
	if err := json.Unmarshal(env.Data, &wire); err != nil {
		return nil, fmt.Errorf("%w: usage data is not an object: %v", ErrMalformedPayload, err)
	}

	usage := &Usage{
		APIKey: wire.APIKey,
		Key:    wire.Key,
		Account: UsageAccount{
			Plan:         wire.Account.Plan,
			CurrentUsage: wire.Account.CurrentUsage,
			MonthlyLimit: wire.Account.MonthlyLimit,
			Remaining:    wire.Account.Remaining,
			IsActive:     wire.Account.IsActive,
		},
	}
	if raw := strings.TrimSpace(wire.Account.PeriodEnd); raw != "" {
		t, err := parseVendorTime(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: usage period_end %q: %v", ErrMalformedPayload, raw, err)
		}
		usage.Account.PeriodEnd = t
	}
	return usage, nil
}

// Usage reads GET /usage for one key. It is free and never consumes quota, so
// the pool may call it at boot and after a period rolls over.
func (c *Client) Usage(ctx context.Context, key string) (*Usage, CallOutcome, error) {
	if strings.TrimSpace(key) == "" {
		return nil, CallOutcome{}, fmt.Errorf("%w: no API key configured (INDEXALPHA_KEYS)", ErrUnauthorized)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/usage", nil)
	if err != nil {
		return nil, CallOutcome{}, fmt.Errorf("indexalpha: build usage request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, CallOutcome{}, fmt.Errorf("indexalpha: get usage: %w", err)
	}
	defer resp.Body.Close()

	outcome := outcomeFromHeader(resp.Header, time.Now)
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, outcome, fmt.Errorf("indexalpha: read usage body: %w", err)
	}
	if err := classifyStatus(resp, body); err != nil {
		return nil, outcome, err
	}
	usage, err := ParseUsage(body)
	return usage, outcome, err
}

// outcomeFromHeader extracts the quota signals the vendor sends on every call.
// Nil means the header was absent or unparseable; the pool then falls back to
// its last known value rather than inventing a number.
func outcomeFromHeader(h http.Header, now func() time.Time) CallOutcome {
	var out CallOutcome
	if v := strings.TrimSpace(h.Get("X-Monthly-Remaining")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			out.MonthlyRemaining = &n
		}
	}
	if t, err := parseVendorTime(h.Get("X-Monthly-Reset")); err == nil {
		out.MonthlyReset = t
	}
	if v := strings.TrimSpace(h.Get("X-RateLimit-Remaining")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			out.RateRemaining = &n
		}
	}
	if v := strings.TrimSpace(h.Get("Retry-After")); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
			d := time.Duration(secs) * time.Second
			out.RetryAfter = &d
		} else if t, err := http.ParseTime(v); err == nil {
			d := t.Sub(now())
			if d < 0 {
				d = 0
			}
			out.RetryAfter = &d
		}
	}
	return out
}

// parseVendorTime accepts the formats Index Alpha actually sends. The live
// capture (2026-10-31) settled two of them:
//
//   - X-Monthly-Reset is Unix epoch seconds (`1795784282`), not ISO.
//   - /usage `period_end` is a zone-less timestamp (`2026-11-27T12:58:02.465000`).
//     Its epoch header resolves to the same instant in UTC, so zone-less values
//     are UTC, not Asia/Jakarta. (This is the opposite of the RSS feeds' naive
//     WIB convention; the vendor's epoch header is the tie-breaker.)
//
// An integer/float in the epoch range is therefore parsed as absolute time;
// everything else is parsed in UTC.
func parseVendorTime(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty timestamp")
	}

	// Unix epoch, integer seconds or fractional (X-RateLimit-Reset is a float).
	if f, err := strconv.ParseFloat(raw, 64); err == nil && f > 1e9 && f < 4.1e9 {
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		t := time.Unix(sec, nsec).UTC()
		return &t, nil
	}

	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, raw); err == nil {
			return &t, nil
		}
	}
	for _, layout := range []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04",
		"2006-01-02",
	} {
		if t, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			return &t, nil
		}
	}
	return nil, fmt.Errorf("unrecognised timestamp")
}

// keyScopedFailure reports whether err condemns the key that produced it, as
// opposed to the request. Unauthorized, monthly quota, daily request-cap and
// rate-limit failures are key scoped; malformed requests, malformed payloads
// and 5xx are not.
func keyScopedFailure(err error) bool {
	return err != nil && (isQuota(err) || isDailyLimit(err) || isRateLimited(err) || isUnauthorized(err))
}

func isQuota(err error) bool       { return errors.Is(err, broker.ErrQuotaExhausted) }
func isDailyLimit(err error) bool  { return errors.Is(err, broker.ErrDailyLimit) }
func isRateLimited(err error) bool { return errors.Is(err, broker.ErrRateLimited) }
func isUnauthorized(err error) bool {
	return errors.Is(err, ErrUnauthorized)
}
