package indexalpha

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
)

// DefaultBaseURL is the Index Alpha API host.
const DefaultBaseURL = "https://api.indexalpha.id"

// DefaultTimeout bounds one API call.
const DefaultTimeout = 30 * time.Second

// maxResponseBytes caps a response body so a hostile or broken upstream cannot
// exhaust memory. A full broker day is well under 1 MB.
const maxResponseBytes = 4 << 20

// Config holds the tunable knobs for the Index Alpha client.
type Config struct {
	BaseURL string
	APIKey  string // single key today; the pool (ticket 03) supplies keys
	Timeout time.Duration
}

// DefaultConfig returns production defaults: the public host, 30s timeout.
func DefaultConfig() Config {
	return Config{
		BaseURL: DefaultBaseURL,
		Timeout: DefaultTimeout,
	}
}

// Client fetches broker summaries from Index Alpha over plain HTTPS with Bearer
// auth. It holds no cache: Postgres is the cache (ticket 07).
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	log     *logrus.Logger
}

// NewClient creates an Index Alpha client. A missing key is not fatal at
// construction so the rest of the server still starts; Fetch reports it as an
// auth error.
func NewClient(cfg Config, log *logrus.Logger) *Client {
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	return &Client{
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		apiKey:  strings.TrimSpace(cfg.APIKey),
		http:    &http.Client{Timeout: cfg.Timeout},
		log:     log,
	}
}

// Fetch returns the broker summary for one ticker on one trading day.
//
// The request is single-day by construction: Fetch takes one date and sends it
// as both `from` and `to`, because the source aggregates ranges instead of
// paginating them. The regular market is pinned (`market=RG`, the stored
// history's board) and the all-investor view is stated explicitly.
func (c *Client) Fetch(ctx context.Context, ticker string, date time.Time) (*broker.Result, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("%w: no API key configured (INDEXALPHA_KEYS)", ErrUnauthorized)
	}

	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	day := date.Format("2006-01-02")

	q := url.Values{}
	q.Set("ticker", ticker)
	q.Set("from", day)
	q.Set("to", day) // from == to: a range would aggregate into one row per broker
	q.Set("investor", "all")
	q.Set("market", "RG") // foreign flow defaults to ALL; never rely on defaults
	u := c.baseURL + "/stocks/broker-summary?" + q.Encode()

	c.log.Debugf("indexalpha: fetching %s %s", ticker, day)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("indexalpha: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("indexalpha: get %s: %w", u, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("indexalpha: read body: %w", err)
	}
	if err := classifyStatus(resp, body); err != nil {
		return nil, err
	}
	return ParseBrokerSummary(ticker, body)
}

// classifyStatus turns a non-200 response into one of the structured failure
// classes. Index Alpha uses 403 for both auth failures and quota exhaustion, so
// the monthly-remaining header (and the detail text for the plan-daily cap)
// separates them.
func classifyStatus(resp *http.Response, body []byte) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	detail := detailText(body)
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return fmt.Errorf("%w: status=401 detail=%s", ErrUnauthorized, detail)
	case http.StatusForbidden:
		if resp.Header.Get("X-Monthly-Remaining") == "0" || isQuotaDetail(body) {
			return fmt.Errorf("%w: status=403 detail=%s reset=%s",
				broker.ErrQuotaExhausted, detail, resp.Header.Get("X-Monthly-Reset"))
		}
		return fmt.Errorf("%w: status=403 detail=%s", ErrUnauthorized, detail)
	case http.StatusTooManyRequests:
		return fmt.Errorf("%w: status=429 detail=%s", broker.ErrRateLimited, detail)
	default:
		return fmt.Errorf("%w: status=%d detail=%s", ErrUpstream, resp.StatusCode, detail)
	}
}

// isQuotaDetail recognises the plan-daily cap, which arrives as a 403 without
// X-Monthly-Remaining: 0 (the usage endpoint does not expose it).
func isQuotaDetail(body []byte) bool {
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "limit exceeded") || strings.Contains(lower, "quota")
}

// detailText extracts the vendor's human-readable detail when present, falling
// back to a truncated raw body. The field is a string on most failures and an
// array on 422 validation errors, so it stays raw here.
func detailText(body []byte) string {
	var env wireEnvelope
	if err := json.Unmarshal(body, &env); err == nil && len(env.Detail) > 0 && string(env.Detail) != "null" {
		return rawText(env.Detail)
	}
	return truncate(strings.TrimSpace(string(body)), 200)
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
