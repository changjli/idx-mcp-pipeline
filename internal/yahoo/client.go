// Package yahoo is the Tier-2 source adapter: it fetches Yahoo Finance's
// post-2026-11-09 Morningstar fundamentals dataset, classifies an issuer's
// statement template, resolves the opaque data-ID codes to normalized concepts,
// and derives the rows the page computes. It is the only network component of
// Tier 2. It does not persist or schedule anything — ticket 05 owns the store
// and the monthly sweep, and the MCP request path never calls this package.
package yahoo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

const (
	defaultBaseURL     = "https://query1.finance.yahoo.com"
	defaultPageBaseURL = "https://finance.yahoo.com"
	defaultUserAgent   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

	// epoch1985 is the page's own period1 lower bound (1985-08-26). It is the
	// widest history the endpoint accepts without a per-ticker start date.
	epoch1985 int64 = 493590046

	// maxBodyBytes caps both JSON payloads and page bodies. The largest
	// observed statement payload is ~53 KB and a page ~1 MB; 8 MB is a safety
	// valve against a runaway body, not a working limit.
	maxBodyBytes = 8 << 20
)

// ErrMalformed reports a payload that parsed or decoded badly. It is distinct
// from a transport error so callers can distinguish a transient fetch failure
// from a source-shape change.
var ErrMalformed = errors.New("yahoo: malformed timeseries payload")

// ErrNoTypeList reports that discovery found no fundamentals-timeseries
// data-url in the statement page. The page may have served a generic shell;
// classification should use the discriminant probe instead of retrying pages.
var ErrNoTypeList = errors.New("yahoo: no fundamentals-timeseries type list in page")

// TypeStatus describes what happened to one requested type. A type that is
// absent from the response entirely is distinguishable from one that came back
// meta-only (present in the taxonomy but with no data). Whether a no-data type
// is not_applicable or unknown is decided by the taxonomy, not by this status.
type TypeStatus string

const (
	TypeObserved TypeStatus = "observed"
	TypeNoData   TypeStatus = "no_data"
	TypeAbsent   TypeStatus = "absent"
)

// Fact is one decoded timeseries value. Code is the global data-ID
// (EQT07, EQ46Q, …); Type is the requested token (EQT07_12M). Value is the
// chosen number in full reporting-currency units; Raw preserves the literal
// JSON text (often E-notation) so no precision is lost for audit.
type Fact struct {
	Code       string
	Type       string
	PeriodType string
	AsOfDate   time.Time
	Currency   string
	Value      *float64
	Raw        string
	Source     financials.ValueSource
	Restated   *float64
	Reported   *float64
}

// Result is one response after decoding. Facts is keyed by the requested type
// (e.g. EQT07_12M); Status records observed / no_data / absent for every type.
type Result struct {
	Facts  map[string][]Fact
	Status map[string]TypeStatus
}

// Flatten returns every fact in a stable order (requested-type order is not
// preserved by the endpoint, so callers must not depend on it).
func (r *Result) Flatten() []Fact {
	if r == nil {
		return nil
	}
	out := make([]Fact, 0)
	for _, facts := range r.Facts {
		out = append(out, facts...)
	}
	return out
}

// HTTPError is a non-retryable or exhausted HTTP failure.
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("yahoo: http %d from %s: %s", e.StatusCode, e.URL, e.Body)
}

// Client fetches the Yahoo timeseries endpoint. It uses a plain
// net/http.Client with only a User-Agent — no crumb and no cookie are required
// for these data URLs. Retries are bounded and only applied to transient
// statuses.
type Client struct {
	httpClient  *http.Client
	baseURL     string
	pageBaseURL string
	userAgent   string
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
	sleep       func(context.Context, time.Duration) error
	now         func() time.Time
	period1     int64
	period2     int64 // 0 means "now" at request time
}

// Option configures a Client. All options are optional; the defaults are the
// production values.
type Option func(*Client)

// WithHTTPClient supplies the transport. Tests use an httptest server or a
// fake RoundTripper.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) {
		if h != nil {
			c.httpClient = h
		}
	}
}

// WithBaseURL overrides the timeseries host.
func WithBaseURL(s string) Option {
	return func(c *Client) {
		if s = strings.TrimRight(strings.TrimSpace(s), "/"); s != "" {
			c.baseURL = s
		}
	}
}

// WithPageBaseURL overrides the statement-page host used by discovery.
func WithPageBaseURL(s string) Option {
	return func(c *Client) {
		if s = strings.TrimRight(strings.TrimSpace(s), "/"); s != "" {
			c.pageBaseURL = s
		}
	}
}

// WithUserAgent overrides the request User-Agent.
func WithUserAgent(s string) Option {
	return func(c *Client) {
		if s = strings.TrimSpace(s); s != "" {
			c.userAgent = s
		}
	}
}

// WithRetry sets the attempt cap and exponential-backoff bounds. maxAttempts
// is the total number of attempts (default 4). A value below 1 is treated as 1.
func WithRetry(maxAttempts int, baseDelay, maxDelay time.Duration) Option {
	return func(c *Client) {
		if maxAttempts < 1 {
			maxAttempts = 1
		}
		c.maxAttempts = maxAttempts
		if baseDelay > 0 {
			c.baseDelay = baseDelay
		}
		if maxDelay > 0 {
			c.maxDelay = maxDelay
		}
	}
}

// WithSleep injects the backoff sleeper. Tests use it to record delays instead
// of waiting. A nil function restores the real timer.
func WithSleep(fn func(context.Context, time.Duration) error) Option {
	return func(c *Client) { c.sleep = fn }
}

// WithClock injects the clock used for the default period2.
func WithClock(fn func() time.Time) Option {
	return func(c *Client) {
		if fn != nil {
			c.now = fn
		}
	}
}

// WithPeriods pins the request window. A non-positive period2 means "now".
func WithPeriods(period1, period2 int64) Option {
	return func(c *Client) {
		if period1 > 0 {
			c.period1 = period1
		}
		c.period2 = period2
	}
}

// NewClient builds a Yahoo adapter with production defaults.
func NewClient(opts ...Option) *Client {
	c := &Client{
		httpClient:  &http.Client{Timeout: 60 * time.Second},
		baseURL:     defaultBaseURL,
		pageBaseURL: defaultPageBaseURL,
		userAgent:   defaultUserAgent,
		maxAttempts: 4,
		baseDelay:   500 * time.Millisecond,
		maxDelay:    8 * time.Second,
		now:         time.Now,
		period1:     epoch1985,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// NormalizeSymbol accepts PTBA or PTBA.JK and returns the Yahoo symbol. The
// adapter is IDX-specific, so a bare ticker always gets .JK.
func NormalizeSymbol(ticker string) string {
	s := strings.ToUpper(strings.TrimSpace(ticker))
	if s == "" {
		return ""
	}
	if strings.Contains(s, ".") {
		return s
	}
	return s + ".JK"
}

// bareTicker returns the store-facing ticker: uppercase, without the .JK
// suffix. The statement key is (ticker, period_end), never a Yahoo symbol.
func bareTicker(ticker string) string {
	s := NormalizeSymbol(ticker)
	return strings.TrimSuffix(s, ".JK")
}

// FetchTypes requests the given timeseries tokens for one ticker. Tokens are
// the full Yahoo form, e.g. EQT07_12M or EQ46Q_TTM. The returned Result has an
// entry in Status for every requested token, including ones the response
// omitted.
func (c *Client) FetchTypes(ctx context.Context, ticker string, types []string) (*Result, error) {
	symbol := NormalizeSymbol(ticker)
	if symbol == "" {
		return nil, fmt.Errorf("yahoo: empty ticker")
	}
	types = uniqueStrings(types)
	if len(types) == 0 {
		return nil, fmt.Errorf("yahoo: no types requested")
	}

	u, err := c.timeseriesURL(symbol, types)
	if err != nil {
		return nil, err
	}
	body, err := c.get(ctx, u, "application/json")
	if err != nil {
		return nil, err
	}
	return parseTimeseries(body, types)
}

func (c *Client) timeseriesURL(symbol string, types []string) (string, error) {
	base, err := url.Parse(strings.TrimRight(c.baseURL, "/") + "/ws/fundamentals-timeseries/v1/finance/timeseries/" + url.PathEscape(symbol))
	if err != nil {
		return "", fmt.Errorf("yahoo: build url: %w", err)
	}
	q := base.Query()
	q.Set("merge", "false")
	q.Set("padTimeSeries", "true")
	q.Set("period1", strconv.FormatInt(c.period1, 10))
	period2 := c.period2
	if period2 <= 0 {
		period2 = c.now().Unix()
	}
	q.Set("period2", strconv.FormatInt(period2, 10))
	q.Set("type", strings.Join(types, ","))
	q.Set("lang", "en-US")
	q.Set("region", "US")
	base.RawQuery = q.Encode()
	return base.String(), nil
}

// get performs a bounded-retry GET. A 429 or 503 (and the gateway 5xx family)
// is retried with exponential backoff; every other 4xx is terminal.
func (c *Client) get(ctx context.Context, rawURL, accept string) ([]byte, error) {
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, fmt.Errorf("yahoo: build request: %w", err)
		}
		req.Header.Set("User-Agent", c.userAgent)
		if accept != "" {
			req.Header.Set("Accept", accept)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt == c.maxAttempts {
				return nil, fmt.Errorf("yahoo: get %s: %w", rawURL, err)
			}
			if werr := c.wait(ctx, c.delayFor(attempt)); werr != nil {
				return nil, werr
			}
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("yahoo: read body: %w", readErr)
		}

		if isRetryableStatus(resp.StatusCode) {
			lastErr = &HTTPError{StatusCode: resp.StatusCode, URL: rawURL, Body: truncate(string(body), 300)}
			if attempt == c.maxAttempts {
				return nil, lastErr
			}
			if werr := c.wait(ctx, c.delayFor(attempt)); werr != nil {
				return nil, werr
			}
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, &HTTPError{StatusCode: resp.StatusCode, URL: rawURL, Body: truncate(string(body), 300)}
		}
		return body, nil
	}
	return nil, lastErr
}

func isRetryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func (c *Client) delayFor(attempt int) time.Duration {
	d := c.baseDelay
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= c.maxDelay {
			return c.maxDelay
		}
	}
	if d > c.maxDelay {
		return c.maxDelay
	}
	return d
}

func (c *Client) wait(ctx context.Context, d time.Duration) error {
	if c.sleep != nil {
		return c.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// wireEnvelope is the v1 envelope the page uses. result is kept as raw maps
// because each entry's data array is keyed by its own type name.
type wireEnvelope struct {
	Timeseries struct {
		Result []map[string]json.RawMessage `json:"result"`
		Error  *wireError                   `json:"error"`
	} `json:"timeseries"`
}

type wireError struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

type wireMeta struct {
	Symbol []string `json:"symbol"`
	Type   []string `json:"type"`
}

type wireFact struct {
	DataIDV2     string     `json:"dataIdV2"`
	AsOfDate     string     `json:"asOfDate"`
	PeriodType   string     `json:"periodType"`
	CurrencyCode string     `json:"currencyCode"`
	Restated     *wireValue `json:"restatedValue"`
	Reported     *wireValue `json:"reportedValue"`
}

type wireValue struct {
	Raw json.Number `json:"raw"`
	Fmt string      `json:"fmt"`
}

func parseTimeseries(body []byte, requested []string) (*Result, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var env wireEnvelope
	if err := dec.Decode(&env); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", ErrMalformed, err)
	}
	if env.Timeseries.Error != nil {
		return nil, fmt.Errorf("%w: %s: %s", ErrMalformed, env.Timeseries.Error.Code, env.Timeseries.Error.Description)
	}

	res := &Result{
		Facts:  make(map[string][]Fact, len(requested)),
		Status: make(map[string]TypeStatus, len(requested)),
	}
	for _, typ := range requested {
		res.Status[typ] = TypeAbsent
	}

	for _, entry := range env.Timeseries.Result {
		metaRaw, ok := entry["meta"]
		if !ok {
			return nil, fmt.Errorf("%w: result entry without meta", ErrMalformed)
		}
		var meta wireMeta
		if err := json.Unmarshal(metaRaw, &meta); err != nil {
			return nil, fmt.Errorf("%w: decode meta: %v", ErrMalformed, err)
		}
		if len(meta.Type) == 0 {
			return nil, fmt.Errorf("%w: result entry without meta.type", ErrMalformed)
		}
		typeName := meta.Type[0]

		arrRaw, ok := entry[typeName]
		if !ok || len(bytes.TrimSpace(arrRaw)) == 0 || string(bytes.TrimSpace(arrRaw)) == "null" {
			if _, requestedType := res.Status[typeName]; requestedType {
				res.Status[typeName] = TypeNoData
			}
			continue
		}

		var wireFacts []*wireFact
		if err := json.Unmarshal(arrRaw, &wireFacts); err != nil {
			return nil, fmt.Errorf("%w: decode %s: %v", ErrMalformed, typeName, err)
		}

		facts := make([]Fact, 0, len(wireFacts))
		for _, wf := range wireFacts {
			if wf == nil {
				continue
			}
			fact, ok, err := factFromWire(typeName, wf)
			if err != nil {
				return nil, err
			}
			if ok {
				facts = append(facts, fact)
			}
		}
		res.Facts[typeName] = facts
		if _, requestedType := res.Status[typeName]; requestedType {
			if len(facts) > 0 {
				res.Status[typeName] = TypeObserved
			} else {
				res.Status[typeName] = TypeNoData
			}
		}
	}
	return res, nil
}

func factFromWire(typeName string, wf *wireFact) (Fact, bool, error) {
	asOf, err := time.Parse("2006-01-02", strings.TrimSpace(wf.AsOfDate))
	if err != nil {
		return Fact{}, false, fmt.Errorf("%w: %s bad asOfDate %q", ErrMalformed, typeName, wf.AsOfDate)
	}

	code := strings.TrimSpace(wf.DataIDV2)
	if code == "" {
		code = codeFromType(typeName)
	}
	periodType := strings.TrimSpace(wf.PeriodType)
	if periodType == "" {
		periodType = periodFromType(typeName)
	}

	restated, restatedRaw, err := parseWireValue(wf.Restated)
	if err != nil {
		return Fact{}, false, fmt.Errorf("%w: %s restated: %v", ErrMalformed, typeName, err)
	}
	reported, reportedRaw, err := parseWireValue(wf.Reported)
	if err != nil {
		return Fact{}, false, fmt.Errorf("%w: %s reported: %v", ErrMalformed, typeName, err)
	}

	fact := Fact{
		Code:       code,
		Type:       typeName,
		PeriodType: periodType,
		AsOfDate:   asOf,
		Currency:   strings.TrimSpace(wf.CurrencyCode),
		Restated:   restated,
		Reported:   reported,
	}
	switch {
	case restated != nil:
		fact.Value, fact.Raw, fact.Source = restated, restatedRaw, financials.ValueRestated
	case reported != nil:
		fact.Value, fact.Raw, fact.Source = reported, reportedRaw, financials.ValueReported
	default:
		return Fact{}, false, nil
	}
	return fact, true, nil
}

func parseWireValue(v *wireValue) (*float64, string, error) {
	if v == nil {
		return nil, "", nil
	}
	raw := strings.TrimSpace(v.Raw.String())
	if raw == "" || raw == "null" {
		return nil, "", nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, raw, fmt.Errorf("parse %q: %w", raw, err)
	}
	return &f, raw, nil
}

// codeFromType strips the _12M / _3M / _TTM suffix from a requested token.
func codeFromType(typeName string) string {
	for _, suffix := range []string{"_" + financials.PeriodAnnual, "_" + financials.PeriodQuarterly, "_" + financials.PeriodTTM} {
		if strings.HasSuffix(typeName, suffix) {
			return strings.TrimSuffix(typeName, suffix)
		}
	}
	return typeName
}

func periodFromType(typeName string) string {
	for _, p := range []string{financials.PeriodAnnual, financials.PeriodQuarterly, financials.PeriodTTM} {
		if strings.HasSuffix(typeName, "_"+p) {
			return p
		}
	}
	return ""
}

// TypeList is the embedded request's type list for one statement page. It is
// discovery evidence, not a classification input.
type TypeList struct {
	Statement string
	Types     []string
	URLs      []string
}

var dataURLPattern = regexp.MustCompile(`data-url="([^"]+)"`)

// DiscoverTypeList reads the statement page and extracts the exact type list
// the page asked for. The page is ~1 MB and can rate-limit, so this is an
// explicit discovery path, never the default classifier. statement is
// "financials", "balance-sheet" or "cash-flow"; empty defaults to financials.
func (c *Client) DiscoverTypeList(ctx context.Context, ticker, statement string) (*TypeList, error) {
	symbol := NormalizeSymbol(ticker)
	if symbol == "" {
		return nil, fmt.Errorf("yahoo: empty ticker")
	}
	if statement == "" {
		statement = "financials"
	}
	switch statement {
	case "financials", "balance-sheet", "cash-flow":
	default:
		return nil, fmt.Errorf("yahoo: unknown statement page %q", statement)
	}

	pageURL := strings.TrimRight(c.pageBaseURL, "/") + "/quote/" + url.PathEscape(symbol) + "/" + statement + "/"
	body, err := c.get(ctx, pageURL, "text/html")
	if err != nil {
		return nil, err
	}

	list := &TypeList{Statement: statement}
	seen := make(map[string]struct{})
	for _, match := range dataURLPattern.FindAllStringSubmatch(string(body), -1) {
		raw := html.UnescapeString(match[1])
		u, err := url.Parse(raw)
		if err != nil || !strings.Contains(u.Path, "fundamentals-timeseries") {
			continue
		}
		q := u.Query()
		types := q.Get("type")
		if types == "" {
			continue
		}
		list.URLs = append(list.URLs, raw)
		for _, typ := range strings.Split(types, ",") {
			typ = strings.TrimSpace(typ)
			if typ == "" {
				continue
			}
			if _, ok := seen[typ]; ok {
				continue
			}
			seen[typ] = struct{}{}
			list.Types = append(list.Types, typ)
		}
	}
	if len(list.Types) == 0 {
		return nil, ErrNoTypeList
	}
	return list, nil
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
