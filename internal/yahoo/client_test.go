package yahoo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
)

func TestNormalizeSymbol(t *testing.T) {
	cases := map[string]string{
		"PTBA":     "PTBA.JK",
		" ptba ":   "PTBA.JK",
		"BBRI.JK":  "BBRI.JK",
		"aa.jk":    "AA.JK",
		"":         "",
		"TUGU.JK ": "TUGU.JK",
	}
	for in, want := range cases {
		if got := NormalizeSymbol(in); got != want {
			t.Errorf("NormalizeSymbol(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseTimeseries_ParsesEnvelope(t *testing.T) {
	body := []byte(`{
	  "timeseries": {
	    "result": [
	      {"meta":{"symbol":["PTBA.JK"],"type":["EQT07_12M"]},"timestamp":[1767139200],"EQT07_12M":[
	        {"dataIdV2":"EQT07","asOfDate":"2025-12-31","periodType":"12M","currencyCode":"IDR",
	         "restatedValue":{"raw":4.2651724E13,"fmt":"42.65T"}},
	        null
	      ]},
	      {"meta":{"symbol":["PTBA.JK"],"type":["EQ46Q_12M"]},"timestamp":[1767139200],"EQ46Q_12M":[
	        {"dataIdV2":"EQ46Q","asOfDate":"2025-12-31","periodType":"12M","currencyCode":"IDR",
	         "reportedValue":{"raw":6257100000000,"fmt":"6.26T"}}
	      ]},
	      {"meta":{"symbol":["PTBA.JK"],"type":["EQ8Y4_12M"]}}
	    ],
	    "error": null
	  }
	}`)

	res, err := parseTimeseries(body, []string{"EQT07_12M", "EQ46Q_12M", "EQ8Y4_12M", "EQYWG_12M"})
	if err != nil {
		t.Fatalf("parseTimeseries: %v", err)
	}

	rev := res.Facts["EQT07_12M"]
	if len(rev) != 1 {
		t.Fatalf("revenue facts = %d, want 1 (null skipped)", len(rev))
	}
	if rev[0].Code != "EQT07" || rev[0].PeriodType != financials.PeriodAnnual {
		t.Errorf("revenue fact = %+v", rev[0])
	}
	if rev[0].Raw != "4.2651724E13" {
		t.Errorf("raw = %q, want literal E-notation", rev[0].Raw)
	}
	if rev[0].Value == nil || *rev[0].Value != 42651724000000 {
		t.Errorf("value = %v, want 42651724000000", rev[0].Value)
	}
	if rev[0].Source != financials.ValueRestated {
		t.Errorf("source = %q, want restated", rev[0].Source)
	}
	if rev[0].Currency != "IDR" {
		t.Errorf("currency = %q, want IDR", rev[0].Currency)
	}

	gp := res.Facts["EQ46Q_12M"]
	if len(gp) != 1 || gp[0].Source != financials.ValueReported {
		t.Errorf("gross profit = %+v, want one reported fact", gp)
	}
	if gp[0].Value == nil || *gp[0].Value != 6257100000000 {
		t.Errorf("gross profit value = %v", gp[0].Value)
	}

	if res.Status["EQT07_12M"] != TypeObserved {
		t.Errorf("EQT07 status = %q, want observed", res.Status["EQT07_12M"])
	}
	if res.Status["EQ8Y4_12M"] != TypeNoData {
		t.Errorf("EQ8Y4 status = %q, want no_data (meta-only)", res.Status["EQ8Y4_12M"])
	}
	if res.Status["EQYWG_12M"] != TypeAbsent {
		t.Errorf("EQYWG status = %q, want absent", res.Status["EQYWG_12M"])
	}
	if got := len(res.Flatten()); got != 2 {
		t.Errorf("flatten = %d, want 2", got)
	}
}

func TestParseTimeseries_PrefersRestatedButKeepsBoth(t *testing.T) {
	body := []byte(`{"timeseries":{"result":[
	  {"meta":{"symbol":["X"],"type":["EQQR7_12M"]},"EQQR7_12M":[
	    {"dataIdV2":"EQQR7","asOfDate":"2025-12-31","periodType":"12M","currencyCode":"IDR",
	     "restatedValue":{"raw":-300136000000,"fmt":"-300.14B"},
	     "reportedValue":{"raw":-300000000000,"fmt":"-300.00B"}}
	  ]}
	],"error":null}}`)

	res, err := parseTimeseries(body, []string{"EQQR7_12M"})
	if err != nil {
		t.Fatalf("parseTimeseries: %v", err)
	}
	f := res.Facts["EQQR7_12M"][0]
	if f.Source != financials.ValueRestated || *f.Value != -300136000000 {
		t.Errorf("chosen = %v/%v, want restated -300136000000", f.Source, f.Value)
	}
	if f.Reported == nil || *f.Reported != -300000000000 {
		t.Errorf("reported alternate = %v, want -300000000000", f.Reported)
	}
}

func TestParseTimeseries_ErrorEnvelope(t *testing.T) {
	body := []byte(`{"timeseries":{"result":null,"error":{"code":"Not Found","description":"No timeseries type(s) specified"}}}`)
	_, err := parseTimeseries(body, []string{"EQT07_12M"})
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed", err)
	}
}

func TestParseTimeseries_BadDateIsMalformed(t *testing.T) {
	body := []byte(`{"timeseries":{"result":[
	  {"meta":{"symbol":["X"],"type":["EQT07_12M"]},"EQT07_12M":[
	    {"dataIdV2":"EQT07","asOfDate":"31/12/2025","periodType":"12M","currencyCode":"IDR",
	     "restatedValue":{"raw":1,"fmt":"1"}}
	  ]}
	],"error":null}}`)
	_, err := parseTimeseries(body, []string{"EQT07_12M"})
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("err = %v, want ErrMalformed", err)
	}
}

func TestFetchTypes_NoCookieAndRetriesTransient(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if r.Header.Get("Cookie") != "" {
			t.Errorf("unexpected cookie: %q", r.Header.Get("Cookie"))
		}
		if r.Header.Get("User-Agent") == "" {
			t.Errorf("missing User-Agent")
		}
		q := r.URL.Query()
		if q.Get("merge") != "false" || q.Get("padTimeSeries") != "true" {
			t.Errorf("query = %v", q)
		}
		if q.Get("type") != "EQT07_12M,EQ46Q_12M" {
			t.Errorf("type = %q", q.Get("type"))
		}
		switch n {
		case 1:
			http.Error(w, "slow down", http.StatusTooManyRequests)
		case 2:
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		default:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"timeseries":{"result":[{"meta":{"symbol":["X"],"type":["EQT07_12M"]},"EQT07_12M":[{"dataIdV2":"EQT07","asOfDate":"2025-12-31","periodType":"12M","currencyCode":"IDR","restatedValue":{"raw":1,"fmt":"1"}}]}],"error":null}}`))
		}
	}))
	defer server.Close()

	var delays []time.Duration
	c := NewClient(
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
		WithRetry(3, 10*time.Millisecond, time.Second),
		WithSleep(func(_ context.Context, d time.Duration) error {
			delays = append(delays, d)
			return nil
		}),
	)
	res, err := c.FetchTypes(context.Background(), "PTBA", []string{"EQT07_12M", "EQ46Q_12M"})
	if err != nil {
		t.Fatalf("FetchTypes: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if len(delays) != 2 || delays[0] != 10*time.Millisecond || delays[1] != 20*time.Millisecond {
		t.Errorf("delays = %v, want [10ms 20ms]", delays)
	}
	if len(res.Facts["EQT07_12M"]) != 1 {
		t.Errorf("facts = %+v", res.Facts)
	}
}

func TestFetchTypes_NonRetryableStatus(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	c := NewClient(WithBaseURL(server.URL), WithHTTPClient(server.Client()), WithRetry(3, time.Millisecond, time.Second))
	_, err := c.FetchTypes(context.Background(), "PTBA", []string{"EQT07_12M"})
	if err == nil {
		t.Fatal("want error")
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("err = %v, want HTTPError 400", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
}

func TestFetchTypes_ExhaustsRetries(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	c := NewClient(
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
		WithRetry(2, time.Millisecond, time.Second),
		WithSleep(func(context.Context, time.Duration) error { return nil }),
	)
	_, err := c.FetchTypes(context.Background(), "PTBA", []string{"EQT07_12M"})
	if err == nil {
		t.Fatal("want error")
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}

func TestDiscoverTypeList(t *testing.T) {
	page := `<!doctype html><html><body>
	<script type="application/json" data-sveltekit-fetched
	  data-url="https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/PTBA.JK?merge=false&amp;type=EQT07_12M%2CEQ46Q_12M&amp;lang=en-US"></script>
	<script type="application/json" data-url="https://example.com/not-timeseries?type=X"></script>
	<script data-url="https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/PTBA.JK?type=EQT07_TTM%2CEQT07_12M"></script>
	</body></html>`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/financials/") {
			t.Errorf("path = %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(page))
	}))
	defer server.Close()

	c := NewClient(WithPageBaseURL(server.URL), WithHTTPClient(server.Client()))
	list, err := c.DiscoverTypeList(context.Background(), "PTBA", "financials")
	if err != nil {
		t.Fatalf("DiscoverTypeList: %v", err)
	}
	want := []string{"EQT07_12M", "EQ46Q_12M", "EQT07_TTM"}
	if strings.Join(list.Types, ",") != strings.Join(want, ",") {
		t.Errorf("types = %v, want %v", list.Types, want)
	}
	if len(list.URLs) != 2 {
		t.Errorf("urls = %d, want 2", len(list.URLs))
	}
	if len(list.Groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(list.Groups))
	}
	if got := strings.Join(list.Groups[0].Types, ","); got != "EQT07_12M,EQ46Q_12M" {
		t.Errorf("group 0 types = %s", got)
	}
	if got := strings.Join(list.Groups[1].Types, ","); got != "EQT07_TTM,EQT07_12M" {
		t.Errorf("group 1 types = %s", got)
	}
}

func TestDiscoverTypeList_NoList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>no data</html>`))
	}))
	defer server.Close()

	c := NewClient(WithPageBaseURL(server.URL), WithHTTPClient(server.Client()))
	_, err := c.DiscoverTypeList(context.Background(), "PTBA", "financials")
	if !errors.Is(err, ErrNoTypeList) {
		t.Fatalf("err = %v, want ErrNoTypeList", err)
	}
}

func TestDiscoverTypeList_RejectsUnknownPage(t *testing.T) {
	c := NewClient()
	_, err := c.DiscoverTypeList(context.Background(), "PTBA", "segment")
	if err == nil {
		t.Fatal("want error for unknown statement page")
	}
}
