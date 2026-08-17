package tasks

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

const (
	// rssSourceName is the source_status row name for the RSS feed bundle.
	rssSourceName = "rss"
	// rssMaxAgeSeconds is the source_status freshness window (24 hours).
	rssMaxAgeSeconds int32 = 86400
	// rssRawRetentionDays is how long claim-checked feed XML stays on R2.
	rssRawRetentionDays int32 = 30
	// RSSHTTPTimeout bounds each feed fetch. Export for main wiring.
	RSSHTTPTimeout = 30 * time.Second
	// rssUserAgent mimics a Chrome browser. Some feeds (tempo) 403 non-browser
	// UAs; the project already uses a Chrome UA for the Cloudflare-protected IDX
	// API, so the same fingerprint here is consistent.
	rssUserAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

// RSSFeed is one market news feed to ingest.
type RSSFeed struct {
	Name string // short source label stored in news_items.source
	URL  string
}

// DefaultRSSFeeds are the three Indonesian market RSS feeds the pipeline pulls.
// Exported so main can pass them to the handler; tests inject their own.
// Verified live 2026-08-10: cnbc + detik respond to a plain client, tempo needs
// a browser User-Agent. kontan (TLS handshake failure) and bisnis (Cloudflare
// 403) were dropped after the same check.
var DefaultRSSFeeds = []RSSFeed{
	{Name: "cnbc", URL: "https://cnbcindonesia.com/market/rss"},
	{Name: "detik", URL: "https://finance.detik.com/rss"},
	{Name: "tempo", URL: "https://rss.tempo.co/bisnis"},
}

// RSSPayload is the payload for an rss:ingest task. The date only keys the
// asynq TaskID for dedup — feeds are bounded windows, so no date window is
// passed downstream.
type RSSPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// RSSArticle is one parsed feed item.
type RSSArticle struct {
	Title       string
	URL         string
	PublishedAt time.Time
	Snippet     string
}

// EnqueueRSS enqueues an rss:ingest task for the given date with a date-keyed
// TaskID for dedup. Extra opts (e.g. asynq.ProcessIn) are appended.
func EnqueueRSS(client *asynq.Client, date time.Time, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	dateKey := date.Format("2006-01-02")
	taskKey := TaskKey(TypeRSS, dateKey)
	payload := RSSPayload{Date: dateKey}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal rss payload: %w", err)
	}

	task := asynq.NewTask(TypeRSS, raw)
	options := []asynq.Option{
		asynq.TaskID(taskKey),
		asynq.Queue("ingest"),
		asynq.MaxRetry(3), // exp-backoff + jitter are asynq defaults
		asynq.Retention(24 * time.Hour),
	}
	options = append(options, opts...)
	return client.Enqueue(task, options...)
}

// NewRSSHandler returns an asynq handler for the rss:ingest task type. It pulls
// each feed, matches articles to tickers by code/company name, stores matched
// items in news_items + news_tickers, and claim-checks each raw feed XML to R2
// (audit/re-parse safety net). Unmatched items are discarded.
//
// Feed fetch/parse failures are isolated per feed — one bad feed doesn't drop
// the others' articles — but any failure degrades the whole run: the handler
// returns an error so asynq retries and source_status records the gap, never a
// false "fully healthy". A DB write failure is likewise fatal (asynq retries;
// news upserts are idempotent via url UNIQUE). R2 claim-check failure is logged
// but does not block article storage: the XML is a re-parse safety net, and the
// content-hash key makes the next run retry the same object.
func NewRSSHandler(
	log *logrus.Logger,
	httpClient *http.Client,
	store storage.ObjectStore,
	feeds []RSSFeed,
	db *sqlx.DB,
	tickerRepo *repository.TickerRepository,
	newsRepo *repository.NewsRepository,
	newsTickerRepo *repository.NewsTickerRepository,
	sourceStatusRepo *repository.SourceStatusRepository,
	alertRepo *repository.AlertRepository,
	rawFileRepo *repository.RawFileRepository,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		var p RSSPayload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("unmarshal payload: %w", err)
		}
		runDate, err := time.Parse("2006-01-02", p.Date)
		if err != nil {
			return fmt.Errorf("invalid date %q: %w", p.Date, err)
		}

		// The matching universe is the DB tickers table (full IDX listing,
		// seeded by stock_summary). Loaded per run so new listings are matched
		// the same day without a redeploy — a stale embed map is not consulted.
		tickers, err := tickerRepo.FindAll(db)
		if err != nil {
			log.Errorf("rss: failed to load tickers: %v", err)
			recordSourceFailure(db, sourceStatusRepo, alertRepo, rssSourceName, rssMaxAgeSeconds, p.Date, err, log)
			return fmt.Errorf("load tickers: %w", err)
		}
		matcher := buildTickerMatcher(tickers)

		log.Infof("rss: ingesting feeds, run date=%s (%d tickers in universe)",
			runDate.Format("2006-01-02"), len(tickers))

		taskID, _ := asynq.GetTaskID(ctx)
		matchedTotal := 0
		fetchedFeeds := 0
		var firstErr error
		for _, feed := range feeds {
			start := time.Now()
			logEvent(log, logrus.InfoLevel, "fetch_start", "fetching feed",
				logrus.Fields{"task_id": taskID, "source": TypeRSS, "feed": feed.Name, "fetch_url": feed.URL})
			_, articles, err := fetchFeed(httpClient, feed)
			latency := time.Since(start).Milliseconds()
			if err != nil {
				logEvent(log, logrus.ErrorLevel, "fetch_failure", "feed fetch failed",
					logrus.Fields{"task_id": taskID, "source": TypeRSS, "feed": feed.Name, "fetch_url": feed.URL, "error": err.Error(), "latency_ms": latency})
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			fetchedFeeds++
			logEvent(log, logrus.InfoLevel, "fetch_success", "feed fetched",
				logrus.Fields{"task_id": taskID, "source": TypeRSS, "feed": feed.Name, "fetch_url": feed.URL, "rows": len(articles), "latency_ms": latency})

			n, err := storeArticles(db, matcher, tickerRepo, newsRepo, newsTickerRepo, feed, articles, log)
			if err != nil {
				// Surface so asynq retries — a row dropped mid-upsert would
				// otherwise be skipped forever on the next run.
				log.Errorf("rss: store failed for %s: %v", feed.Name, err)
				recordSourceFailure(db, sourceStatusRepo, alertRepo, rssSourceName, rssMaxAgeSeconds, p.Date, err, log)
				return err
			}
			matchedTotal += n

			// Raw-XML claim-check DISABLED 2026-08-10: the feed dump duplicates
			// the DB rows for matched articles and adds nothing the AI consumes
			// (it reads news_items.snippet, or web-fetches the article URL
			// itself). Recovery-from-XML for rule changes was a hypothetical —
			// re-enable by uncommenting and restoring `raw` from fetchFeed.
			// if store != nil {
			// 	if err := claimCheckRSS(ctx, store, feed, raw, db, rawFileRepo); err != nil {
			// 		log.Errorf("rss: claim-check failed for %s: %v", feed.Name, err)
			// 	}
			// }
		}

		// Any feed failure is a degraded run: surface it so asynq retries and
		// source_status reflects the gap. Already-stored articles are idempotent
		// (url UNIQUE), so a re-run only fills the missing feed's window.
		if firstErr != nil {
			log.Errorf("rss: %d/%d feed(s) failed, first error: %v",
				len(feeds)-fetchedFeeds, len(feeds), firstErr)
			recordSourceFailure(db, sourceStatusRepo, alertRepo, rssSourceName, rssMaxAgeSeconds, p.Date, firstErr, log)
			return firstErr
		}

		log.Infof("rss: stored %d matched article(s) from %d feed(s)", matchedTotal, len(feeds))

		// Feeds are bounded windows with url-UNIQUE dedup — there is no
		// incremental cursor to advance. nil preserves any existing watermark.
		recordSourceSuccess(db, sourceStatusRepo, rssSourceName, rssMaxAgeSeconds, nil, log)
		return nil
	}
}

// fetchFeed GETs one feed and parses it into articles. Returns the raw XML (for
// the R2 claim-check) alongside the parsed items.
func fetchFeed(hc *http.Client, feed RSSFeed) ([]byte, []RSSArticle, error) {
	req, err := http.NewRequest(http.MethodGet, feed.URL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", rssUserAgent)
	req.Header.Set("Accept", "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8")

	resp, err := hc.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, nil, fmt.Errorf("feed http error: status=%d body=%s", resp.StatusCode, truncate(string(body), 200))
	}

	articles, err := parseRSS(body)
	if err != nil {
		return nil, nil, fmt.Errorf("parse rss: %w", err)
	}
	return body, articles, nil
}

// rssDocument mirrors the RSS 2.0 structure the three feeds emit.
type rssDocument struct {
	Channel struct {
		Items []struct {
			Title       string `xml:"title"`
			Link        string `xml:"link"`
			PubDate     string `xml:"pubDate"`
			Description string `xml:"description"`
			DCDate      string `xml:"http://purl.org/dc/elements/1.1/ date"`
		} `xml:"item"`
	} `xml:"channel"`
}

// parseRSS unmarshals RSS 2.0 XML into articles. Items missing a title, link,
// or parseable publication date are skipped — a news item must be placeable in
// time and point somewhere.
func parseRSS(data []byte) ([]RSSArticle, error) {
	var doc rssDocument
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	var out []RSSArticle
	for _, it := range doc.Channel.Items {
		title := strings.TrimSpace(stripHTML(it.Title))
		link := strings.TrimSpace(it.Link)
		if title == "" || link == "" {
			continue
		}

		pubDate, err := parseRSSTime(it.PubDate)
		if err != nil && it.DCDate != "" {
			pubDate, err = parseRSSTime(it.DCDate)
		}
		if err != nil {
			continue
		}

		snippet := strings.TrimSpace(stripHTML(it.Description))
		out = append(out, RSSArticle{Title: title, URL: link, PublishedAt: pubDate, Snippet: snippet})
	}
	return out, nil
}

// rssTimeLayouts are the date formats the three feeds may emit. naive marks a
// zone-less wall time: Indonesian feeds emit these in WIB local time, so they
// must be relabeled Asia/Jakarta — parsing them bare would pin them to UTC and
// store every article 7 hours late.
var rssTimeLayouts = []struct {
	format string
	naive  bool
}{
	{time.RFC1123Z, false}, // "Mon, 02 Jan 2006 15:04:05 -0700"
	{time.RFC1123, false},  // "Mon, 02 Jan 2006 15:04:05 GMT"
	{time.RFC3339, false},  // "2006-01-02T15:04:05+07:00"
	{"2006-01-02 15:04:05", true},
	{"2006-01-02 15:04", true},
}

func parseRSSTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty time")
	}
	for _, l := range rssTimeLayouts {
		if t, err := time.Parse(l.format, s); err == nil {
			if l.naive {
				if loc, err := time.LoadLocation("Asia/Jakarta"); err == nil {
					return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, loc), nil
				}
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized time %q", s)
}

// ─── ticker matching ────────────────────────────────────────────────────────

// rssCodeStopwords are ticker codes that are also common English/Indonesian
// words ("BUMI" = earth). They only match when written uppercase in the
// article, so a lowercase dictionary use ("bumi", "true", "edge") can't tag a
// company.
var rssCodeStopwords = map[string]bool{
	"bumi": true,
	"edge": true,
	"true": true,
}

// rssNameStopwords are generic company-name tokens dropped before name
// matching — legal suffixes, country/sector words, and other high-frequency
// tokens that would otherwise cause false positives. "indonesia", "bank",
// "energy", and friends appear in too many names to be distinctive alone.
var rssNameStopwords = map[string]bool{
	"pt": true, "tbk": true, "persero": true, "dan": true, "the": true,
	"of": true, "and": true, "co": true, "inc": true, "ltd": true,
	"indonesia": true, "bank": true, "group": true, "energy": true, "holdings": true,
	"international": true, "corporation": true, "company": true,
	"media": true, "sukses": true, "makmur": true, "tunggal": true,
	"prakarsa": true, "megah": true, "nusantara": true, "citra": true,
	"aneka": true, "surya": true, "tambang": true, "resources": true,
	"jaya": true, "karya": true, "internasional": true, "multiartha": true,
}

// codePattern is a compiled code-matching regex. UppercaseOnly forces a
// case-sensitive match (used for stopword codes).
type codePattern struct {
	re            *regexp.Regexp
	uppercaseOnly bool
}

// tickerMatchEntry is one ticker's match rules derived from the bundled map.
type tickerMatchEntry struct {
	code   string
	name   string // full company name (for ticker seeding)
	sig    []string
	single string // set for names with exactly one significant token
}

type tickerMatcher struct {
	entries []tickerMatchEntry
	codes   []codePattern
}

var (
	wordRe    = regexp.MustCompile(`[a-z0-9]+`)
	htmlTagRe = regexp.MustCompile(`<[^>]*>`)
)

// significantTokens reduces a company name to its distinctive lowercase
// tokens: legal suffixes dropped, generic stopwords removed, short tokens
// discarded.
func significantTokens(companyName string) []string {
	s := strings.ToLower(companyName)
	for _, suffix := range []string{"(persero)", "tbk.", "tbk", "pt"} {
		s = strings.ReplaceAll(s, suffix, " ")
	}

	seen := make(map[string]bool)
	var out []string
	for _, tok := range wordRe.FindAllString(s, -1) {
		if len(tok) < 3 || rssNameStopwords[tok] || seen[tok] {
			continue
		}
		seen[tok] = true
		out = append(out, tok)
	}
	return out
}

// buildTickerMatcher compiles a ticker list (code + company name) into match
// rules. The caller supplies the universe — the DB tickers table, seeded by
// stock_summary from the full IDX listing, never the stale 49-ticker embed map.
// Single-token names (e.g. "Telkom" from "Telkom Indonesia Tbk.") only match
// when the token is long enough AND unambiguous — a token shared with any other
// ticker's name ("astra" is in both Astra International and Astra Agro Lestari)
// is never used alone, keeping one company's short name from tagging another.
func buildTickerMatcher(tickers []entity.Ticker) *tickerMatcher {
	m := &tickerMatcher{}
	tokenOwners := make(map[string]int)
	for _, tk := range tickers {
		entry := tickerMatchEntry{code: tk.Code, name: tk.Name, sig: significantTokens(tk.Name)}
		for _, tok := range entry.sig {
			tokenOwners[tok]++
		}
		m.entries = append(m.entries, entry)
	}

	for i := range m.entries {
		if len(m.entries[i].sig) == 1 {
			tok := m.entries[i].sig[0]
			if len(tok) >= 5 && tokenOwners[tok] == 1 {
				m.entries[i].single = tok
			}
		}
		quoted := regexp.QuoteMeta(m.entries[i].code)
		upperOnly := rssCodeStopwords[strings.ToLower(m.entries[i].code)]
		pattern := `\b` + quoted + `\b`
		if !upperOnly {
			pattern = `(?i)` + pattern
		}
		m.codes = append(m.codes, codePattern{
			re:            regexp.MustCompile(pattern),
			uppercaseOnly: upperOnly,
		})
	}
	return m
}

// tickerMatch is one matched ticker for an article. name is the full company
// name from the bundled map, used to seed the tickers row without clobbering
// the real name with a bare code.
type tickerMatch struct {
	code   string
	name   string
	method string // "code" or "name"
}

// match returns the tickers matched in an article. Code matches win over name
// matches for the same ticker (never two rows for one code). Case-insensitive
// code matching is used except for stopword codes, which must appear uppercase.
func (m *tickerMatcher) match(title, snippet string) []tickerMatch {
	rawText := title + " " + snippet
	normText := normalizeText(rawText)

	var out []tickerMatch
	matched := make(map[string]bool)

	// Code matching first: a literal ticker code mention is the strongest
	// signal, so it claims the ticker before name matching can.
	for i := range m.entries {
		if m.codes[i].uppercaseOnly {
			if !m.codes[i].re.MatchString(rawText) {
				continue
			}
		} else if !m.codes[i].re.MatchString(rawText) {
			continue
		}
		if !matched[m.entries[i].code] {
			matched[m.entries[i].code] = true
			out = append(out, tickerMatch{code: m.entries[i].code, name: m.entries[i].name, method: "code"})
		}
	}

	textWords := make(map[string]bool)
	for _, w := range wordRe.FindAllString(normText, -1) {
		textWords[w] = true
	}

	for i := range m.entries {
		e := &m.entries[i]
		if matched[e.code] {
			continue
		}
		if e.single != "" {
			if textWords[e.single] {
				matched[e.code] = true
				out = append(out, tickerMatch{code: e.code, name: e.name, method: "name"})
			}
			continue
		}
		if len(e.sig) >= 2 && allWordsPresent(textWords, e.sig) {
			matched[e.code] = true
			out = append(out, tickerMatch{code: e.code, name: e.name, method: "name"})
		}
	}
	return out
}

func allWordsPresent(textWords map[string]bool, toks []string) bool {
	for _, tok := range toks {
		if !textWords[tok] {
			return false
		}
	}
	return true
}

// normalizeText lowercases and strips HTML/entities from article text for name
// matching.
func normalizeText(s string) string {
	s = htmlTagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	return strings.ToLower(s)
}

// stripHTML removes tags and unescapes entities, keeping text readable.
func stripHTML(s string) string {
	return strings.TrimSpace(htmlTagRe.ReplaceAllString(html.UnescapeString(s), " "))
}

// ─── storage ────────────────────────────────────────────────────────────────

// storeArticles matches every article, upserts matched items into news_items
// (idempotent via url UNIQUE), and inserts one news_tickers row per matched
// ticker with its match_method. Unmatched articles are discarded. Returns the
// number of stored articles; a failed upsert fails the whole batch so the
// caller retries rather than silently advancing past unpersisted rows.
func storeArticles(
	db *sqlx.DB,
	m *tickerMatcher,
	tickerRepo *repository.TickerRepository,
	newsRepo *repository.NewsRepository,
	newsTickerRepo *repository.NewsTickerRepository,
	feed RSSFeed,
	articles []RSSArticle,
	log *logrus.Logger,
) (int, error) {
	stored := 0
	for _, a := range articles {
		matches := m.match(a.Title, a.Snippet)
		if len(matches) == 0 {
			log.Debugf("rss: discarding unmatched article %q", truncate(a.Title, 80))
			continue
		}

		var snippet *string
		if a.Snippet != "" {
			snippet = &a.Snippet
		}
		id, err := newsRepo.Upsert(db, &entity.NewsItem{
			Title:       a.Title,
			URL:         a.URL,
			Source:      feed.Name,
			PublishedAt: a.PublishedAt,
			Snippet:     snippet,
		})
		if err != nil {
			return stored, fmt.Errorf("news upsert %q: %w", a.URL, err)
		}

		for _, mt := range matches {
			if err := tickerRepo.InsertIfAbsent(db, mt.code, mt.name); err != nil {
				return stored, fmt.Errorf("ticker seed %s: %w", mt.code, err)
			}
			if err := newsTickerRepo.Insert(db, &entity.NewsTicker{
				NewsID:      id,
				Ticker:      mt.code,
				MatchMethod: mt.method,
			}); err != nil {
				return stored, fmt.Errorf("news_ticker insert %s: %w", mt.code, err)
			}
		}
		stored++
	}
	return stored, nil
}

// claimCheckRSS uploads raw feed XML to R2 under a content-hash key and records
// the pointer in raw_files. The content-addressed key makes re-runs idempotent:
// identical XML maps to the same object, so the ON CONFLICT upsert is a no-op.
// ctx is the asynq task context, so a hung R2 endpoint is cancelled by task
// timeout/shutdown instead of pinning the worker.
func claimCheckRSS(ctx context.Context, store storage.ObjectStore, feed RSSFeed, raw []byte, db *sqlx.DB, rawFileRepo *repository.RawFileRepository) error {
	sum := sha256.Sum256(raw)
	key := fmt.Sprintf("rss_xml/%s/%x.xml", feed.Name, sum[:16])

	if err := store.PutObject(ctx, key, raw); err != nil {
		return fmt.Errorf("r2 put: %w", err)
	}

	size := int64(len(raw))
	sourceRef := feed.URL
	rf := &entity.RawFile{
		StorageKey:    key,
		Kind:          "rss_xml",
		SourceRef:     &sourceRef,
		SizeBytes:     &size,
		RetentionDays: rssRawRetentionDays,
	}
	if err := rawFileRepo.Insert(db, rf); err != nil {
		return fmt.Errorf("raw_files insert: %w", err)
	}
	return nil
}
