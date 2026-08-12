package tasks

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

func TestParseRSS_ValidFeed(t *testing.T) {
	xml := `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Market</title>
    <item>
      <title>Bank Central Asia Raup Laba &amp; Naik</title>
      <link>https://example.com/bbc-1</link>
      <pubDate>Mon, 10 Aug 2026 09:00:00 +0700</pubDate>
      <description>&lt;p&gt;BBCA mencetak laba bersih.&lt;/p&gt;</description>
    </item>
    <item>
      <title>Telkom Jual Obligasi</title>
      <link>https://example.com/tlkm-1</link>
      <pubDate>Tue, 11 Aug 2026 10:30:00 GMT</pubDate>
      <description>TLKM menerbitkan surat utang.</description>
    </item>
  </channel>
</rss>`

	articles, err := parseRSS([]byte(xml))
	if err != nil {
		t.Fatalf("parseRSS: %v", err)
	}
	if len(articles) != 2 {
		t.Fatalf("expected 2 articles, got %d", len(articles))
	}

	first := articles[0]
	if first.Title != "Bank Central Asia Raup Laba & Naik" {
		t.Errorf("unexpected title %q", first.Title)
	}
	if first.URL != "https://example.com/bbc-1" {
		t.Errorf("unexpected url %q", first.URL)
	}
	if first.Snippet != "BBCA mencetak laba bersih." {
		t.Errorf("unexpected snippet %q", first.Snippet)
	}
	if first.PublishedAt.Format("2006-01-02 15:04") != "2026-08-10 09:00" {
		t.Errorf("unexpected published_at %s", first.PublishedAt)
	}

	// RFC1123 (GMT) layout.
	if articles[1].PublishedAt.Format("2006-01-02 15:04") != "2026-08-11 10:30" {
		t.Errorf("unexpected published_at %s", articles[1].PublishedAt.Format("2006-01-02 15:04"))
	}
}

func TestParseRSS_SkipsIncompleteItems(t *testing.T) {
	xml := `<?xml version="1.0"?>
<rss version="2.0"><channel>
  <item><title>No Link</title><pubDate>Mon, 10 Aug 2026 09:00:00 +0700</pubDate></item>
  <item><link>https://example.com/no-date</link><title>No Date</title></item>
  <item><title></title><link>https://example.com/empty-title</link><pubDate>Mon, 10 Aug 2026 09:00:00 +0700</pubDate></item>
  <item><title>Valid</title><link>https://example.com/valid</link><pubDate>Mon, 10 Aug 2026 09:00:00 +0700</pubDate></item>
</channel></rss>`

	articles, err := parseRSS([]byte(xml))
	if err != nil {
		t.Fatalf("parseRSS: %v", err)
	}
	if len(articles) != 1 || articles[0].URL != "https://example.com/valid" {
		t.Fatalf("expected only the valid item, got %+v", articles)
	}
}

func TestParseRSS_InvalidXML(t *testing.T) {
	if _, err := parseRSS([]byte("<rss><channel>")); err == nil {
		t.Fatal("expected error for truncated xml")
	}
}

func TestSignificantTokens(t *testing.T) {
	cases := []struct {
		name string
		want []string
	}{
		{"Telkom Indonesia (Persero) Tbk.", []string{"telkom"}},
		{"Bank Central Asia Tbk.", []string{"central", "asia"}},
		{"Media Nusantara Citra Tbk.", nil},
		{"PP (Persero) Tbk.", nil},
		{"H.M. Sampoerna Tbk.", []string{"sampoerna"}},
		{"GoTo Gojek Tokopedia Tbk.", []string{"goto", "gojek", "tokopedia"}},
	}
	for _, c := range cases {
		got := significantTokens(c.name)
		if !equalStrings(got, c.want) {
			t.Errorf("significantTokens(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// testTickerUniverse is a fixed matching universe covering every matcher rule:
// code match, code stopword (BUMI), single-token names (TLKM, BMRI),
// multi-token names (BBCA), and ambiguous single tokens (astra, indofood).
func testTickerUniverse() []entity.Ticker {
	return []entity.Ticker{
		{Code: "AALI", Name: "Astra Agro Lestari Tbk."},
		{Code: "ASII", Name: "Astra International Tbk."},
		{Code: "BBCA", Name: "Bank Central Asia Tbk."},
		{Code: "BMRI", Name: "Bank Mandiri (Persero) Tbk."},
		{Code: "BUMI", Name: "Bumi Resources Tbk."},
		{Code: "ICBP", Name: "Indofood CBP Sukses Makmur Tbk."},
		{Code: "INDF", Name: "Indofood Sukses Makmur Tbk."},
		{Code: "KOTA", Name: "DMS Propertindo Tbk."},
		{Code: "TLKM", Name: "Telkom Indonesia (Persero) Tbk."},
		{Code: "WIKA", Name: "Wijaya Karya (Persero) Tbk."},
		{Code: "WTON", Name: "Wijaya Karya Beton Tbk."},
	}
}

// buildMatcherForTest builds a matcher over the fixed test universe.
func buildMatcherForTest(t *testing.T) *tickerMatcher {
	t.Helper()
	return buildTickerMatcher(testTickerUniverse())
}

func matchCodes(m *tickerMatcher, title, snippet string) []tickerMatch {
	return m.match(title, snippet)
}

func TestMatcher_CodeMatch(t *testing.T) {
	m := buildMatcherForTest(t)

	matches := matchCodes(m, "Saham BBCA Naik 5%", "")
	if len(matches) != 1 || matches[0].code != "BBCA" || matches[0].method != "code" {
		t.Fatalf("expected one code match BBCA, got %+v", matches)
	}

	// Case-insensitive for non-stopword codes.
	matches = matchCodes(m, "saham bbca", "")
	if len(matches) != 1 || matches[0].code != "BBCA" {
		t.Fatalf("expected case-insensitive match, got %+v", matches)
	}

	// Code inside a longer word must not match (word boundary).
	if got := matchCodes(m, "notbbca stock", ""); len(got) != 0 {
		t.Fatalf("expected no match for embedded code, got %+v", got)
	}
}

func TestMatcher_CodeStopword(t *testing.T) {
	m := buildMatcherForTest(t)

	// "bumi" lowercase is the Indonesian word for earth — must not tag BUMI.
	if got := matchCodes(m, "Energi dari dalam bumi", ""); len(got) != 0 {
		t.Fatalf("expected no match for lowercase stopword, got %+v", got)
	}
	// Uppercase "BUMI" is the ticker mention.
	if got := matchCodes(m, "Saham BUMI menguat", ""); len(got) != 1 || got[0].code != "BUMI" {
		t.Fatalf("expected uppercase BUMI match, got %+v", got)
	}
}

func TestMatcher_NameMatch_MultiToken(t *testing.T) {
	m := buildMatcherForTest(t)

	// Full legal name (minus Tbk.) matches all significant tokens.
	matches := matchCodes(m, "Bank Central Asia Bukukan Laba Bersih", "")
	if len(matches) != 1 || matches[0].code != "BBCA" || matches[0].method != "name" {
		t.Fatalf("expected BBCA name match, got %+v", matches)
	}

	// A generic overlap ("Bank" + "Asia") is not enough.
	if got := matchCodes(m, "Bank Asia Tenggara", ""); len(got) != 0 {
		t.Fatalf("expected no match for partial name, got %+v", got)
	}
}

func TestMatcher_NameMatch_SingleToken(t *testing.T) {
	m := buildMatcherForTest(t)

	// Short forms: "Telkom" for TLKM, "Mandiri" for BMRI.
	matches := matchCodes(m, "Telkom Siapkan Belanja Modal", "")
	if len(matches) != 1 || matches[0].code != "TLKM" || matches[0].method != "name" {
		t.Fatalf("expected TLKM name match via Telkom, got %+v", matches)
	}

	matches = matchCodes(m, "Mandiri Garap Kredit Hijau", "")
	if len(matches) != 1 || matches[0].code != "BMRI" {
		t.Fatalf("expected BMRI name match via Mandiri, got %+v", matches)
	}
}

func TestMatcher_AmbiguousSingleTokenBlocked(t *testing.T) {
	m := buildMatcherForTest(t)

	// "Astra" is shared by Astra International (ASII) and Astra Agro Lestari
	// (AALI) — never used alone, so it can't tag the wrong company.
	if got := matchCodes(m, "Astra Meluncurkan Produk Baru", ""); len(got) != 0 {
		t.Fatalf("expected no match for ambiguous single token, got %+v", got)
	}

	// "Indofood" alone is shared by INDF and ICBP.
	if got := matchCodes(m, "Indofood Ekspansi Pabrik", ""); len(got) != 0 {
		t.Fatalf("expected no match for ambiguous Indofood, got %+v", got)
	}

	// Full AALI name still matches its own ticker.
	matches := matchCodes(m, "Astra Agro Lestari Raih Rekor", "")
	if len(matches) != 1 || matches[0].code != "AALI" {
		t.Fatalf("expected AALI match, got %+v", matches)
	}
}

func TestMatcher_MultipleTickers(t *testing.T) {
	m := buildMatcherForTest(t)

	matches := matchCodes(m, "BBCA dan Telkom Bagi Dividen", "")
	codes := map[string]bool{}
	for _, mt := range matches {
		codes[mt.code] = true
	}
	if !codes["BBCA"] || !codes["TLKM"] || len(matches) != 2 {
		t.Fatalf("expected BBCA + TLKM, got %+v", matches)
	}
	// Name and code matches coexist on different tickers.
	var sawName, sawCode bool
	for _, mt := range matches {
		if mt.method == "name" {
			sawName = true
		}
		if mt.method == "code" {
			sawCode = true
		}
	}
	if !sawName || !sawCode {
		t.Errorf("expected mixed methods, got %+v", matches)
	}
}

func TestMatcher_CodeWinsOverName(t *testing.T) {
	m := buildMatcherForTest(t)

	// Article mentions the code AND the company name — one row, method=code.
	matches := matchCodes(m, "BBCA Bank Central Asia Cetak Laba", "")
	if len(matches) != 1 || matches[0].code != "BBCA" || matches[0].method != "code" {
		t.Fatalf("expected single code match, got %+v", matches)
	}
}

func TestMatcher_Unmatched(t *testing.T) {
	m := buildMatcherForTest(t)

	if got := matchCodes(m, "Inflasi Indonesia Naik di Agustus", ""); len(got) != 0 {
		t.Fatalf("expected no match for generic article, got %+v", got)
	}
	if got := matchCodes(m, "Tips Investasi untuk Pemula", ""); len(got) != 0 {
		t.Fatalf("expected no match for investing tips, got %+v", got)
	}
}

// TestMatcher_RealWorldKOTA mirrors the real-world scenario from the ticket
// discussion: a headline naming a small-cap by its code in parentheses must
// match once the code is in the universe — the exact gap the stale 49-ticker
// embed map had (KOTA was absent).
func TestMatcher_RealWorldKOTA(t *testing.T) {
	m := buildMatcherForTest(t)

	matches := matchCodes(m, "Kenapa Saham KOTA Naik 17% dalam Sepekan?", "PT DMS Propertindo Tbk (KOTA) didorong oleh sentimen positif.")
	if len(matches) != 1 || matches[0].code != "KOTA" {
		t.Fatalf("expected KOTA code match, got %+v", matches)
	}
	// Full name match also works, but a lone generic-ish token ("propertindo")
	// without the code or the paired "dms" token must not tag KOTA.
	matches = matchCodes(m, "PT DMS Propertindo Tbk Catat Kenaikan", "")
	if len(matches) != 1 || matches[0].code != "KOTA" || matches[0].method != "name" {
		t.Fatalf("expected KOTA name match for full name, got %+v", matches)
	}
	matches = matchCodes(m, "Sektor propertindo menguat", "")
	if len(matches) != 0 {
		t.Fatalf("expected no partial-token match, got %+v", matches)
	}
}

func TestRSSPayload_Marshal(t *testing.T) {
	p := RSSPayload{Date: "2026-08-10"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	var got RSSPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-08-10" {
		t.Errorf("expected date 2026-08-10, got %s", got.Date)
	}
}

func TestEnqueueRSS_TaskTypeAndQueue(t *testing.T) {
	date := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)

	payload := RSSPayload{Date: "2026-08-10"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeRSS, raw)

	if task.Type() != TypeRSS {
		t.Errorf("expected type %s, got %s", TypeRSS, task.Type())
	}

	var got RSSPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != date.Format("2006-01-02") {
		t.Errorf("expected date %s, got %s", date.Format("2006-01-02"), got.Date)
	}
}

func TestTaskKeyRSS(t *testing.T) {
	key := TaskKey(TypeRSS, "2026-08-10")
	if key != "rss:ingest:2026-08-10" {
		t.Errorf("expected rss:ingest:2026-08-10, got %s", key)
	}
}

func TestParseRSSTime(t *testing.T) {
	for _, s := range []string{
		"Mon, 10 Aug 2026 09:00:00 +0700",
		"Tue, 11 Aug 2026 10:30:00 GMT",
		"2026-08-10T09:00:00+07:00",
		"2026-08-10 09:00:00",
	} {
		if _, err := parseRSSTime(s); err != nil {
			t.Errorf("parseRSSTime(%q): %v", s, err)
		}
	}
	if _, err := parseRSSTime("not a date"); err == nil {
		t.Error("expected error for garbage date")
	}
	if _, err := parseRSSTime(""); err == nil {
		t.Error("expected error for empty date")
	}
}

func TestParseRSSTime_NaiveWallTimeIsWIB(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		t.Fatalf("load Asia/Jakarta: %v", err)
	}

	// Zone-less wall time is Indonesian local time — relabeled Asia/Jakarta,
	// not silently stored as UTC (7 hours off).
	got, err := parseRSSTime("2026-08-10 09:00:00")
	if err != nil {
		t.Fatalf("parseRSSTime: %v", err)
	}
	want := time.Date(2026, 8, 10, 9, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Errorf("naive time = %s (%s), want %s in Asia/Jakarta", got, got.Location(), want)
	}
	if got.Format("2006-01-02 15:04") != "2026-08-10 09:00" {
		t.Errorf("wall clock shifted: got %s", got.Format("2006-01-02 15:04"))
	}

	// Zone-qualified times are left untouched.
	z, err := parseRSSTime("Mon, 10 Aug 2026 09:00:00 +0700")
	if err != nil {
		t.Fatalf("parseRSSTime zone'd: %v", err)
	}
	if _, offset := z.Zone(); offset != 7*3600 {
		t.Errorf("expected +0700 offset preserved, got %d", offset)
	}
}

func TestNormalizeText(t *testing.T) {
	got := normalizeText(`<p>Bank Central &amp; Asia</p>`)
	if !strings.Contains(got, "bank central & asia") {
		t.Errorf("unexpected normalized text %q", got)
	}
}
