package pipeline

import (
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

// ─── fakes ───────────────────────────────────────────────────────────────────

type fakeNewsStore struct {
	upserted []entity.NewsItem
	failAt   int // -1 = never; 1-based article index whose Upsert fails
	nextID   int64
}

var errFake = errors.New("fake store failure")

func (f *fakeNewsStore) Upsert(news *entity.NewsItem) (int64, error) {
	if f.failAt == len(f.upserted)+1 {
		return 0, errFake
	}
	f.nextID++
	f.upserted = append(f.upserted, *news)
	return f.nextID, nil
}

type fakeTickerSeeder struct{ seeded []string }

func (f *fakeTickerSeeder) InsertIfAbsent(code, name string) error {
	f.seeded = append(f.seeded, code+"|"+name)
	return nil
}

type fakeNewsTickerStore struct{ codes []string }

func (f *fakeNewsTickerStore) Insert(link *entity.NewsTicker) error {
	f.codes = append(f.codes, link.Ticker)
	return nil
}

type fakeDisclosureSink struct {
	upserted []*entity.Disclosure
	failAt   int // -1 = never; 1-based row index
}

func (f *fakeDisclosureSink) Upsert(d *entity.Disclosure) error {
	if f.failAt == len(f.upserted)+1 {
		return errFake
	}
	f.upserted = append(f.upserted, d)
	return nil
}

type fakeTickerRegistrar struct{ codes []string }

func (f *fakeTickerRegistrar) Upsert(ticker *entity.Ticker) error {
	f.codes = append(f.codes, ticker.Code)
	return nil
}

type fakeDailyPriceStore struct{ tickers []string }

func (f *fakeDailyPriceStore) Upsert(price *entity.DailyPrice) error {
	f.tickers = append(f.tickers, price.Ticker)
	return nil
}

type fixedMatcher struct{ matches map[string][]MatchedTicker }

func (f fixedMatcher) Match(title, snippet string) []MatchedTicker { return f.matches[title] }

func newNoopLog() *logrus.Logger {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	return log
}

// ─── NewsIngest ──────────────────────────────────────────────────────────────

func TestNewsIngest_StoreLinksAndSeeds(t *testing.T) {
	news := &fakeNewsStore{}
	seeded := &fakeTickerSeeder{}
	links := &fakeNewsTickerStore{}
	matcher := fixedMatcher{matches: map[string][]MatchedTicker{
		"BBCA Laba Naik": {{Code: "BBCA", Name: "Bank Central Asia", Method: "code"}},
	}}
	n := NewNewsIngest(news, seeded, links, newNoopLog())

	published := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	stored, err := n.Store(matcher, "cnbc", []NewsArticle{
		{Title: "BBCA Laba Naik", URL: "https://x/1", PublishedAt: published, Snippet: "laba"},
		{Title: "No Ticker Here", URL: "https://x/2"},
	})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if stored != 1 {
		t.Errorf("expected 1 stored (unmatched discarded), got %d", stored)
	}
	if len(news.upserted) != 1 || news.upserted[0].Source != "cnbc" {
		t.Errorf("expected 1 news item via cnbc, got %+v", news.upserted)
	}
	if news.upserted[0].Snippet == nil || *news.upserted[0].Snippet != "laba" {
		t.Error("expected snippet carried through")
	}
	if len(seeded.seeded) != 1 || seeded.seeded[0] != "BBCA|Bank Central Asia" {
		t.Errorf("expected ticker seeded, got %v", seeded.seeded)
	}
	if len(links.codes) != 1 || links.codes[0] != "BBCA" {
		t.Errorf("expected news_ticker link, got %v", links.codes)
	}
}

func TestNewsIngest_StoreFailFast(t *testing.T) {
	news := &fakeNewsStore{failAt: 2}
	matcher := fixedMatcher{matches: map[string][]MatchedTicker{
		"a": {{Code: "AAA", Name: "A A", Method: "code"}},
	}}
	n := NewNewsIngest(news, &fakeTickerSeeder{}, &fakeNewsTickerStore{}, newNoopLog())

	_, err := n.Store(matcher, "cnbc", []NewsArticle{
		{Title: "a", URL: "https://x/1"},
		{Title: "a", URL: "https://x/2"},
	})
	if err == nil {
		t.Fatal("expected first write failure to abort the batch (declared fail-fast policy)")
	}
}

// ─── DisclosureIngest ────────────────────────────────────────────────────────

func TestDisclosureIngest_UpsertRowsSeedsTickers(t *testing.T) {
	sink := &fakeDisclosureSink{}
	registrar := &fakeTickerRegistrar{}
	n := NewDisclosureIngest(sink, registrar, newNoopLog())

	tk := "BBCA"
	upserted, err := n.UpsertRows([]*entity.Disclosure{
		{Ticker: &tk, Title: "Pemanggilan RUPS Tahunan", PdfURL: "https://x/a.pdf"},
	})
	if err != nil {
		t.Fatalf("UpsertRows: %v", err)
	}
	if upserted != 1 || len(sink.upserted) != 1 {
		t.Fatalf("expected 1 row upserted, got %d", upserted)
	}
	if len(registrar.codes) != 1 || registrar.codes[0] != "BBCA" {
		t.Errorf("expected ticker ensured, got %v", registrar.codes)
	}
}

func TestDisclosureIngest_FailFastPreservesCount(t *testing.T) {
	sink := &fakeDisclosureSink{failAt: 2}
	n := NewDisclosureIngest(sink, &fakeTickerRegistrar{}, newNoopLog())

	tk := "BBCA"
	upserted, err := n.UpsertRows([]*entity.Disclosure{
		{Ticker: &tk, PdfURL: "u1", Title: "A"},
		{Ticker: &tk, PdfURL: "u2", Title: "B"},
	})
	if err == nil {
		t.Fatal("expected failure to propagate")
	}
	if upserted != 1 {
		t.Errorf("expected 1 upserted before the failure, got %d", upserted)
	}
}

// ─── StockSummaryIngest ──────────────────────────────────────────────────────

func summaryRow(code string) StockSummaryItem {
	open, high, low, close := 100.0, 110.0, 99.0, 105.0
	vol, val, freq := 1000.0, 100000.0, 50.0
	shares := 100.0
	return StockSummaryItem{
		StockCode: code, StockName: "N " + code,
		OpenPrice: &open, High: &high, Low: &low, Close: &close,
		Volume: &vol, Value: &val, Frequency: &freq, ListedShares: &shares,
	}
}

func TestStockSummaryIngest_LogAndSkip(t *testing.T) {
	prices := &fakeDailyPriceStore{}
	registrar := &fakeTickerRegistrar{}
	n := NewStockSummaryIngest(prices, registrar, newNoopLog())

	got := n.UpsertRows([]StockSummaryItem{
		summaryRow("AALI"),
		summaryRow("BBCA"),
	}, "2026-08-29")
	if got != 2 || len(prices.tickers) != 2 || len(registrar.codes) != 2 {
		t.Errorf("expected both rows upserted, got %d", got)
	}
}

func TestStockSummaryIngest_FailedRowSkipped(t *testing.T) {
	// A row whose daily_price upsert fails is logged and skipped; the loop
	// continues and the healthy row still lands (declared policy).
	prices := &rejectingPriceStore{reject: "AALI"}
	registrar := &fakeTickerRegistrar{}
	n := NewStockSummaryIngest(prices, registrar, newNoopLog())

	got := n.UpsertRows([]StockSummaryItem{summaryRow("AALI"), summaryRow("BBCA")}, "2026-08-29")
	if got != 1 {
		t.Errorf("expected 1 upserted (failed row skipped), got %d", got)
	}
	if len(registrar.codes) != 2 {
		t.Errorf("expected ticker registration attempted for both rows, got %v", registrar.codes)
	}
}

type rejectingPriceStore struct{ reject string }

func (s *rejectingPriceStore) Upsert(price *entity.DailyPrice) error {
	if price.Ticker == s.reject {
		return errFake
	}
	return nil
}
