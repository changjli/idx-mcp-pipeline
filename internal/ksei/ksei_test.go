package ksei

import (
	"archive/zip"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// loadFixture reads the committed sample slice of a real KSEI balance-position
// file (2026-08-31).
func loadFixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/balancepos_sample.txt")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

func TestParseBalancepos(t *testing.T) {
	holdings, err := ParseBalancepos(bytes.NewReader(loadFixture(t)))
	if err != nil {
		t.Fatalf("ParseBalancepos: %v", err)
	}
	// Fixture: 3 EQUITY rows + 1 bond + 1 warrant — only EQUITY survives.
	if len(holdings) != 3 {
		t.Fatalf("expected 3 equity holdings, got %d", len(holdings))
	}

	bbca := holdings[1]
	if bbca.Code != "BBCA" {
		t.Errorf("code = %q, want BBCA", bbca.Code)
	}
	wantDate := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	if !bbca.Date.Equal(wantDate) {
		t.Errorf("date = %v, want %v", bbca.Date, wantDate)
	}
	if bbca.SecNum != 123_275_050_000 {
		t.Errorf("sec_num = %d, want 123275050000", bbca.SecNum)
	}
	if bbca.Price != 6475 {
		t.Errorf("price = %d, want 6475", bbca.Price)
	}
	// Cross-checked against the BBCA LBE PDF (2026-01): the KSEI total
	// 52,453,691,120 equals the LBE's scripless <5% figure.
	if bbca.LocalTotal != 16_135_449_399 {
		t.Errorf("local_total = %d, want 16135449399", bbca.LocalTotal)
	}
	if bbca.ForeignTotal != 36_318_241_721 {
		t.Errorf("foreign_total = %d, want 36318241721", bbca.ForeignTotal)
	}
	if bbca.LocalID != 11_009_825_687 { // ID = Individu per the KSEI guide
		t.Errorf("local_id = %d, want 11009825687", bbca.LocalID)
	}

	// HADE: foreign total 11,793,561 matches the LBE's PEMODAL ASING exactly.
	hade := holdings[2]
	if hade.ForeignTotal != 11_793_561 {
		t.Errorf("HADE foreign_total = %d, want 11793561", hade.ForeignTotal)
	}
	if hade.LocalTotal != 1_391_006_439 {
		t.Errorf("HADE local_total = %d, want 1391006439", hade.LocalTotal)
	}
}

func TestParseBalancepos_RejectsBadShape(t *testing.T) {
	// Wrong column count.
	_, err := ParseBalancepos(strings.NewReader("Date|Code|Type\n31-AUG-2026|BBCA|EQUITY\n"))
	if err == nil {
		t.Error("expected error for wrong column count")
	}
	// Wrong header opening.
	_, err = ParseBalancepos(strings.NewReader("Foo|Bar|Baz|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x|x\n"))
	if err == nil {
		t.Error("expected error for wrong header")
	}
	// Empty file.
	if _, err := ParseBalancepos(strings.NewReader("")); err == nil {
		t.Error("expected error for empty file")
	}
	// Malformed data row (bad date) fails the parse — layout changes must not
	// silently persist partial data.
	fixture := string(loadFixture(t))
	broken := fixture + "\nBAD-DATE|XXX|EQUITY|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0"
	if _, err := ParseBalancepos(strings.NewReader(broken)); err == nil {
		t.Error("expected error for unparseable date")
	}
}

func TestLatestFileDate(t *testing.T) {
	cases := []struct {
		runDate  time.Time
		wantFile string
	}{
		// Mid-month and first-of-month runs both target the previous
		// month-end: KSEI publishes month M's file at the start of M+1.
		{time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC), "2026-08-31"},
		{time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), "2026-09-30"},
		{time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC), "2025-12-31"},
		{time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), "2026-02-28"},
	}
	for _, c := range cases {
		got := LatestFileDate(c.runDate)
		if got.Format("2006-01-02") != c.wantFile {
			t.Errorf("LatestFileDate(%s) = %s, want %s", c.runDate.Format("2006-01-02"), got.Format("2006-01-02"), c.wantFile)
		}
	}
}

func TestClient_FetchBalancepos(t *testing.T) {
	fixture := loadFixture(t)

	// Build the zip in memory with the file name KSEI uses inside the archive.
	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	w, err := zw.Create("Balancepos20260831.txt")
	if err != nil {
		t.Fatalf("create zip entry: %v", err)
	}
	if _, err := w.Write(fixture); err != nil {
		t.Fatalf("write zip entry: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}

	var gotReferer, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReferer = r.Header.Get("Referer")
		gotPath = r.URL.Path
		w.Write(zipBuf.Bytes())
	}))
	defer srv.Close()

	client := NewClient(srv.URL, srv.Client())
	holdings, err := client.FetchBalancepos(context.Background(), time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FetchBalancepos: %v", err)
	}
	if gotPath != "/Download/BalanceposEfek20260831.zip" {
		t.Errorf("path = %q, want /Download/BalanceposEfek20260831.zip", gotPath)
	}
	if gotReferer != balanceposReferer {
		t.Errorf("referer = %q, want %q (KSEI rejects direct downloads without it)", gotReferer, balanceposReferer)
	}
	if len(holdings) != 3 {
		t.Errorf("expected 3 equity holdings, got %d", len(holdings))
	}
}

func TestClient_FetchBalancepos_StatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, srv.Client())
	// A 404 (e.g. file not yet published) is an error, not empty data.
	if _, err := client.FetchBalancepos(context.Background(), time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Error("expected error for 404")
	}
}
