package tasks

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/extract"
)

func TestFilterDisclosuresPayload_Marshal(t *testing.T) {
	p := FilterDisclosuresPayload{Date: "2026-08-10", Attempt: 4}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got FilterDisclosuresPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-08-10" {
		t.Errorf("expected date 2026-08-10, got %s", got.Date)
	}
	if got.Attempt != 4 {
		t.Errorf("expected attempt 4, got %d", got.Attempt)
	}
}

func TestTaskKeyFilterDisclosures(t *testing.T) {
	key := TaskKey(TypeFilterDisclosures, "2026-08-10")
	expected := "filter:disclosures:2026-08-10"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}

func TestDisclosureTextKey_DeterministicAndScoped(t *testing.T) {
	ticker := "BBCA"
	d := &entity.Disclosure{Ticker: &ticker, PdfURL: "https://www.idx.co.id/announcement/abc.pdf"}

	k1 := extract.DisclosureTextKey(d)
	k2 := extract.DisclosureTextKey(d)
	if k1 != k2 {
		t.Errorf("expected deterministic key, got %q vs %q", k1, k2)
	}
	if !strings.HasPrefix(k1, "disclosure_text/BBCA/") {
		t.Errorf("expected ticker-scoped key, got %q", k1)
	}
	if !strings.HasSuffix(k1, ".txt") {
		t.Errorf("expected .txt suffix, got %q", k1)
	}
}

func TestDisclosureTextKey_NoTicker(t *testing.T) {
	d := &entity.Disclosure{PdfURL: "https://www.idx.co.id/announcement/abc.pdf"}
	k := extract.DisclosureTextKey(d)
	if !strings.HasPrefix(k, "disclosure_text/unknown/") {
		t.Errorf("expected unknown ticker scope, got %q", k)
	}
}
