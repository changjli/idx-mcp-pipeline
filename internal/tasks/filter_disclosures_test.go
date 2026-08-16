package tasks

import (
	"reflect"
	"strings"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
)

func TestEvaluateDisclosure_GateFails(t *testing.T) {
	passed, cats := evaluateDisclosure("Pemanggilan RUPS Tahunan", false)
	if passed {
		t.Error("expected rejected when anomaly-gate fails")
	}
	if cats != nil {
		t.Errorf("expected no categories, got %v", cats)
	}
}

func TestEvaluateDisclosure_WhitelistSubstringMatch(t *testing.T) {
	// Real IDX titles are longer than the category name — substring match.
	passed, cats := evaluateDisclosure("Pemanggilan RUPS Tahunan PT ABC", true)
	if !passed {
		t.Fatal("expected passed for RUPS summons")
	}
	if !reflect.DeepEqual(cats, []string{"Pemanggilan RUPS"}) {
		t.Errorf("expected [Pemanggilan RUPS], got %v", cats)
	}
}

func TestEvaluateDisclosure_CaseInsensitive(t *testing.T) {
	passed, cats := evaluateDisclosure("pemanggilan rups tahunan", true)
	if !passed {
		t.Fatal("expected case-insensitive match")
	}
	if !reflect.DeepEqual(cats, []string{"Pemanggilan RUPS"}) {
		t.Errorf("expected canonical category, got %v", cats)
	}
}

func TestEvaluateDisclosure_MultipleCategories(t *testing.T) {
	passed, cats := evaluateDisclosure("Informasi dan Fakta Material dan Pembagian Dividen", true)
	if !passed {
		t.Fatal("expected passed")
	}
	want := []string{"Informasi dan Fakta Material", "Dividen"}
	if !reflect.DeepEqual(cats, want) {
		t.Errorf("expected %v, got %v", want, cats)
	}
}

func TestEvaluateDisclosure_ExclusionWins(t *testing.T) {
	// Laporan Keuangan is excluded even when a whitelist keyword also matches.
	passed, cats := evaluateDisclosure("Laporan Keuangan dan Informasi dan Fakta Material", true)
	if passed {
		t.Error("expected rejected for Laporan Keuangan")
	}
	if cats != nil {
		t.Errorf("expected no categories, got %v", cats)
	}
}

func TestEvaluateDisclosure_NoMatch(t *testing.T) {
	passed, cats := evaluateDisclosure("Laporan Bulanan Registrasi Pemegang Efek", true)
	if passed {
		t.Error("expected rejected for non-material title")
	}
	if cats != nil {
		t.Errorf("expected no categories, got %v", cats)
	}
}

func TestDisclosureTextKey_DeterministicAndScoped(t *testing.T) {
	ticker := "BBCA"
	d := &entity.Disclosure{Ticker: &ticker, PdfURL: "https://www.idx.co.id/announcement/abc.pdf"}

	k1 := disclosureTextKey(d)
	k2 := disclosureTextKey(d)
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
	k := disclosureTextKey(d)
	if !strings.HasPrefix(k, "disclosure_text/unknown/") {
		t.Errorf("expected unknown ticker scope, got %q", k)
	}
}
