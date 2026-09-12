package usecase

import (
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
)

// TestScreenerRowsToEntities verifies the conversion: sector fields map onto
// the entity, index membership splits the comma-separated list into one row
// per index with the effective_date, and a null indexCode yields no membership
// rows while the sector columns are still kept.
func TestScreenerRowsToEntities(t *testing.T) {
	day := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	rows := []client.ScreenerRow{
		{
			StockCode: "AADI", Sector: "Energy", SubSector: "Oil, Gas & Coal",
			Industry: strPtr("Coal"), SubIndustry: strPtr("Coal Production"), SubIndustryCode: strPtr("A121"),
			IndexCode: strPtr("COMPOSITE, IDX30, LQ45"),
		},
		{
			StockCode: "FIMP", Sector: "Energy", SubSector: "Oil, Gas & Coal",
			Industry: strPtr("Coal"), SubIndustry: strPtr("Coal Production"), SubIndustryCode: strPtr("A121"),
			IndexCode: nil, // suspended — no membership
		},
	}

	tickers, indices := screenerRowsToEntities(rows, day)
	if len(tickers) != 2 {
		t.Fatalf("expected 2 ticker rows, got %d", len(tickers))
	}
	if len(indices) != 3 {
		t.Fatalf("expected 3 membership rows, got %d", len(indices))
	}

	// AADI sector fields mapped.
	tr := tickers[0]
	if tr.Code != "AADI" || tr.Sektor == nil || *tr.Sektor != "Energy" {
		t.Errorf("ticker[0] = %+v, want AADI/Energy", tr)
	}
	if tr.SubSektor == nil || *tr.SubSektor != "Oil, Gas & Coal" {
		t.Errorf("ticker[0].sub_sektor = %v, want Oil, Gas & Coal", tr.SubSektor)
	}
	if tr.Industri == nil || *tr.Industri != "Coal" {
		t.Errorf("ticker[0].industri = %v, want Coal", tr.Industri)
	}
	if tr.SubIndustry == nil || *tr.SubIndustry != "Coal Production" {
		t.Errorf("ticker[0].sub_industry = %v, want Coal Production", tr.SubIndustry)
	}
	if tr.SubIndustryCode == nil || *tr.SubIndustryCode != "A121" {
		t.Errorf("ticker[0].sub_industry_code = %v, want A121", tr.SubIndustryCode)
	}

	// FIMP sector kept, no membership.
	if tickers[1].Sektor == nil || *tickers[1].Sektor != "Energy" {
		t.Errorf("FIMP sektor = %v, want Energy (sector kept despite null indexCode)", tickers[1].Sektor)
	}

	// Membership rows: one per index, effective_date stamped.
	want := []struct{ ticker, index string }{
		{"AADI", "COMPOSITE"}, {"AADI", "IDX30"}, {"AADI", "LQ45"},
	}
	for i, w := range want {
		if indices[i].TickerCode != w.ticker || indices[i].IndexCode != w.index {
			t.Errorf("indices[%d] = %+v, want %s/%s", i, indices[i], w.ticker, w.index)
		}
		if !indices[i].EffectiveDate.Equal(day) {
			t.Errorf("indices[%d].effective_date = %v, want %v", i, indices[i].EffectiveDate, day)
		}
	}
}

// TestScreenerRowsToEntities_dirtySectorCleanup verifies the data-quality
// cleanup: KETR "No Sector" → nil sector, VICI "CONSUMER GOODS INDUSTRY" →
// Consumer Non-Cyclicals.
func TestScreenerRowsToEntities_dirtySectorCleanup(t *testing.T) {
	day := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	rows := []client.ScreenerRow{
		{StockCode: "KETR", Sector: "No Sector", SubSector: "No Subsector", IndexCode: strPtr("COMPOSITE")},
		{StockCode: "VICI", Sector: "CONSUMER GOODS INDUSTRY", SubSector: "Cosmetics and Household", IndexCode: strPtr("COMPOSITE")},
		{StockCode: "CLEAN", Sector: "  Financials  ", SubSector: "Banks", IndexCode: strPtr("LQ45")},
	}

	tickers, _ := screenerRowsToEntities(rows, day)
	if len(tickers) != 3 {
		t.Fatalf("expected 3 ticker rows, got %d", len(tickers))
	}
	if tickers[0].Sektor != nil {
		t.Errorf("KETR sektor = %v, want nil (No Sector)", tickers[0].Sektor)
	}
	if tickers[0].SubSektor != nil {
		t.Errorf("KETR sub_sektor = %v, want nil (No Subsector)", tickers[0].SubSektor)
	}
	if tickers[1].Sektor == nil || *tickers[1].Sektor != "Consumer Non-Cyclicals" {
		t.Errorf("VICI sektor = %v, want Consumer Non-Cyclicals", tickers[1].Sektor)
	}
	if tickers[2].Sektor == nil || *tickers[2].Sektor != "Financials" {
		t.Errorf("CLEAN sektor = %v, want trimmed Financials", tickers[2].Sektor)
	}
}

// TestScreenerRowsToEntities_emptyIndexList verifies an empty-string indexCode
// (not just null) also yields no membership rows.
func TestScreenerRowsToEntities_emptyIndexList(t *testing.T) {
	day := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	rows := []client.ScreenerRow{
		{StockCode: "EMPTY", Sector: "Energy", SubSector: "Oil, Gas & Coal", IndexCode: strPtr("")},
		{StockCode: "SPACES", Sector: "Energy", SubSector: "Oil, Gas & Coal", IndexCode: strPtr("  ")},
	}

	_, indices := screenerRowsToEntities(rows, day)
	if len(indices) != 0 {
		t.Errorf("expected 0 membership rows for empty/blank indexCode, got %d", len(indices))
	}
}
