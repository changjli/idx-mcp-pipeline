package usecase

import (
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
)

// TestCompanyProfilesToEntities verifies the conversion: profile fields map
// onto the entity, a nil listing date stays nil, and shares/sektor/industri
// are left unset so the upsert doesn't touch them.
func TestCompanyProfilesToEntities(t *testing.T) {
	date := time.Date(2024, 12, 5, 0, 0, 0, 0, time.UTC)
	profiles := []client.CompanyProfile{
		{Ticker: "AADI", Name: "PT Adaro Andalan Indonesia Tbk", ListingBoard: "Utama", ListingDate: &date, Active: true},
		{Ticker: "NODATE", Name: "No Date Co", ListingBoard: "Pengembangan", ListingDate: nil, Active: false},
	}
	rows := companyProfilesToEntities(profiles)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Code != "AADI" || rows[0].Name != "PT Adaro Andalan Indonesia Tbk" {
		t.Errorf("row[0] = %+v, want AADI profile", rows[0])
	}
	if rows[0].ListingBoard == nil || *rows[0].ListingBoard != "Utama" {
		t.Errorf("row[0].listing_board = %v, want Utama", rows[0].ListingBoard)
	}
	if rows[0].ListingDate == nil || rows[0].ListingDate.Format("2006-01-02") != "2024-12-05" {
		t.Errorf("row[0].listing_date = %v, want 2024-12-05", rows[0].ListingDate)
	}
	if !rows[0].Active {
		t.Errorf("row[0].active = false, want true")
	}
	if rows[1].ListingDate != nil {
		t.Errorf("row[1].listing_date = %v, want nil", rows[1].ListingDate)
	}
	if rows[1].Active {
		t.Errorf("row[1].active = true, want false")
	}
	// Non-profile columns stay unset.
	if rows[0].Shares != nil || rows[0].Sektor != nil || rows[0].Industri != nil {
		t.Errorf("non-profile columns must stay nil, got %+v", rows[0])
	}
}
