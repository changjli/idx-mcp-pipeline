package scheduler

import (
	"testing"
)

func TestStockSummaryDateFromID(t *testing.T) {
	tests := []struct {
		id      string
		want    string
		wantErr bool
	}{
		{"idx:stock_summary:2026-08-08", "2026-08-08", false},
		{"idx:stock_summary:2026-01-02", "2026-01-02", false},
		{"idx:stock_summary:2026-08-08:extra", "", true}, // wrong shape
		{"idx:stock_summary:not-a-date", "", true},
		{"noop:2026-08-08", "", true}, // wrong type prefix
		{"", "", true},
	}

	for _, tt := range tests {
		got, err := stockSummaryDateFromID(tt.id)
		if tt.wantErr {
			if err == nil {
				t.Errorf("stockSummaryDateFromID(%q) expected error, got date %v", tt.id, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("stockSummaryDateFromID(%q) unexpected error: %v", tt.id, err)
			continue
		}
		if got.Format("2006-01-02") != tt.want {
			t.Errorf("stockSummaryDateFromID(%q) = %s, want %s", tt.id, got.Format("2006-01-02"), tt.want)
		}
	}
}

func TestAnnouncementsDateFromID(t *testing.T) {
	tests := []struct {
		id      string
		want    string
		wantErr bool
	}{
		{"idx:announcements:2026-08-09", "2026-08-09", false},
		{"idx:announcements:2026-01-02", "2026-01-02", false},
		{"idx:announcements:2026-08-09:extra", "", true}, // wrong shape
		{"idx:announcements:not-a-date", "", true},
		{"idx:stock_summary:2026-08-09", "", true}, // wrong type prefix
		{"", "", true},
	}

	for _, tt := range tests {
		got, err := announcementsDateFromID(tt.id)
		if tt.wantErr {
			if err == nil {
				t.Errorf("announcementsDateFromID(%q) expected error, got date %v", tt.id, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("announcementsDateFromID(%q) unexpected error: %v", tt.id, err)
			continue
		}
		if got.Format("2006-01-02") != tt.want {
			t.Errorf("announcementsDateFromID(%q) = %s, want %s", tt.id, got.Format("2006-01-02"), tt.want)
		}
	}
}
