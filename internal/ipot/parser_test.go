package ipot

import (
	"os"
	"testing"
)

func TestParseBrokerSummary_RAJAGoldenFile(t *testing.T) {
	html, err := os.ReadFile("testdata/raja.html")
	if err != nil {
		t.Fatalf("read golden file: %v", err)
	}

	res, err := ParseBrokerSummary(html)
	if err != nil {
		t.Fatalf("ParseBrokerSummary error: %v", err)
	}

	if len(res.Buyers) != 10 {
		t.Errorf("expected 10 buyers, got %d", len(res.Buyers))
	}
	if len(res.Sellers) != 10 {
		t.Errorf("expected 10 sellers, got %d", len(res.Sellers))
	}

	// First buyer row from the real RAJA response.
	b0 := res.Buyers[0]
	if b0.BrokerCode != "AK" {
		t.Errorf("buyer[0].BrokerCode = %q, want AK", b0.BrokerCode)
	}
	if b0.Lot != 169544 {
		t.Errorf("buyer[0].Lot = %d, want 169544", b0.Lot)
	}
	if b0.Value != 15_000_000_000 {
		t.Errorf("buyer[0].Value = %d, want 15000000000", b0.Value)
	}
	if b0.AvgPrice != 883 {
		t.Errorf("buyer[0].AvgPrice = %d, want 883", b0.AvgPrice)
	}
	if b0.Rank != 1 {
		t.Errorf("buyer[0].Rank = %d, want 1", b0.Rank)
	}

	// First seller row.
	s0 := res.Sellers[0]
	if s0.BrokerCode != "XL" {
		t.Errorf("seller[0].BrokerCode = %q, want XL", s0.BrokerCode)
	}
	if s0.Lot != 139188 {
		t.Errorf("seller[0].Lot = %d, want 139188", s0.Lot)
	}
	if s0.Value != 12_300_000_000 {
		t.Errorf("seller[0].Value = %d, want 12300000000", s0.Value)
	}
	if s0.AvgPrice != 881 {
		t.Errorf("seller[0].AvgPrice = %d, want 881", s0.AvgPrice)
	}
	if s0.Rank != 1 {
		t.Errorf("seller[0].Rank = %d, want 1", s0.Rank)
	}

	// Last rows keep rank 10.
	if res.Buyers[9].Rank != 10 || res.Sellers[9].Rank != 10 {
		t.Errorf("expected rank 10 on last rows, got buyer=%d seller=%d",
			res.Buyers[9].Rank, res.Sellers[9].Rank)
	}

	// Footer totals.
	if res.Totals.TVal != 71_200_000_000 {
		t.Errorf("Totals.TVal = %d, want 71200000000", res.Totals.TVal)
	}
	if res.Totals.FNVal != 11_100_000_000 {
		t.Errorf("Totals.FNVal = %d, want 11100000000", res.Totals.FNVal)
	}
	if res.Totals.TLot != 808975 {
		t.Errorf("Totals.TLot = %d, want 808975", res.Totals.TLot)
	}
	if res.Totals.Avg != 880 {
		t.Errorf("Totals.Avg = %d, want 880", res.Totals.Avg)
	}
}

func TestParseBrokerSummary_EmptyTable(t *testing.T) {
	html, err := os.ReadFile("testdata/empty.html")
	if err != nil {
		t.Fatalf("read empty fixture: %v", err)
	}

	res, err := ParseBrokerSummary(html)
	if err != nil {
		t.Fatalf("ParseBrokerSummary on empty table should not error, got: %v", err)
	}
	if len(res.Buyers) != 0 {
		t.Errorf("expected 0 buyers on empty table, got %d", len(res.Buyers))
	}
	if len(res.Sellers) != 0 {
		t.Errorf("expected 0 sellers on empty table, got %d", len(res.Sellers))
	}
	if res.Totals.TVal != 0 || res.Totals.FNVal != 0 || res.Totals.TLot != 0 || res.Totals.Avg != 0 {
		t.Errorf("expected zero totals on empty table, got %+v", res.Totals)
	}
}

func TestParseBrokerSummary_NotATable(t *testing.T) {
	_, err := ParseBrokerSummary([]byte("<html><body>no table here</body></html>"))
	if err == nil {
		t.Fatal("expected error for HTML without a broker summary table")
	}
}

func TestParseBrokerSummary_IgnoresEarlierTables(t *testing.T) {
	// A nav/header table appearing before the summary table must not be parsed.
	html, err := os.ReadFile("testdata/raja.html")
	if err != nil {
		t.Fatalf("read golden file: %v", err)
	}
	navTable := `<table class="nav-table"><tbody><tr><td>Home</td></tr></tbody></table>`
	page := []byte("<html><body>" + navTable + string(html) + "</body></html>")

	res, err := ParseBrokerSummary(page)
	if err != nil {
		t.Fatalf("ParseBrokerSummary error: %v", err)
	}
	if len(res.Buyers) != 10 {
		t.Errorf("expected 10 buyers from summary table, got %d", len(res.Buyers))
	}
	if res.Buyers[0].BrokerCode != "AK" {
		t.Errorf("buyer[0].BrokerCode = %q, want AK", res.Buyers[0].BrokerCode)
	}
}
