package indexalpha

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestParseBrokerSummary_ConvertsSharesToLotsAndRanks drives the fixture day:
// values arrive as JSON floats, volumes as shares, zero sides are real, and the
// source lists brokers in no particular order.
func TestParseBrokerSummary_ConvertsSharesToLotsAndRanks(t *testing.T) {
	body := mustReadFixture(t, "bbri-2026-10-22.json")

	res, err := ParseBrokerSummary("BBRI", body)
	if err != nil {
		t.Fatalf("ParseBrokerSummary: %v", err)
	}

	if len(res.Buyers) != 3 {
		t.Fatalf("buyers = %d, want 3 (zero-buy and all-zero rows dropped)", len(res.Buyers))
	}
	if len(res.Sellers) != 2 {
		t.Fatalf("sellers = %d, want 2 (zero-sell rows dropped)", len(res.Sellers))
	}

	// Lots are shares ÷ 100, integer division.
	if got := res.Buyers[0]; got.BrokerCode != "AK" || got.Lot != 2_000_000 || got.Value != 600_000_000_000 {
		t.Fatalf("buyers[0] = %+v, want AK/2000000 lots/600B", got)
	}

	// Rank is dense by value within side, not source order.
	wantBuy := []struct {
		code string
		rank int
	}{{"AK", 1}, {"YP", 2}, {"XL", 3}}
	for i, want := range wantBuy {
		if res.Buyers[i].BrokerCode != want.code || res.Buyers[i].Rank != want.rank {
			t.Errorf("buyers[%d] = %s/rank %d, want %s/rank %d",
				i, res.Buyers[i].BrokerCode, res.Buyers[i].Rank, want.code, want.rank)
		}
	}
	wantSell := []struct {
		code string
		rank int
	}{{"CC", 1}, {"SQ", 2}}
	for i, want := range wantSell {
		if res.Sellers[i].BrokerCode != want.code || res.Sellers[i].Rank != want.rank {
			t.Errorf("sellers[%d] = %s/rank %d, want %s/rank %d",
				i, res.Sellers[i].BrokerCode, res.Sellers[i].Rank, want.code, want.rank)
		}
	}
}

// TestParseBrokerSummary_ReconcilesAgainstPublishedValue is the ticket's
// cross-check: for the fixture day, Σbuy == Σsell == IDX's published value.
func TestParseBrokerSummary_ReconcilesAgainstPublishedValue(t *testing.T) {
	body := mustReadFixture(t, "bbri-2026-10-22.json")

	res, err := ParseBrokerSummary("BBRI", body)
	if err != nil {
		t.Fatalf("ParseBrokerSummary: %v", err)
	}

	var buy, sell int64
	for _, r := range res.Buyers {
		buy += r.Value
	}
	for _, r := range res.Sellers {
		sell += r.Value
	}
	const published = int64(1_510_725_997_000) // IDX published value, BBRI 2026-10-22
	if buy != published || sell != published {
		t.Fatalf("Σbuy=%d Σsell=%d, want both %d", buy, sell, published)
	}

	// Totals are derived from the full set, not a footer.
	if res.Totals.TVal != published {
		t.Errorf("Totals.TVal = %d, want %d", res.Totals.TVal, published)
	}
	if res.Totals.TLot != 4_500_000 {
		t.Errorf("Totals.TLot = %d, want 4500000", res.Totals.TLot)
	}
	if res.Totals.Avg != 3357 { // 1,510,725,997,000 ÷ 450,000,000 shares
		t.Errorf("Totals.Avg = %d, want 3357", res.Totals.Avg)
	}
	if res.Totals.FNVal != nil {
		t.Errorf("Totals.FNVal = %v, want nil (unobserved on the broker endpoint)", *res.Totals.FNVal)
	}
}

func TestParseBrokerSummary_EmptyDayIsNotAnError(t *testing.T) {
	res, err := ParseBrokerSummary("BBRI", []byte(`{"success":true,"data":[],"error":null}`))
	if err != nil {
		t.Fatalf("ParseBrokerSummary: %v", err)
	}
	if len(res.Buyers) != 0 || len(res.Sellers) != 0 || res.Totals.TVal != 0 {
		t.Fatalf("empty day = %+v, want zero rows and totals", res)
	}
}

func TestParseBrokerSummary_MalformedPayloads(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"invalid json", `{"success":true,"data":[`},
		{"success false", `{"success":false,"data":null,"error":"Internal server error"}`},
		{"volume not in lots", `{"success":true,"data":[{"code":"AK","buy_volume":150,"buy_value":100.0}]}`},
		{"negative value", `{"success":true,"data":[{"code":"AK","buy_volume":100,"buy_value":-1.0}]}`},
		{"missing broker code", `{"success":true,"data":[{"code":"","buy_volume":100,"buy_value":100.0}]}`},
		{"duplicate broker on a side", `{"success":true,"data":[
			{"code":"AK","buy_volume":100,"buy_value":100.0},
			{"code":"AK","buy_volume":200,"buy_value":200.0}]}`},
		{"buy and sell totals diverge", `{"success":true,"data":[
			{"code":"AK","buy_volume":100,"buy_value":100.0,"sell_volume":100,"sell_value":90.0}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseBrokerSummary("BBRI", []byte(tc.body))
			if !errors.Is(err, ErrMalformedPayload) {
				t.Fatalf("err = %v, want ErrMalformedPayload", err)
			}
		})
	}
}

func mustReadFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return b
}
