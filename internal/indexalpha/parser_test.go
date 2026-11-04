package indexalpha

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
)

// TestParseBrokerSummary_ConvertsSharesToLotsAndRanks drives the fixture day:
// values arrive as JSON floats, volumes as shares, zero sides are real, and the
// source lists brokers in no particular order.
func TestParseBrokerSummary_ConvertsSharesToLotsAndRanks(t *testing.T) {
	body := mustReadFixture(t, "bbri-2026-10-22.json")

	res, err := ParseBrokerSummary("BBRI", broker.InvestorAll, true, body)
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

	res, err := ParseBrokerSummary("BBRI", broker.InvestorAll, true, body)
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
	res, err := ParseBrokerSummary("BBRI", broker.InvestorAll, true, []byte(`{"success":true,"data":[],"error":null}`))
	if err != nil {
		t.Fatalf("ParseBrokerSummary: %v", err)
	}
	if len(res.Buyers) != 0 || len(res.Sellers) != 0 || res.Totals.TVal != 0 {
		t.Fatalf("empty day = %+v, want zero rows and totals", res)
	}
}

// TestParseBrokerSummary_ForeignLaneCarriesNetAndSkipsReconciliation is the
// ticket-10a parser change: an f payload has a net (Σbuy ≠ Σsell) and must parse,
// with f_nval derived from the rows and the all-investor day totals left
// unobserved.
func TestParseBrokerSummary_ForeignLaneCarriesNetAndSkipsReconciliation(t *testing.T) {
	body := mustReadFixture(t, "bbri-2026-10-22-f.json")

	res, err := ParseBrokerSummary("BBRI", broker.InvestorForeign, true, body)
	if err != nil {
		t.Fatalf("ParseBrokerSummary(f): %v", err)
	}
	if res.Investor != broker.InvestorForeign {
		t.Errorf("Investor = %q, want %q", res.Investor, broker.InvestorForeign)
	}
	if len(res.Buyers) != 4 || len(res.Sellers) != 3 {
		t.Fatalf("rows = %d buys/%d sells, want 4/3 (PD buy-only, MI zero dropped)",
			len(res.Buyers), len(res.Sellers))
	}
	const wantNet = int64(-90_681_503_000)
	if res.Totals.FNVal == nil || *res.Totals.FNVal != wantNet {
		t.Fatalf("Totals.FNVal = %v, want %d", res.Totals.FNVal, wantNet)
	}
	if res.Totals.TVal != 0 || res.Totals.TLot != 0 || res.Totals.Avg != 0 {
		t.Errorf("f day totals = %d/%d/%d, want zero (unobserved; the writer stores NULL)",
			res.Totals.TVal, res.Totals.TLot, res.Totals.Avg)
	}
}

// TestParseBrokerSummary_AllReconciliationIsDayGrain pins the probe-10-0
// finding: a multi-day all window may differ slightly between the two sides
// and must still parse; only the single-day all lane refuses a mismatch.
func TestParseBrokerSummary_AllReconciliationIsDayGrain(t *testing.T) {
	body := []byte(`{"success":true,"data":[
		{"code":"AK","buy_freq":1,"buy_volume":100,"buy_value":1000.0,
		 "sell_freq":1,"sell_volume":100,"sell_value":999.0,
		 "buy_avg":10.0,"sell_avg":9.99}]}`)

	if _, err := ParseBrokerSummary("X", broker.InvestorAll, true, body); !errors.Is(err, ErrMalformedPayload) {
		t.Fatalf("single-day all mismatch err = %v, want ErrMalformedPayload", err)
	}
	res, err := ParseBrokerSummary("X", broker.InvestorAll, false, body)
	if err != nil {
		t.Fatalf("multi-day all window rejected: %v", err)
	}
	if res.Totals.TVal != 999 {
		t.Errorf("Totals.TVal = %d, want 999 (the vendor's window total)", res.Totals.TVal)
	}
}

func TestParseBrokerSummary_ForeignLaneValidatesAverages(t *testing.T) {
	body := []byte(`{"success":true,"data":[
		{"code":"AK","buy_volume":100,"buy_value":1000.0,"buy_avg":9.0}]}`)
	if _, err := ParseBrokerSummary("X", broker.InvestorForeign, true, body); !errors.Is(err, ErrMalformedPayload) {
		t.Fatalf("err = %v, want ErrMalformedPayload (avg 9 ≠ value/volume 10)", err)
	}
}

func TestParseBrokerSummary_RejectsUnsupportedInvestor(t *testing.T) {
	if _, err := ParseBrokerSummary("X", "d", true, []byte(`{"success":true,"data":[]}`)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("err = %v, want ErrInvalidRequest", err)
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
			_, err := ParseBrokerSummary("BBRI", broker.InvestorAll, true, []byte(tc.body))
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

// TestParseBrokerSummaryBatch_PerTickerIsolation drives the batch response
// shape: data is an object keyed by ticker, a null value is an empty day, and a
// ticker that violates a contract invariant lands in Invalid without discarding
// the rest of the batch.
func TestParseBrokerSummaryBatch_PerTickerIsolation(t *testing.T) {
	body := []byte(`{"success":true,"data":{
		"BBCA":[{"code":"AF","buy_freq":1,"buy_volume":100,"buy_value":1000000.0,
		         "sell_freq":1,"sell_volume":100,"sell_value":1000000.0,
		         "buy_avg":100.0,"sell_avg":100.0}],
		"TLKM":[{"code":"AK","buy_freq":1,"buy_volume":200,"buy_value":2000000.0,
		         "sell_freq":2,"sell_volume":200,"sell_value":2000000.0,
		         "buy_avg":100.0,"sell_avg":100.0}],
		"BAD":[{"code":"ZZ","buy_freq":1,"buy_volume":100,"buy_value":1000000.0,
		        "sell_freq":1,"sell_volume":100,"sell_value":900000.0,
		        "buy_avg":100.0,"sell_avg":90.0}],
		"NULLY":null
	},"error":null}`)

	batch, err := ParseBrokerSummaryBatch(broker.InvestorAll, true, body)
	if err != nil {
		t.Fatalf("ParseBrokerSummaryBatch: %v", err)
	}
	if len(batch.Results) != 3 {
		t.Fatalf("results = %d, want 3 (BBCA, TLKM, NULLY)", len(batch.Results))
	}
	if len(batch.Invalid) != 1 {
		t.Fatalf("invalid = %d, want 1", len(batch.Invalid))
	}
	if !errors.Is(batch.Invalid["BAD"], ErrMalformedPayload) {
		t.Errorf("BAD err = %v, want ErrMalformedPayload", batch.Invalid["BAD"])
	}
	if got := batch.Results["BBCA"].Buyers[0].Lot; got != 1 {
		t.Errorf("BBCA lot = %d, want 1 (100 shares ÷ 100)", got)
	}
	if got := batch.Results["TLKM"].Buyers[0].Lot; got != 2 {
		t.Errorf("TLKM lot = %d, want 2 (200 shares ÷ 100)", got)
	}
	if r := batch.Results["NULLY"]; r == nil || len(r.Buyers) != 0 || len(r.Sellers) != 0 {
		t.Errorf("NULLY = %+v, want an empty result", r)
	}
	if _, present := batch.Results["ABSENT"]; present {
		t.Error("ABSENT should not appear in a batch it was omitted from")
	}
}

func TestParseBrokerSummaryBatch_BatchLevelErrors(t *testing.T) {
	cases := []struct{ name, body string }{
		{"invalid json", `{"success":true,"data":`},
		{"success false", `{"success":false,"data":null,"error":"Internal server error"}`},
		{"data is an array", `{"success":true,"data":[]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseBrokerSummaryBatch(broker.InvestorAll, true, []byte(tc.body)); !errors.Is(err, ErrMalformedPayload) {
				t.Fatalf("err = %v, want ErrMalformedPayload", err)
			}
		})
	}
}
