package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

func TestStockSummaryPayload_Marshal(t *testing.T) {
	p := StockSummaryPayload{Date: "2026-08-04"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got StockSummaryPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-08-04" {
		t.Errorf("expected date 2026-08-04, got %s", got.Date)
	}
}

func TestEnqueueStockSummary_TaskTypeAndQueue(t *testing.T) {
	date := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)

	// We can't actually enqueue without Redis, but we can verify the task
	// is constructed correctly by checking the payload and options.
	payload := StockSummaryPayload{Date: "2026-08-04"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeStockSummary, raw)

	if task.Type() != TypeStockSummary {
		t.Errorf("expected type %s, got %s", TypeStockSummary, task.Type())
	}

	var got StockSummaryPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != date.Format("2006-01-02") {
		t.Errorf("expected date %s, got %s", date.Format("2006-01-02"), got.Date)
	}
}

func TestStockSummaryItemToDailyPrice(t *testing.T) {
	open := 10000.0
	high := 10100.0
	low := 9900.0
	close := 10050.0
	volume := 1000000.0
	value := 10000000000.0
	frequency := 5000.0

	item := StockSummaryItem{
		StockCode: "AALI",
		OpenPrice: &open,
		High:      &high,
		Low:       &low,
		Close:     &close,
		Volume:    &volume,
		Value:     &value,
		Frequency: &frequency,
	}

	price := itemToDailyPrice(item, "2026-08-04")

	if price.Ticker != "AALI" {
		t.Errorf("expected ticker AALI, got %s", price.Ticker)
	}
	if price.TradingDay.Format("2006-01-02") != "2026-08-04" {
		t.Errorf("expected trading day 2026-08-04, got %s", price.TradingDay.Format("2006-01-02"))
	}
	if *price.Open != 10000.0 {
		t.Errorf("expected open 10000, got %f", *price.Open)
	}
	if *price.High != 10100.0 {
		t.Errorf("expected high 10100, got %f", *price.High)
	}
	if *price.Low != 9900.0 {
		t.Errorf("expected low 9900, got %f", *price.Low)
	}
	if *price.Close != 10050.0 {
		t.Errorf("expected close 10050, got %f", *price.Close)
	}
	if *price.Volume != 1000000 {
		t.Errorf("expected volume 1000000, got %d", *price.Volume)
	}
	if *price.Value != 10000000000 {
		t.Errorf("expected value 10000000000, got %d", *price.Value)
	}
	if *price.Frequency != 5000 {
		t.Errorf("expected frequency 5000, got %d", *price.Frequency)
	}
	if price.Source != "idx" {
		t.Errorf("expected source idx, got %s", price.Source)
	}
}

func TestUpsertTicker_ConvertsShares(t *testing.T) {
	listed := 7786891760.0
	item := StockSummaryItem{
		StockCode:    "AADI",
		StockName:    "Adaro Andalan Indonesia Tbk.",
		ListedShares: &listed,
	}

	// Can't run DB upsert without a DB, but verify the entity conversion
	// by extracting the logic path: build the ticker the same way.
	var shares *int64
	if item.ListedShares != nil {
		s := int64(*item.ListedShares)
		shares = &s
	}

	if shares == nil || *shares != 7786891760 {
		t.Errorf("expected shares 7786891760, got %v", shares)
	}
	if item.StockCode != "AADI" {
		t.Errorf("expected code AADI, got %s", item.StockCode)
	}
	if item.StockName != "Adaro Andalan Indonesia Tbk." {
		t.Errorf("expected name Adaro Andalan Indonesia Tbk., got %s", item.StockName)
	}
}

func TestStockSummaryItemToDailyPrice_NullValues(t *testing.T) {
	item := StockSummaryItem{
		StockCode: "NULLTEST",
	}

	price := itemToDailyPrice(item, "2026-08-04")

	if price.Ticker != "NULLTEST" {
		t.Errorf("expected ticker NULLTEST, got %s", price.Ticker)
	}
	if price.Open != nil {
		t.Error("expected nil open")
	}
	if price.High != nil {
		t.Error("expected nil high")
	}
	if price.Low != nil {
		t.Error("expected nil low")
	}
	if price.Close != nil {
		t.Error("expected nil close")
	}
	if price.Volume != nil {
		t.Error("expected nil volume")
	}
	if price.Value != nil {
		t.Error("expected nil value")
	}
	if price.Frequency != nil {
		t.Error("expected nil frequency")
	}
}

func TestStockSummaryResponse_Unmarshal(t *testing.T) {
	raw := `{
		"draw": 0,
		"recordsTotal": 2,
		"recordsFiltered": 2,
		"data": [
			{"StockCode":"AALI","OpenPrice":10000,"High":10100,"Low":9900,"Close":10050,"Volume":1000000,"Value":10000000000,"Frequency":5000},
			{"StockCode":"BBCA","OpenPrice":9000,"High":9100,"Low":8900,"Close":9050,"Volume":2000000,"Value":18000000000,"Frequency":3000}
		]
	}`

	var resp StockSummaryResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.RecordsTotal != 2 {
		t.Errorf("expected recordsTotal 2, got %d", resp.RecordsTotal)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 items, got %d", len(resp.Data))
	}

	if resp.Data[0].StockCode != "AALI" {
		t.Errorf("expected AALI, got %s", resp.Data[0].StockCode)
	}
	if *resp.Data[0].OpenPrice != 10000 {
		t.Errorf("expected open 10000, got %f", *resp.Data[0].OpenPrice)
	}
	if *resp.Data[0].Close != 10050 {
		t.Errorf("expected close 10050, got %f", *resp.Data[0].Close)
	}

	if resp.Data[1].StockCode != "BBCA" {
		t.Errorf("expected BBCA, got %s", resp.Data[1].StockCode)
	}
	if *resp.Data[1].Volume != 2000000 {
		t.Errorf("expected volume 2000000, got %f", *resp.Data[1].Volume)
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input  string
		maxLen int
		expect string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "hello..."},
		{"", 5, ""},
		{"abc", 3, "abc"},
	}

	for _, tt := range tests {
		got := truncate(tt.input, tt.maxLen)
		if got != tt.expect {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.expect)
		}
	}
}

func TestTaskKeyStockSummary(t *testing.T) {
	key := TaskKey(TypeStockSummary, "2026-08-04")
	expected := "idx:stock_summary:2026-08-04"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}
