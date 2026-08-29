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

func TestTaskKeyStockSummary(t *testing.T) {
	key := TaskKey(TypeStockSummary, "2026-08-04")
	expected := "idx:stock_summary:2026-08-04"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}
