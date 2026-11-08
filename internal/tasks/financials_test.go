package tasks

import (
	"encoding/json"
	"testing"

	"github.com/hibiken/asynq"
)

func TestFinancialsPayload_Marshal(t *testing.T) {
	p := FinancialsPayload{Date: "2026-11-08"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	var got FinancialsPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-11-08" {
		t.Errorf("payload = %+v, want 2026-11-08", got)
	}
}

func TestFinancialsTask_TypeAndPayload(t *testing.T) {
	raw, _ := json.Marshal(FinancialsPayload{Date: "2026-11-08"})
	task := asynq.NewTask(TypeFinancials, raw)
	if task.Type() != TypeFinancials {
		t.Errorf("type = %q, want %q", task.Type(), TypeFinancials)
	}
	var got FinancialsPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != "2026-11-08" {
		t.Errorf("task payload = %+v", got)
	}
}

func TestFinancialsTaskKey_DedupFormat(t *testing.T) {
	if got, want := TaskKey(TypeFinancials, "2026-11-08"), "idx:financials:2026-11-08"; got != want {
		t.Errorf("task key = %q, want %q", got, want)
	}
}

// TestFinancialsGraphNode verifies the node is registered and fires in the
// daily wave.
func TestFinancialsGraphNode(t *testing.T) {
	node := Graph.Node(TypeFinancials)
	if node == nil {
		t.Fatal("idx:financials is not registered in the graph")
	}
	if node.Name != "financials" || !node.SelfHeal {
		t.Errorf("node = %+v, want name financials and self-heal", node)
	}
	found := false
	for _, typ := range Graph.Node(TypePipelineDaily).Wave {
		if typ == TypeFinancials {
			found = true
		}
	}
	if !found {
		t.Error("idx:financials is not in the pipeline:daily wave")
	}
}
