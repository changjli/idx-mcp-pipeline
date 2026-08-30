package tasks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

// graphEnqueuer records enqueued task types and injects per-type errors so
// Chain's conflict-swallow policy is assertable without Redis.
type graphEnqueuer struct {
	enqueued  []string
	errByType map[string]error
}

func (f *graphEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	f.enqueued = append(f.enqueued, task.Type())
	if err := f.errByType[task.Type()]; err != nil {
		return nil, err
	}
	return &asynq.TaskInfo{ID: "fake", Type: task.Type(), Queue: "ingest"}, nil
}

func TestGraphAllChainAndWaveNamesRegistered(t *testing.T) {
	for _, node := range Graph.nodes {
		for _, name := range node.Chain {
			if Graph.Node(name) == nil {
				t.Errorf("chain %q -> %q: successor not registered", node.Type, name)
			}
		}
		for _, name := range node.Wave {
			if Graph.Node(name) == nil {
				t.Errorf("wave %q -> %q: member not registered", node.Type, name)
			}
		}
	}
}

func TestGraphAcyclic(t *testing.T) {
	const (
		white = iota
		gray
		black
	)
	color := map[string]int{}
	var visit func(typ string) bool
	visit = func(typ string) bool {
		color[typ] = gray
		if node := Graph.Node(typ); node != nil {
			for _, next := range node.Chain {
				switch color[next] {
				case gray:
					return true // back edge
				case white:
					if visit(next) {
						return true
					}
				}
			}
		}
		color[typ] = black
		return false
	}
	for _, node := range Graph.nodes {
		if color[node.Type] == white && visit(node.Type) {
			t.Errorf("graph has a cycle through %q", node.Type)
		}
	}
}

func TestGraphKeyDayRoundTrip(t *testing.T) {
	for _, node := range Graph.nodes {
		if node.Key == nil || node.Day == nil {
			continue // per-item nodes (extract, broker) aren't date-keyed
		}
		for _, date := range []string{"2026-08-30", "2026-01-02"} {
			id := node.Key(date)
			got, err := node.Day(id)
			if err != nil {
				t.Errorf("%s: Day(Key(%q)) error: %v", node.Type, date, err)
				continue
			}
			if got.Format("2006-01-02") != date {
				t.Errorf("%s: Day(Key(%q)) = %s, want %s", node.Type, date, got.Format("2006-01-02"), date)
			}
		}
	}
}

func TestGraphWaveMembershipComplete(t *testing.T) {
	node := Graph.Node(TypePipelineDaily)
	want := []string{TypeStockSummary, TypeAnnouncements, TypeRSS, TypeCleanup}
	if len(node.Wave) != len(want) {
		t.Fatalf("pipeline:daily wave = %v, want %v", node.Wave, want)
	}
	for i, w := range want {
		if node.Wave[i] != w {
			t.Errorf("pipeline:daily wave[%d] = %s, want %s", i, node.Wave[i], w)
		}
	}
}

func TestGraphSelfHealSetExact(t *testing.T) {
	got := map[string]bool{}
	for _, node := range Graph.nodes {
		if node.SelfHeal {
			got[node.Type] = true
		}
	}
	want := map[string]bool{TypeStockSummary: true, TypeAnnouncements: true, TypeRSS: true}
	if len(got) != len(want) {
		t.Fatalf("self-heal set = %v, want %v", got, want)
	}
	for typ := range want {
		if !got[typ] {
			t.Errorf("self-heal set missing %s", typ)
		}
	}
}

func TestGraphChainFiresSuccessors(t *testing.T) {
	day := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)

	enq := &graphEnqueuer{}
	if err := Graph.Chain(context.Background(), enq, TypeStockSummary, day); err != nil {
		t.Fatalf("Chain(stock_summary): %v", err)
	}
	if len(enq.enqueued) != 1 || enq.enqueued[0] != TypeDetectAnomalies {
		t.Errorf("Chain(stock_summary) fired %v, want [detect:anomalies]", enq.enqueued)
	}

	enq = &graphEnqueuer{}
	if err := Graph.Chain(context.Background(), enq, TypeDetectAnomalies, day); err != nil {
		t.Fatalf("Chain(detect:anomalies): %v", err)
	}
	if len(enq.enqueued) != 1 || enq.enqueued[0] != TypeFilterDisclosures {
		t.Errorf("Chain(detect:anomalies) fired %v, want [filter:disclosures]", enq.enqueued)
	}
}

func TestGraphChainSwallowsTaskIDConflict(t *testing.T) {
	enq := &graphEnqueuer{errByType: map[string]error{TypeDetectAnomalies: asynq.ErrTaskIDConflict}}
	if err := Graph.Chain(context.Background(), enq, TypeStockSummary, time.Now()); err != nil {
		t.Errorf("Chain should swallow ErrTaskIDConflict, got %v", err)
	}
}

func TestGraphChainReturnsNonConflictError(t *testing.T) {
	boom := errors.New("boom")
	enq := &graphEnqueuer{errByType: map[string]error{TypeDetectAnomalies: boom}}
	if err := Graph.Chain(context.Background(), enq, TypeStockSummary, time.Now()); !errors.Is(err, boom) {
		t.Errorf("Chain = %v, want %v", err, boom)
	}
}

func TestGraphNodeByName(t *testing.T) {
	for _, node := range Graph.nodes {
		got, err := Graph.NodeByName(node.Name)
		if err != nil {
			t.Errorf("NodeByName(%q): %v", node.Name, err)
			continue
		}
		if got != node {
			t.Errorf("NodeByName(%q) = %p, want %p", node.Name, got, node)
		}
	}
	if _, err := Graph.NodeByName("nope"); err == nil {
		t.Error("NodeByName(unknown) expected error, got nil")
	}
}
