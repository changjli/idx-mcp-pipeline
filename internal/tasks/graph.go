package tasks

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
)

// Node is one task type's declarative registration in the pipeline graph
// (ADR-0008). The registry is the single source of truth for what runs daily,
// how tasks chain, and what self-heal recovers — the scheduler, enqueue-daily,
// and the handlers are projections over it.
type Node struct {
	// Name is the CLI --task name (enqueue-daily). Mirrors the pre-graph
	// boolean flags so invocation muscle memory transfers.
	Name string
	// Type is the asynq task type; also the "source" label in log events and
	// source_status rows.
	Type string
	// Key builds the date-keyed dedup TaskID from a YYYY-MM-DD date key
	// (TaskKey + inverse). Nil for per-item nodes (extract, broker) whose IDs
	// aren't date-shaped.
	Key func(date string) string
	// Day parses the date back out of a date-keyed TaskID. Nil for per-item
	// nodes.
	Day func(id string) (time.Time, error)
	// Enqueue enqueues the node's task for a day. args carries per-node extras
	// (--arg values: broker ticker, extract disclosure id); date-keyed nodes
	// ignore it. opts are appended (e.g. asynq.ProcessIn for self-heal).
	Enqueue func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error)
	// Chain lists the 1:1 successor types fired after this node's stage body
	// succeeds (graph.Chain).
	Chain []string
	// Wave lists the fixed fan-out types fired by pipeline:daily.
	Wave []string
	// Delay is the ProcessIn applied when this node fires as part of a Wave
	// (cleanup runs 3h after the ingestion wave). 0 = fire immediately.
	Delay time.Duration
	// SelfHeal marks wave-1 ingestion nodes eligible for archived-task
	// recovery (date-keyed TaskIDs).
	SelfHeal bool
}

// Graph is the declarative pipeline registry: one Node per task type.
type Registry struct {
	nodes  []*Node
	byType map[string]*Node
	byName map[string]*Node
}

// NewRegistry builds a registry from nodes, indexed by Type and Name. It
// panics on a duplicate Type or Name — a registry bug, not a runtime condition.
func NewRegistry(nodes ...*Node) *Registry {
	g := &Registry{
		nodes:  nodes,
		byType: make(map[string]*Node, len(nodes)),
		byName: make(map[string]*Node, len(nodes)),
	}
	for _, n := range nodes {
		if _, dup := g.byType[n.Type]; dup {
			panic(fmt.Sprintf("graph: duplicate node type %q", n.Type))
		}
		if _, dup := g.byName[n.Name]; dup {
			panic(fmt.Sprintf("graph: duplicate node name %q", n.Name))
		}
		g.byType[n.Type] = n
		g.byName[n.Name] = n
	}
	return g
}

// Node returns the node for an asynq task type, or nil.
func (g *Registry) Node(typ string) *Node {
	return g.byType[typ]
}

// NodeByName returns the node for a CLI --task name.
func (g *Registry) NodeByName(name string) (*Node, error) {
	if n, ok := g.byName[name]; ok {
		return n, nil
	}
	return nil, fmt.Errorf("unknown task %q", name)
}

// Chain fires every node in typ's Chain for day after the stage body succeeds.
// asynq.ErrTaskIDConflict is swallowed — a chained task already enqueued is
// dedup, not an error. Returns the first non-conflict error.
func (g *Registry) Chain(ctx context.Context, enq pipeline.Enqueuer, typ string, day time.Time) error {
	node := g.Node(typ)
	if node == nil {
		return fmt.Errorf("graph: unknown node %q", typ)
	}
	var firstErr error
	for _, name := range node.Chain {
		child := g.Node(name)
		if child == nil {
			return fmt.Errorf("graph: %s chains to unregistered node %q", typ, name)
		}
		if _, err := child.Enqueue(enq, day, nil); err != nil {
			if errors.Is(err, asynq.ErrTaskIDConflict) {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// SelfHealEligible returns the nodes eligible for archived-task recovery:
// wave-1 ingestion nodes with date-keyed TaskIDs.
func (g *Registry) SelfHealEligible() []*Node {
	var out []*Node
	for _, n := range g.nodes {
		if n.SelfHeal {
			out = append(out, n)
		}
	}
	return out
}

// dateKey returns the standard date-keyed Key closure for a task type: the
// dedup TaskID from a YYYY-MM-DD date key.
func dateKey(typ string) func(string) string {
	return func(date string) string { return TaskKey(typ, date) }
}

// dateDay returns the standard date-keyed Day closure for a task type: the
// date parsed back out of a TaskID of the form "{type}:{date}".
func dateDay(typ string) func(string) (time.Time, error) {
	return func(id string) (time.Time, error) { return dateFromTaskID(typ, id) }
}

// dateFromTaskID parses the YYYY-MM-DD date out of a date-keyed TaskID of the
// form "{type}:{date}".
func dateFromTaskID(typ, id string) (time.Time, error) {
	dateStr := strings.TrimPrefix(id, typ+":")
	return time.Parse("2006-01-02", dateStr)
}

// argValue returns the value of the --arg key=value entry, or "" if absent.
func argValue(args []string, key string) string {
	prefix := key + "="
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			return strings.TrimPrefix(a, prefix)
		}
	}
	return ""
}

// Graph is the declarative pipeline registry (ADR-0008). The chain edges are
// stock_summary → detect:anomalies → filter:disclosures; pipeline:daily's Wave
// is the fixed wave-1 fan-out. Per-row fan-outs (broker-summary per flagged
// anomaly, extract per passing disclosure) stay in the handlers that own the
// row data — an edge is 1:1, a conditional fan-out is the stage's business
// (spec Q5-a).
var Graph = NewRegistry(
	&Node{
		Name: "stock-summary",
		Type: TypeStockSummary,
		Key:  dateKey(TypeStockSummary),
		Day:  dateDay(TypeStockSummary),
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			dateKey := day.Format("2006-01-02")
			stage := pipeline.NewIngestStage(TypeStockSummary, nil, enq, 3)
			return stage.EnqueueWithOpts(TaskKey(TypeStockSummary, dateKey), StockSummaryPayload{Date: dateKey}, opts...)
		},
		Chain:    []string{TypeDetectAnomalies},
		SelfHeal: true,
	},
	&Node{
		Name: "announcements",
		Type: TypeAnnouncements,
		Key:  dateKey(TypeAnnouncements),
		Day:  dateDay(TypeAnnouncements),
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			dateKey := day.Format("2006-01-02")
			stage := pipeline.NewIngestStage(TypeAnnouncements, nil, enq, 3)
			return stage.EnqueueWithOpts(TaskKey(TypeAnnouncements, dateKey), AnnouncementsPayload{Date: dateKey}, opts...)
		},
		SelfHeal: true,
	},
	&Node{
		Name: "rss",
		Type: TypeRSS,
		Key:  dateKey(TypeRSS),
		Day:  dateDay(TypeRSS),
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			dateKey := day.Format("2006-01-02")
			stage := pipeline.NewIngestStage(TypeRSS, nil, enq, 3)
			return stage.EnqueueWithOpts(TaskKey(TypeRSS, dateKey), RSSPayload{Date: dateKey}, opts...)
		},
		SelfHeal: true,
	},
	&Node{
		Name: "cleanup",
		Type: TypeCleanup,
		Key:  dateKey(TypeCleanup),
		Day:  dateDay(TypeCleanup),
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			dateKey := day.Format("2006-01-02")
			stage := pipeline.NewIngestStage(TypeCleanup, nil, enq, 3)
			return stage.EnqueueWithOpts(TaskKey(TypeCleanup, dateKey), CleanupPayload{Date: dateKey}, opts...)
		},
		// Cleanup runs after the ingestion wave: delayed so extraction
		// (chained off anomalies → filter) has time to finish before eviction.
		Delay: cleanupDelay,
	},
	&Node{
		Name: "detect",
		Type: TypeDetectAnomalies,
		Key:  dateKey(TypeDetectAnomalies),
		Day:  dateDay(TypeDetectAnomalies),
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			dateKey := day.Format("2006-01-02")
			stage := pipeline.NewIngestStage(TypeDetectAnomalies, nil, enq, 3)
			return stage.Enqueue(TaskKey(TypeDetectAnomalies, dateKey), DetectAnomaliesPayload{Date: dateKey})
		},
		Chain: []string{TypeFilterDisclosures},
	},
	&Node{
		Name: "filter",
		Type: TypeFilterDisclosures,
		Key:  dateKey(TypeFilterDisclosures),
		Day:  dateDay(TypeFilterDisclosures),
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			dateKey := day.Format("2006-01-02")
			stage := pipeline.NewIngestStage(TypeFilterDisclosures, nil, enq, 3)
			return stage.Enqueue(TaskKey(TypeFilterDisclosures, dateKey), FilterDisclosuresPayload{Date: dateKey})
		},
	},
	&Node{
		Name: "extract",
		Type: TypeExtractDisclosure,
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			idStr := argValue(args, "id")
			if idStr == "" {
				return nil, errors.New("extract:disclosure requires --arg id=<disclosure id>")
			}
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("extract:disclosure: invalid id %q: %w", idStr, err)
			}
			stage := pipeline.NewIngestStage(TypeExtractDisclosure, nil, enq, extractMaxRetry)
			return stage.Enqueue(fmt.Sprintf("%s:%d", TypeExtractDisclosure, id), ExtractDisclosurePayload{DisclosureID: id})
		},
	},
	&Node{
		Name: "broker-summary",
		Type: TypeBrokerStockSummary,
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			ticker := argValue(args, "ticker")
			if ticker == "" {
				return nil, errors.New("idx:broker_stock_summary requires --arg ticker=<ticker>")
			}
			dateKey := day.Format("2006-01-02")
			stage := pipeline.NewIngestStage(TypeBrokerStockSummary, nil, enq, 3)
			return stage.Enqueue(TaskKey(TypeBrokerStockSummary, ticker+":"+dateKey), BrokerStockSummaryPayload{Ticker: ticker, Date: dateKey})
		},
	},
	&Node{
		Name: "pipeline",
		Type: TypePipelineDaily,
		Key:  dateKey(TypePipelineDaily),
		Day:  dateDay(TypePipelineDaily),
		Enqueue: func(enq pipeline.Enqueuer, day time.Time, args []string, opts ...asynq.Option) (*asynq.TaskInfo, error) {
			dateKey := day.Format("2006-01-02")
			taskKey := TaskKey(TypePipelineDaily, dateKey)
			task := asynq.NewTask(TypePipelineDaily, nil)
			return enq.Enqueue(task, asynq.TaskID(taskKey), asynq.Queue("default"))
		},
		Wave: []string{TypeStockSummary, TypeAnnouncements, TypeRSS, TypeCleanup},
	},
)
