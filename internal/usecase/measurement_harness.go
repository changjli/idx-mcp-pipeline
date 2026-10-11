package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// MeasurementHarness is the Measurement Harness's single implementation: the one
// place that turns a database and object storage into runnable Graders and the
// journal/config/report operations. cmd/measure (humans, local) and the MCP
// server (the AI, remote) both construct it and call the same methods, so the
// two interfaces can never grade differently (ADR-0017). The Graders are
// read-only; the only writes are LogVerdict and the Active Filter Set.
type MeasurementHarness struct {
	db       *sqlx.DB
	log      *logrus.Logger
	objects  storage.ObjectStore
	revision string

	priceRepo      *repository.DailyPriceRepository
	brokerRepo     *repository.BrokerStockSummaryRepository
	anomalyRepo    *repository.AnomalyRepository
	disclosureRepo *repository.DisclosureRepository
	journalWriter  MeasurementJournalWriter

	// Config and Reports are the object-storage artifact stores. Both are safe
	// on a nil object store; the methods that need one return a structured error.
	Config  *MeasurementConfigStore
	Reports *MeasurementReportStore
}

// NewMeasurementHarness wires the shared harness around a database, an object
// store (may be nil: the MCP server runs without R2), and the git revision
// stamped into every report.
func NewMeasurementHarness(db *sqlx.DB, log *logrus.Logger, objects storage.ObjectStore, revision string) *MeasurementHarness {
	h := &MeasurementHarness{
		db:             db,
		log:            log,
		objects:        objects,
		revision:       revision,
		priceRepo:      repository.NewDailyPriceRepository(log),
		brokerRepo:     repository.NewBrokerStockSummaryRepository(log),
		anomalyRepo:    repository.NewAnomalyRepository(log),
		disclosureRepo: repository.NewDisclosureRepository(log),
		Config:         NewMeasurementConfigStore(objects),
		Reports:        NewMeasurementReportStore(objects),
	}
	if db != nil {
		h.journalWriter = NewJournalSQLWriter(db)
	}
	return h
}

// baseEngine returns an engine carrying the price source and the revision; each
// Grader sets the extra sources it needs.
func (h *MeasurementHarness) baseEngine() *MeasurementEngine {
	return NewMeasurementEngine(h.db, h.log, nil, h.priceRepo, nil, h.revision)
}

// registry resolves the Active Filter Set. With no object storage configured the
// harness grades the built-in default (an empty bucket's semantics), so a
// screen Grading still works on an R2-less server.
func (h *MeasurementHarness) registry(ctx context.Context) (*MeasurementFilterRegistry, string, string, error) {
	if h.objects == nil {
		return NewMeasurementFilterRegistry(), defaultMeasurementFilterSet, "", nil
	}
	return h.Config.Registry(ctx)
}

// GradeScreen replays a named filter set (or two, compared) across the Replay
// Dates and pools each set's observations. It resolves the filter sets and the
// Config Hash from the Active Filter Set on R2.
func (h *MeasurementHarness) GradeScreen(ctx context.Context, opts ScreenGradeOptions) (*ScreenGradeReport, error) {
	registry, active, hash, err := h.registry(ctx)
	if err != nil {
		return nil, err
	}
	opts.ActiveFilterSet = active
	opts.ConfigHash = hash

	engine := h.baseEngine()
	engine.Screen = NewScreenStocksUseCase(h.db, h.log, h.priceRepo, h.brokerRepo)
	engine.Filters = registry
	return engine.GradeScreen(ctx, opts)
}

// GradeAnomalies scores Anomalies against the Liquidity Baseline, split by
// Direction Bucket and Anomaly Confluence.
func (h *MeasurementHarness) GradeAnomalies(ctx context.Context, opts AnomalyGradeOptions) (*AnomalyGradeReport, error) {
	engine := h.baseEngine()
	engine.Anomalies = h.anomalyRepo
	engine.Disclosures = h.disclosureRepo
	engine.BrokerNet = h.brokerRepo
	return engine.GradeAnomalies(ctx, opts)
}

// GradeFlow buckets the Liquidity Baseline by the Flow Column's foreign_net.
func (h *MeasurementHarness) GradeFlow(ctx context.Context, opts FlowGradeOptions) (*FlowGradeReport, error) {
	engine := h.baseEngine()
	engine.ForeignFlow = h.brokerRepo
	return engine.GradeFlow(ctx, opts)
}

// GradeDisclosures scores the passed-filter Disclosures' Forward Returns.
func (h *MeasurementHarness) GradeDisclosures(ctx context.Context, opts DisclosureGradeOptions) (*DisclosureGradeReport, error) {
	engine := h.baseEngine()
	engine.DisclosureFilings = h.disclosureRepo
	return engine.GradeDisclosures(ctx, opts)
}

// GradeJournal resolves the logged Verdicts in the measurement_journal table
// against stored prices. It replays nothing and reads no Liquidity Baseline.
func (h *MeasurementHarness) GradeJournal(ctx context.Context, opts JournalGradeOptions) (*JournalGradeReport, error) {
	engine := h.baseEngine()
	engine.Journal = NewJournalSQLSource(h.db)
	engine.JournalPrices = h.priceRepo
	return engine.GradeJournal(ctx, opts)
}

// LogVerdict appends one Verdict to the Decision Journal (ADR-0016's rule).
func (h *MeasurementHarness) LogVerdict(ctx context.Context, in LogVerdictInput) (*LogVerdictResult, error) {
	if h.journalWriter == nil {
		return nil, fmt.Errorf("%w: journal writer is not configured (no database)", ErrInvalidArgument)
	}
	return LogVerdict(ctx, h.journalWriter, in)
}

// GetConfig returns the Active Filter Set's Config Document and its Config Hash.
// With nothing published it returns an empty document around the built-in
// default, so the caller sees a valid, empty config rather than an error.
func (h *MeasurementHarness) GetConfig(ctx context.Context) (*MeasurementConfigDocument, string, error) {
	if h.objects == nil {
		return nil, "", fmt.Errorf("%w: no object storage is configured for the Active Filter Set", ErrInvalidArgument)
	}
	doc, hash, err := h.Config.Get(ctx)
	if err != nil {
		return nil, "", err
	}
	if doc == nil {
		return &MeasurementConfigDocument{
			Active: defaultMeasurementFilterSet,
			Sets:   map[string]MeasurementFilterSetFile{},
		}, "", nil
	}
	return doc, hash, nil
}

// PutConfig publishes doc as a new content-addressed Config Document version and
// moves the active pointer, returning the new Config Hash. When expectedHash is
// non-empty it is a best-effort compare-and-swap: a moved pointer rejects the
// write so two writers cannot silently clobber each other.
func (h *MeasurementHarness) PutConfig(ctx context.Context, doc MeasurementConfigDocument, expectedHash string) (string, error) {
	if h.objects == nil {
		return "", fmt.Errorf("%w: no object storage is configured for the Active Filter Set", ErrInvalidArgument)
	}
	if strings.TrimSpace(expectedHash) != "" {
		_, current, err := h.Config.Get(ctx)
		if err != nil {
			return "", err
		}
		if current != expectedHash {
			return "", fmt.Errorf("%w: the Active Filter Set has moved (current %s, expected %s); re-read it and retry",
				ErrInvalidArgument, current, expectedHash)
		}
	}
	return h.Config.Put(ctx, doc)
}

// ListReports returns the bare latest report base names, newest-first. Sealed
// versions (measurement/reports/versions/...) are excluded: they are audit, and
// the bare key always addresses the latest.
func (h *MeasurementHarness) ListReports(ctx context.Context) ([]string, error) {
	if h.objects == nil {
		return nil, fmt.Errorf("%w: no object storage is configured for reports", ErrInvalidArgument)
	}
	keys, err := h.Reports.List(ctx, MeasurementReportPrefix+"/")
	if err != nil {
		return nil, err
	}
	prefix := MeasurementReportPrefix + "/"
	bases := make([]string, 0, len(keys))
	for _, key := range keys {
		rest := strings.TrimPrefix(key, prefix)
		if !strings.HasSuffix(rest, ".json") || strings.Contains(rest, "/") {
			continue
		}
		bases = append(bases, strings.TrimSuffix(rest, ".json"))
	}
	sort.Sort(sort.Reverse(sort.StringSlice(bases)))
	return bases, nil
}

// MeasurementReportDocument is one stored Grade report served over MCP: the base
// name, the report JSON body, and the markdown summary.
type MeasurementReportDocument struct {
	Base     string          `json:"base"`
	Report   json.RawMessage `json:"report"`
	Markdown string          `json:"markdown"`
}

// GetReport returns the latest JSON body and markdown summary for one report
// base. A missing base is ErrNotFound, never an empty report.
func (h *MeasurementHarness) GetReport(ctx context.Context, base string) (*MeasurementReportDocument, error) {
	if h.objects == nil {
		return nil, fmt.Errorf("%w: no object storage is configured for reports", ErrInvalidArgument)
	}
	base = strings.TrimSpace(base)
	if base == "" {
		return nil, fmt.Errorf("%w: report base is required", ErrInvalidArgument)
	}
	data, markdown, err := h.Reports.Get(ctx, base)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, fmt.Errorf("%w: report %q", ErrNotFound, base)
		}
		return nil, err
	}
	return &MeasurementReportDocument{Base: base, Report: data, Markdown: string(markdown)}, nil
}
