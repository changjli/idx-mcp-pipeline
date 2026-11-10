package tasks

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

type depthHandlerStore struct{}

func (depthHandlerStore) ListDepthSweepTickers(db *sqlx.DB, includeNotCovered bool) ([]string, error) {
	return nil, nil
}
func (depthHandlerStore) FindTaxonomy(db *sqlx.DB, ticker string) (*entity.TickerFinancialTaxonomy, error) {
	return nil, nil
}
func (depthHandlerStore) UpsertTaxonomy(db *sqlx.DB, row *entity.TickerFinancialTaxonomy) error {
	return nil
}
func (depthHandlerStore) FindDepthRun(db *sqlx.DB, month time.Time, ticker string) (*entity.FinancialDepthRun, error) {
	return nil, nil
}
func (depthHandlerStore) UpsertDepthRun(db *sqlx.DB, row *entity.FinancialDepthRun) error {
	return nil
}
func (depthHandlerStore) ApplyDepth(db *sqlx.DB, observedMonth time.Time, rows []entity.FinancialStatementDepth) (int, error) {
	return len(rows), nil
}

type depthHandlerSource struct{}

func (depthHandlerSource) ClassifyDepth(ctx context.Context, ticker string) (financials.Taxonomy, string, error) {
	return financials.TaxonomyOther, "discriminant_probe", nil
}
func (depthHandlerSource) FetchDepth(ctx context.Context, ticker string, taxonomy financials.Taxonomy) ([]financials.DepthStatement, error) {
	return nil, nil
}

type depthHandlerStatements struct{}

func (depthHandlerStatements) FindByTicker(db *sqlx.DB, ticker string) ([]entity.FinancialStatement, error) {
	return nil, nil
}

type depthStatusStore struct{}

func (depthStatusStore) FindBySource(source string) (*entity.SourceStatus, error) { return nil, nil }
func (depthStatusStore) Upsert(status *entity.SourceStatus) error                 { return nil }

type depthAlertStore struct{}

func (depthAlertStore) Insert(alert *entity.Alert) error { return nil }

// TestFinancialDepthHandler_NilPayloadIsMonthlySweep pins the scheduler
// contract: the monthly cron fires with a nil payload, so the handler must
// default the run date and sweep rather than failing JSON decoding.
func TestFinancialDepthHandler_NilPayloadIsMonthlySweep(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	uc := usecase.NewFinancialDepthUseCase(
		nil, log, depthHandlerSource{}, depthHandlerStore{}, depthHandlerStatements{},
		usecase.WithDepthPacing(1, 0),
		usecase.WithDepthClock(func() time.Time { return time.Date(2026, 11, 1, 21, 0, 0, 0, time.UTC) }),
	)
	recorder := pipeline.NewSourceStatusRecorder(depthStatusStore{}, depthAlertStore{}, log)
	handler := NewFinancialDepthHandler(log, uc, recorder)

	if err := handler(context.Background(), asynq.NewTask(TypeFinancialsDepth, nil)); err != nil {
		t.Fatalf("nil-payload monthly sweep: %v", err)
	}
}

// TestFinancialDepthHandler_OnDemandTicker pins the issue-07 payload shape: a
// ticker runs the single-ticker path, not the sweep.
func TestFinancialDepthHandler_OnDemandTicker(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	uc := usecase.NewFinancialDepthUseCase(
		nil, log, depthHandlerSource{}, depthHandlerStore{}, depthHandlerStatements{},
		usecase.WithDepthPacing(1, 0),
		usecase.WithDepthClock(func() time.Time { return time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC) }),
	)
	recorder := pipeline.NewSourceStatusRecorder(depthStatusStore{}, depthAlertStore{}, log)
	handler := NewFinancialDepthHandler(log, uc, recorder)

	raw, _ := json.Marshal(FinancialDepthPayload{Date: "2026-11-10", Ticker: "RAJA", Month: "2026-11"})
	if err := handler(context.Background(), asynq.NewTask(TypeFinancialsDepth, raw)); err != nil {
		t.Fatalf("on-demand ticker: %v", err)
	}
}
