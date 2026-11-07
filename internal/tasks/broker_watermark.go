package tasks

import (
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// recordBrokerWatermark advances the broker source's high-water mark to the
// newest stored foreign window end. SuccessMonotonic carries the previous mark
// forward when the run wrote nothing, and never regresses it when the daily
// refresher (today) and the monthly archiver (last month) report in either
// order.
func recordBrokerWatermark(uc *usecase.BrokerStockSummaryUseCase, recorder *pipeline.SourceStatusRecorder) {
	var watermark *time.Time
	if newest, err := uc.Repo.MaxWindowEnd(uc.DB, broker.InvestorForeign); err == nil {
		watermark = newest
	}
	recorder.SuccessMonotonic(TypeBrokerStockSummary, BrokerStockSummaryMaxAgeSeconds, watermark)
}
