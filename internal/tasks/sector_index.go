package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
)

// SectorIndexPayload is the payload for an idx:sector_index task: one date
// (the run date, which becomes the membership snapshot's effective_date).
type SectorIndexPayload struct {
	Date string `json:"date"` // YYYY-MM-DD
}

// NewSectorIndexHandler returns an asynq handler for the idx:sector_index task
// type (issue 15b): fetch the stock-screener snapshot, upsert the sector
// taxonomy into tickers, replace the index-membership snapshot for the run
// date, and record source_status. The scheduler fires it every 6 months
// (Feb+Jul rebalance cadence); the enqueue-daily CLI bulk mode runs the same
// usecase synchronously.
func NewSectorIndexHandler(log *logrus.Logger, uc *usecase.SectorIndexUseCase) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		// The 6-month scheduler fires this task with a nil payload; the run
		// date defaults to today server-side, same convention as pipeline:daily
		// and the weekly sweep.
		runDate := time.Now()
		if len(t.Payload()) > 0 {
			p, err := pipeline.DecodeTask[SectorIndexPayload](t)
			if err != nil {
				return err
			}
			runDate, err = pipeline.ParseTaskDay(p.Date)
			if err != nil {
				return err
			}
		}

		n, m, err := uc.SeedSectorIndex(ctx, runDate)
		if err != nil {
			return err
		}
		log.Infof("sector/index: upserted %d ticker(s), %d membership row(s), effective=%s",
			n, m, runDate.Format("2006-01-02"))
		return nil
	}
}
