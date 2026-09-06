package tasks

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ksei"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// kseiBalanceposMaxAgeSeconds is the source_status max age for
	// ksei:balancepos (35 days). The source is monthly (month-end position,
	// published at the start of the next month); 35 days keeps the envelope
	// fresh across one skipped publication while tolerating the monthly
	// cadence — anything older means KSEI stopped publishing.
	kseiBalanceposMaxAgeSeconds int32 = 35 * 86400
)

// KSEIBalanceposPayload is the payload for a ksei:balancepos task.
type KSEIBalanceposPayload struct {
	Date string `json:"date"` // YYYY-MM-DD run date
}

// KSEIBalanceposFetcher downloads and parses one KSEI balance-position
// archive. *ksei.Client satisfies this; tests use a fake.
type KSEIBalanceposFetcher interface {
	FetchBalancepos(ctx context.Context, fileDate time.Time) ([]ksei.Holding, error)
}

// NewKSEIBalanceposHandler returns an asynq handler for the ksei:balancepos
// task type. The source is monthly: on each run the handler targets the
// preceding month-end file (KSEI publishes month M's zip at the start of
// M+1) and no-ops when the watermark already covers it, so the daily
// schedule costs one watermark read per day and one download per month.
// The MCP tool reads the stored rows — a pure DB read (issue 08).
func NewKSEIBalanceposHandler(
	log *logrus.Logger,
	fetcher KSEIBalanceposFetcher,
	db *sqlx.DB,
	repo *repository.ShareholderCompositionRepository,
	recorder *pipeline.SourceStatusRecorder,
) asynq.HandlerFunc {
	return func(ctx context.Context, t *asynq.Task) error {
		p, err := pipeline.DecodeTask[KSEIBalanceposPayload](t)
		if err != nil {
			return err
		}
		runDate, err := pipeline.ParseTaskDay(p.Date)
		if err != nil {
			return err
		}

		fileDate := ksei.LatestFileDate(runDate)
		if wm, wmErr := recorder.CurrentWatermark(TypeKSEIBalancepos); wmErr == nil && wm != nil && !wm.Before(fileDate) {
			log.Infof("ksei:balancepos: watermark %s already covers %s, nothing to ingest",
				wm.Format("2006-01-02"), fileDate.Format("2006-01-02"))
			recorder.Success(TypeKSEIBalancepos, kseiBalanceposMaxAgeSeconds, nil)
			return nil
		}

		log.Infof("ksei:balancepos: fetching file, run date=%s file date=%s",
			p.Date, fileDate.Format("2006-01-02"))

		holdings, fetchErr := fetcher.FetchBalancepos(ctx, fileDate)
		if fetchErr != nil {
			recorder.Failure(TypeKSEIBalancepos, kseiBalanceposMaxAgeSeconds, p.Date, fetchErr)
			return fetchErr
		}

		rows := kseiHoldingsToEntities(holdings)
		if err := repo.Upsert(db, rows); err != nil {
			recorder.Failure(TypeKSEIBalancepos, kseiBalanceposMaxAgeSeconds, p.Date, err)
			return err
		}

		recorder.Success(TypeKSEIBalancepos, kseiBalanceposMaxAgeSeconds, &fileDate)
		log.Infof("ksei:balancepos: upserted %d row(s) for %s", len(rows), fileDate.Format("2006-01-02"))
		return nil
	}
}

// kseiHoldingsToEntities converts parsed holdings into entity rows. The file
// has no grand-total column (its 25th field is the foreign block's Total), so
// Total = LocalTotal + ForeignTotal.
func kseiHoldingsToEntities(holdings []ksei.Holding) []entity.ShareholderComposition {
	rows := make([]entity.ShareholderComposition, 0, len(holdings))
	for _, h := range holdings {
		rows = append(rows, entity.ShareholderComposition{
			Ticker:       h.Code,
			PositionDate: h.Date,
			SecNum:       h.SecNum,
			Price:        h.Price,
			LocalIS:      h.LocalIS,
			LocalCP:      h.LocalCP,
			LocalPF:      h.LocalPF,
			LocalIB:      h.LocalIB,
			LocalID:      h.LocalID,
			LocalMF:      h.LocalMF,
			LocalSC:      h.LocalSC,
			LocalFD:      h.LocalFD,
			LocalOT:      h.LocalOT,
			LocalTotal:   h.LocalTotal,
			ForeignIS:    h.ForeignIS,
			ForeignCP:    h.ForeignCP,
			ForeignPF:    h.ForeignPF,
			ForeignIB:    h.ForeignIB,
			ForeignID:    h.ForeignID,
			ForeignMF:    h.ForeignMF,
			ForeignSC:    h.ForeignSC,
			ForeignFD:    h.ForeignFD,
			ForeignOT:    h.ForeignOT,
			ForeignTotal: h.ForeignTotal,
			Total:        h.LocalTotal + h.ForeignTotal,
		})
	}
	return rows
}
