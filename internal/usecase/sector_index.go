package usecase

import (
	"context"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/client"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

const (
	// sectorIndexSource is the source_status label for the sector/index seeder.
	// It matches the task type (idx:sector_index) so the statusz dump and the
	// task registry agree on the source name.
	sectorIndexSource = "idx:sector_index"
	// sectorIndexMaxAgeSeconds is the source_status freshness window (200 days
	// ≈ 6.5 months). The scheduler fires every 6 months (Feb+Jul rebalance
	// cadence); 200 days keeps the envelope fresh through a late fire without
	// going stale mid-cycle.
	sectorIndexMaxAgeSeconds int32 = 200 * 86400
)

// SectorIndexFetcher fetches the IDX stock-screener list. *client.Client
// satisfies this; tests use a fake.
type SectorIndexFetcher interface {
	FetchScreener(ctx context.Context) ([]client.ScreenerRow, error)
}

// SectorIndexUseCase orchestrates the sector/industry + index-membership
// seeder (issue 15b): fetch the stock-screener snapshot, upsert the sector
// taxonomy into tickers, replace the index-membership snapshot for the run
// date, and record source_status. Shared by the asynq handler (scheduler,
// 6-monthly) and the enqueue-daily CLI bulk mode.
type SectorIndexUseCase struct {
	DB         *sqlx.DB
	Log        *logrus.Logger
	Fetcher    SectorIndexFetcher
	TickerRepo *repository.TickerRepository
	IndexRepo  *repository.TickerIndexRepository
	Recorder   *pipeline.SourceStatusRecorder
}

func NewSectorIndexUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	fetcher SectorIndexFetcher,
	tickerRepo *repository.TickerRepository,
	indexRepo *repository.TickerIndexRepository,
	recorder *pipeline.SourceStatusRecorder,
) *SectorIndexUseCase {
	return &SectorIndexUseCase{DB: db, Log: log, Fetcher: fetcher, TickerRepo: tickerRepo, IndexRepo: indexRepo, Recorder: recorder}
}

// SeedSectorIndex fetches the stock-screener snapshot and persists it: sector
// columns upserted into tickers, index membership replaced for the run date
// (point-in-time — historical screens must not read today's constituents).
// Returns the number of ticker rows upserted and membership rows written.
// On any failure source_status records the error and the error is returned.
func (uc *SectorIndexUseCase) SeedSectorIndex(ctx context.Context, runDate time.Time) (int, int, error) {
	runDateStr := runDate.Format("2006-01-02")

	uc.Log.Info("sector/index seeder: fetching stock screener")
	rows, err := uc.Fetcher.FetchScreener(ctx)
	if err != nil {
		uc.Recorder.Failure(sectorIndexSource, sectorIndexMaxAgeSeconds, runDateStr, err)
		return 0, 0, err
	}

	tickerRows, indexRows := screenerRowsToEntities(rows, runDate)
	if err := uc.TickerRepo.UpsertSectorIndex(uc.DB, tickerRows); err != nil {
		uc.Recorder.Failure(sectorIndexSource, sectorIndexMaxAgeSeconds, runDateStr, err)
		return 0, 0, err
	}
	if err := uc.IndexRepo.ReplaceMembership(uc.DB, runDate, indexRows); err != nil {
		uc.Recorder.Failure(sectorIndexSource, sectorIndexMaxAgeSeconds, runDateStr, err)
		return 0, 0, err
	}

	uc.Recorder.Success(sectorIndexSource, sectorIndexMaxAgeSeconds, &runDate)
	return len(tickerRows), len(indexRows), nil
}

// screenerRowsToEntities converts fetched screener rows into (ticker sector
// rows, index-membership rows) for the run date. Data-quality cleanup (issue
// 15b):
//   - null/empty indexCode (8 suspended rows: FIMP, FLMC, FREN, GOTOM, KING,
//     MENN, MSIE, PGJO) → no membership rows; the sector columns are still kept.
//   - KETR "No Sector" → nil sector (no classification, not a bogus label).
//   - VICI "CONSUMER GOODS INDUSTRY" → "Consumer Non-Cyclicals" (legacy label).
func screenerRowsToEntities(rows []client.ScreenerRow, effectiveDate time.Time) ([]entity.Ticker, []entity.TickerIndex) {
	tickers := make([]entity.Ticker, 0, len(rows))
	var indices []entity.TickerIndex
	for _, r := range rows {
		tickers = append(tickers, entity.Ticker{
			Code:            r.StockCode,
			Sektor:          normalizeSector(r.Sector),
			Industri:        r.Industry,
			SubSektor:       normalizeSubSektor(r.SubSector),
			SubIndustry:     r.SubIndustry,
			SubIndustryCode: r.SubIndustryCode,
		})

		if r.IndexCode == nil || strings.TrimSpace(*r.IndexCode) == "" {
			continue // no membership (suspended rows) — sector kept above
		}
		for _, code := range strings.Split(*r.IndexCode, ",") {
			code = strings.TrimSpace(code)
			if code == "" {
				continue
			}
			indices = append(indices, entity.TickerIndex{
				TickerCode:    r.StockCode,
				IndexCode:     code,
				EffectiveDate: effectiveDate,
			})
		}
	}
	return tickers, indices
}

// normalizeSector maps the screener's dirty sector labels to clean values:
// "No Sector" (KETR) → nil (no classification); "CONSUMER GOODS INDUSTRY"
// (VICI, a legacy label) → "Consumer Non-Cyclicals". Everything else passes
// through trimmed.
func normalizeSector(s string) *string {
	switch strings.TrimSpace(s) {
	case "", "No Sector":
		return nil
	case "CONSUMER GOODS INDUSTRY":
		return strPtr("Consumer Non-Cyclicals")
	default:
		return strPtr(strings.TrimSpace(s))
	}
}

// normalizeSubSektor maps the screener's dirty sub-sector label to nil — KETR
// emits "No Subsector" alongside its "No Sector", the same garbage class, so
// it is not stored verbatim.
func normalizeSubSektor(s string) *string {
	switch strings.TrimSpace(s) {
	case "", "No Subsector":
		return nil
	default:
		return strPtr(strings.TrimSpace(s))
	}
}

// strPtr returns a pointer to s.
func strPtr(s string) *string { return &s }
