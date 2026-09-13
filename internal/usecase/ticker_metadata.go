package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// TickerMetadataReader is the read surface the MCP server depends on, so its
// handler tests can wire a fake and run without a DB (CorporateActionsReader
// precedent).
type TickerMetadataReader interface {
	GetTickerMetadata(ctx context.Context, ticker *string, asOf *time.Time) (*TickerMetadataResponse, error)
}

// TickerMetadataUseCase reads ticker metadata from the seeded data: the
// sector/industry taxonomy columns the 15b seeder owns (current state — the
// screener snapshot has no history) plus point-in-time index membership from
// ticker_indices. Pure DB read — the MCP request path never touches the
// nodriver sidecar (Heroku H12 constraint, ADR-0009).
type TickerMetadataUseCase struct {
	DB         *sqlx.DB
	Log        *logrus.Logger
	TickerRepo *repository.TickerRepository
	IndexRepo  *repository.TickerIndexRepository
}

func NewTickerMetadataUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	tickerRepo *repository.TickerRepository,
	indexRepo *repository.TickerIndexRepository,
) *TickerMetadataUseCase {
	return &TickerMetadataUseCase{DB: db, Log: log, TickerRepo: tickerRepo, IndexRepo: indexRepo}
}

// TickerMetadata is one ticker's metadata in the response. Sector/industry are
// the screener's canonical English labels (nullable — a few dirty rows have no
// classification); Indices are the index codes of the effective membership
// snapshot, ascending.
type TickerMetadata struct {
	Ticker          string   `json:"ticker"`
	Sector          *string  `json:"sector"`
	Industry        *string  `json:"industry"`
	SubSector       *string  `json:"sub_sector"`
	SubIndustry     *string  `json:"sub_industry"`
	SubIndustryCode *string  `json:"sub_industry_code"`
	Indices         []string `json:"indices"`
}

// TickerMetadataResponse is the structured MCP tool result. Tickers carries
// one entry when queried per ticker, the active universe when ticker is
// omitted. EffectiveDate is the membership snapshot date the read resolved
// to (empty when no snapshot exists); sector columns are current state, not
// dated by it.
type TickerMetadataResponse struct {
	Count         int              `json:"count"`
	EffectiveDate string           `json:"effective_date"`
	Tickers       []TickerMetadata `json:"tickers"`
}

// GetTickerMetadata returns sector/industry metadata + effective index
// membership, optionally for one ticker (nil = full active universe) at an
// asOf date (nil = latest membership snapshot). Point-in-time semantics: the
// membership rows come from the single latest snapshot at or before asOf —
// historical screens must not see today's constituents mixed with older ones.
func (uc *TickerMetadataUseCase) GetTickerMetadata(ctx context.Context, ticker *string, asOf *time.Time) (*TickerMetadataResponse, error) {
	var norm string
	if ticker != nil {
		norm = strings.ToUpper(strings.TrimSpace(*ticker))
		if !tickerPattern.MatchString(norm) {
			return nil, ErrInvalidTicker
		}
	}

	var tickerPtr *string
	if norm != "" {
		tickerPtr = &norm
	}
	snapshot, err := uc.IndexRepo.FindMembershipAt(uc.DB, tickerPtr, asOf)
	if err != nil {
		return nil, fmt.Errorf("read index membership: %w", err)
	}

	// Group the membership rows by ticker (the read is ordered ticker, index —
	// so per-ticker lists stay ascending).
	indicesByTicker := make(map[string][]string)
	for _, r := range snapshot.Rows {
		indicesByTicker[r.TickerCode] = append(indicesByTicker[r.TickerCode], r.IndexCode)
	}

	var tickers []entity.Ticker
	if tickerPtr != nil {
		t, err := uc.TickerRepo.FindByCode(uc.DB, norm)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("read ticker metadata: %w", err)
			}
			// Unknown ticker row (the validator passed it from the embedded
			// list): still report what the membership snapshot knows — sector
			// stays null rather than failing the call.
			if _, ok := indicesByTicker[norm]; ok {
				tickers = []entity.Ticker{{Code: norm}}
			}
		} else {
			tickers = []entity.Ticker{*t}
		}
	} else {
		tickers, err = uc.TickerRepo.FindAll(uc.DB)
		if err != nil {
			return nil, fmt.Errorf("read ticker metadata: %w", err)
		}
	}

	metas := make([]TickerMetadata, 0, len(tickers))
	for _, t := range tickers {
		indices := indicesByTicker[t.Code]
		if indices == nil {
			indices = []string{}
		}
		metas = append(metas, TickerMetadata{
			Ticker:          t.Code,
			Sector:          t.Sektor,
			Industry:        t.Industri,
			SubSector:       t.SubSektor,
			SubIndustry:     t.SubIndustry,
			SubIndustryCode: t.SubIndustryCode,
			Indices:         indices,
		})
	}
	return &TickerMetadataResponse{
		Count:         len(metas),
		EffectiveDate: formatSnapshotDate(snapshot.SnapshotDate),
		Tickers:       metas,
	}, nil
}

// formatSnapshotDate renders the snapshot date or "" when zero (no snapshot).
func formatSnapshotDate(d time.Time) string {
	if d.IsZero() {
		return ""
	}
	return d.Format("2006-01-02")
}
