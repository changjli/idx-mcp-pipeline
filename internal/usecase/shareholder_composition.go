package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// ShareholderCompositionReader is the read surface the MCP server depends on,
// so its handler tests can wire a fake and run without a DB
// (CorporateActionsReader precedent).
type ShareholderCompositionReader interface {
	GetShareholderComposition(ctx context.Context, ticker string, from, to time.Time) (*ShareholderCompositionResponse, error)
}

// ShareholderCompositionUseCase reads the KSEI balance-position rows from the
// shareholder_composition table. The table is populated by the monthly
// ksei:balancepos task (issue 08) — this usecase is a pure DB read, so the
// MCP request path never touches an upstream host (Heroku H12 constraint,
// ADR-0009).
type ShareholderCompositionUseCase struct {
	DB   *sqlx.DB
	Log  *logrus.Logger
	Repo *repository.ShareholderCompositionRepository
}

func NewShareholderCompositionUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	repo *repository.ShareholderCompositionRepository,
) *ShareholderCompositionUseCase {
	return &ShareholderCompositionUseCase{DB: db, Log: log, Repo: repo}
}

// CompositionBreakdown is one KSEI investor-type block. Field names keep the
// KSEI codes verbatim: IS=Insurance, CP=Corporate, PF=Pension Fund,
// IB=Financial Institution, ID=Individu, MF=Mutual Fund, SC=Securities
// Company, FD=Foundation, OT=Others. Counts are scripless (SID-held)
// positions.
type CompositionBreakdown struct {
	IS int64 `json:"is"`
	CP int64 `json:"cp"`
	PF int64 `json:"pf"`
	IB int64 `json:"ib"`
	ID int64 `json:"id"`
	MF int64 `json:"mf"`
	SC int64 `json:"sc"`
	FD int64 `json:"fd"`
	OT int64 `json:"ot"`
}

// ShareholderCompositionRow is one month-end position for one ticker.
type ShareholderCompositionRow struct {
	PositionDate string               `json:"position_date"` // YYYY-MM-DD
	SecNum       int64                `json:"sec_num"`       // registered securities (listed shares)
	Price        int64                `json:"price"`
	Local        CompositionBreakdown `json:"local"`
	LocalTotal   int64                `json:"local_total"`
	Foreign      CompositionBreakdown `json:"foreign"`
	ForeignTotal int64                `json:"foreign_total"`
	Total        int64                `json:"total"` // local_total + foreign_total
}

// ShareholderCompositionResponse is the structured MCP tool result. Rows are
// ascending by position date (the repository's ordering); an empty range
// yields an empty list.
type ShareholderCompositionResponse struct {
	Ticker string                      `json:"ticker"`
	From   string                      `json:"from"`
	To     string                      `json:"to"`
	Count  int                         `json:"count"`
	Rows   []ShareholderCompositionRow `json:"rows"`
}

// GetShareholderComposition returns one ticker's stored KSEI balance-position
// rows with position_date in [from, to] (inclusive). Pure DB read — no
// upstream call. Coverage is bounded by what the monthly task has ingested
// (the preceding month-end and older, after the first successful month).
func (uc *ShareholderCompositionUseCase) GetShareholderComposition(ctx context.Context, ticker string, from, to time.Time) (*ShareholderCompositionResponse, error) {
	if from.After(to) {
		return nil, ErrInvalidRange
	}
	norm := strings.ToUpper(strings.TrimSpace(ticker))
	if !tickerPattern.MatchString(norm) {
		return nil, ErrInvalidTicker
	}

	rows, err := uc.Repo.FindByTickerRange(uc.DB, norm, from, to)
	if err != nil {
		return nil, fmt.Errorf("read shareholder composition: %w", err)
	}

	out := make([]ShareholderCompositionRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, ShareholderCompositionRow{
			PositionDate: r.PositionDate.Format("2006-01-02"),
			SecNum:       r.SecNum,
			Price:        r.Price,
			Local: CompositionBreakdown{
				IS: r.LocalIS, CP: r.LocalCP, PF: r.LocalPF, IB: r.LocalIB, ID: r.LocalID,
				MF: r.LocalMF, SC: r.LocalSC, FD: r.LocalFD, OT: r.LocalOT,
			},
			LocalTotal: r.LocalTotal,
			Foreign: CompositionBreakdown{
				IS: r.ForeignIS, CP: r.ForeignCP, PF: r.ForeignPF, IB: r.ForeignIB, ID: r.ForeignID,
				MF: r.ForeignMF, SC: r.ForeignSC, FD: r.ForeignFD, OT: r.ForeignOT,
			},
			ForeignTotal: r.ForeignTotal,
			Total:        r.Total,
		})
	}
	return &ShareholderCompositionResponse{
		Ticker: norm,
		From:   from.Format("2006-01-02"),
		To:     to.Format("2006-01-02"),
		Count:  len(out),
		Rows:   out,
	}, nil
}
