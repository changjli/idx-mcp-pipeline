package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// BrokerStockSummarySource is the source_status key the broker writers record.
// It is redeclared here (rather than imported from internal/tasks) because tasks
// imports usecase; the value must stay in sync with tasks.TypeBrokerStockSummary.
const BrokerStockSummarySource = "idx:broker_stock_summary"

// brokerFreshnessBar returns the last stored trading day of the previous
// calendar month (WIB). The monthly archiver legitimately writes only month
// grain, so "fresh" means the newest stored window reaches this bar; stale
// means a closed month is missing.
func brokerFreshnessBar(db *sqlx.DB, dailyPriceRepo *repository.DailyPriceRepository, now time.Time) time.Time {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		loc = time.UTC
	}
	local := now.In(loc)
	firstOfMonth := time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, loc)
	prevMonthStart := firstOfMonth.AddDate(0, -1, 0)
	calendarEnd := firstOfMonth.AddDate(0, 0, -1)
	if dailyPriceRepo == nil {
		return calendarEnd
	}
	days, err := dailyPriceRepo.TradingDaysInRangeAll(db, prevMonthStart, calendarEnd)
	if err != nil || len(days) == 0 {
		return calendarEnd
	}
	return days[len(days)-1]
}

type PipelineUseCase struct {
	DB               *sqlx.DB
	Log              *logrus.Logger
	Validate         *validator.Validate
	SourceStatusRepo *repository.SourceStatusRepository
	AlertRepo        *repository.AlertRepository
	BrokerRepo       *repository.BrokerStockSummaryRepository
	DailyPriceRepo   *repository.DailyPriceRepository
}

func NewPipelineUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	validate *validator.Validate,
	sourceStatusRepo *repository.SourceStatusRepository,
	alertRepo *repository.AlertRepository,
	brokerRepo *repository.BrokerStockSummaryRepository,
	dailyPriceRepo *repository.DailyPriceRepository,
) *PipelineUseCase {
	return &PipelineUseCase{
		DB:               db,
		Log:              log,
		Validate:         validate,
		SourceStatusRepo: sourceStatusRepo,
		AlertRepo:        alertRepo,
		BrokerRepo:       brokerRepo,
		DailyPriceRepo:   dailyPriceRepo,
	}
}

// SourceHealth is one source's health row in a get_pipeline_status response. For
// the broker source, Stale is computed from the age of the newest stored window
// end in trading days, not from wall-clock time: a window ending today is fresh
// even when the last HTTP success was hours ago.
type SourceHealth struct {
	Source           string  `json:"source"`
	LastSuccessAt    *string `json:"last_success_at"`
	Stale            bool    `json:"stale"`
	LastError        *string `json:"last_error"`
	HighWaterMark    *string `json:"high_water_mark"`
	StaleTradingDays *int    `json:"stale_trading_days"`
}

// AlertRow is one recent alert in a get_pipeline_status response.
type AlertRow struct {
	ID        int64  `json:"id"`
	Source    string `json:"source"`
	AlertType string `json:"alert_type"`
	Message   string `json:"message"`
	RaisedAt  string `json:"raised_at"`
}

// PipelineStatusData is the data payload of a get_pipeline_status response.
type PipelineStatusData struct {
	Sources      []SourceHealth `json:"sources"`
	RecentAlerts []AlertRow     `json:"recent_alerts"`
}

// GetPipelineStatus returns per-source health from source_status plus the most
// recent alerts. Staleness is computed live, time-based (now - last_success_at
// > max_age) rather than trusting the stored flag, which is only sampled at
// the last write.
func (uc *PipelineUseCase) GetPipelineStatus(ctx context.Context) (*PipelineStatusData, error) {
	statuses, err := uc.SourceStatusRepo.FindAll(uc.DB)
	if err != nil {
		return nil, fmt.Errorf("query source status: %w", err)
	}
	alerts, err := uc.AlertRepo.FindRecent(uc.DB, 10)
	if err != nil {
		return nil, fmt.Errorf("query alerts: %w", err)
	}

	resp := &PipelineStatusData{Sources: []SourceHealth{}, RecentAlerts: []AlertRow{}}
	var latest time.Time
	latestOK := false
	if uc.DailyPriceRepo != nil {
		if l, err := uc.DailyPriceRepo.LatestTradingDayAll(uc.DB); err == nil {
			latest, latestOK = *l, true
		}
	}
	now := time.Now()
	brokerBar := brokerFreshnessBar(uc.DB, uc.DailyPriceRepo, now)
	for _, s := range statuses {
		var last *string
		if s.LastSuccessAt != nil {
			v := s.LastSuccessAt.Format(time.RFC3339)
			last = &v
		}

		// The broker source's freshness is the age of the newest window end, in
		// trading days. The live MAX is preferred over the stored watermark: a
		// window written by the read path advances freshness even before a writer
		// updates source_status.
		hwm := s.HighWaterMark
		if s.Source == BrokerStockSummarySource && uc.BrokerRepo != nil {
			if live, err := uc.BrokerRepo.MaxWindowEnd(uc.DB, broker.InvestorForeign); err == nil {
				hwm = live
			}
		}
		var hwmText *string
		if hwm != nil {
			v := hwm.Format("2006-01-02")
			hwmText = &v
		}
		var staleDays *int
		if hwm != nil && latestOK {
			n := 0
			if latest.After(*hwm) {
				if counted, err := uc.DailyPriceRepo.TradingDaysAfter(uc.DB, *hwm, latest); err == nil {
					n = counted
				}
			}
			staleDays = &n
		}

		stale := s.IsStale(now)
		if s.Source == BrokerStockSummarySource {
			stale = hwm == nil || hwm.Before(brokerBar)
		}
		resp.Sources = append(resp.Sources, SourceHealth{
			Source:           s.Source,
			LastSuccessAt:    last,
			Stale:            stale,
			LastError:        s.LastError,
			HighWaterMark:    hwmText,
			StaleTradingDays: staleDays,
		})
	}
	for _, a := range alerts {
		resp.RecentAlerts = append(resp.RecentAlerts, AlertRow{
			ID:        a.ID,
			Source:    a.Source,
			AlertType: a.AlertType,
			Message:   a.Message,
			RaisedAt:  a.RaisedAt.Format(time.RFC3339),
		})
	}
	return resp, nil
}
