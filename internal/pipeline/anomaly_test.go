package pipeline

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// fakeDailyPriceSource serves a canned candidate list; the detector does not
// use ExistsForDate (that self-sync lives in the asynq handler).
type fakeDailyPriceSource struct {
	candidates []repository.AnomalyCandidate
	err        error
}

func (f *fakeDailyPriceSource) AnomalyCandidates(tradingDay time.Time) ([]repository.AnomalyCandidate, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.candidates, nil
}

// fakeAnomalySink records inserts; failedTickers makes Insert fail so the
// detector's skip-and-continue policy can be asserted.
type fakeAnomalySink struct {
	inserted      []*entity.Anomaly
	failedTickers map[string]bool
}

func (f *fakeAnomalySink) Insert(anomaly *entity.Anomaly) error {
	if f.failedTickers[anomaly.Ticker] {
		return errFakeInsert
	}
	f.inserted = append(f.inserted, anomaly)
	return nil
}

var errFakeInsert = errors.New("fake insert failure")

func newTestDetector(cands []repository.AnomalyCandidate, sink AnomalySink) (*AnomalyDetector, *fakeDailyPriceSource) {
	src := &fakeDailyPriceSource{candidates: cands}
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel) // silence skipped-ticker infos in test output
	return NewAnomalyDetector(src, sink, log, DefaultADTVMinValue), src
}

func TestVolumeAnomaly(t *testing.T) {
	today := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)

	spikeVol := int64(2800000)
	belowVol := int64(1500000)
	atThresholdVol := int64(2000000)
	baseline := 1000000.0

	tests := []struct {
		name    string
		cand    repository.AnomalyCandidate
		wantNil bool
		wantMag float64
	}{
		{
			name: "spike above threshold",
			cand: repository.AnomalyCandidate{
				Ticker: "AALI", TodayVolume: &spikeVol,
				BaselineVolume: &baseline, BaselineDays: 20,
			},
			wantMag: 180.0,
		},
		{
			name: "below threshold",
			cand: repository.AnomalyCandidate{
				Ticker: "AALI", TodayVolume: &belowVol,
				BaselineVolume: &baseline, BaselineDays: 20,
			},
			wantNil: true,
		},
		{
			name: "exactly at threshold is not a spike",
			cand: repository.AnomalyCandidate{
				Ticker: "AALI", TodayVolume: &atThresholdVol,
				BaselineVolume: &baseline, BaselineDays: 20,
			},
			wantNil: true,
		},
		{
			name: "insufficient history skipped",
			cand: repository.AnomalyCandidate{
				Ticker: "AALI", TodayVolume: &spikeVol,
				BaselineVolume: &baseline, BaselineDays: 19,
			},
			wantNil: true,
		},
		{
			name: "nil volume",
			cand: repository.AnomalyCandidate{
				Ticker: "AALI", BaselineVolume: &baseline, BaselineDays: 20,
			},
			wantNil: true,
		},
		{
			name: "zero baseline",
			cand: repository.AnomalyCandidate{
				Ticker: "AALI", TodayVolume: &spikeVol,
				BaselineVolume: &baseline, BaselineDays: 20,
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "zero baseline" {
				zero := 0.0
				tt.cand.BaselineVolume = &zero
			}
			a := volumeAnomaly(tt.cand, today)
			if tt.wantNil {
				if a != nil {
					t.Errorf("expected nil anomaly, got %+v", a)
				}
				return
			}
			if a == nil {
				t.Fatal("expected anomaly, got nil")
			}
			if a.Type != "volume" {
				t.Errorf("expected type volume, got %s", a.Type)
			}
			if a.Direction != "up" {
				t.Errorf("expected direction up, got %s", a.Direction)
			}
			if a.MagnitudePct == nil || math.Abs(*a.MagnitudePct-tt.wantMag) > 0.001 {
				t.Errorf("expected magnitude %v, got %v", tt.wantMag, a.MagnitudePct)
			}
			if a.ObservedValue == nil || *a.ObservedValue != float64(spikeVol) {
				t.Errorf("expected observed %v, got %v", float64(spikeVol), a.ObservedValue)
			}
			if a.BaselineRef == nil || math.Abs(*a.BaselineRef-baseline) > 0.001 {
				t.Errorf("expected baseline_ref %v, got %v", baseline, a.BaselineRef)
			}
			if a.PriorValue != nil {
				t.Errorf("expected nil prior_value, got %v", a.PriorValue)
			}
		})
	}
}

func TestPriceAnomaly(t *testing.T) {
	today := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)

	rise := 110.0
	drop := 95.0
	small := 104.0
	atThreshold := 105.0
	prev := 100.0

	tests := []struct {
		name    string
		cand    repository.AnomalyCandidate
		wantNil bool
		wantDir string
		wantMag float64
	}{
		{
			name:    "rise above threshold",
			cand:    repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &rise, PrevClose: &prev},
			wantDir: "up", wantMag: 10.0,
		},
		{
			name:    "drop at threshold",
			cand:    repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &drop, PrevClose: &prev},
			wantDir: "down", wantMag: -5.0,
		},
		{
			name:    "below threshold",
			cand:    repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &small, PrevClose: &prev},
			wantNil: true,
		},
		{
			name:    "exactly at threshold",
			cand:    repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &atThreshold, PrevClose: &prev},
			wantDir: "up", wantMag: 5.0,
		},
		{
			name:    "nil prev close",
			cand:    repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &rise},
			wantNil: true,
		},
		{
			name:    "zero prev close",
			cand:    repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &rise, PrevClose: &zeroClose},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := priceAnomaly(tt.cand, today)
			if tt.wantNil {
				if a != nil {
					t.Errorf("expected nil anomaly, got %+v", a)
				}
				return
			}
			if a == nil {
				t.Fatal("expected anomaly, got nil")
			}
			if a.Type != "price" {
				t.Errorf("expected type price, got %s", a.Type)
			}
			if a.Direction != tt.wantDir {
				t.Errorf("expected direction %s, got %s", tt.wantDir, a.Direction)
			}
			if a.MagnitudePct == nil || math.Abs(*a.MagnitudePct-tt.wantMag) > 0.001 {
				t.Errorf("expected magnitude %v, got %v", tt.wantMag, a.MagnitudePct)
			}
			if a.ObservedValue == nil || *a.ObservedValue != *tt.cand.TodayClose {
				t.Errorf("expected observed %v, got %v", *tt.cand.TodayClose, a.ObservedValue)
			}
			if a.PriorValue == nil || *a.PriorValue != *tt.cand.PrevClose {
				t.Errorf("expected prior %v, got %v", *tt.cand.PrevClose, a.PriorValue)
			}
			if a.BaselineRef != nil {
				t.Errorf("expected nil baseline_ref, got %v", a.BaselineRef)
			}
		})
	}
}

var zeroClose = 0.0

func TestPassesADTV(t *testing.T) {
	illiquid := int64(2_000_000)   // Rp 2M trade
	liquid := int64(6_000_000_000) // Rp 6B trade
	minADTV := DefaultADTVMinValue // Rp 5B

	tests := []struct {
		name    string
		cand    repository.AnomalyCandidate
		minADTV int64
		want    bool
	}{
		{
			name:    "liquid ticker passes",
			cand:    repository.AnomalyCandidate{Ticker: "BBCA", TodayValue: &liquid},
			minADTV: minADTV,
			want:    true,
		},
		{
			name:    "illiquid gorengan excluded",
			cand:    repository.AnomalyCandidate{Ticker: "GORENG", TodayValue: &illiquid},
			minADTV: minADTV,
			want:    false,
		},
		{
			name:    "exactly at threshold passes",
			cand:    repository.AnomalyCandidate{Ticker: "BBCA", TodayValue: &minADTV},
			minADTV: minADTV,
			want:    true,
		},
		{
			name:    "nil value does not trigger filter",
			cand:    repository.AnomalyCandidate{Ticker: "BBCA"},
			minADTV: minADTV,
			want:    true,
		},
		{
			name:    "filter disabled when minADTV <= 0",
			cand:    repository.AnomalyCandidate{Ticker: "GORENG", TodayValue: &illiquid},
			minADTV: 0,
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := passesADTV(tt.cand, tt.minADTV); got != tt.want {
				t.Errorf("passesADTV(%+v, %d) = %v, want %v", tt.cand, tt.minADTV, got, tt.want)
			}
		})
	}
}

func TestNewAnomalyDetector_DefaultsMinADTV(t *testing.T) {
	log := logrus.New()
	d := NewAnomalyDetector(&fakeDailyPriceSource{}, &fakeAnomalySink{}, log, 0)
	if d.minADTV != DefaultADTVMinValue {
		t.Errorf("expected minADTV %d, got %d", DefaultADTVMinValue, d.minADTV)
	}
}

func TestDetect_HappyPath(t *testing.T) {
	today := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	vol := int64(2800000)
	val := int64(6_000_000_000)
	baseline := 1000000.0
	close, prev := 110.0, 100.0

	d, sink := func() (*AnomalyDetector, *fakeAnomalySink) {
		sink := &fakeAnomalySink{}
		d, _ := newTestDetector([]repository.AnomalyCandidate{
			{
				Ticker: "AALI", TodayVolume: &vol, TodayValue: &val,
				BaselineVolume: &baseline, BaselineDays: 20,
				TodayClose: &close, PrevClose: &prev,
			},
		}, sink)
		return d, sink
	}()

	got, err := d.Detect(context.Background(), today)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 detected anomalies, got %d", len(got))
	}
	if got[0].Ticker != "AALI" || got[0].Type != "volume" {
		t.Errorf("expected volume anomaly first, got %+v", got[0])
	}
	if got[1].Ticker != "AALI" || got[1].Type != "price" {
		t.Errorf("expected price anomaly second, got %+v", got[1])
	}
	if len(sink.inserted) != 2 {
		t.Fatalf("expected 2 sink inserts, got %d", len(sink.inserted))
	}
	for _, a := range sink.inserted {
		if a.TradingDay != today {
			t.Errorf("expected TradingDay %v, got %v", today, a.TradingDay)
		}
	}
}

func TestDetect_ADTVFilterSkipsIlliquid(t *testing.T) {
	today := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	vol := int64(2800000)
	val := int64(2_000_000) // below 5B ADTV threshold
	baseline := 1000000.0
	close, prev := 110.0, 100.0

	d, sink := func() (*AnomalyDetector, *fakeAnomalySink) {
		sink := &fakeAnomalySink{}
		d, _ := newTestDetector([]repository.AnomalyCandidate{
			{
				Ticker: "GORENG", TodayVolume: &vol, TodayValue: &val,
				BaselineVolume: &baseline, BaselineDays: 20,
				TodayClose: &close, PrevClose: &prev,
			},
		}, sink)
		return d, sink
	}()

	got, err := d.Detect(context.Background(), today)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if len(got) != 0 || len(sink.inserted) != 0 {
		t.Errorf("expected illiquid ticker skipped entirely, got %d detected / %d inserted", len(got), len(sink.inserted))
	}
}

func TestDetect_InsertFailureSkipsRowButContinues(t *testing.T) {
	today := time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC)
	baseline := 1000000.0
	close, prev := 110.0, 100.0

	d, sink := func() (*AnomalyDetector, *fakeAnomalySink) {
		sink := &fakeAnomalySink{failedTickers: map[string]bool{"VFAIL": true}}
		d, _ := newTestDetector([]repository.AnomalyCandidate{
			// Ticker with spike but failing sink: both anomaly kinds must be
			// dropped without aborting the run.
			{
				Ticker: "VFAIL", TodayVolume: ptrInt64(2800000),
				TodayValue:     ptrInt64(6_000_000_000),
				BaselineVolume: &baseline, BaselineDays: 20,
				TodayClose: &close, PrevClose: &prev,
			},
			// Ticker after the failure must still be detected.
			{
				Ticker: "POK", TodayValue: ptrInt64(6_000_000_000),
				TodayClose: &close, PrevClose: &prev,
			},
		}, sink)
		return d, sink
	}()

	got, err := d.Detect(context.Background(), today)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if len(sink.inserted) != 1 {
		t.Fatalf("expected only POK inserted, got %d: %+v", len(sink.inserted), sink.inserted)
	}
	if len(got) != 1 || got[0].Ticker != "POK" || got[0].Type != "price" {
		t.Errorf("expected only POK price anomaly detected, got %+v", got)
	}
}

func TestDetect_SourceErrorPropagates(t *testing.T) {
	d := NewAnomalyDetector(&fakeDailyPriceSource{err: errFakeInsert}, &fakeAnomalySink{}, logrus.New(), DefaultADTVMinValue)
	if _, err := d.Detect(context.Background(), time.Now()); err == nil {
		t.Error("expected source error to propagate")
	}
}

func ptrInt64(v int64) *int64 { return &v }
