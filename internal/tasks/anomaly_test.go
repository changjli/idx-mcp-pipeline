package tasks

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

func TestDetectAnomaliesPayload_Marshal(t *testing.T) {
	p := DetectAnomaliesPayload{Date: "2026-08-08", Attempt: 3}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got DetectAnomaliesPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-08-08" {
		t.Errorf("expected date 2026-08-08, got %s", got.Date)
	}
	if got.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", got.Attempt)
	}
}

func TestDetectAnomaliesTask_TypeAndPayload(t *testing.T) {
	task, err := detectAnomaliesTask("2026-08-08", 0)
	if err != nil {
		t.Fatalf("detectAnomaliesTask: %v", err)
	}

	if task.Type() != TypeDetectAnomalies {
		t.Errorf("expected type %s, got %s", TypeDetectAnomalies, task.Type())
	}

	var got DetectAnomaliesPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != "2026-08-08" {
		t.Errorf("expected date 2026-08-08, got %s", got.Date)
	}
	if got.Attempt != 0 {
		t.Errorf("expected attempt 0, got %d", got.Attempt)
	}
}

func TestTaskKeyDetectAnomalies(t *testing.T) {
	key := TaskKey(TypeDetectAnomalies, "2026-08-08")
	expected := "detect:anomalies:2026-08-08"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
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
			name: "rise above threshold",
			cand: repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &rise, PrevClose: &prev},
			wantDir: "up", wantMag: 10.0,
		},
		{
			name: "drop at threshold",
			cand: repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &drop, PrevClose: &prev},
			wantDir: "down", wantMag: -5.0,
		},
		{
			name: "below threshold",
			cand: repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &small, PrevClose: &prev},
			wantNil: true,
		},
		{
			name: "exactly at threshold",
			cand: repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &atThreshold, PrevClose: &prev},
			wantDir: "up", wantMag: 5.0,
		},
		{
			name: "nil prev close",
			cand: repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &rise},
			wantNil: true,
		},
		{
			name: "zero prev close",
			cand: repository.AnomalyCandidate{Ticker: "AALI", TodayClose: &rise, PrevClose: &zeroClose},
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
	illiquid := int64(2_000_000)    // Rp 2M trade
	liquid := int64(6_000_000_000)  // Rp 6B trade
	minADTV := DefaultADTVMinValue  // Rp 5B

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
