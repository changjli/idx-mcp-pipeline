package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/pipeline"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// ─── Anomaly Grade fakes ─────────────────────────────────────────────────────

type fakeAnomalySource struct {
	byDate map[string][]entity.Anomaly
	err    error
}

func (f *fakeAnomalySource) FindByDate(_ *sqlx.DB, day string) ([]entity.Anomaly, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.byDate[day], nil
}

type fakeDisclosureSource struct {
	passed     []string
	err        error
	gotFrom    time.Time
	gotTo      time.Time
	gotTickers []string
}

func (f *fakeDisclosureSource) PassedTickersInWindow(_ *sqlx.DB, tickers []string, from, to time.Time) ([]string, error) {
	f.gotTickers = tickers
	f.gotFrom = from
	f.gotTo = to
	if f.err != nil {
		return nil, f.err
	}
	want := make(map[string]bool, len(tickers))
	for _, ticker := range tickers {
		want[ticker] = true
	}
	out := make([]string, 0, len(f.passed))
	for _, ticker := range f.passed {
		if want[ticker] {
			out = append(out, ticker)
		}
	}
	return out, nil
}

type fakeBrokerNetSource struct {
	nets           map[string]repository.TickerNet
	err            error
	gotTickers     []string
	gotTo          time.Time
	gotTradingDays int
}

func (f *fakeBrokerNetSource) SumNetByTickers(_ *sqlx.DB, tickers []string, to time.Time, tradingDays int) ([]repository.TickerNet, error) {
	f.gotTickers = tickers
	f.gotTo = to
	f.gotTradingDays = tradingDays
	if f.err != nil {
		return nil, f.err
	}
	out := make([]repository.TickerNet, 0, len(tickers))
	for _, ticker := range tickers {
		if net, ok := f.nets[ticker]; ok {
			out = append(out, net)
		}
	}
	return out, nil
}

// ─── Fixture ─────────────────────────────────────────────────────────────────

// anomalyGradeScenario is a hand-computed one-anchor fixture. Six anomaly
// ticker-days across the three Direction Buckets, with all Broker Confirmation
// states and both disclosure states represented. MIX1 carries two anomaly rows
// (volume + price down) to prove the ticker-day dedupe and the price-wins rule.
//
//	anchor 100, every horizon is the same return:
//	UP1 +10% (up, net +5bn, disclosure)   UP2 +10% (up, net -5bn)
//	DN1 -10% (down, net -5bn)             MIX1 -10% (vol+price:down, net 0)
//	VOL1 +20% (volume, net +5bn, disclosure)   VOL2 +20% (volume, unobserved)
//	NORM 0% (baseline only)
//
// Baseline = all seven; baseline median = +10%.
func anomalyGradeScenario() (time.Time, []time.Time, *fakeMeasurementPrices, *fakeAnomalySource, *fakeDisclosureSource, *fakeBrokerNetSource) {
	anchor := measurementAnchor()
	days := measurementCalendar(anchor, 55)
	targetDays := []time.Time{anchor, days[5], days[10], days[20]}

	returns := map[string]float64{
		"UP1": 0.10, "UP2": 0.10, "DN1": -0.10, "MIX1": -0.10, "VOL1": 0.20, "VOL2": 0.20, "NORM": 0,
	}
	candidates := make([]repository.ScreenCandidate, 0, len(returns))
	prices := make(map[string][]entity.DailyPrice, len(returns))
	for ticker, ret := range returns {
		candidates = append(candidates, repository.ScreenCandidate{
			Ticker: ticker, Value: i64p(100_000_000_000), Close: f64p(100),
		})
		prices[ticker] = measurementSeries(ticker, targetDays, 100, 100*(1+ret), 100*(1+ret), 100*(1+ret))
	}

	priceFake := &fakeMeasurementPrices{calendar: days, candidates: candidates, prices: prices}
	anomalies := &fakeAnomalySource{byDate: map[string][]entity.Anomaly{
		anchor.Format("2006-01-02"): {
			{Ticker: "UP1", Type: "price", Direction: "up"},
			{Ticker: "UP2", Type: "price", Direction: "up"},
			{Ticker: "DN1", Type: "price", Direction: "down"},
			{Ticker: "VOL1", Type: "volume", Direction: "up"},
			{Ticker: "VOL2", Type: "volume", Direction: "up"},
			{Ticker: "MIX1", Type: "volume", Direction: "up"},
			{Ticker: "MIX1", Type: "price", Direction: "down"},
		},
	}}
	disclosures := &fakeDisclosureSource{passed: []string{"UP1", "VOL1"}}
	brokers := &fakeBrokerNetSource{nets: map[string]repository.TickerNet{
		"UP1":  {Ticker: "UP1", Net: 5_000_000_000, Days: 5},
		"UP2":  {Ticker: "UP2", Net: -5_000_000_000, Days: 4},
		"DN1":  {Ticker: "DN1", Net: -5_000_000_000, Days: 3},
		"VOL1": {Ticker: "VOL1", Net: 5_000_000_000, Days: 6},
		"MIX1": {Ticker: "MIX1", Net: 0, Days: 2},
		// VOL2 absent: broker-unobserved.
	}}
	return anchor, days, priceFake, anomalies, disclosures, brokers
}

func newAnomalyGradeEngine(prices MeasurementPriceSource, anomalies MeasurementAnomalySource, disclosures MeasurementDisclosureSource, brokers MeasurementBrokerNetSource) *MeasurementEngine {
	engine := NewMeasurementEngine(nil, logrus.New(), nil, prices, nil, "testrev")
	engine.Anomalies = anomalies
	engine.Disclosures = disclosures
	engine.BrokerNet = brokers
	return engine
}

func findDirection(t *testing.T, grade *AnomalyGradeReport, direction string) AnomalyDirectionGrade {
	t.Helper()
	for _, block := range grade.Directions {
		if block.Direction == direction {
			return block
		}
	}
	t.Fatalf("direction %q not in report: %+v", direction, grade.Directions)
	return AnomalyDirectionGrade{}
}

func findBucket(t *testing.T, buckets []AnomalyBucket, state string) AnomalyBucket {
	t.Helper()
	for _, bucket := range buckets {
		if bucket.State == state {
			return bucket
		}
	}
	t.Fatalf("bucket %q not in %+v", state, buckets)
	return AnomalyBucket{}
}

func horizonExcess(t *testing.T, horizons []HorizonStats, horizon int) *float64 {
	t.Helper()
	for _, stats := range horizons {
		if stats.Horizon == horizon {
			return stats.Excess
		}
	}
	t.Fatalf("horizon %d not in %+v", horizon, horizons)
	return nil
}

// ─── Direction and dedupe tests ──────────────────────────────────────────────

// TestGradeAnomalies_DirectionBuckets — the three-way split, the ticker-day
// dedupe, and the price-wins rule (a volume + price:down day is `down`, and a
// volume-only day is a directionless `volume_spike` even though the detector
// stamped it `up`).
func TestGradeAnomalies_DirectionBuckets(t *testing.T) {
	anchor, _, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	grade, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
		GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if grade.Kind != anomalyGradeKind || grade.GitRevision != "testrev" {
		t.Fatalf("kind/revision = %q/%q", grade.Kind, grade.GitRevision)
	}
	// Seven anomaly rows dedupe to six ticker-days (MIX1 carried two rows).
	if grade.Counts.Observations != 6 || grade.Counts.DistinctTickers != 6 {
		t.Fatalf("observations/distinct = %d/%d, want 6/6", grade.Counts.Observations, grade.Counts.DistinctTickers)
	}
	if grade.Counts.Baseline != 7 {
		t.Fatalf("baseline = %d, want 7", grade.Counts.Baseline)
	}

	up := findDirection(t, grade, directionUp)
	down := findDirection(t, grade, directionDown)
	volume := findDirection(t, grade, directionVolumeSpike)
	if up.N != 2 || down.N != 2 || volume.N != 2 {
		t.Fatalf("bucket sizes = up %d / down %d / volume_spike %d, want 2/2/2", up.N, down.N, volume.N)
	}

	// MIX1 lands in `down` (price wins over the volume row), never volume_spike.
	if row := anomalyRow(down.Rows, "MIX1"); row == nil || len(row.AnomalyTypes) != 2 {
		t.Fatalf("MIX1 drill-down = %+v, want a `down` row with both anomaly types", row)
	}

	// Baseline median is +10% at every horizon.
	measurementApprox(t, "up 5d excess", *horizonExcess(t, up.Overall, 5), 0.00)
	measurementApprox(t, "down 20d excess", *horizonExcess(t, down.Overall, 20), -0.20)
	measurementApprox(t, "volume_spike 20d excess", *horizonExcess(t, volume.Overall, 20), 0.10)
}

// TestGradeAnomalies_BrokerStates — directionally aligned confirmation, the
// directionless volume_spike split by net sign, the observed net-zero boundary,
// and the unobserved bucket.
func TestGradeAnomalies_BrokerStates(t *testing.T) {
	anchor, _, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	grade, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
		GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	up := findDirection(t, grade, directionUp)
	down := findDirection(t, grade, directionDown)
	volume := findDirection(t, grade, directionVolumeSpike)

	// up: UP1 confirmed (net +), UP2 not confirmed (net -).
	if b := findBucket(t, up.Broker, brokerConfirmed); b.N != 1 {
		t.Fatalf("up/confirmed n = %d, want 1", b.N)
	}
	if b := findBucket(t, up.Broker, brokerNotConfirmed); b.N != 1 {
		t.Fatalf("up/not_confirmed n = %d, want 1", b.N)
	}
	// down: DN1 confirmed (net -), MIX1 observed net-zero -> not confirmed.
	if b := findBucket(t, down.Broker, brokerConfirmed); b.N != 1 {
		t.Fatalf("down/confirmed n = %d, want 1 (DN1)", b.N)
	}
	if b := findBucket(t, down.Broker, brokerNotConfirmed); b.N != 1 {
		t.Fatalf("down/not_confirmed n = %d, want 1 (MIX1 net-zero)", b.N)
	}
	// volume_spike: VOL1 accumulation, VOL2 unobserved.
	if b := findBucket(t, volume.Broker, brokerAccumulation); b.N != 1 {
		t.Fatalf("volume_spike/accumulation n = %d, want 1", b.N)
	}
	if b := findBucket(t, volume.Broker, brokerDistribution); b.N != 0 {
		t.Fatalf("volume_spike/distribution n = %d, want 0", b.N)
	}
	if b := findBucket(t, volume.Broker, brokerNetZero); b.N != 0 {
		t.Fatalf("volume_spike/net_zero n = %d, want 0", b.N)
	}
	unobserved := findBucket(t, volume.Broker, brokerUnobserved)
	if unobserved.N != 1 {
		t.Fatalf("volume_spike/unobserved n = %d, want 1 (VOL2)", unobserved.N)
	}
	// The unobserved bucket still carries the observation's Forward Return.
	if median := anomalousMedian(unobserved.Horizons, 20); median == nil {
		t.Fatalf("unobserved bucket 20d median is nil; the observation was dropped, not reported")
	} else {
		measurementApprox(t, "unobserved bucket 20d median", *median, 0.20)
	}
	if grade.Counts.Unobserved != 1 {
		t.Fatalf("counts.unobserved = %d, want 1", grade.Counts.Unobserved)
	}
}

// TestGradeAnomalies_DisclosureAndCross — the disclosure split and the
// broker x disclosure cross, restricted to observed-flow rows.
func TestGradeAnomalies_DisclosureAndCross(t *testing.T) {
	anchor, _, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	grade, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
		GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	up := findDirection(t, grade, directionUp)
	if b := findBucket(t, up.Disclosure, disclosurePresent); b.N != 1 {
		t.Fatalf("up/present n = %d, want 1 (UP1)", b.N)
	}
	if b := findBucket(t, up.Disclosure, disclosureAbsent); b.N != 1 {
		t.Fatalf("up/absent n = %d, want 1 (UP2)", b.N)
	}

	// The cross is over observed-flow rows only: the volume_spike cross has
	// accumulation x present/absent but no unobserved cell.
	if b := findBucket(t, up.Cross, brokerConfirmed+"+"+disclosurePresent); b.N != 1 {
		t.Fatalf("up cross confirmed+present n = %d, want 1", b.N)
	}
	if b := findBucket(t, up.Cross, brokerNotConfirmed+"+"+disclosureAbsent); b.N != 1 {
		t.Fatalf("up cross not_confirmed+absent n = %d, want 1", b.N)
	}
	volume := findDirection(t, grade, directionVolumeSpike)
	for _, bucket := range volume.Cross {
		if strings.Contains(bucket.State, brokerUnobserved) {
			t.Fatalf("volume_spike cross contains an unobserved cell: %q", bucket.State)
		}
	}
	if b := findBucket(t, volume.Cross, brokerAccumulation+"+"+disclosurePresent); b.N != 1 {
		t.Fatalf("volume_spike cross accumulation+present n = %d, want 1", b.N)
	}
}

// TestGradeAnomalies_Windows — Broker Confirmation reads the Flow Lead-in
// Window (ending the day BEFORE the anchor) and disclosure presence reads the
// anomaly-gate lookback, not a same-day-only match.
func TestGradeAnomalies_Windows(t *testing.T) {
	anchor, _, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	grade, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
		GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}, FlowWindowDays: 15})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if want := anchor.AddDate(0, 0, -1); !brokers.gotTo.Equal(want) {
		t.Fatalf("broker window end = %s, want anchor-1 %s", brokers.gotTo, want)
	}
	if brokers.gotTradingDays != 15 {
		t.Fatalf("broker window length = %d, want the request's 15", brokers.gotTradingDays)
	}
	if want := anchor.AddDate(0, 0, -pipeline.DisclosureFilterLookbackDays); !disclosures.gotFrom.Equal(want) {
		t.Fatalf("disclosure from = %s, want anchor-%d %s", disclosures.gotFrom, pipeline.DisclosureFilterLookbackDays, want)
	}
	if !disclosures.gotTo.Equal(anchor) {
		t.Fatalf("disclosure to = %s, want the anchor %s", disclosures.gotTo, anchor)
	}
	if grade.FlowWindowDays != 15 || grade.BrokerWindowEnd != brokerWindowEndLabel {
		t.Fatalf("report window = %d/%s, want 15/%s", grade.FlowWindowDays, grade.BrokerWindowEnd, brokerWindowEndLabel)
	}
}

// TestGradeAnomalies_PoolsAnchors — two anchors pool their observations and the
// per-date blocks survive; an anchor with no stored anomalies contributes only
// a baseline.
func TestGradeAnomalies_PoolsAnchors(t *testing.T) {
	anchor, days, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	second := days[30]
	anomalies.byDate[second.Format("2006-01-02")] = []entity.Anomaly{
		{Ticker: "UP1", Type: "price", Direction: "up"},
	}

	grade, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
		GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor, second}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(grade.ReplayDates) != 2 || len(grade.PerDate) != 2 {
		t.Fatalf("replay dates/per-date = %d/%d, want 2/2", len(grade.ReplayDates), len(grade.PerDate))
	}
	if grade.Counts.Observations != 7 {
		t.Fatalf("pooled observations = %d, want 7 (6 at the first anchor, 1 at the second)", grade.Counts.Observations)
	}
	// The second anchor's prices are keyed off the same fixture calendar, so its
	// UP1 row has a forward window; the pooled up bucket is 3 observations.
	if up := findDirection(t, grade, directionUp); up.N != 3 {
		t.Fatalf("pooled up n = %d, want 3", up.N)
	}
}

// TestGradeAnomalies_SkipsAndGuards — an unreplayable anchor is skipped and
// recorded, none-gradable is an error, a holdout breach is a hard error, and an
// out-of-range flow window is a structured error.
func TestGradeAnomalies_SkipsAndGuards(t *testing.T) {
	anchor, days, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	engine := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers)

	grade, err := engine.GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor, days[50]}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(grade.SkippedAnchors) != 1 || grade.SkippedAnchors[0].AsOf != days[50].Format("2006-01-02") {
		t.Fatalf("skipped anchors = %+v, want the too-recent day50", grade.SkippedAnchors)
	}

	if _, err := engine.GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{days[50]}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("none-gradable err = %v, want ErrInvalidArgument", err)
	}

	seal := days[10]
	if _, err := engine.GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}, HoldoutStart: &seal}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("holdout breach err = %v, want ErrInvalidArgument", err)
	}

	if _, err := engine.GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}, FlowWindowDays: 999}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("bad flow window err = %v, want ErrInvalidArgument", err)
	}
	if _, err := engine.GradeAnomalies(context.Background(), AnomalyGradeOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("no anchors err = %v, want ErrInvalidArgument", err)
	}
}

// TestGradeAnomalies_SuccessCriteria — the third standing question (the
// confluence split the screen report stubs out) is answered from the numbers.
func TestGradeAnomalies_SuccessCriteria(t *testing.T) {
	anchor, _, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
	grade, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
		GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(grade.SuccessCriteria) != 3 {
		t.Fatalf("criteria = %d, want 3", len(grade.SuccessCriteria))
	}
	third := grade.SuccessCriteria[2]
	if !strings.Contains(third.Answer, "broker-confirmed") || !strings.Contains(third.Answer, "disclosure-present") {
		t.Fatalf("confluence answer = %q, want the broker and disclosure split", third.Answer)
	}
	if !strings.Contains(grade.SuccessCriteria[0].Answer, "positive Excess") {
		t.Fatalf("excess answer = %q, want the positive horizons", grade.SuccessCriteria[0].Answer)
	}
}

// TestGradeAnomalies_ReadErrorsPropagate — a database failure is an error, never
// an empty report.
func TestGradeAnomalies_ReadErrorsPropagate(t *testing.T) {
	boom := errors.New("db exploded")
	cases := []struct {
		name   string
		mutate func(*fakeAnomalySource, *fakeDisclosureSource, *fakeBrokerNetSource)
	}{
		{name: "anomalies", mutate: func(a *fakeAnomalySource, _ *fakeDisclosureSource, _ *fakeBrokerNetSource) { a.err = boom }},
		{name: "disclosures", mutate: func(_ *fakeAnomalySource, d *fakeDisclosureSource, _ *fakeBrokerNetSource) { d.err = boom }},
		{name: "broker net", mutate: func(_ *fakeAnomalySource, _ *fakeDisclosureSource, b *fakeBrokerNetSource) { b.err = boom }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			anchor, _, prices, anomalies, disclosures, brokers := anomalyGradeScenario()
			tc.mutate(anomalies, disclosures, brokers)
			_, err := newAnomalyGradeEngine(prices, anomalies, disclosures, brokers).
				GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{anchor}})
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want the read failure", err)
			}
		})
	}
}

// TestGradeAnomalies_RequiresSources — a mis-wired engine refuses rather than
// panicking or writing an empty report.
func TestGradeAnomalies_RequiresSources(t *testing.T) {
	_, _, prices, _, _, _ := anomalyGradeScenario()
	engine := NewMeasurementEngine(nil, logrus.New(), nil, prices, nil, "testrev")
	if _, err := engine.GradeAnomalies(context.Background(), AnomalyGradeOptions{Anchors: []time.Time{measurementAnchor()}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument for missing sources", err)
	}
}

// anomalyRow finds a drill-down row by ticker (any anchor).
func anomalyRow(rows []TickerForwardReturns, ticker string) *TickerForwardReturns {
	for i := range rows {
		if rows[i].Ticker == ticker {
			return &rows[i]
		}
	}
	return nil
}

// anomalousMedian is a bucket's survivor median at one horizon.
func anomalousMedian(horizons []HorizonStats, horizon int) *float64 {
	for _, stats := range horizons {
		if stats.Horizon == horizon {
			return stats.Survivor.Median
		}
	}
	return nil
}
