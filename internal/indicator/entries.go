package indicator

import (
	cinar "github.com/cinar/indicator"
)

// The v1 registry (spec: ~13 entries; 14 registered). Every entry computes one
// number per row, in the units its Summary declares, so the screener's filter
// DSL can compare the latest value against a caller threshold and a deep-dive
// can read the same numbers as a trajectory (Evaluate). ADX, Stochastic, and
// Supertrend are deliberately absent — deferred follow-up registry entries, not
// v1 scope.
//
// Compute returns the array over the series it is handed, ascending and
// parallel to it; the registry masks the rows below RequiredRows and the last
// element is therefore exactly the value Value reports. Entries leave positions
// they have no value for at zero or a garbage head value — the mask covers
// them — but must never emit a plausible number where there is none: a zero
// volume would read as a real, maximally bearish observation.
//
// Library-sourced: sma, ema, rsi, macd, atr, obv, volume_ma, range_position's
// window extremes. In-package (library gap or hardcoded parameters): roc,
// bb_width, volume_ratio, ma_slope, range_position's normalization, and the
// derived indicators ma_distance / ma_spread.
var registry = map[string]Entry{
	"sma": {
		Name:           "sma",
		Summary:        "simple moving average of close, in IDR",
		DefaultPeriods: []int{20},
		RequiredRows:   func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			return cinar.Sma(periods[0], s.Close)
		},
	},
	"ema": {
		Name:           "ema",
		Summary:        "exponential moving average of close, in IDR",
		DefaultPeriods: []int{20},
		// The library seeds the EMA at the first close, so shorter windows
		// exist numerically but are still mostly the seed. One period is the
		// conventional "settled" bar.
		RequiredRows: func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			return cinar.Ema(periods[0], s.Close)
		},
	},
	"rsi": {
		Name:           "rsi",
		Summary:        "relative strength index (Wilder smoothing), 0..100",
		DefaultPeriods: []int{14},
		// Period price changes need period+1 closes; the library's RMA seed
		// window is the first period of those changes.
		RequiredRows: func(periods []int) int { return periods[0] + 1 },
		Compute: func(periods []int, s Series) []float64 {
			_, rsi := cinar.RsiPeriod(periods[0], s.Close)
			return rsi
		},
	},
	"macd": {
		Name: "macd",
		// Fixed-parameter entry: the library's MACD is the 12/26/9 line pair,
		// and the histogram (line minus signal) is the number a screener
		// filters on — positive means momentum is improving.
		Summary: "MACD histogram (12/26/9 line minus signal), in IDR",
		// Periods deliberately empty: `macd:12` would have to mean something
		// the library cannot compute, so Parse rejects it instead of quietly
		// computing 12/26/9 under a key the caller read as 12-period.
		DefaultPeriods: nil,
		// The slow EMA is settled after 26 rows; the signal is a 9-period EMA
		// of that line, so the first settled histogram is at row 26+9-1.
		RequiredRows: func([]int) int { return macdWarmup },
		Compute: func(_ []int, s Series) []float64 {
			macd, signal := cinar.Macd(s.Close)
			return subtractElements(macd, signal)
		},
	},
	"bb_width": {
		Name:           "bb_width",
		Summary:        "Bollinger band width as percent of the middle band (volatility contraction)",
		DefaultPeriods: []int{20},
		// Computed in-package rather than via cinar.BollingerBands, which
		// hardcodes 20 periods and 2 standard deviations; the registry exposes
		// the period, so the band must follow it. Width = (upper-lower)/middle
		// = 4*std/middle over a trailing window (population std, as the
		// library's Std computes it).
		RequiredRows: func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			period := periods[0]
			std := cinar.Std(period, s.Close)
			middle := cinar.Sma(period, s.Close)
			width := make([]float64, len(std))
			for i := range std {
				width[i] = 4 * std[i] / middle[i] * 100
			}
			return width
		},
	},
	"atr": {
		Name:           "atr",
		Summary:        "average true range, in IDR (absolute volatility)",
		DefaultPeriods: []int{14},
		Needs:          UsesHighLow,
		// TR is a same-bar range (no previous close), so ATR is a plain
		// period-SMA of it and needs exactly period rows.
		RequiredRows: func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			_, atr := cinar.Atr(periods[0], s.High, s.Low, s.Close)
			return atr
		},
	},
	"obv": {
		Name: "obv",
		// Cumulative from the series' first row, so the level is relative to
		// where the window starts; read it against the same window (rising or
		// falling), never as an absolute figure.
		Summary:        "on-balance volume: cumulative volume signed by close direction, window-relative",
		DefaultPeriods: nil,
		Needs:          UsesVolume,
		// OBV[0] is the seed (0) and needs no volume; the first signed row is
		// the second, so a value needs two rows.
		RequiredRows: func([]int) int { return 2 },
		Compute: func(_ []int, s Series) []float64 {
			return cinar.Obv(s.Close, s.Volume)
		},
	},
	"volume_ma": {
		Name:           "volume_ma",
		Summary:        "simple moving average of volume, in shares",
		DefaultPeriods: []int{20},
		Needs:          UsesVolume,
		RequiredRows:   func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			return cinar.Sma(periods[0], s.Volume)
		},
	},
	"volume_ratio": {
		Name:           "volume_ratio",
		Summary:        "latest volume as a multiple of its period average (below 1 is contraction)",
		DefaultPeriods: []int{20},
		Needs:          UsesVolume,
		// The average includes the latest row, so the ratio is exactly the
		// ratio of the last volume to the trailing period average.
		RequiredRows: func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			average := cinar.Sma(periods[0], s.Volume)
			return divideElements(s.Volume, average)
		},
	},
	"range_position": {
		Name:           "range_position",
		Summary:        "close's position within the period high-low range, 0 (at low) .. 1 (at high)",
		DefaultPeriods: []int{60},
		Needs:          UsesHighLow,
		RequiredRows:   func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			period := periods[0]
			highest := cinar.Max(period, s.High)
			lowest := cinar.Min(period, s.Low)
			positions := make([]float64, len(s.Close))
			for i := range s.Close {
				// A flat range has no position to report: 0/0 is NaN and the
				// caller reads it as no value rather than as "at the low".
				positions[i] = (s.Close[i] - lowest[i]) / (highest[i] - lowest[i])
			}
			return positions
		},
	},
	"roc": {
		Name: "roc",
		// The library has no rate-of-change, so this is in-package: the
		// percent change in close over the period.
		Summary:        "rate of change of close over the period, in percent",
		DefaultPeriods: []int{20},
		// Comparing the latest close against the one period rows back needs
		// period+1 rows.
		RequiredRows: func(periods []int) int { return periods[0] + 1 },
		Compute: func(periods []int, s Series) []float64 {
			period := periods[0]
			changes := make([]float64, len(s.Close))
			for i := period; i < len(s.Close); i++ {
				changes[i] = percentChange(s.Close[i-period], s.Close[i])
			}
			return changes
		},
	},
	"ma_slope": {
		Name:           "ma_slope",
		Summary:        "the period MA's own change across one full period, in percent",
		DefaultPeriods: []int{20},
		// Both MA endpoints must be settled: the MA one period back is only
		// fully warmed once the series holds two full periods.
		RequiredRows: func(periods []int) int { return 2 * periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			period := periods[0]
			ma := cinar.Sma(period, s.Close)
			slopes := make([]float64, len(ma))
			for i := 2*period - 1; i < len(ma); i++ {
				slopes[i] = percentChange(ma[i-period], ma[i])
			}
			return slopes
		},
	},
	"ma_distance": {
		Name: "ma_distance",
		// Derived indicator: collapses "price above MA" into one filterable
		// number, so the filter DSL can express it as a threshold (`> 0`)
		// instead of comparing two indicator values.
		Summary:        "percent distance of close above (+) or below (-) the fast period MA",
		DefaultPeriods: []int{20, 50},
		// Only the fast MA enters the value; the slow period is the pair's
		// second half, carried so `ma_distance` and `ma_spread` name the same
		// two periods.
		RequiredRows: func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) []float64 {
			return percentChanges(cinar.Sma(periods[0], s.Close), s.Close)
		},
	},
	"ma_spread": {
		Name: "ma_spread",
		// Derived indicator, the other half of the MA pair: positive is the
		// bullish fast-above-slow alignment.
		Summary:        "percent spread of the fast period MA over (+) or under (-) the slow period MA",
		DefaultPeriods: []int{20, 50},
		// The fast MA is settled by the time the slow one is.
		RequiredRows: func(periods []int) int { return periods[1] },
		Compute: func(periods []int, s Series) []float64 {
			fast := cinar.Sma(periods[0], s.Close)
			slow := cinar.Sma(periods[1], s.Close)
			return percentChanges(slow, fast)
		},
	},
}

// macdWarmup is the first row index (1-based) whose MACD histogram is fully
// warmed: the slow EMA settles at row 26, then the signal EMA needs 9 settled
// MACD values after it.
const macdWarmup = 26 + 9 - 1

// percentChange reports to's change from, in percent: (to/from - 1) * 100. A
// zero base yields an infinity, which the caller reports as no value.
func percentChange(from, to float64) float64 {
	return (to/from - 1) * 100
}

// percentChanges is percentChange element-wise over parallel arrays, so a value
// stays aligned with the row it belongs to.
func percentChanges(from, to []float64) []float64 {
	changes := make([]float64, len(to))
	for i := range to {
		changes[i] = percentChange(from[i], to[i])
	}
	return changes
}

// subtractElements is a - b element-wise.
func subtractElements(a, b []float64) []float64 {
	differences := make([]float64, len(a))
	for i := range a {
		differences[i] = a[i] - b[i]
	}
	return differences
}

// divideElements is a / b element-wise. A zero denominator yields an infinity,
// which the caller reports as no value.
func divideElements(a, b []float64) []float64 {
	quotients := make([]float64, len(a))
	for i := range a {
		quotients[i] = a[i] / b[i]
	}
	return quotients
}
