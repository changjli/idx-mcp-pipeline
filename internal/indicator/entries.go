package indicator

import (
	cinar "github.com/cinar/indicator"
)

// The v1 registry (spec: ~13 entries; 14 registered). Every entry reports a
// single number in the units its Summary declares, so the screener's filter DSL
// can compare it against a caller threshold. ADX, Stochastic, and Supertrend
// are deliberately absent — deferred follow-up registry entries, not v1 scope.
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
		Compute: func(periods []int, s Series) float64 {
			return last(cinar.Sma(periods[0], s.Close))
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
		Compute: func(periods []int, s Series) float64 {
			return last(cinar.Ema(periods[0], s.Close))
		},
	},
	"rsi": {
		Name:           "rsi",
		Summary:        "relative strength index (Wilder smoothing), 0..100",
		DefaultPeriods: []int{14},
		// Period price changes need period+1 closes; the library's RMA seed
		// window is the first period of those changes.
		RequiredRows: func(periods []int) int { return periods[0] + 1 },
		Compute: func(periods []int, s Series) float64 {
			_, rsi := cinar.RsiPeriod(periods[0], s.Close)
			return last(rsi)
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
		Compute: func(_ []int, s Series) float64 {
			macd, signal := cinar.Macd(s.Close)
			return last(macd) - last(signal)
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
		Compute: func(periods []int, s Series) float64 {
			period := periods[0]
			std := last(cinar.Std(period, s.Close))
			middle := last(cinar.Sma(period, s.Close))
			return 4 * std / middle * 100
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
		Compute: func(periods []int, s Series) float64 {
			_, atr := cinar.Atr(periods[0], s.High, s.Low, s.Close)
			return last(atr)
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
		Compute: func(_ []int, s Series) float64 {
			return last(cinar.Obv(s.Close, s.Volume))
		},
	},
	"volume_ma": {
		Name:           "volume_ma",
		Summary:        "simple moving average of volume, in shares",
		DefaultPeriods: []int{20},
		Needs:          UsesVolume,
		RequiredRows:   func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) float64 {
			return last(cinar.Sma(periods[0], s.Volume))
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
		Compute: func(periods []int, s Series) float64 {
			average := last(cinar.Sma(periods[0], s.Volume))
			return last(s.Volume) / average
		},
	},
	"range_position": {
		Name:           "range_position",
		Summary:        "close's position within the period high-low range, 0 (at low) .. 1 (at high)",
		DefaultPeriods: []int{60},
		Needs:          UsesHighLow,
		RequiredRows:   func(periods []int) int { return periods[0] },
		Compute: func(periods []int, s Series) float64 {
			period := periods[0]
			highest := last(cinar.Max(period, s.High))
			lowest := last(cinar.Min(period, s.Low))
			// A flat range has no position to report: 0/0 is NaN and the
			// caller reads it as no value rather than as "at the low".
			return (last(s.Close) - lowest) / (highest - lowest)
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
		Compute: func(periods []int, s Series) float64 {
			period := periods[0]
			return percentChange(s.Close[len(s.Close)-1-period], last(s.Close))
		},
	},
	"ma_slope": {
		Name:           "ma_slope",
		Summary:        "the period MA's own change across one full period, in percent",
		DefaultPeriods: []int{20},
		// Both MA endpoints must be settled: the MA one period back is only
		// fully warmed once the series holds two full periods.
		RequiredRows: func(periods []int) int { return 2 * periods[0] },
		Compute: func(periods []int, s Series) float64 {
			period := periods[0]
			ma := cinar.Sma(period, s.Close)
			return percentChange(ma[len(ma)-1-period], last(ma))
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
		Compute: func(periods []int, s Series) float64 {
			ma := last(cinar.Sma(periods[0], s.Close))
			return percentChange(ma, last(s.Close))
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
		Compute: func(periods []int, s Series) float64 {
			fast := last(cinar.Sma(periods[0], s.Close))
			slow := last(cinar.Sma(periods[1], s.Close))
			return percentChange(slow, fast)
		},
	},
}

// macdWarmup is the first row index (1-based) whose MACD histogram is fully
// warmed: the slow EMA settles at row 26, then the signal EMA needs 9 settled
// MACD values after it.
const macdWarmup = 26 + 9 - 1

// percentChange reports to's change from, in percent: (to/from - 1) * 100. A
// zero base yields an infinity, which Value reports as no value.
func percentChange(from, to float64) float64 {
	return (to/from - 1) * 100
}
