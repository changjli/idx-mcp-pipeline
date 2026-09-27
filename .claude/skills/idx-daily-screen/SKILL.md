---
name: idx-daily-screen
description: IDX whole-market screen (stage 1 of the daily flow) — translate trading criteria into one deterministic screen_stocks call, present the ranked shortlist, and hand candidates to idx-deep-dive. Use when asked to screen stocks, find candidates, list stocks matching criteria, or run the daily screener.
---

# IDX Daily Screen

Stage 1 of the swing-trading flow. One deterministic call replaces iterating tickers by hand. The tool computes; you interpret.

## Inputs

- Criteria in natural language. Optional: an anchor date (`as_of`) for re-running a past day, a sort preference.
- No criteria given → run the default set (below) and say so.

## Criteria → params translation (fixed mapping — same words, same filters, every time)

| User phrase | Param |
|---|---|
| "liquid", "above X billion volume", "active" | `min_value: 5e9` (default) or user's number |
| "above MA20" | filter `ma_distance:20:50 gt 0` |
| "above MA50" | filter `ema:50`-based — prefer `ma_distance:20:50` fast-above-slow unless user names both MAs explicitly |
| "sideways breakout zone", "not broken out yet", "base near resistance" | filter `range_position:60 between [0.7, 0.95]` |
| "overbought" / "oversold" | filter `rsi:14 gt 70` / `rsi:14 lte 30` |
| "quiet volume", "volume contraction", "pullback on low volume" | filter `volume_ratio:20 lte 1` |
| "high volume today" | sort `value`, or filter `volume_ratio:20 gte 2` if they mean a spike |
| "foreign accumulation", "foreign buying" | sort `foreign_net` desc, and/or filter `foreign_net gt 0` — always note coverage flags |
| "MACD positive" | filter `macd gt 0` |
| "strong trend" | filter `ma_spread:20:50 gt 0` |
| "recently suspended", "skip suspended" | raise `suspension_window_days` (default 5) |

Combination rules:
- Multiple phrases AND-combine as separate filter entries.
- A filter list **replaces** the default set, it does not add to it. When the user's criteria are a superset of the default breakout-zone shape, restate all of them explicitly.
- Unknown criterion → say what the registry supports (valid indicator names come back in the structured error) and propose the closest mapping; never silently drop a criterion.
- Filters cannot compare two indicators to each other; if the user asks that (e.g. "close above yesterday's high"), say it's out of the screener's scope and offer `compute_indicators` series mode instead.

## Steps

1. Translate criteria using the table. If the user references the market regime from this week's `/idx-market-recap`, mention it in the interpretation but don't encode it in filters (sector filtering is not in the tool yet).
2. Call `screen_stocks` with the translated params. Default limit 30 is fine; use sort from the user when given.
3. Read the `funnel` block:
   - `after_structure` 0 → filters too tight; report which filter most likely killed the set and propose relaxing it (widen the band, lower the volume bound).
   - `after_structure` > limit (`total_matches` much larger) → filters too loose; say so and suggest tightening.
4. Read the rows. For each: close, chg, value, indicator columns, `foreign_net` **only when `coverage.foreign_net.observed` is true** — unobserved means unknown, never zero. Note `insufficient` arrays: a null RSI/MA means not enough history, not a bad stock.
5. Present the shortlist (template below). Offer `/idx-deep-dive` on the top 3-5 by rank + interest — the screen ranks, judgment picks.

## Output template

```
## Screen — <date> (<one-line criteria summary>)

**Funnel:** universe N → value M → structure K (total_matches pre-cap)
**Shortlist:**
| # | Ticker | Close | Chg% | Value | <key columns> | foreign_net (coverage) |
**Read:** 2-3 lines — what the group looks like as a whole (sector clustering, how tight the list is, anything anomalous).
**Next:** names worth a deep dive, and why those.
```

## Rules

- The screener output is data, not advice — no buy calls at this stage. Candidates only.
- Never present a coverage-unobserved `foreign_net` as zero or "no foreign activity".
- If `data_stale` is true, lead with that: results reflect `last_good_date`, not today.
- A screen that returns 0 rows is a result, not a failure — report the funnel, propose the loosening, let the user choose.