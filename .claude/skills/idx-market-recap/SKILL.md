---
name: idx-market-recap
description: IDX market regime read (stage 0 of the daily flow) — foreign flow stance, volume anomalies, UMA/suspensions, big-cap trend. Run weekly on Monday and optionally after each trading day. Use when asked "how did the market do", "market recap", "market regime", or before a screening session.
---

# IDX Market Recap

Stage 0 of the swing-trading flow. Purpose: set the week's risk posture (aggressive / neutral / defensive) and note where institutional money is moving, so the daily screen inherits a context instead of starting cold.

## Inputs

- Optional: a specific trading date or week range. Default: the most recent trading day (or the trailing week for a Monday run).

## Steps

Run in this order. All are pure DB reads — no live fetches.

1. **Foreign / institutional stance.** Call `get_broker_net_flow` with no ticker (market-wide mode) over the window (default: trailing 30 calendar days for weekly, trailing 5 trading days for daily). Read `tickers_covered` and `covered_days` — the population is anomaly-gated and skews to busy stocks. Report the top 5 accumulators and top 5 distributors **with their by_ticker breakdowns**, so "AK accumulated BBRI" is visible, not just "AK net +12B".
2. **Volume anomalies.** Call `get_market_anomalies` for the target day. Note clusters: how many anomalies, which sectors/tickers repeat across days, whether spikes came with big price moves or died flat (distribution vs accumulation hint).
3. **UMA / suspensions.** Call `get_suspensions` for the week range. Flag new suspensions (SPT), resumes (UPT), and UMA warnings — a UMA on a name you're watching changes its risk immediately.
4. **Big-cap trend (IHSG proxy).** The pipeline stores stocks, not the index. Call `get_daily_prices` for a fixed basket — `BBCA, BBRI, BMRI, BBNI, TLKM, ASII, ADRO, ANTM` — over the last ~20 trading days and summarize: how many are above their visible short-term trend, breadth of the basket. State plainly that this is a proxy, not IHSG itself.
5. **Synthesize.** Fill the output template. Every claim must trace to a tool result; no vibes.

## Output template

```
## Market Recap — <date or week>

**Regime: AGGRESSIVE / NEUTRAL / DEFENSIVE** (one line of justification)

**Foreign & institutional stance:** top accumulators (broker → which tickers), top distributors.
**Volume anomalies:** notable names + what the clusters suggest.
**UMA / suspensions:** list, or "none this week".
**Big-cap breadth:** X/8 basket names trending up; brief read.
**Implication for the screen:** e.g. "favor commodity names, size down on consumer" — one sentence.
```

## Rules

- The regime verdict must cite evidence (breadth count, flow direction), not sentiment.
- If `data_stale` is true on any source, say so in the recap.
- This stage is context only — it never produces trade entries.