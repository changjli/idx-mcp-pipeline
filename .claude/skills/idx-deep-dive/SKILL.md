---
name: idx-deep-dive
description: Full analysis of one IDX ticker (stages 2-4 of the daily flow) — price structure, broker accumulation anatomy, fundamental sanity, catalyst check, and a mandatory trading plan with entry/TP/SL and a pass/no-trade verdict. Use when asked to analyze, deep-dive, or stress-test a specific IDX ticker, or when handed shortlist names from a screen.
---

# IDX Deep Dive

Stages 2–4 of the swing-trading flow for one ticker. Swing horizon: weeks to months. Every step produces evidence; the final plan is only as good as the weakest step. **A "no-trade" verdict is a legitimate, often correct, outcome.**

## Inputs

- Ticker (4-letter code, e.g. `BBHI`). Required.
- If the user holds it: average price, position size, and time horizon. Ask once if analyzing a holding; do not block the analysis on it.
- Optional: base window for broker analysis. Default 15 trading sessions.

## Steps

1. **Price structure.** Call `compute_indicators` in series mode (`mode: "series"`) for the ticker — `ema:20`, `ema:50`, `rsi:14`, `range_position:60`, `volume_ratio:20`, `atr:14`, window ~250 trading days. Read shape, not just latest values: where are the support/resistance shelves, is the stock in a base near the top of its range (breakout-zone) or sliding, was the volume spike one session or a sequence, is RSI rising off a pullback or falling from extension. Nulls at the head of a series are warm-up, not missing data — a series shorter than the indicator's period starts later, which is expected. Classify the tentative phase. Read shape, not just latest values: where are the support/resistance shelves, is the stock in a base near the top of its range (breakout-zone) or sliding, was the volume spike one session or a sequence, is RSI rising off a pullback or falling from extension. Classify the tentative phase.
2. **Broker anatomy.** Call `get_stock_broker_summary_history` over the base window. Identify: dominant accumulators (net buyers across the window) and dominant distributors; the **accumulated volume-weighted average price of the top accumulator** — this anchors the entry zone; whether retail brokers are selling into institutional buying (healthy) or institutions distributing into retail buying (warning). Check `others_net` tails too. Also call `get_broker_net_flow` with the ticker over the same window for the per-broker cumulative view and its `covered_days` vs `trade_days_in_window` — sparse coverage must be stated, not glossed.
3. **Fundamental sanity.** Call `get_financials` with `period: "recent"`. Compare same-duration columns only (all are cumulative YTD). Pass criteria: operating cash flow not deeply negative against cash on hand, no existential debt wall, revenue not collapsing. This is a sanity filter, not a valuation thesis — one paragraph max.
4. **Catalyst & risk check.** Call `list_idx_disclosures` (recent filings — buyback = green, rights issue/warrant = dilution yellow, volatility explanation = read it), `get_corporate_actions` (upcoming dividends/splits/RI dates), `get_suspensions` for the last ~3 months (UMA history = risk), `get_ticker_news` (beware false positives — see Gotchas).
5. **Verdict + plan.** Fill the output template. If any hard gate fails, the plan section says **PASS** with the reason. If it passes: entry zone, TP, SL, R:R math shown, and a one-line invalidation condition ("thesis is dead if X").

## Hard gates (any one fails → PASS)

- Broker data coverage over the base window is near-zero (`covered_days` ≈ 0) and no on-demand fetch is possible.
- Fundamentals fail sanity (going-concern-shaped: burning cash faster than holdings, collapsing revenue).
- Active dilution event (rights issue / large warrant expiration) inside the trade horizon.
- Entry-to-SL distance > 7% because no structural stop level exists closer.
- The stock is in markdown with no base: catching a falling knife is not a setup.

## Output template

```
## Deep Dive — <TICKER> (<company name>)

**Phase: ACCUMULATION / MARKUP / DISTRIBUTION / MARKDOWN** (+1 line why)

**Structure:** shelves (support/resistance levels), MA posture, volume anatomy, range position.
**Flow:** top accumulators with avg cost, top distributors, retail vs institutional behavior, coverage note.
**Fundamentals:** one-paragraph sanity verdict.
**Catalysts & risks:** disclosures, corporate actions, UMA history, news (with reliability note).

**Verdict: SETUP / WATCH / PASS**

Plan (only if SETUP):
- Entry zone: <range> (anchored to Bandar avg cost: <value>)
- TP: <level> (+X%) — based on <next shelf / measured move>
- SL: <level> (-Y%) — based on <structural level>
- R:R: 1 : <ratio> (min 1:2, else no trade)
- Invalidation: <condition that kills the thesis>
```

## Discipline rules

- Never guarantee profits; frame in probabilities.
- Entry closer to the accumulator's avg cost beats chasing above resistance — say so when the user wants to chase.
- If the user asks about holding a large loss with no setup, or buying hype with no structure: reality check, plainly. Recommend exit or a hard SL; never "wait and see" without a level.
- Use IDX terminology naturally (ARA, ARB, Bandar, Haka, Haki, nyangkut) where it's the precise word.

## Gotchas (encode these, they are real)

- **Disclosure PDF extraction** can sit in `status: pending` indefinitely. Poll `read_idx_disclosure` a couple of times; if still pending, note "text pending" and continue the analysis without it — never block on it, never invent its contents.
- **News ticker matching** false-positives on common Indonesian words (e.g. `BUKA` matches "buka"). Verify a headline actually concerns the company before citing it; when in doubt, say the match is unreliable.
- **Broker coverage is anomaly-gated** and sparse by design. A quiet stock may have no stored broker rows; read `covered_days` and say "no flow data" rather than "no flow activity".
- **`others_net` sign convention**: buy − sell, positive = the unlisted tail net-bought. Top-10 rows never include sub-top-10 brokers; never infer a named broker from `others_net`.
- **Foreign flags can be inconsistent** in the source feed (observed on broker `KZ`). Trust per-broker buy/sell values; treat foreign classification as best-effort and say so when it drives a conclusion.
- **Financials are cumulative YTD** — never compare a 6M column against a 12M column.