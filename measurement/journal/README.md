# Decision Journal

The append-only log of the AI's deep-dive **Verdicts**, written at decision time
by the `idx-deep-dive` skill so the grader can score them later. Reasoning is
subjective; a logged verdict is falsifiable. See `CONTEXT.md` (Decision Journal)
and `docs/adr/0014-journal-grading-resolves-tradable-verdicts-on-intraday-touch.md`.

## Layout

One JSONL file per decision month, named `YYYY-MM.jsonl`:

```
measurement/journal/2026-06.jsonl
measurement/journal/2026-07.jsonl
```

The month is the month of the **decision date** — the **last stored Trading Day
whose close the plan reads**, not the wall-clock day you happened to run it. The
grader's entry search always starts the next trading day, so this one rule covers
a plan made after a close and a plan made before an open.

## Line format

One JSON object per line, no trailing commas, one Verdict per deep-dive. A
`SETUP` carries three **zones** (each a low/high pair; use the same number for
both for a point):

```json
{"date":"2026-06-01","ticker":"BBRI","verdict":"SETUP","entryLow":4200,"entryHigh":4300,"clLow":3950,"clHigh":4000,"tpLow":4800,"tpHigh":4900,"rationale":"breakout from base at bandar avg cost 4150"}
{"date":"2026-06-01","ticker":"TLKM","verdict":"WATCH","rationale":"coiling but flow coverage too thin to size"}
{"date":"2026-06-01","ticker":"GOTO","verdict":"PASS","rationale":"markdown, no base — falling knife"}
```

| Field | Required | Rule |
|---|---|---|
| `date` | yes | `YYYY-MM-DD`; the last stored Trading Day the plan reads. |
| `ticker` | yes | The IDX code; stored upper-cased. |
| `verdict` | yes | `SETUP` (tradable), `WATCH`, or `PASS`. |
| `entryLow` / `entryHigh` | SETUP only | Positive; the entry zone (`entryLow ≤ entryHigh`). |
| `clLow` / `clHigh` | SETUP only | Positive; the cut-loss zone, below entry (`clHigh ≤ entryLow`). |
| `tpLow` / `tpHigh` | SETUP only | Positive; the target zone, above entry (`entryHigh ≤ tpLow`). |
| `rationale` | yes | One line of why. |

Zones must be ordered `clLow ≤ clHigh ≤ entryLow ≤ entryHigh ≤ tpLow ≤ tpHigh`.
A `WATCH` or `PASS` is a **No-Trade Verdict**: no zones, never resolved; logged
so the report can show how often the gate declined (Calibration).

## Rules

- **Append only.** Never edit or delete a written line. To correct one, append a
  new line for the same `date`+`ticker`; the grader keeps the **last** line and
  reports the shadowed one as a duplicate.
- **Log every verdict**, including `WATCH`/`PASS` — a skipped line is invisible
  to the grader forever.
- A malformed or schema-invalid line is reported (with file and line) and
  skipped, never silently dropped.

## Grading

```
measure grade journal --from 2026-06-01 --until 2026-06-30
```

Entry-first: a verdict has **20 trading days** after the decision day to tap its
entry zone, then **20 trading days** from the fill to touch TP or CL. TP first →
win, CL first → loss, both the same day → loss (conservative), a full window with
neither → expired, a window not yet elapsed → unresolved. A verdict whose entry
zone is never tapped is **unfilled** — no trade — and carries a shadow read
(`missed_tp` / `dodged_cl` / `flat`) that is never counted in the win rate. The
report also gives the win rate, return distribution, Expectancy, the Breakeven
Win Rate at the planned R:R, and the Calibration of no-trade verdicts.
