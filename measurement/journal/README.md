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

The month is the month of the **decision date** — the latest stored Trading Day
the analysis was based on, not the wall-clock day you happen to run it.

## Line format

One JSON object per line, no trailing commas, one Verdict per deep-dive:

```json
{"date":"2026-06-01","ticker":"BBRI","verdict":"SETUP","entry":4200,"tp":4800,"sl":3950,"rationale":"breakout from base at bandar avg cost 4150"}
{"date":"2026-06-01","ticker":"TLKM","verdict":"WATCH","rationale":"coiling but flow coverage too thin to size"}
{"date":"2026-06-01","ticker":"GOTO","verdict":"PASS","rationale":"markdown, no base — falling knife"}
```

| Field | Required | Rule |
|---|---|---|
| `date` | yes | `YYYY-MM-DD`; the decision date (the Trading Day the plan reads). |
| `ticker` | yes | The IDX code; stored upper-cased. |
| `verdict` | yes | `SETUP` (tradable), `WATCH`, or `PASS`. |
| `entry` | SETUP only | Positive number; above `sl`, below `tp`. The assumed fill. |
| `tp` | SETUP only | Positive number; above `entry`. |
| `sl` | SETUP only | Positive number; below `entry`. |
| `rationale` | yes | One line of why. |

`WATCH` and `PASS` are **No-Trade Verdicts**: they carry no levels and are never
resolved; they are logged so the report can show how often the gate declined
(Calibration).

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

Resolves each `SETUP` against stored prices over a 20-trading-day Resolution
Window (TP touched first → win; SL first → loss; both the same day → loss; window
elapsed with neither → expired; window not yet elapsed → unresolved), and reports
win rate, return distribution, Expectancy, the Breakeven Win Rate at the planned
R:R, and the Calibration of no-trade verdicts.
