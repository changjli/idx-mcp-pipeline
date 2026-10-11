# Decision Journal

The append-only log of the AI's deep-dive **Verdicts**, written at decision time
so the grader can score them later. Reasoning is subjective; a logged verdict is
falsifiable. See `CONTEXT.md` (Decision Journal, Verdict Conflict, Committed
Verdict) and ADR-0016.

> **Where it lives now (ADR-0016):** the journal is a Postgres table,
> `measurement_journal` (migration `000019`), read and written over MCP — no
> Verdict is a repo file. This document is the row schema and the grading rules;
> the write path is the MCP `log_verdict` tool (ticket 08), the read/grade path
> is `measure_journal` over MCP and `measure grade journal` on the CLI.

## Rows

One row per Verdict write, append-only. A `SETUP` carries three **zones** (each a
low/high pair; use the same number for both for a point):

| Column | Required | Rule |
|---|---|---|
| `decision_day` | yes | `DATE`; the last stored Trading Day the plan reads. |
| `ticker` | yes | The IDX code; stored upper-cased (enforced by a CHECK). |
| `verdict` | yes | `SETUP` (tradable), `WATCH`, or `PASS`. |
| `entry_low` / `entry_high` | SETUP only | Positive; the entry zone (`entry_low ≤ entry_high`). |
| `cl_low` / `cl_high` | SETUP only | Positive; the cut-loss zone, below entry (`cl_high ≤ entry_low`). |
| `tp_low` / `tp_high` | SETUP only | Positive; the target zone, above entry (`entry_high ≤ tp_low`). |
| `rationale` | yes | One line of why. |
| `session` | yes | The agent run that wrote the row; distinguishes a double-write from a conflict. |
| `supersedes` | no | The `id` of the row a deliberate correction replaces; provenance. |
| `committed` | no | Marks the Verdict the grader scores on a key that holds more than one. |
| `created_at` | default | Insert time; same-session last-wins orders on `(created_at, id)`. |

Zones must be ordered `cl_low ≤ cl_high ≤ entry_low ≤ entry_high ≤ tp_low ≤
tp_high`; the table's CHECK constraints enforce the verdict enum and the SETUP
zone shape, so a malformed row cannot be stored. A `WATCH` or `PASS` is a
**No-Trade Verdict**: no zones, never resolved; logged so the report can show how
often the gate declined (Calibration).

## Rules

- **Append only.** Never edit or delete a written row. To correct one, insert a
  new row for the same `(decision_day, ticker)` carrying `supersedes`.
- **One session's duplicate is last-wins.** Two rows of the *same* `session` on
  one key are an accidental double-write; the later one is graded and the other
  is reported as a shadowed duplicate.
- **Different sessions that disagree are a Verdict Conflict.** Rows from
  different sessions whose `(verdict, zones)` differ are surfaced, never resolved
  by timestamp; the key is excluded from grading until a **Committed Verdict** is
  marked. Rows that agree (rationale wording aside) are a benign duplicate.
- **Log every verdict**, including `WATCH`/`PASS` — a skipped row is invisible to
  the grader forever.

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
