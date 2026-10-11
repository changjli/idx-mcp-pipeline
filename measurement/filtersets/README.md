# Filter sets

A **named filter set** is one tuning candidate: a `description` plus a
`ScreenStocks` request. Named sets are how a tuning decision is versioned — a
report snapshots the resolved params and the **Config Hash**, so a later edit
shows up as a diff against the report that graded the previous version.

> **Where they live now (ADR-0016):** the sets are carried by the **Active Filter
> Set**'s Config Document on R2 (`measurement/config/versions/<sha256>.json`),
> read whole on every screen run. Tuning edits the document, never the repo. This
> directory is a **format reference** and the one-shot source `measure migrate`
> reads to seed the document; it is no longer read by a Grade.

A filter-set entry is data, not code: adding or tuning one is a small JSON edit,
so the loop's "learning" never risks breaking a build.

## The Config Document

One document holds every named set and the name in force:

```json
{
  "active": "volume-tolerant",
  "sets": {
    "default": {},
    "volume-tolerant": {"description": "volume_ratio:20 lte 1.5", "window": 60}
  }
}
```

`GET` follows the `active.json` pointer to the current immutable version; `PUT`
writes a new version and moves the pointer. The `sha256` of the version bytes is
the document's **Config Hash**, recorded in every screen Grade report. The
built-in `default` (the zero-value request) is always available, so an empty
bucket grades the shipped screener defaults.

## Set fields

Every field is optional. An omitted field means the shipped screener default
applies (the same defaults the live `screen_stocks` tool uses).

| field | type | omitted means |
|---|---|---|
| `description` | string | blank in the report |
| `window` | int | screener default (250 trading days) |
| `min_value` | int (IDR) | screener default floor (Rp 5bn); `0` = no floor |
| `suspension_window_days` | int | screener default (5 trading days) |
| `flow_window_days` | int | screener default (20 trading days) |
| `indicators` | string[] | `ScreenerDefaultIndicators` |
| `filters` | object[] | `ScreenerDefaultFilters`; `[]` = no structural filters |
| `sort` | string | `value` |
| `order` | string | `desc` |

Two fields are deliberately three-state, because a JSON key's presence carries
meaning:

- **`filters`** — omitted runs the shipped default set; `[]` asks for no
  structural filters at all. They are different requests, and the report's
  funnel makes the difference visible.
- **`min_value`** — omitted uses the shipped floor; `0` is a legitimate "no
  floor".

## Example

```json
{
  "description": "rsi >= 60 and above the 20/50 MA, ranked by RSI",
  "indicators": ["rsi:14", "ma_distance:20:50", "volume_ratio:20"],
  "filters": [
    {"indicator": "rsi:14",            "op": "gte", "value": 60},
    {"indicator": "ma_distance:20:50", "op": "gt",  "value": 0}
  ],
  "sort": "rsi:14",
  "order": "desc"
}
```

```sh
# Publish this directory's sets as the Config Document, then grade the active set.
go run ./cmd/measure migrate --filtersets-dir measurement/filtersets --active momentum
go run ./cmd/measure --as-of 2026-05-04 --filters momentum
```

Operators are `lt`, `gt`, `lte`, `gte`, and `between` (which takes
`[low, high]`). Indicators are resolved through the same registry
`screen_stocks` uses, so an unknown name is rejected with the valid names
listed.

## The built-in `default`

`default` is built in and always available (it is the zero-value request: run
the shipped screener). A `default` entry in the Config Document overrides it. A
missing `filtersets` directory is fine — `migrate` then seeds the document with
the built-in default alone.

## Validation

The document is decoded strictly: an unknown field (a typo), invalid JSON, a
wrong type, or trailing content is a structured error — never a silently skipped
or partially applied set. A pointed-at version whose bytes do not hash to the
pointer is refused as corrupt. Two set names differing only by case are one
name, and the registry cannot decide between them.
