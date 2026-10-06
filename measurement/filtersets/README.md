# Filter sets

One file per named filter set: `<name>.json`. The filename stem **is** the
name — it is what you pass to `cmd/measure --filters <name>`, what appears in
the report's `filter_set`, and the `<name>` segment of the report filename
`measurement/reports/<as-of>_<name>.{json,md}`.

Filter sets are data, not code: adding or tuning one is a small JSON edit, not a
Go edit, so the loop's "learning" (filter changes committed to the repo) never
risks breaking a build.

## Fields

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

`measurement/filtersets/momentum.json`:

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
go run ./cmd/measure --as-of 2026-05-04 --filters momentum
```

Operators are `lt`, `gt`, `lte`, `gte`, and `between` (which takes
`[low, high]`). Indicators are resolved through the same registry
`screen_stocks` uses, so an unknown name is rejected with the valid names
listed.

## The built-in `default`

`default` is built in and always available (it is the zero-value request: run
the shipped screener). A `default.json` file overrides it if present. A missing
`filtersets` directory is fine — the tool still runs the default set.

## Validation

Files are decoded strictly: an unknown field (a typo), invalid JSON, a wrong
type, or trailing content is a structured error naming the file — never a
silently skipped or partially applied set. Two filenames differing only by case
are refused as ambiguous.
