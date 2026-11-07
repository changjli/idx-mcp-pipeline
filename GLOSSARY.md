# Glossary

The language of the IDX pipeline. Terms only — no implementation, no decisions, no schemas.

## Broker flow

**Window** — a contiguous range of trading days for one ticker, identified by its first and last day. A single day
is a one-day window.

**Day row** — a stored window that is exactly one trading day.

**Archive chunk** — a stored window that is month-aligned and kept indefinitely.

**Partition** — the set of day rows and archive chunks, aligned to months. Day rows are short-lived observations;
archive chunks are kept indefinitely. Its purpose is to make aligned queries cost nothing.

**Daily refresh** — the unattended observation of a trading day's anomaly hits as day rows. It is capped per day and
is distinct from a screen's own enrichment.

**Screener enrichment** — the trailing-window fetch a screen performs for its top rows before ranking them. It is
bounded per day and never covers the whole survivor set.

**Answer cache** — a stored window kept so an exact repeat of the same query costs nothing. A cache is not excluded
from a sum; what is excluded is any interval that overlaps another interval in the same sum.

**Composition** — answering a range by summing stored windows that exactly fill it, with no overlap and no gaps.
A range that is not exactly filled is fetched instead, never partially composed. Aggregation tools that deliberately
cannot fetch (Net Flow, Sector Flow) instead sum a disjoint selection and declare the covered days beside the
window's span; they never sum overlapping windows, and the measurement replays accept only an exact fill.

**Investor lane** — the dimension a window was fetched on: foreign only, or all investors. Lanes are never summed
together.

**Foreign-executed flow** — flow executed by foreign investors, irrespective of which broker executed it. Distinct
from a broker's total flow, which mixes domestic and foreign execution.

**Foreign gross** — foreign-executed buy or sell value over a window. Buy and sell are reported separately; a
foreign net is their difference, never a gross.

**Foreign net** — foreign gross buy minus foreign gross sell. Positive means accumulation. Unobserved is null,
never zero.

**Window VWAP** — the volume-weighted average price of a window. It is correct for accumulation and is not any single
day's price.

**Coverage** — the extent to which the stored material actually supports the figures reported for a window.

**Uncovered** — a figure that was not observed. Never zero, and never inferred.
