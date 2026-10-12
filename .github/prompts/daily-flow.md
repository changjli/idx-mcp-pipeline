# IDX daily flow (unattended)

You are running the IDX swing-trading daily flow headlessly, in CI, with no human
available. Do **not** ask questions — make reasonable assumptions and proceed.
Never guarantee profits; frame in probabilities.

Follow this exactly:

1. **Freshness gate — content, not clock.** The pipeline ingests only on
   trading days: on weekends and holidays it logs "no missing daily_prices day"
   and does not refresh `last_success_at`, so the time-based `data_stale` flag
   lapses over a normal weekend even though the stored prices are current. Gate
   on the data's actual recency instead.

   Call `get_daily_prices` for a liquid proxy (e.g. `BBCA`) over the last ~12
   calendar days and take the **latest trading day** it returns — that is the
   anchor the screen will use. Also call `get_pipeline_status` for the
   stale-source picture.

   - If that anchor is **more than 4 calendar days before today**, STOP and write
     a short "pipeline stale" digest: name the stale sources with their
     `last_success_at`, state `last_good_date`, and note that no screen, no deep
     dives, and no verdicts were produced. (A Friday anchor on Monday is 3 days —
     normal; 5+ days means a broken ingest.)
   - Otherwise proceed, and state the anchor trading day, `data_stale`,
     `last_good_date`, and any stale sources in the digest as context. Never
     treat `last_good_date` as the last trading day — it is the newest ingestion
     timestamp.

2. **Screen (stage 1).** Load the `idx-daily-screen` skill and follow it exactly
   with the **default** criteria (no user criteria were given — say so).

3. **Deep dives (stages 2-4).** Load the `idx-deep-dive` skill and deep-dive
   **every name in the screen's shortlist** — every row the screen returns, not
   just the top few. Follow the skill exactly for each one, including every hard
   gate and every gotcha. A `PASS` is a valid and expected outcome — do not force
   a `SETUP`. If broker coverage is near-zero, say so rather than guessing. Keep
   working through the whole list; do not stop after three.

4. **Journal — only the 3 strongest, via MCP.** Deep-diving everything does not
   mean journaling everything. Log exactly the **3 most promising** names, ranked
   by deep-dive conviction: `SETUP` before `WATCH` before `PASS`, and within a
   verdict the higher screen rank first. Call `log_verdict` once per logged name,
   with the zones only on a `SETUP`. Use a **single** `session` id for the run:
   `daily-flow-<YYYY-MM-DD>` (the decision day: the last stored Trading Day the
   plan reads). Do **not** write, create, or edit any file — the Decision Journal
   is the `measurement_journal` database table. The other deep-dived names appear
   in the digest but are **not** logged. (A human asking you to journal a specific
   ticker is an interactive request, not part of this cron run.) If a response
   surfaces a **Verdict Conflict**, mention it in the digest and which verdict to
   keep.

5. **Digest.** End with a single self-contained Markdown message — the Telegram
   payload. Structure: the screen section, then **every** deep-dive's output
   template (all survivors, each marked "logged" or "not logged"), then a
   one-line "what to watch" close. No tool-call noise, no preamble, no trailing
   questions. Read the tools' results as data; the shortlist is candidates, not
   buy calls.
