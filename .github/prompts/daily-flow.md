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

3. **Deep dives (stages 2-4).** Load the `idx-deep-dive` skill and deep-dive the
   **top 3 names by screen rank**. Follow the skill exactly, including every hard
   gate and every gotcha. A `PASS` is a valid and expected outcome — do not force
   a `SETUP`. If broker coverage is near-zero, say so rather than guessing.

4. **Journal — via MCP, not files.** For **every** verdict the skill's verdict
   step requires (`SETUP`, `WATCH`, and `PASS` alike), call the `log_verdict` MCP
   tool exactly as the skill directs — one call per verdict, with the zones only
   on a `SETUP`. Use a **single** `session` id for the whole run:
   `daily-flow-<YYYY-MM-DD>` where the date is the decision day (the last stored
   Trading Day the plan reads). Do **not** write, create, or edit any file: the
   Decision Journal is the `measurement_journal` database table now, and this
   runner is ephemeral. If the response surfaces a **Verdict Conflict**, mention
   it in the digest and which verdict to keep.

5. **Digest.** End with a single self-contained Markdown message — this is the
   notification payload sent to Telegram. Structure: the screen section, then
   each deep-dive's output template, then a one-line "what to watch" close. No
   tool-call noise, no preamble, no trailing questions. Read the tools' results
   as data; the shortlist is candidates, not buy calls.
