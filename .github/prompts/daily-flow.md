# IDX daily flow (unattended)

You are running the IDX swing-trading daily flow headlessly, in CI, with no human
available. Do **not** ask questions — make reasonable assumptions and proceed.
Never guarantee profits; frame in probabilities.

Follow this exactly:

1. **Freshness gate.** Call `get_pipeline_status`. If the market data source is
   stale (`data_stale` true) or `last_good_date` is not the most recent trading
   day, STOP after writing a short "pipeline stale" digest that names the source,
   its `last_good_date`, and what you expected. Do not screen over stale data.

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
