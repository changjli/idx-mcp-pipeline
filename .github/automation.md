# Unattended daily flow (GitHub Actions + Telegram)

Runs the IDX swing flow in the cloud and pings Telegram, triggered externally.
Entry point: `.github/workflows/daily-flow.yml` (via the `workflow_dispatch` API).

```
cron-job.org (07:00 WIB, Mon-Fri)
  └─ POST workflow_dispatch ──►  GitHub Actions runner
       ├─ opencode run  ──►  idx-daily-screen skill   (stage 1)
       │                     idx-deep-dive skill      (stages 2-4, every survivor)
       │                     log_verdict MCP tool     → measurement_journal (top 3)
       └─ Telegram: headline message + full digest document
```

## Why an agent, not a cron-curl

`idx-daily-screen` and `idx-deep-dive` are **prompt instructions**, not code.
The MCP tools compute; the reasoning (reading the funnel, ranking, classifying
the phase, writing a falsifiable verdict) is the model. So the scheduler runs an
**agent** — `opencode run` — not an HTTP call. OpenCode has no built-in cron, so
GitHub Actions hosts the run and an external scheduler (cron-job.org) triggers it.

## Setup — do these in order

### 1. Deploy ticket 08 to Heroku (precondition)

The workflow calls `log_verdict`, which only exists on the MCP server once
**ticket 08** (commit `c198804`) is deployed. Until then every deep-dive fails.

```sh
git push heroku master
opencode mcp list      # idx-pipeline connected
```

Verify the harness tools resolve on the live server: `measure_screen`,
`measure_journal`, `log_verdict`, `get_screener_config`. If they are absent, the
deploy did not take.

### 2. Commit and push the workflow

The workflow must be on `master` — that is the ref the dispatch call targets.

```sh
git commit -m "ci: add scheduled daily flow (screen + deep dives + Telegram), journal via MCP"
git push origin master
```

Open the repo → **Actions** tab → **IDX daily flow** appears in the sidebar.
Actions is already enabled on this repo; nothing else to turn on.

### 3. Create a Telegram bot (skip if you have one)

1. Message **@BotFather** → `/newbot` → copy the token (`123456:ABC…`).
2. **Send your bot a message** (e.g. "hi") so it may reply to you.
3. Read your chat id:

```sh
curl -s "https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates" | grep -o '"chat":{"id":[-0-9]*'
```

### 4. Add repository secrets

Settings → Secrets and variables → Actions → **Secrets** (or `gh`, below).

| Secret | What |
|---|---|
| `MCP_TOKEN` | The `Authorization: Bearer …` token for your Heroku MCP. |
| `OPENCODE_AUTH` | Your OpenCode Go credential, as `opencode auth export` JSON (below). |
| `TELEGRAM_BOT_TOKEN` | From step 3. |
| `TELEGRAM_CHAT_ID` | From step 3. |

```sh
gh secret set MCP_TOKEN            # prompts, hidden; paste your Heroku MCP token
gh secret set TELEGRAM_BOT_TOKEN
gh secret set TELEGRAM_CHAT_ID
```

**OpenCode Go credential (no Anthropic account needed).** Go is a $10/mo
subscription keyed by its own API key, stored as a *credential* — not an
`ANTHROPIC_API_KEY` env var. Export it from the machine where Go is connected:

```sh
opencode auth export > /tmp/opencode-auth.json     # contains your secret
gh secret set OPENCODE_AUTH < /tmp/opencode-auth.json
rm /tmp/opencode-auth.json
```

The workflow imports it with `opencode auth import` before the flow. Rotate by
re-exporting and re-setting the secret.

### 5. Choose the model (optional)

Variable `OPENCODE_MODEL` defaults to `opencode-go/deepseek-v4.1-flash`. Other Go
models share the prefix:

```sh
gh variable set OPENCODE_MODEL --body "opencode-go/deepseek-v4-pro"           # stronger
gh variable set OPENCODE_MODEL --body "opencode-go/longcat-2.5-preview-free"  # free, limited time
```

The Go plan's per-model monthly usage limits apply. A single screen + 3 deep
dives per day is a tiny fraction of DeepSeek V4.1 Flash's allowance.

### 6. Test with a manual run

Do not wait for the cron — trigger it once now.

- **Web**: Actions → **IDX daily flow** → **Run workflow** (branch `master`) → Run.
- **CLI**: `gh workflow run "IDX daily flow"` (if it complains about scope:
  `gh auth refresh -s workflow`).

### 7. Verify

```sh
gh run list --workflow "IDX daily flow"
gh run watch
```

Success looks like: the log prints `digest.md`, **Telegram receives a message +
a `.md` document**, and the verdicts land in the journal (check with the
`measure_journal` MCP tool, or locally `go run ./cmd/measure grade journal`).

Once green, the external trigger takes over (see Timing).

## Where state lives

Nothing persists on the runner — the VM is wiped after the job. All durable
state is server-side:

- **Verdicts** → the `measurement_journal` Postgres table, written by the MCP
  `log_verdict` tool. Grade them anytime with the `measure_journal` MCP tool or
  `cmd/measure grade journal` locally.
- **Screener config + reports** → Cloudflare R2 (`get_screener_config`,
  `list_measurement_reports`).

That is why the runner needs only `contents: read` (to fetch the skills) and why
a public repo no longer risks exposing your verdicts.

## Timing

- **Trigger**: an external scheduler at **07:00 WIB, Mon–Fri** (before the open,
  reading the prior close). The workflow has **no `schedule:` block on purpose** —
  GitHub's cron is best-effort (this repo's one scheduled run fired ~6h late,
  landing after the open), and a second trigger would fire the flow twice, a second
  `session` re-logging verdicts as duplicates or a Verdict Conflict.
- **cron-job.org setup** (free, zero code):
  - URL `https://api.github.com/repos/changjli/idx-mcp-pipeline/actions/workflows/daily-flow.yml/dispatches`
  - Method `POST`; schedule `0 7 * * 1-5`; timezone `Asia/Jakarta`
  - Headers: `Authorization: Bearer <PAT>`, `Accept: application/vnd.github+json`,
    `Content-Type: application/json`, `X-GitHub-Api-Version: 2022-11-28`
  - Body: `{"ref":"master"}`
  - `<PAT>` is a fine-grained token scoped to this repo with **Actions: Read and
    write** (or a classic token with the `workflow` scope). Turn on failure emails.
  - The call returns **`204` immediately** — it only queues the run. The ~30-minute
    job runs on GitHub, so cron-job.org's request timeout is not the run's timeout.
  - Equivalent from a shell:

    ```sh
    curl -sS -o /dev/null -w "HTTP %{http_code}\n" -X POST \
      -H "Authorization: Bearer $GH_PAT" \
      -H "Accept: application/vnd.github+json" \
      -H "Content-Type: application/json" \
      -d '{"ref":"master"}' \
      "https://api.github.com/repos/changjli/idx-mcp-pipeline/actions/workflows/daily-flow.yml/dispatches"
    ```
- **The pipeline ingests on trading days only**, at 20:05 WIB (`DailyCronSpec`).
  On weekends and holidays it logs "no missing daily_prices day" and no-ops, so
  `last_success_at` does not advance and the time-based `data_stale` lapses even
  though the stored prices are current. The gate therefore checks **content
  recency** (the latest stored trading day, via a liquid proxy), not the clock:
  it stops only when that anchor is more than 4 calendar days old (a broken
  ingest), and otherwise runs, reporting the anchor day and staleness in the
  digest. A Friday anchor on a Monday is normal.
- **IDX holidays** produce no new trading day; the content gate tolerates a short
  holiday (anchor up to 4 days back) instead of mistaking it for breakage.
- **Catch-up**: Actions → IDX daily flow → Run workflow, optionally with an
  `as_of` date.
- If you ever switch back to GitHub's own `schedule`, note it auto-disables after
  ~60 days of repo inactivity (any push resets it) — one more reason to keep the
  external trigger.

## Cost & reliability notes

- One run/day ≈ one screen + **a deep dive per survivor** (only the 3 strongest
  are journaled). Token spend scales with the survivor count, and Deep-dive's
  `get_financials` is a live upstream fetch; a slow/hung MCP call is the main
  failure mode (the job has a 120-minute ceiling). On the Go plan, watch the
  chosen model's monthly allowance if survivor counts run high.
- `--auto` auto-approves permissions that are not explicitly denied, so the run
  never blocks on an approval prompt. `--standalone` gives it a private server.
- Secrets (`MCP_TOKEN`, `OPENCODE_AUTH`) are injected via env, never written into
  a tracked file.

## Repo changes this relies on

- **The `idx-*` skills under `.claude/skills/` must be tracked** (OpenCode
  discovers project skills there, and CI needs them). They already are.
- **`opencode.json` is ignored**, with a token-free `opencode.json.example`
  committed, so the local bearer token cannot be accidentally committed. CI
  generates its own `opencode.json` from the `MCP_TOKEN` secret.

## Troubleshooting

- **No Telegram message / no digest**: open the run log — the digest and the last
  50 lines of `flow.err` are printed under "Job-log the digest".
- **`log_verdict` / harness tool not found**: the deployed server predates ticket
  08 (see step 1). Redeploy.
- **"pipeline stale" digest**: the latest stored trading day is more than 4 days
  old — the ingest is genuinely behind. Check `heroku logs` for fetch errors
  (a weekend `no missing daily_prices day` no-op is expected, not a failure).
- **Journal empty after a run**: the screen returned no survivors (a valid
  result) or the run aborted at the freshness gate.
- **`No model is available for session …` / auth error**: `OPENCODE_AUTH` is
  missing or stale. Re-run `opencode auth export` locally and re-set the secret.
- **`opencode auth import` fails**: the secret must be the raw JSON from
  `opencode auth export` (an array), not a hand-edited `{"apiKey": …}` object.
