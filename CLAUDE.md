## Agent skills

### Issue tracker

Issues live as markdown files under `.scratch/<feature-slug>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five canonical roles: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout: `CONTEXT.md` at root, `docs/adr/` for decisions. See `docs/agents/domain.md`.

## Git workflow

After implementation, **do not stage automatically** — leave the working tree as-is and present a commit message for review. Stage and commit only when the user explicitly instructs it. (User preference, 2026-09-06; amended same day: no auto-staging either.)

Commits are authored and committed **solely by the user**. Never add an AI/agent as a contributor — no `Co-authored-by` trailers, no agent author or committer. The user is the repository's only contributor. (User preference, 2026-10-10.)
