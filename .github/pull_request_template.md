## What

<!-- One or two sentences. Link the issue and the OpenSpec change. -->

- Issue: `LF-###`
- OpenSpec change: `openspec/changes/<change-id>/`

## Scope

<!-- What this PR deliberately does NOT do. Keep it short but explicit. -->

## Status / parity rows touched

<!-- The docs that must move with the behaviour. Tick what applies and name the row. -->

- [ ] `docs/uc-lakebase-status.md` — row(s): 
- [ ] `docs/parity.md` — row(s): 
- [ ] `docs/api-surface.md` — route(s): 
- [ ] `docs/issues.md` (issue marked done) and `openspec/changes/*/tasks.md` ticked
- [ ] None of the above needed — reason:

No row may claim more than a test demonstrates. Do not upgrade a row to
"Implemented"/"Full" without the test in this PR.

## Honesty checks

- [ ] Lakebase is still described as emulation wherever it is emulation (never
      presented as a reachable PostgreSQL)
- [ ] No secret value appears in the repo, audit events, query history, logs,
      fixtures or docs (secret *identifiers* are fine)
- [ ] Nothing claims Databricks parity without a test showing the documented
      Databricks behaviour
- [ ] Any residual this change does not close is stated as a residual, not as
      settled behaviour

## Gates

Paste the exact commands and their results — not a summary.

```
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p lakeforge-api -p forge-sql -p forge-scheduler -p forge-shuffle
(cd python/lakeforge-sdk && python -m pytest -q && python -m compileall -q lakeforge)
(cd web && npm run lint && npm run build)
bash tests/smoke/platform-smoke.sh      # report the passed=/failed= line
bash tests/smoke/uc-lakebase-smoke.sh   # report the passed=/failed= line
bash scripts/check-docs.sh              # warn-only
```

## Evidence

<!-- The commands you ran and what they actually returned, including failures you
     hit and how you resolved them. If a check could not be run, say so and why. -->

## Review

- [ ] Independent review recorded (reviewer model + verdict)
- [ ] All blocking findings resolved, not merely acknowledged
