---
name: testing-workspace
description: Run Lakeforge local browser golden-path tests with persisted notebooks, clusters, SQL and job output.
---

# Local workspace UI testing

Use the repository blueprint for dependency installation and builds. Build both
`lakeforge-api` and `forge-cli`, plus `web/dist`, before testing changed code.
The API serves the SPA when `LAKEFORGE_UI_DIR` points to the built frontend.

On prepared machines, inspect `/home/ubuntu/lf-run/restart.sh` before using it:
its default behavior wipes runtime state; `--keep` preserves notebooks and jobs.
API restarts terminate local compute processes. Restore the test cluster through
Compute and wait for RUNNING before notebook/job execution. Hard-reload the
browser after a frontend rebuild.

## Devin Secrets Needed

None for the default local bootstrap workspace. Use the local bootstrap account
documented in the blueprint; do not reuse local defaults on shared deployments.

## High-value browser checks

- Create a one-worker cluster and a Python notebook under Shared.
- Exercise Python stdout, Spark SQL, `%sql` table output, `%md` rendering,
  and `dbutils.fs` writes. Reload to check both source and output persistence.
- Run an error cell, recover, cancel a sleeping cell, then execute immediately
  again. A disappearing spinner alone is not evidence that cancellation worked.
- SQL CREATE/INSERT/SELECT should yield exact rows; rerun and reload the draft.
- Check Catalog columns, Delta metadata and sample data against SQL results.
- Create a manual notebook job with Schedule blank. Check both the Tasks count
  in Workflows and the actual run output, including table and Markdown events;
  SUCCESS alone is insufficient.
- Revoke any test PAT and stop compute at completion. Retain named notebooks,
  tables and jobs only when useful for follow-up inspection.

Prefer browser interactions over authenticated API calls. When a fix arrives
mid-test, restart only the affected services and recheck changed behavior;
preserve unaffected evidence and clearly label coverage boundaries.
