# Handoff — read this first

You are picking up Lakeforge, a Databricks-compatible lakehouse platform with
a Rust compute engine (Forge). This page is the entry point for an agent or
engineer who has never seen the repository.

## 1. Orientation (15 minutes)

| Read | Why |
| --- | --- |
| [`README.md`](../README.md) | what is in the box, quick start, gates |
| [`docs/architecture.md`](architecture.md) | Forge engine, control plane, governance choke points, Lakebase |
| [`docs/development.md`](development.md) | build/run/test commands, env vars, module map, extension recipes |
| [`docs/api-surface.md`](api-surface.md) | every REST route by module |
| [`docs/uc-lakebase-status.md`](uc-lakebase-status.md) | exact Implemented/Partial/Placeholder status of Unity Catalog and Lakebase |
| [`docs/parity.md`](parity.md) | honest Databricks parity matrix for the whole product |
| [`docs/continuation-plan.md`](continuation-plan.md) | waves of work and their exit criteria |
| [`docs/issues.md`](issues.md) | 28 self-contained work items (`LF-###`) |
| [`openspec/`](../openspec/) | OpenSpec specs (`specs/`) and change proposals (`changes/`) |
| [`tests/smoke/README.md`](../tests/smoke/README.md) | the two end-to-end smoke scripts |
| [`.agents/skills/testing-workspace/SKILL.md`](../.agents/skills/testing-workspace/SKILL.md) | browser golden-path testing procedure |

## 2. Get it running (10 minutes on a warm cargo cache)

```bash
cargo build -p lakeforge-api -p forge-cli
(cd web && npm ci && npm run build)
mkdir -p .lakeforge && LAKEFORGE_UI_DIR=web/dist target/debug/lakeforge-api
# → http://localhost:8080  admin@lakeforge.local / admin
```

Then, in another shell:

```bash
bash tests/smoke/platform-smoke.sh      # 39 checks: clusters, SQL, notebooks, jobs, …
bash tests/smoke/uc-lakebase-smoke.sh   # 50 checks: UC grants/policies/lineage/system tables, Lakebase; 1 known-defective check, so the recorded result is 49/1 (LF-028)
```

`platform-smoke.sh` must end with `failed=0`; `uc-lakebase-smoke.sh` currently
ends with `failed=1` because of a known script defect (LF-028), so treat `49/1`
as its expected result and do not introduce new failures. Run them from the
directory that holds the
API's `.lakeforge/` (or set `LF_DB=/path/to/lakeforge.db`) so the
"secret not stored in clear" check inspects the right SQLite file; it is
skipped with a notice otherwise. They assume a fresh `.lakeforge/` directory
(LF-028 tracks making them fully idempotent).

Gates before any PR:

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p lakeforge-api -p forge-sql -p forge-scheduler -p forge-shuffle
(cd python/lakeforge-sdk && python -m pytest -q && python -m compileall -q lakeforge)
(cd web && npm run lint && npm run build)
```

## 3. State of the branches

- `main` — PR #1 merged: engine, control plane, UI, SDK, deploy, docs.
- `devin/1789466312-unity-catalog-lakebase` — this handoff branch: Unity
  Catalog governance (enforced), Lakebase control plane (emulated), the smoke
  scripts, and all documentation/specs. Open PR: see the repository's pull
  requests. Merge it before starting new work; new work branches from `main`.

## 4. The two things you must not break

1. **The SQL choke point.** Every SQL statement — Statement API, notebook
   `spark.sql`/`%sql`, jobs, pipelines, SQL editor — goes through
   `AppState::execute_sql` → `prepare_sql` (`crates/lakeforge-api/src/api/sql.rs`,
   `crates/lakeforge-api/src/uc/sqlauth.rs`). Authorization, policy rewrite,
   audit, history and lineage all hang off it. Never call the Forge driver
   directly from a handler.
2. **Honest status.** Lakebase is metadata emulation by default; temporary
   credentials are not cloud credentials; constraints are not enforced;
   object ACLs are not enforced. The docs say so explicitly. When you change
   any of that, update `uc-lakebase-status.md` and `parity.md` in the same
   PR, with a test that proves the new claim.

## 5. Picking work

Start with Wave 0 in [`continuation-plan.md`](continuation-plan.md) unless the
user directs otherwise. Each issue in [`issues.md`](issues.md) lists files,
evidence, acceptance criteria, focused tests and the OpenSpec spec it must
satisfy. The OpenSpec change folders contain task checklists; tick them as
you go.

## 6. Local conveniences that are *not* in the repo

The previous sessions used helper scripts in `~/lf-run/` (`lf.sh`,
`restart.sh`, `uc-smoke.sh`) and a Python venv at `~/lf-run/venv` with
`pytest`. The repository copies under `tests/smoke/` supersede the smoke
scripts; the venv is only needed if the system Python lacks `pytest`
(`pip install -e "python/lakeforge-sdk[dev]"`).

## 7. Known rough edges

- `npm run lint` reports two pre-existing warnings
  (`Workspace.tsx` react-refresh export, `Dashboards.tsx` set-state-in-effect);
  they are warnings, not errors.
- `cargo clippy` prints a future-incompat note for `proc-macro-error2`
  (dependency of a dependency); harmless.
- The smoke scripts write a token to `/tmp/lakeforge.tok`; override with
  `LF_TOKEN_FILE`.
- The API auto-creates the SQLite store and storage under `./.lakeforge/`
  relative to the working directory — run it from the repo root or set
  `LAKEFORGE_DATABASE_URL` / `LAKEFORGE_STORAGE_ROOT`.
