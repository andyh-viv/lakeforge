# Change: implement-lakebase-control-plane

Status: **control plane implemented (metadata-only)** on branch
`devin/1789466312-unity-catalog-lakebase`; **data plane not started**.
Remaining tasks in `tasks.md`; issues LF-013..LF-019, LF-022, LF-023.

## Why

Databricks Lakebase gives every workspace a managed PostgreSQL that is
governed by Unity Catalog (database catalogs), fed from Delta (synced
tables) and reachable with short-lived UC credentials. Lakeforge had no
OLTP story at all. Delivering the Databricks `/api/2.0/database/*` API
shapes first lets SDKs, the UI, Terraform providers and smoke tests be
built against stable contracts, while the PostgreSQL backend is developed
behind a clearly labelled abstraction — without ever pretending emulated
metadata is a running database.

## What Changes

Implemented on the branch (`crates/lakeforge-api/src/api/lakebase.rs`):

1. **Instances** — CRUD, `findByUid`, validation (name, capacity,
   node_count, retention), simulated lifecycle
   (`STARTING → AVAILABLE`, `UPDATING`, `DELETING`, `STOPPED`), branching
   metadata (`parent_instance_ref`/`child_instance_refs`), owner/admin
   gating, audit.
2. **Roles** — per-instance roles with `identity_type`, `membership_role`,
   `attributes`.
3. **Credentials** — `POST /database/credentials` mints a 1-hour Lakeforge
   token after checking creator/role/admin access to every instance; labels
   the backend.
4. **Database catalogs** — create a `CATALOG_DATABASE` UC catalog linked to
   an instance; instance deletion is blocked while catalogs depend on it.
5. **Synced tables** — spec validation against the source UC table
   (`SELECT` required; PK/timeseries columns must exist), UC mirror with
   `data_source_format = POSTGRESQL`, simulated `PROVISIONING_* → ONLINE_*`
   status.
6. **Database tables** — register PostgreSQL tables as `EXTERNAL` UC tables
   with `table_serving_url`.
7. **Backend abstraction (minimal)** — `Backend { kind, host, port }`
   from `LAKEFORGE_LAKEBASE_POSTGRES_URL`; `emulated` uses the reserved host
   `lakebase.invalid`; `GET /api/2.0/lakeforge/lakebase/backend` discloses
   it; every document records its backend.
8. **System tables** — `system.lakebase.instances`,
   `system.lakebase.synced_tables`.

Proposed (this change, not yet started):

9. `LakebaseBackend` trait with `EmulatedBackend`, `ExternalBackend`
   (SQLx Postgres), later `KubernetesBackend`; instance/role/database
   provisioning; credential → PostgreSQL role password with `VALID UNTIL`.
10. Synced-table pipeline: Lakeflow pipeline kind `synced_table` that
    snapshots/upserts Delta rows into PostgreSQL via `COPY`/`INSERT … ON
    CONFLICT`; `TRIGGERED` refresh endpoint; `CONTINUOUS` polling on Delta
    version.
11. Forge federation: DataFusion `TableProvider` for PostgreSQL so database
    catalogs and synced tables are queryable from SQL with UC enforcement.
12. Background settler for lifecycle states; `next_page_token` pagination;
    instance permissions (`CAN_USE`/`CAN_MANAGE`) via the permissions API.
13. Lakebase UI page, SDK `database` service, Compose/Helm/Terraform
    PostgreSQL profiles.

## Impact

- **Behaviour**: new `/api/2.0/database/*` routes; new UC catalog kind
  `CATALOG_DATABASE`; new token comment prefix `lakebase credential for`.
  No existing behaviour changes.
- **Code**: `api/lakebase.rs` (new), `uc/system_tables.rs` (two tables),
  `config.rs` (`LAKEFORGE_LAKEBASE_POSTGRES_URL`), `main.rs` (router).
  Proposed: `crates/lakeforge-api/src/lakebase/{backend.rs, emulated.rs,
  external.rs}`, `api/pipelines.rs` (synced_table kind), `forge-driver`
  (PostgreSQL provider), `web/src/pages/Lakebase.tsx`,
  `python/lakeforge-sdk/lakeforge/services/database.py`, `deploy/*`.
- **Specs**: `lakebase-instances`, `lakebase-credentials`,
  `lakebase-catalogs`, `lakebase-synced-tables`, `lakebase-emulation`;
  deltas in `specs/` here.
- **Docs**: `docs/uc-lakebase-status.md` §Lakebase, `docs/parity.md`,
  `docs/deploy.md` (PostgreSQL profile), `docs/api-surface.md`.
- **Tests**: smoke checks 30–46 of `uc-lakebase-smoke.sh`; proposed CI
  job with a `postgres:16` service container.

## Risks

- **Misrepresentation**: the single biggest risk is a reader assuming an
  `AVAILABLE` instance is a database. Mitigations already in place:
  `.invalid` DNS, `backend.emulated`, status `message`, README/parity
  wording; keep them until a real backend exists.
- Credentials are Lakeforge API tokens and therefore grant API access as the
  caller; scope them (LF-014) before exposing Lakebase to untrusted users.
- Synced tables on a real backend need PK-based upserts; Forge has no
  `MERGE`, so the pipeline must be implemented in the control plane or as a
  Forge extension.
- External backend against a shared PostgreSQL requires per-instance
  database isolation and careful role naming to avoid collisions across
  workspaces.
