# Tasks: implement-lakebase-control-plane

`[x]` = on branch and covered by `tests/smoke/uc-lakebase-smoke.sh`. Open
items name their `docs/issues.md` entry. Order within a section is the
suggested implementation order; sections 3–5 depend on 2.

## 1. Control plane (metadata)

- [x] 1.1 Instance CRUD, `findByUid`, validation, simulated lifecycle,
      branching refs, owner gating, audit
- [x] 1.2 Roles CRUD
- [x] 1.3 Credentials endpoint with access check and backend label
- [x] 1.4 Database catalogs → `CATALOG_DATABASE` UC catalog; instance
      delete blocked while catalogs depend
- [x] 1.5 Synced tables with spec validation, UC mirror, simulated status
- [x] 1.6 Database tables → `EXTERNAL`/`POSTGRESQL` UC tables
- [x] 1.7 `system.lakebase.instances`, `system.lakebase.synced_tables`
- [ ] 1.8 Background settler task (`STARTING_SECS`) so unread instances
      still transition; `FAILING_OVER` path (LF-013)
- [ ] 1.9 `next_page_token` pagination on all list routes (LF-013)
- [ ] 1.10 Pure state-machine unit tests; branch cascade; non-owner
      denials; role validation (LF-013)
- [ ] 1.11 Instance permissions `CAN_USE`/`CAN_MANAGE` through
      `/api/2.0/permissions/database-instances/{name}`; list filtered by
      permission instead of creator (LF-013, LF-027)
- [ ] 1.12 Role → permission mapping: `DATABRICKS_SUPERUSER` implies
      `CAN_MANAGE`; `PG_ONLY` roles excluded from credential access (LF-015)
- [ ] 1.13 UC-side protection: deleting a `CATALOG_DATABASE` catalog via
      the UC API removes the link (or is blocked) (LF-016)
- [ ] 1.14 Credential token scoping: mint a token whose `scopes =
      ["lakebase:<instance>"]` and reject it on non-Lakebase routes (LF-014)

## 2. Backend abstraction

- [x] 2.1 `Backend { kind, host, port }`, `emulated` default with
      `lakebase.invalid`, disclosure endpoint, per-document backend record
- [ ] 2.2 `LakebaseBackend` trait (`lakebase/backend.rs`) with the
      contract in `openspec/specs/lakebase-emulation/spec.md` (LF-017)
- [ ] 2.3 `EmulatedBackend` no-op implementation; wire existing handlers
      through the trait (LF-017)
- [ ] 2.4 Hard startup error on unparsable
      `LAKEFORGE_LAKEBASE_POSTGRES_URL`; readiness probe (LF-017)
- [ ] 2.5 `ExternalBackend` (SQLx): database per instance
      (`lf_<workspace>_<instance>`), roles per `InstanceRole`, `VALID
      UNTIL` passwords from credentials, `ensure_database`,
      `introspect_table` (LF-017)
- [ ] 2.6 Instance `DELETE` with `purge`: drop database; without purge:
      rename to `_deleted_<ts>` and reap after retention (LF-017)
- [ ] 2.7 CI job with `postgres:16` service exercising the trait contract
      (LF-017, LF-025)

## 3. Synced tables (data movement)

- [ ] 3.1 Pipeline kind `synced_table` in `api/pipelines.rs`; created from
      `spec.new_pipeline_spec` or linked via `existing_pipeline_id`
      (LF-018)
- [ ] 3.2 `SNAPSHOT`: read Delta via Forge (`SELECT *`), `COPY` into a
      staging table, swap; record `last_sync.delta_table_version` (LF-018)
- [ ] 3.3 `TRIGGERED`: `POST /api/2.0/database/synced_tables/{name}/refresh`
      → upsert by `primary_key_columns` (`INSERT … ON CONFLICT DO UPDATE`),
      delete rows missing from source (LF-018)
- [ ] 3.4 `CONTINUOUS`: poll Delta version every `tick_secs`, apply 3.3
      (LF-018)
- [ ] 3.5 Status: `synced_row_count`, `total_row_count`,
      `sync_progress_completion`, `triggered_update_status`,
      `continuous_update_status`, failure states with `message` (LF-018)
- [ ] 3.6 Mirror table columns refreshed from source on schema change
      (LF-018)

## 4. Federation (read path)

- [ ] 4.1 DataFusion `TableProvider` for PostgreSQL in `forge-driver`
      (connection from `table_serving_url` + credential) (LF-019)
- [ ] 4.2 Catalog registration of `CATALOG_DATABASE` tables in Forge
      sessions; UC enforcement unchanged (`prepare_sql`) (LF-019)
- [ ] 4.3 Predicate pushdown for equality/range filters (LF-019)
- [ ] 4.4 Writes to database tables (`INSERT`) — decide scope (LF-019)

## 5. Clients, UI, deploy

- [ ] 5.1 SDK `w.database.*` service mirroring Databricks SDK method names
      (LF-020)
- [ ] 5.2 UI `Lakebase` page: instances list/create/delete, roles,
      credential copy, catalogs, synced tables with status; emulation
      banner (LF-022)
- [ ] 5.3 Compose profile `lakebase` (postgres:16), Helm values
      `lakebase.postgres.*`, Terraform managed PostgreSQL (RDS / Cloud SQL
      / Flexible Server) wired to `LAKEFORGE_LAKEBASE_POSTGRES_URL`
      (LF-023)
- [ ] 5.4 Smoke: external-backend variant of checks 30–46 guarded by
      `LAKEFORGE_LAKEBASE_POSTGRES_URL` (LF-028)

## 6. Docs

- [x] 6.1 Status §Lakebase, parity rows, README wording (emulation only)
- [ ] 6.2 `docs/deploy.md` PostgreSQL profile; update status/parity as
      items close (LF-026)
- [ ] 6.3 On completion: apply deltas in `specs/` here to
      `openspec/specs/`, archive this change
