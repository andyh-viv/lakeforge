# Lakebase Synced Tables Specification

## Purpose

A *synced table* continuously or periodically copies a Unity Catalog Delta
table into a PostgreSQL table inside a database catalog so applications can
read it with low latency. Databricks exposes this as
`/api/2.0/database/synced_tables` with a `spec` (source, scheduling policy,
primary keys) and a `data_synchronization_status`. Lakeforge implements the
control-plane object, validation and a simulated status lifecycle; the data
movement pipeline is not implemented.

## Scope

In scope: synced-table CRUD, spec validation against the source table's UC
metadata, status state machine, UC table mirror, audit. Out of scope: the
sync pipeline (LF-018), PostgreSQL DDL (LF-017), reading the synced copy
(LF-019).

## Data Model

`SyncedTable` (`kind = lakebase_synced_table`, id = `c.s.t`, parent =
instance id):

```
name, database_instance_name, logical_database_name, effective_logical_database_name,
unity_catalog_provisioning_state: ACTIVE,
spec {
  source_table_full_name, scheduling_policy ∈ SNAPSHOT|TRIGGERED|CONTINUOUS,
  primary_key_columns [..] (≥1, must exist in source), timeseries_key? (must exist),
  create_database_objects_if_missing (default true),
  existing_pipeline_id? | new_pipeline_spec?   (mutually exclusive)
},
data_synchronization_status {
  detailed_state, message, pipeline_id,
  last_sync? { timestamp, delta_table_version },
  provisioning_status { initial_pipeline_sync_progress { sync_progress_completion, synced_row_count, total_row_count } },
  continuous_update_status? / triggered_update_status?
},
creator, created_time
```

Mirror: a `uc_table` at the same name with `table_type = MANAGED`,
`data_source_format = POSTGRESQL`, columns copied from the source table,
`properties = { "lakebase.synced_table": true, "lakebase.source_table",
"lakebase.scheduling_policy" }`.

State machine (`detailed_state`):

```
SNAPSHOT:            PROVISIONING_INITIAL_SNAPSHOT ──3s──► ONLINE_NO_PENDING_UPDATE
TRIGGERED:           PROVISIONING_PIPELINE_RESOURCES ──3s──► ONLINE_TRIGGERED_UPDATE
CONTINUOUS:          PROVISIONING_PIPELINE_RESOURCES ──3s──► ONLINE_CONTINUOUS_UPDATE
any ──DELETE──► (gone)
```

Settling is lazy on read (`settle_synced_table`); `last_sync` is set to
`{ now, delta_table_version: 0 }` on settle.

## API

| Route | Auth | Behaviour |
| --- | --- | --- |
| `POST /api/2.0/database/synced_tables` `{ name, spec }` | `USE` path + `CREATE_TABLE` on target schema; `SELECT` on source | create |
| `GET /api/2.0/database/synced_tables?page_size` | authenticated | list |
| `GET /api/2.0/database/synced_tables/{name}` | authenticated | get (settles state) |
| `DELETE /api/2.0/database/synced_tables/{name}` | owner of mirror table / admin | delete synced table and mirror |

## Requirements

### Requirement: Target must be a database catalog
`name` MUST be three-level and its catalog MUST be a Lakebase database
catalog; otherwise 400.

#### Scenario: Wrong catalog
- **WHEN** `POST /synced_tables { name: "main.default.orders_sync", spec: {…} }`
- **THEN** 400 "not a Lakebase database catalog".

### Requirement: Spec validation against UC metadata
`spec.source_table_full_name` MUST resolve to a UC table the caller can
`SELECT`; `spec.primary_key_columns` MUST be non-empty and each MUST be a
column of the source (case-insensitive); `timeseries_key`, if set, MUST be a
column; `scheduling_policy` MUST be one of `SCHEDULING_POLICIES`; at most one
of `existing_pipeline_id` / `new_pipeline_spec` MAY be set.

#### Scenario: Unknown primary key
- **GIVEN** `main.s.orders(id, region)`
- **WHEN** spec has `primary_key_columns: ["nope"]`
- **THEN** 400 "primary key column 'nope' not found in main.s.orders".

#### Scenario: Both pipeline fields
- **WHEN** spec has both `existing_pipeline_id` and `new_pipeline_spec`
- **THEN** 400.

### Requirement: Status lifecycle
Creation SHALL return a `PROVISIONING_*` state with `sync_progress_completion
= 0.0`; after `STARTING_SECS` a read SHALL return the policy's `ONLINE_*`
state with `last_sync` populated. The `message` SHALL state that data
movement is not implemented while that is true.

#### Scenario: Triggered goes online
- **WHEN** a `TRIGGERED` synced table is created and read after 3 s
- **THEN** `detailed_state == ONLINE_TRIGGERED_UPDATE`.

### Requirement: UC mirror
Creating a synced table SHALL create/overwrite the UC table at `name` with the
source's columns and `data_source_format = POSTGRESQL`; deleting SHALL remove
both; grants on the mirror govern reads of the synced copy (once readable).

#### Scenario: Mirror columns
- **GIVEN** source `main.s.orders(id INT, region STRING)`
- **WHEN** `pgcat.public.orders_sync` is created
- **THEN** `GET /unity-catalog/tables/pgcat.public.orders_sync` lists both
  columns.

### Requirement: Uniqueness and ownership
Duplicate names → 409; delete requires owner of the mirror table or admin.

#### Scenario: Duplicate
- **GIVEN** the synced table exists
- **WHEN** it is created again
- **THEN** 409.

### Requirement: Audit
`createSyncedDatabaseTable` / `deleteSyncedDatabaseTable` events
(`service_name = database`) SHALL be recorded.

#### Scenario: Audited
- **WHEN** a synced table is created
- **THEN** `system.access.audit` contains the event with
  `request_params.name`.

## Negative Cases

- Source table missing → 404.
- Caller lacks `SELECT` on source → 403.
- Caller lacks `CREATE_TABLE` on target schema → 403.
- Instance backing the catalog is `DELETING` → 400 `INVALID_STATE`.
- `spec` missing → 400.

## Tests

- Smoke: `uc-lakebase-smoke.sh` — `lakebase synced table`, `invalid primary
  key rejected`, `synced table ONLINE`; the script deletes an existing synced
  table first so reruns are idempotent.
- Planned (LF-018): pipeline execution tests once movement exists; LF-028
  unique namespaces.

## Parity Boundaries

- **No rows are copied.** Status transitions are simulated; `last_sync`
  reports `delta_table_version: 0` regardless of the source.
- `new_pipeline_spec` is stored, not used to create a Lakeflow pipeline
  (LF-018 wires a pipeline of kind `synced_table` into `api/pipelines.rs`).
- No `TRIGGERED` refresh endpoint (`POST …/synced_tables/{name}/refresh`)
  and no `CONTINUOUS` CDC.
- `system.lakebase.synced_tables` exposes the stored status only.
