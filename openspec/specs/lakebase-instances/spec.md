# Lakebase Database Instances Specification

## Purpose

Lakebase is Databricks' managed PostgreSQL. A *database instance* is the unit
of compute (capacity `CU_n`, PG 16) that hosts databases, roles and synced
tables. Lakeforge implements the Databricks `/api/2.0/database/instances`
surface with the same field names and lifecycle states, backed by a pluggable
backend (`lakebase-emulation` spec).

## Scope

In scope: instance CRUD, lifecycle states, capacity/node/retention
validation, branching metadata (`parent_instance_ref`), roles, ownership,
pagination, `findByUid`. Out of scope: credentials (`lakebase-credentials`),
catalogs (`lakebase-catalogs`), synced tables (`lakebase-synced-tables`),
the actual PostgreSQL server (`lakebase-emulation`).

## Data Model

`DatabaseInstance` (`kind = lakebase_instance`, id = name):

```
name (1–63 [a-z0-9-]), uid (uuid), creator, state, stopped, effective_stopped,
capacity ∈ CAPACITIES = [CU_1, CU_2, CU_4, CU_8], effective_capacity,
node_count 1..4, effective_node_count, enable_readable_secondaries (default node_count > 1),
retention_window_in_days 2..35 (default 7), enable_pg_native_login, usage_policy_id,
custom_tags [{key,value}], pg_version = "PG_VERSION_16",
read_write_dns, read_only_dns?, port,
creation_time, updated_time (RFC3339),
parent_instance_ref? { name, uid, branch_time, lsn }, child_instance_refs [ { name, uid, branch_time } ],
backend { kind: emulated|external, host, emulated: bool }
```

`effective_*` mirror the requested values (Databricks reports them
separately when a change is pending).

States: `STARTING → AVAILABLE`, `AVAILABLE → UPDATING → AVAILABLE`,
`AVAILABLE|STOPPED → DELETING → (gone)`, `stopped = true` → `STOPPED`,
`FAILING_OVER → AVAILABLE`. Transitions from `STARTING/UPDATING/FAILING_OVER`
occur after `STARTING_SECS = 3` seconds and are applied lazily
(`settle_instance`) whenever the instance is read or listed; there is no
background worker yet (LF-013).

`InstanceRole` (`kind = lakebase_role`, id = `<instance>:<role>`):
`name`, `identity_type ∈ USER|GROUP|SERVICE_PRINCIPAL|PG_ONLY`,
`membership_role` (free-form string, default `DATABRICKS_SUPERUSER`),
`attributes { createdb, createrole, bypassrls }`, `instance_name`,
`created_time`, `created_by`.

## API

| Route | Behaviour |
| --- | --- |
| `POST /api/2.0/database/instances` | create; body = instance fields; returns the document in `STARTING` |
| `GET /api/2.0/database/instances?page_size` | list (admins: all; others: own); `page_size` truncates, no `next_page_token` yet |
| `GET /api/2.0/database/instances/{name}` | get (applies pending transition) |
| `GET /api/2.0/database/instances:findByUid?uid=` | lookup by uid |
| `PATCH /api/2.0/database/instances/{name}?update_mask=capacity,node_count,…` | update masked fields; state → `UPDATING` |
| `DELETE /api/2.0/database/instances/{name}?force&purge` | state → `DELETING`; `force` required if children or catalogs depend on it |
| `POST /api/2.0/database/instances/{name}/roles` | create role |
| `GET …/roles`, `GET …/roles/{role}`, `DELETE …/roles/{role}` | role CRUD |
| `GET /api/2.0/lakeforge/lakebase/backend` | backend descriptor (see emulation spec) |

Authorization: any authenticated user may create; mutation (`PATCH`,
`DELETE`, role changes) requires the instance `creator` or admin; `GET` by
name is open to authenticated users while the list is filtered to the
caller's own instances for non-admins (see Parity Boundaries).

## Requirements

### Requirement: Names and parameters are validated
The system MUST reject instance names outside `^[a-z0-9-]{1,63}$`,
capacities outside `CAPACITIES`, `node_count` outside 1..4,
`retention_window_in_days` outside 2..35 with 400 `INVALID_PARAMETER_VALUE`,
and duplicate names with 409 `RESOURCE_ALREADY_EXISTS`.

#### Scenario: Invalid capacity
- **WHEN** `POST /instances { name: "db1", capacity: "CU_3" }`
- **THEN** 400 and no document is created.

#### Scenario: Duplicate
- **GIVEN** `db1` exists
- **WHEN** `POST /instances { name: "db1", capacity: "CU_1" }`
- **THEN** 409.

### Requirement: Lifecycle
A new instance SHALL be returned in `STARTING` and SHALL read as `AVAILABLE`
(or `STOPPED` if created with `stopped = true`) after `STARTING_SECS`; a
`PATCH` SHALL move it to `UPDATING` then back; a `DELETE` SHALL move it to
`DELETING` and it SHALL disappear from `GET`/list after `STARTING_SECS`.
Mutations on a `DELETING` instance SHALL fail with `INVALID_STATE`.

#### Scenario: Becomes available
- **WHEN** an instance is created and read again after 3 s
- **THEN** `state == AVAILABLE` and `updated_time` advanced.

#### Scenario: Update while deleting
- **GIVEN** `db1` in `DELETING`
- **WHEN** `PATCH /instances/db1`
- **THEN** 400 `INVALID_STATE`.

### Requirement: Branching metadata
Creating with `parent_instance_ref { name }` SHALL require the parent to
exist, copy its `uid`, default `branch_time` to now, and append a
`child_instance_refs` entry on the parent. Deleting a parent with children
SHALL require `force=true` and SHALL cascade-delete the children.

#### Scenario: Branch
- **WHEN** `POST /instances { name: "db1-dev", capacity: "CU_1",
  parent_instance_ref: { name: "db1" } }`
- **THEN** `GET /instances/db1` lists `db1-dev` in `child_instance_refs`.

### Requirement: Ownership
Only the creator or an admin SHALL update or delete an instance or manage its
roles; others receive 403 `PERMISSION_DENIED`.

#### Scenario: Non-owner delete
- **WHEN** `bob` deletes `db1` created by `alice`
- **THEN** 403.

### Requirement: Roles
Role names SHALL be non-empty without whitespace; `identity_type` SHALL be one
of the four values; `membership_role` SHALL default to
`DATABRICKS_SUPERUSER`; deleting a role that does not exist SHALL be 404.
The creator holds implicit access without a role document.

#### Scenario: Create group role
- **WHEN** `POST /instances/db1/roles { name: "data-eng", identity_type:
  "GROUP" }`
- **THEN** `GET /instances/db1/roles` lists it with default attributes
  `{ createdb: false, createrole: false, bypassrls: false }`.

### Requirement: DNS fields
`read_write_dns` SHALL be `<name>.<backend host>` (emulated) or the external
host; `read_only_dns` SHALL be set only when `enable_readable_secondaries`;
`port` SHALL be the backend port (5432 by default).

#### Scenario: Emulated DNS
- **WHEN** an instance `db1` is created on the emulated backend
- **THEN** `read_write_dns == "db1.lakebase.invalid"` and `backend.emulated
  == true`.

### Requirement: Audit
Create/update/delete/role changes SHALL emit audit events with
`service_name = database` and Databricks action names
(`createDatabaseInstance`, `updateDatabaseInstance`,
`deleteDatabaseInstance`, `createDatabaseInstanceRole`,
`deleteDatabaseInstanceRole`).

#### Scenario: Create audited
- **WHEN** an instance is created
- **THEN** `system.access.audit` has a `createDatabaseInstance` row with
  `request_params.capacity`.

## Negative Cases

- `findByUid` with unknown uid → 404.
- `PATCH` with unknown field in `update_mask` → 400.
- `PATCH` without `update_mask` → applies all provided mutable fields
  (Databricks tolerates this; Lakeforge does too).
- Delete instance backing a database catalog without `force` → 400
  `INVALID_STATE` listing the catalogs.
- `parent_instance_ref` not an object → 400.
- `page_size = 0` → treated as 1.

## Tests

- Smoke: `uc-lakebase-smoke.sh` — `lakebase backend`, `lakebase create
  instance`, `invalid capacity rejected`, `instance AVAILABLE`, `findByUid`,
  `role`, `instance update`, `instance delete`.
- Planned (LF-013): pure state-machine unit tests; branch cascade; non-owner
  denial; page tokens.

## Parity Boundaries

- Instances are metadata only unless the `external` backend is configured,
  and even then no server-side resources are created yet (LF-017).
- Databricks uses instance permissions (`CAN_USE`, `CAN_MANAGE`) through the
  permissions API; Lakeforge uses creator/admin ownership (LF-013/LF-027).
- No `next_page_token`; lists are truncated to `page_size` (LF-013).
- Lifecycle transitions are lazy on read; an instance nobody reads never
  settles (LF-013 adds a worker).
- `FAILING_OVER` is never entered by Lakeforge itself.
- `usage_policy_id`, `custom_tags`, `enable_pg_native_login` are stored, not
  acted upon.
- No `lsn`-based point-in-time branching; `branch_time`/`lsn` are recorded
  only.
