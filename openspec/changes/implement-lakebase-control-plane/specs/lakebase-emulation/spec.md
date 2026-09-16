# Delta: lakebase-emulation (implement-lakebase-control-plane)

Applies to `openspec/specs/lakebase-emulation/spec.md`. **Proposed**
(LF-017); merge into the capability spec when the external backend lands.

## ADDED Requirements

### Requirement: Backend trait boundary
All PostgreSQL side effects SHALL go through a single `LakebaseBackend`
trait object held by `AppState`; REST handlers and the synced-table pipeline
MUST NOT open PostgreSQL connections directly.

#### Scenario: Emulated is a no-op implementation
- **WHEN** the emulated backend receives `create_instance`
- **THEN** it returns an `Endpoint { host: "<name>.lakebase.invalid", port:
  5432 }` without I/O.

### Requirement: External backend provisions per-instance databases
On `create_instance`, `ExternalBackend` SHALL `CREATE DATABASE
lf_<workspace_id>_<instance>` (idempotent) and record the endpoint; on
`delete_instance` with `purge` it SHALL `DROP DATABASE`; without `purge` it
SHALL rename to `lf_deleted_<ts>_<instance>` and a reaper SHALL drop it
after `retention_window_in_days`.

#### Scenario: Create and purge
- **GIVEN** `LAKEFORGE_LAKEBASE_POSTGRES_URL` points at a reachable server
- **WHEN** instance `db1` is created then deleted with `purge=true`
- **THEN** `pg_database` gains and then loses `lf_<ws>_db1`.

### Requirement: Roles and credentials map to PostgreSQL roles
`ensure_role` SHALL `CREATE ROLE "<instance>__<role>" LOGIN` with
`CREATEDB/CREATEROLE/BYPASSRLS` per `attributes`; credential generation
SHALL `ALTER ROLE … PASSWORD '<token>' VALID UNTIL '<expiration_time>'` for
the caller's role; `drop_role` SHALL `DROP ROLE`.

#### Scenario: psql with token
- **GIVEN** role `bob` on `db1` and a fresh credential
- **WHEN** `psql "host=<rw dns> port=<port> dbname=lf_<ws>_db1 user=db1__bob
  password=<token>"`
- **THEN** the connection succeeds; after `expiration_time` it fails with
  `password authentication failed`.

### Requirement: Failures are surfaced, state unchanged
Backend errors SHALL map to 503 `TEMPORARILY_UNAVAILABLE` with the
PostgreSQL SQLSTATE in `details`; the instance document SHALL remain in its
previous state and an audit event with `response.status_code = 503` SHALL
be recorded.

#### Scenario: Server down
- **WHEN** the external server is unreachable during `POST /instances`
- **THEN** 503 and no instance document exists.

### Requirement: Startup validation
An unparsable `LAKEFORGE_LAKEBASE_POSTGRES_URL` SHALL abort startup; a
parsable but unreachable URL SHALL log a warning and mark
`GET /lakeforge/lakebase/backend` with `ready: false`.

#### Scenario: Bad URL
- **WHEN** `LAKEFORGE_LAKEBASE_POSTGRES_URL=not-a-url`
- **THEN** `lakeforge-api` exits non-zero with a message naming the
  variable.

## MODIFIED Requirements

### Requirement: External backend reports the real host
(Modified: adds `ready`.) With `LAKEFORGE_LAKEBASE_POSTGRES_URL` set, `kind`
MUST be `external`, `read_write_dns` MUST be the URL's host, `port` its
port, and the backend endpoint MUST include `ready: bool` reflecting the
last successful probe.

#### Scenario: Ready
- **WHEN** the probe succeeds
- **THEN** `{ backend: "external", ready: true, … }`.

## REMOVED Requirements

None.
