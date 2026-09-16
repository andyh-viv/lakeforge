# Lakebase Backend and Emulation Specification

## Purpose

Lakeforge separates the Lakebase *control plane* (instances, roles,
credentials, catalogs, synced tables — the four sibling specs) from the
*data plane* (a PostgreSQL server). This spec defines the backend
abstraction, the default metadata-only **emulated** backend, the
**external** backend that points at an operator-provided PostgreSQL, and the
disclosure rules that keep users from mistaking emulation for a managed
database.

## Scope

In scope: backend selection, `Backend` descriptor, DNS/port derivation,
disclosure endpoint and fields, behaviour differences per backend, the
contract a real backend must satisfy. Out of scope: provisioning code for
any backend (LF-017), Forge federation (LF-019), deployment manifests
(LF-023).

## Data Model

```rust
pub struct Backend { pub kind: &'static str /* "emulated" | "external" */, pub host: String, pub port: u16 }
```

Selection (`AppState::lakebase_backend`):

| `LAKEFORGE_LAKEBASE_POSTGRES_URL` | kind | host | port |
| --- | --- | --- | --- |
| unset / unparsable | `emulated` | `lakebase.invalid` | 5432 |
| `postgres://user:pw@h:p/db` | `external` | `h` | `p` (default 5432) |

Every instance, catalog and credential document carries the backend it was
created on (`backend.kind`, `backend.host`, `backend.emulated`).

Proposed trait (LF-017; not yet in code):

```rust
#[async_trait]
pub trait LakebaseBackend: Send + Sync {
    fn kind(&self) -> &'static str;
    async fn create_instance(&self, inst: &DatabaseInstance) -> ApiResult<Endpoint>;
    async fn delete_instance(&self, name: &str) -> ApiResult<()>;
    async fn ensure_role(&self, instance: &str, role: &InstanceRole, password: &str) -> ApiResult<()>;
    async fn drop_role(&self, instance: &str, role: &str) -> ApiResult<()>;
    async fn ensure_database(&self, instance: &str, database: &str) -> ApiResult<()>;
    async fn set_role_password(&self, instance: &str, role: &str, password: &str, expires_at: i64) -> ApiResult<()>;
    async fn introspect_table(&self, instance: &str, database: &str, schema: &str, table: &str) -> ApiResult<Vec<ColumnInfo>>;
}
```

with implementations `EmulatedBackend` (no-ops), `ExternalBackend` (SQLx
Postgres against the configured URL, one database per instance or one
schema per instance), and later `KubernetesBackend` (StatefulSet per
instance).

## API

| Route | Response |
| --- | --- |
| `GET /api/2.0/lakeforge/lakebase/backend` | `{ backend, host, port, emulated: bool, pg_version, capacities[], scheduling_policies[] }` |

All Lakebase responses that describe a connection target
(`read_write_dns`, `read_only_dns`, `table_serving_url`, credential
`backend`) SHALL derive from the `Backend`.

## Requirements

### Requirement: Emulated is the default and is explicit
With no PostgreSQL URL configured, the system MUST run the `emulated`
backend, MUST return `emulated: true` from the backend endpoint, MUST use the
reserved, unresolvable host `lakebase.invalid` in every DNS field, and MUST
say so in `data_synchronization_status.message` and in the UI.

#### Scenario: Default backend
- **WHEN** `GET /api/2.0/lakeforge/lakebase/backend` on a stock dev server
- **THEN** `{ backend: "emulated", emulated: true, host: "lakebase.invalid",
  port: 5432, pg_version: "PG_VERSION_16" }`.

#### Scenario: Connection attempt fails clearly
- **WHEN** a client resolves `db1.lakebase.invalid`
- **THEN** DNS resolution fails (`.invalid` is reserved by RFC 6761); no
  Lakeforge component listens on 5432.

### Requirement: External backend reports the real host
With `LAKEFORGE_LAKEBASE_POSTGRES_URL` set, `kind` MUST be `external`,
`read_write_dns` MUST be the URL's host and `port` its port, and the
database name in `table_serving_url` MUST be the catalog's `database_name`.

#### Scenario: External
- **GIVEN** `LAKEFORGE_LAKEBASE_POSTGRES_URL=postgres://lf:x@pg.internal:5433/postgres`
- **WHEN** instance `db1` is created
- **THEN** `read_write_dns == "pg.internal"`, `port == 5433`,
  `backend.kind == "external"`.

### Requirement: Backend does not change control-plane semantics
Validation, state machines, authorization, audit and UC registration MUST be
identical across backends; only side effects on PostgreSQL differ.

#### Scenario: Same validation
- **WHEN** an invalid capacity is posted on either backend
- **THEN** 400 with the same message.

### Requirement: Real backends implement the trait contract
A backend that claims `emulated: false` MUST (LF-017):
1. create a database (or schema) per instance and drop it on delete
   (`purge=true` drops data; otherwise it is retained for
   `retention_window_in_days`),
2. create/drop PostgreSQL roles for `InstanceRole`s, granting
   `createdb/createrole/bypassrls` per `attributes`,
3. on credential generation, set the caller's role password to the minted
   token with `VALID UNTIL expiration_time`, so the token works as the
   PostgreSQL password,
4. create the database for a database catalog when
   `create_database_if_not_exists`,
5. introspect columns for `database/tables` registration,
6. surface connection failures as 503 `TEMPORARILY_UNAVAILABLE` with the
   instance state left unchanged.

#### Scenario: Credential works as password (target)
- **GIVEN** an external backend and role `bob` on `db1`
- **WHEN** `bob` mints a credential and runs `psql "host=<rw dns> user=bob
  password=<token> dbname=databricks_postgres"`
- **THEN** the connection succeeds until `expiration_time`.

### Requirement: Never misrepresent emulation
Documentation, the `parity.md` matrix, the backend endpoint, the UI banner and
`system.lakebase.instances.backend` MUST all agree on the backend kind. No
document, log line or message may call an emulated instance a running
PostgreSQL server.

#### Scenario: System table
- **WHEN** `SELECT DISTINCT backend FROM system.lakebase.instances`
- **THEN** every row equals the backend endpoint's `backend` value.

## Negative Cases

- Unparsable `LAKEFORGE_LAKEBASE_POSTGRES_URL` → silently falls back to
  `emulated` (LF-017: should be a hard startup error).
- Changing the URL after instances exist → existing documents keep their
  recorded `backend` (they are not migrated); the backend endpoint reports
  the new value. LF-017 defines a migration check.
- External URL reachable but database unreachable → today nothing checks;
  LF-017 adds a readiness probe.

## Tests

- Smoke: `uc-lakebase-smoke.sh` — `lakebase backend` (`emulated` and
  `pg_version` reported).
- Planned: LF-017 integration test with a PostgreSQL service container
  (`services: postgres` in CI) exercising the six contract items; LF-023
  Compose/Helm profiles.

## Parity Boundaries

- Emulated: **no PostgreSQL**, no data, no connections; tokens are
  Lakeforge API tokens.
- External: metadata points at a real host but Lakeforge creates nothing on
  it yet; the token is not a valid PostgreSQL password.
- Databricks-managed features with no Lakeforge counterpart: point-in-time
  restore (`lsn`), autoscaling compute, readable secondaries as real
  replicas, PG extensions management, `pg_native_login` accounts, network
  policies, usage/billing for `CU` hours.
