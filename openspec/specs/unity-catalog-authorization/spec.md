# Unity Catalog Authorization Specification

## Purpose

Define how Lakeforge decides whether a principal may act on a Unity Catalog
securable, on REST and on SQL, and how privileges are granted, revoked,
inspected and inherited. This spec describes behaviour that exists on the
`devin/1789466312-unity-catalog-lakebase` branch; proposed extensions are in
`changes/integrate-uc-sql-enforcement/`.

## Scope

In scope: privilege vocabulary, ownership, inheritance, `ALL_PRIVILEGES`,
`Authorizer` semantics, the UC grants REST API, the SQL choke point,
statement-level authorization, and the privilege SQL grammar subset.
Out of scope: row filters / column masks (`unity-catalog-policies`), audit and
lineage (`unity-catalog-audit-lineage`), workspace object ACLs
(`docs/issues.md` LF-027).

## Data Model

- Securable types: `metastore`, `catalog`, `schema`, `table`, `volume`,
  `function`, `external_location`, `storage_credential`, `connection`,
  `share`, `recipient`, `provider` (`uc::privileges::Securable`).
- Every securable document carries `owner: String`.
- Grants document (`kind = uc_grants`, id derived from securable type + full
  name): `{ "privilege_assignments": [ { "principal": String,
  "privileges": [String] } ] }`.
- Privilege names are upper-case with underscores, exactly as Databricks:
  `ALL_PRIVILEGES`, `USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `MODIFY`,
  `CREATE_TABLE`, `CREATE_SCHEMA`, `CREATE_VOLUME`, `CREATE_FUNCTION`,
  `CREATE_MODEL`, `READ_VOLUME`, `WRITE_VOLUME`, `EXECUTE`, `MANAGE`,
  `READ_FILES`, `WRITE_FILES`, and the rest of the list in
  `uc::privileges::PRIVILEGES` (`CREATE_CATALOG`, `CREATE_EXTERNAL_*`,
  `CREATE_FOREIGN_*`, `CREATE_MANAGED_STORAGE`, `CREATE_MATERIALIZED_VIEW`,
  `CREATE_VIEW`, `CREATE_SHARE/RECIPIENT/PROVIDER`,
  `CREATE_STORAGE_CREDENTIAL`, `CREATE_SERVICE_CREDENTIAL`, `USE_CONNECTION`,
  `USE_SHARE/RECIPIENT/PROVIDER`, `USE_MARKETPLACE_ASSETS`, `APPLY_TAG`,
  `BROWSE`, `REFRESH`, `SET_SHARE_PERMISSION`, `MANAGE_ALLOWLIST`,
  `READ_PRIVATE_FILES`, `WRITE_PRIVATE_FILES`).
- Inheritance chain: `metastore → catalog → schema → {table, volume,
  function}`. Metastore-level securables (`external_location`,
  `storage_credential`, `connection`, `share`, `recipient`, `provider`)
  inherit from `metastore` only.
- Principals: user emails, group display names, service-principal
  application ids. Implicit groups: `users` (every workspace user),
  `account users`, `admins` (workspace admins).

## API

| Route | Behaviour |
| --- | --- |
| `GET /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}` | direct assignments; requires visibility of the securable |
| `PATCH /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}` | body `{ "changes": [ { "principal", "add": [..], "remove": [..] } ] }`; requires owner or `MANAGE` |
| `GET /api/2.1/unity-catalog/effective-permissions/{securable_type}/{full_name}` | direct + inherited with `inherited_from_type` / `inherited_from_name` |
| `PATCH /api/2.1/unity-catalog/{catalogs,schemas,tables,volumes,functions,…}/{name}` with `owner` | ownership transfer; requires current owner or admin; new owner must exist |
| `POST /api/2.0/sql/statements`, notebooks, jobs, SQL editor | all go through `AppState::execute_sql` |

SQL grammar accepted by `uc::grant_sql::parse` (statement is intercepted
before analysis, never sent to Forge):

```
GRANT <priv>[, <priv>…] ON [<securable-type>] <name> TO <principal>
REVOKE <priv>[, <priv>…] ON [<securable-type>] <name> FROM <principal>
SHOW GRANTS [<principal>] ON [<securable-type>] <name>
ALTER <securable-type> <name> [SET] OWNER TO <principal>
```

where `<priv>` is a Databricks privilege spelled with spaces or underscores
(`USE CATALOG` ≡ `USE_CATALOG`) or `ALL PRIVILEGES`; `<securable-type>` is
one of `CATALOG | SCHEMA | DATABASE | TABLE | VIEW | VOLUME | FUNCTION |
EXTERNAL LOCATION | STORAGE CREDENTIAL | CONNECTION | SHARE | RECIPIENT |
PROVIDER | METASTORE`; `<name>` may be 1–3 dotted parts, backtick-quoted, and
resolves against the session's default catalog/schema. `<principal>` may be
backtick-quoted (for spaces or hyphens).

## Requirements

### Requirement: Privilege vocabulary is validated
The system MUST reject any privilege name outside the Databricks vocabulary
with `INVALID_PARAMETER_VALUE`, on REST and SQL.

#### Scenario: Unknown privilege via REST
- **GIVEN** an admin
- **WHEN** they `PATCH /permissions/table/main.s.t` with `add: ["SELCT"]`
- **THEN** the response is 400 with `error_code = INVALID_PARAMETER_VALUE`
  and no grants document changes.

#### Scenario: Unknown privilege via SQL
- **WHEN** a user runs `GRANT FROBNICATE ON TABLE main.s.t TO bob`
- **THEN** the statement fails with `INVALID_PARAMETER_VALUE` and is not sent
  to Forge.

### Requirement: Owners hold all privileges
The owner of a securable SHALL be authorised for every privilege on that
securable and on everything beneath it, without an explicit grant.

#### Scenario: Owner reads their table
- **GIVEN** `alice` owns `main.sales.orders` and has no grants
- **WHEN** `alice` runs `SELECT * FROM main.sales.orders`
- **THEN** the query executes.

### Requirement: Grants inherit downward
A privilege granted on a securable SHALL apply to all descendants, and
`effective-permissions` SHALL report the inherited source.

#### Scenario: Catalog-level SELECT applies to a table
- **GIVEN** `GRANT SELECT ON CATALOG main TO bob`
- **WHEN** `GET /effective-permissions/table/main.sales.orders`
- **THEN** the response contains `{ principal: bob, privilege: SELECT,
  inherited_from_type: CATALOG, inherited_from_name: main }`.

### Requirement: Access requires the USE chain
Reading or writing an object SHALL require `USE_CATALOG` on its catalog and
`USE_SCHEMA` on its schema in addition to the object privilege, unless the
principal owns the parent or holds `ALL_PRIVILEGES` on it.

#### Scenario: SELECT without USE_SCHEMA is denied
- **GIVEN** `bob` has `SELECT` on `main.sales.orders` and `USE_CATALOG` on
  `main` but no `USE_SCHEMA` on `main.sales`
- **WHEN** `bob` runs `SELECT 1 FROM main.sales.orders`
- **THEN** the statement fails with `PERMISSION_DENIED` before reaching
  Forge.

### Requirement: ALL_PRIVILEGES expands
`ALL_PRIVILEGES` SHALL satisfy any privilege check on the securable and its
descendants and SHALL be reported as `ALL_PRIVILEGES` (not expanded) in
`permissions`.

#### Scenario: ALL_PRIVILEGES on schema allows CREATE TABLE
- **GIVEN** `GRANT ALL PRIVILEGES ON SCHEMA main.sales TO bob` and `USE
  CATALOG` on `main`
- **WHEN** `bob` runs `CREATE TABLE main.sales.t2 (id INT)`
- **THEN** the table is created and mirrored as a UC table owned by `bob`.

### Requirement: Grant mutation is gated
Changing grants SHALL require the caller to be the owner, hold `MANAGE` on
the securable (directly or inherited), or be a workspace admin; principals
named in changes MUST exist.

#### Scenario: Non-owner cannot grant
- **GIVEN** `bob` has only `SELECT` on `main.sales.orders`
- **WHEN** `bob` runs `GRANT SELECT ON TABLE main.sales.orders TO carol`
- **THEN** the statement fails with `PERMISSION_DENIED`.

#### Scenario: Unknown principal
- **WHEN** an admin grants to `nobody@example.com` (not a user, group or
  service principal)
- **THEN** the response is 404 `RESOURCE_DOES_NOT_EXIST`.

### Requirement: All SQL is authorized through one path
Every SQL statement submitted through any surface SHALL pass through
`AppState::execute_sql → prepare_sql`, which SHALL classify the statement
(`Select`, `Insert`, `Update`, `Delete`, `CreateTable`, `CreateView`, `Drop`,
`Alter`, `Describe`, `Show`, `Use`, `Set`, `Explain`, `Other`) and check the
required privileges on every referenced table, path and function before
sending anything to Forge.

#### Scenario: Notebook and Statement API agree
- **GIVEN** `bob` lacks `SELECT` on `main.s.secret`
- **WHEN** `bob` runs `SELECT * FROM main.s.secret` from a notebook cell and
  from `POST /api/2.0/sql/statements`
- **THEN** both fail with `PERMISSION_DENIED`, and neither statement appears
  in Forge's job list.

#### Scenario: Write requires MODIFY
- **GIVEN** `bob` has `SELECT` but not `MODIFY` on `main.s.t`
- **WHEN** `bob` runs `INSERT INTO main.s.t VALUES (1)`
- **THEN** the statement fails with `PERMISSION_DENIED`.

#### Scenario: System tables are read-only
- **WHEN** any principal runs `INSERT INTO system.access.audit VALUES (...)`
- **THEN** the statement fails with `PERMISSION_DENIED`.

### Requirement: Path access requires an external location privilege
Statements reading or writing a storage path (`CREATE TABLE … LOCATION`,
`COPY INTO`, `read_files`, path-backed `CREATE EXTERNAL LOCATION`) SHALL
require `READ_FILES`/`WRITE_FILES` on an external location whose URL is a
prefix of the path, or admin.

#### Scenario: Unregistered path denied
- **WHEN** `bob` runs `CREATE TABLE main.s.ext LOCATION 's3://other/x'` with
  no covering external location
- **THEN** the statement fails with `PERMISSION_DENIED`.

### Requirement: Privilege SQL executes as a metastore operation
`GRANT`, `REVOKE`, `SHOW GRANTS` and `ALTER … OWNER TO` SHALL be parsed by
`grant_sql::parse`, executed against the metastore through the same code as
the REST endpoints, and SHALL NOT be forwarded to Forge. `SHOW GRANTS` SHALL
return rows with columns `Principal`, `ActionType`, `ObjectType`,
`ObjectKey`, including inherited privileges (whose `ObjectType`/`ObjectKey`
name the ancestor).

#### Scenario: GRANT visible via REST
- **WHEN** an admin runs `GRANT SELECT ON TABLE main.s.t TO \`users\``
- **THEN** `GET /permissions/table/main.s.t` lists `SELECT` for `users`.

#### Scenario: SHOW GRANTS with default resolution
- **GIVEN** session conf `forge.sql.defaultCatalog = main` and
  `forge.sql.defaultSchema = s`
- **WHEN** `SHOW GRANTS ON TABLE t`
- **THEN** rows are returned for `main.s.t` and its ancestors.

#### Scenario: Owner transfer
- **WHEN** the owner runs `ALTER TABLE main.s.t OWNER TO \`alice\``
- **THEN** `GET /tables/main.s.t` returns `owner = alice` and the previous
  owner loses implicit privileges.

### Requirement: Admins bypass
Workspace admins (`admins` group) SHALL pass every UC authorization check.

#### Scenario: Admin drops another user's table
- **WHEN** an admin runs `DROP TABLE main.s.t` owned by `bob`
- **THEN** the table is dropped and the UC document deleted.

## Negative Cases

- Unknown securable type in REST path → 400 `INVALID_PARAMETER_VALUE`.
- Missing securable → 404 `RESOURCE_DOES_NOT_EXIST` (after visibility check;
  a principal without any privilege on an object receives 404, not 403, on
  reads, matching Databricks' non-disclosure).
- Malformed `GRANT` (missing `TO`) → 400 with parser message.
- Grant to self by non-owner → `PERMISSION_DENIED`.
- Revoking a privilege not held → no-op 200.
- `SHOW GRANTS` on an object the caller cannot see → 404.

## Tests

- Unit: `uc::grant_sql::tests` (parsing, defaults, errors);
  `uc::privileges` inheritance and `ALL_PRIVILEGES` unit tests;
  `uc::sqlguard` statement classification.
- Smoke: `tests/smoke/uc-lakebase-smoke.sh` checks `grant`, `effective
  permissions`, `sql show grants`, `sql grant`, `sql revoke`, `sql owner to`,
  `sql grant visible via REST`, `sql invalid privilege rejected`, `non-admin
  denied`, `non-admin write denied`, `system table write denied`.
- Planned (LF-001): authorization matrix fixture ≥40 statements ×
  {owner, granted, USE-only, none}.

## Parity Boundaries

- Grammar subset only: no `DENY`, no `SHOW GRANTS TO <principal>` across the
  metastore, no `ON ALL TABLES IN SCHEMA`, no `GRANT … WITH GRANT OPTION`
  (LF-002).
- `MANAGE` is honoured for grant mutation but not yet for all
  DDL (`ALTER`/`DROP` still require ownership or admin) (LF-001).
- No metastore-level `CREATE CATALOG` privilege check separate from admin.
- Workspace-catalog bindings are stored but not consulted by the authorizer
  (LF-001).
- Databricks returns `PERMISSION_DENIED` for some non-visible objects where
  Lakeforge returns 404; exact code parity per endpoint is not guaranteed.
