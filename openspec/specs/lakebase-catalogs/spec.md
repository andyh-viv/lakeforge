# Lakebase Database Catalogs Specification

## Purpose

A *database catalog* registers a PostgreSQL database on a Lakebase instance as
a Unity Catalog catalog (`securable_kind = CATALOG_DATABASE`), so its tables
are governed by UC privileges and, on Databricks, are queryable from SQL
through Lakehouse Federation. Lakeforge implements the registration and UC
linkage; query federation is a follow-up.

## Scope

In scope: `/api/2.0/database/catalogs` CRUD, the UC catalog it creates,
dependency checks with instances, `database/tables` registration of
PostgreSQL tables as UC tables. Out of scope: reading PostgreSQL data from
Forge (LF-019), creating the PostgreSQL database itself (LF-017).

## Data Model

`DatabaseCatalog` (`kind = lakebase_catalog`, id = name, parent = instance id):

```
name, database_instance_name, database_name (default "databricks_postgres"),
create_database_if_not_exists (default true), uid, creator, created_time,
backend: emulated|external, catalog: <the UC CatalogInfo created>
```

UC side effects: a `uc_catalog` named `name` with
`catalog_type = MANAGED_CATALOG`, `securable_kind = CATALOG_DATABASE`,
`options = { database_instance_name, database_name }`,
`properties = { "lakebase.instance", "lakebase.database" }`, owner = creator.

`DatabaseTable` (`kind = lakebase_table`, id = `c.s.t`): `name`,
`database_instance_name`, `logical_database_name`, `table_serving_url?`,
`creator`, `created_time`; plus a mirrored `uc_table` with
`table_type = EXTERNAL`, `data_source_format = POSTGRESQL`.

## API

| Route | Auth | Behaviour |
| --- | --- | --- |
| `POST /api/2.0/database/catalogs` `{ name, database_instance_name, database_name?, create_database_if_not_exists? }` | instance owner/admin + `CREATE_CATALOG` on metastore (via `uc_create_catalog`) | create link + UC catalog |
| `GET /api/2.0/database/catalogs` | authenticated | list |
| `GET /api/2.0/database/catalogs/{name}` | authenticated | get |
| `DELETE /api/2.0/database/catalogs/{name}` | UC catalog owner/admin | force-deletes the UC catalog and the link |
| `POST /api/2.0/database/tables` `{ name: c.s.t, database_instance_name?, logical_database_name?, table_serving_url? }` | `USE` path + `CREATE_TABLE` on schema | register PostgreSQL table in a database catalog |
| `GET /api/2.0/database/tables/{name}` | authenticated | get |
| `DELETE /api/2.0/database/tables/{name}` | table owner | delete registration and UC table |

## Requirements

### Requirement: Catalog creation registers a UC catalog
Creating a database catalog SHALL create a UC catalog with the same name
(subject to `CREATE_CATALOG` and name uniqueness), tag it as
`CATALOG_DATABASE`, and record the link; the instance MUST exist and the
caller MUST own it or be admin.

#### Scenario: Create
- **GIVEN** instance `db1` owned by `alice`
- **WHEN** `alice` posts `{ name: "pgcat", database_instance_name: "db1" }`
- **THEN** `GET /api/2.1/unity-catalog/catalogs/pgcat` returns
  `securable_kind = CATALOG_DATABASE` and `options.database_instance_name =
  db1`.

#### Scenario: Name clash with a UC catalog
- **GIVEN** a UC catalog `main`
- **WHEN** `POST /catalogs { name: "main", database_instance_name: "db1" }`
- **THEN** 409 and no link document exists.

### Requirement: Governed like any catalog
Grants on the database catalog and its schemas SHALL apply to its tables;
`information_schema.catalogs` SHALL list it; deleting the UC catalog through
the UC API SHALL be blocked while a link exists (LF-016) — today the link is
left dangling (see Parity Boundaries).

#### Scenario: Grant
- **WHEN** `GRANT USE CATALOG ON CATALOG pgcat TO bob`
- **THEN** `effective-permissions` on `pgcat` lists it and `bob` can `USE`
  it in SQL.

### Requirement: Instance dependency
Deleting an instance that backs one or more catalogs SHALL fail with
`INVALID_STATE` listing the catalogs unless `force=true`, in which case the
catalogs and links SHALL be removed too.

#### Scenario: Blocked delete
- **GIVEN** `pgcat` on `db1`
- **WHEN** `DELETE /instances/db1`
- **THEN** 400 mentioning `pgcat`.

### Requirement: Database tables
Registering `c.s.t` SHALL require `c` to be a database catalog, `s` to exist,
`CREATE_TABLE` on `c.s`, and SHALL create a UC table of type `EXTERNAL`
with `data_source_format = POSTGRESQL` and
`table_serving_url = postgresql://<rw dns>/<database>` (columns taken from
the request body; empty until introspection is implemented).

#### Scenario: Register
- **WHEN** `POST /database/tables { name: "pgcat.public.orders" }`
- **THEN** `GET /api/2.1/unity-catalog/tables/pgcat.public.orders` returns
  `table_type = EXTERNAL`, `data_source_format = POSTGRESQL`.

#### Scenario: Not a database catalog
- **WHEN** `POST /database/tables { name: "main.default.x" }`
- **THEN** 400 "not a Lakebase database catalog".

## Negative Cases

- Missing `database_instance_name` → 400.
- Instance in `DELETING` → 400 `INVALID_STATE`.
- Duplicate database catalog → 409.
- Non-owner of instance → 403.
- Two-level table name → 400.

## Tests

- Smoke: `uc-lakebase-smoke.sh` — `lakebase catalog` (created, UC catalog
  present with `CATALOG_DATABASE`).
- Planned (LF-016): blocked instance delete; UC-side delete protection;
  grants flow; database table registration.

## Parity Boundaries

- No PostgreSQL database is created; `create_database_if_not_exists` is
  recorded only (LF-017).
- Tables in a database catalog are not queryable from Forge SQL; `SELECT`
  fails at DataFusion with "table not found" (LF-019 adds a PostgreSQL
  table provider and schema introspection).
- Deleting the UC catalog directly leaves the link document (LF-016).
- Databricks also exposes `database_catalogs` inside instance
  `GET`; Lakeforge does not.
