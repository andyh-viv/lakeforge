# Unity Catalog System Tables and information_schema Specification

## Purpose

Databricks exposes platform telemetry as `system.<schema>.<table>` and
metastore metadata as `<catalog>.information_schema.<table>`, all queryable
with plain SQL. Lakeforge registers the same tables, generates their rows from
the control-plane store on demand, and materialises them as Parquet so Forge
can query them like any other table.

## Scope

In scope: the system catalog and schemas, table registry and column
definitions, row generation, materialisation and refresh, access control,
system-schema enable/disable API, `information_schema` views.
Out of scope: billing meters (`billing.usage` is proposed), streaming
delivery, `system.access.outbound_network`.

## Data Model

- Virtual catalog `system` (owner = admin user), virtual schemas
  `SYSTEM_SCHEMAS = information_schema, access, query, compute, lakeflow,
  billing, mlflow, serving, lakebase`. Virtual documents are synthesised by
  `system_tables::virtual_table_doc` — nothing is stored for them except the
  enabled-schema list (`kind = uc_system_schemas`).
- Registry `system_tables::tables() -> Vec<SysTable { schema, name,
  comment, columns: Vec<(name, Ty)> }>`. Tables registered today:

| Schema | Tables |
| --- | --- |
| `access` | `audit`, `table_lineage`, `column_lineage`, `workspaces_latest` |
| `query` | `history` |
| `compute` | `clusters`, `warehouses`, `node_types`, `warehouse_events` |
| `lakeflow` | `jobs`, `job_tasks`, `job_run_timeline`, `job_task_run_timeline`, `pipelines`, `pipeline_update_timeline` |
| `billing` | `list_prices` |
| `mlflow` | `experiments_latest`, `runs_latest` |
| `serving` | `endpoint_usage` |
| `lakebase` | `instances`, `synced_tables` |
| `information_schema` | `information_schema_catalog_name`, `metastores`, `catalogs`, `schemata`, `tables`, `views`, `columns`, `volumes`, `routines`, `parameters`, `models`, `model_versions`, `model_version_aliases`, `catalog_privileges`, `schema_privileges`, `table_privileges`, `volume_privileges`, `routine_privileges`, `external_location_privileges`, `storage_credential_privileges`, `connection_privileges`, `metastore_privileges`, `catalog_tags`, `schema_tags`, `table_tags`, `column_tags`, `volume_tags`, `table_constraints`, `key_column_usage`, `referential_constraints`, `constraint_column_usage`, `check_constraints`, `row_filters`, `column_masks`, `external_locations`, `storage_credentials`, `connections`, `shares`, `recipients`, `providers` |

- Materialised location: `<storage_root>/system/<schema>/<name>/data.parquet`;
  the table is registered on every running Forge driver as
  `system.<schema>.<name>` with `table_type = SYSTEM`, format `PARQUET`.

## API

| Route | Behaviour |
| --- | --- |
| `GET /api/2.1/unity-catalog/metastores/{id}/systemschemas` | `{ schemas: [{ schema, state: ENABLE_COMPLETED\|AVAILABLE }] }` |
| `PUT /api/2.1/unity-catalog/metastores/{id}/systemschemas/{schema}` | enable (admin) |
| `DELETE …/systemschemas/{schema}` | disable (admin); `information_schema` cannot be disabled |
| `GET /api/2.0/lakeforge/system-tables` | registry with columns |
| `GET /api/2.0/lakeforge/system-tables/{schema}/{name}?limit` | rows as JSON (same authorization as SQL) |
| `GET /api/2.1/unity-catalog/schemas?catalog_name=system`, `tables?catalog_name=system&schema_name=access` | virtual listings |
| `SELECT … FROM system.<schema>.<table>` / `<catalog>.information_schema.<table>` | via `execute_sql` |

## Requirements

### Requirement: System tables are queryable from SQL
Any statement referencing `system.<schema>.<table>` SHALL, after
authorization, cause that table to be (re)materialised from the current
store contents and registered on the target cluster before the statement is
sent to Forge, so the query reflects state as of statement start.

#### Scenario: Audit table reflects a just-made call
- **GIVEN** an admin has just granted a privilege via REST
- **WHEN** they run `SELECT action_name FROM system.access.audit ORDER BY
  event_time DESC LIMIT 1`
- **THEN** the result is `updatePermissions`.

#### Scenario: Unknown system table
- **WHEN** `SELECT * FROM system.access.nope`
- **THEN** the statement fails with `TABLE_OR_VIEW_NOT_FOUND` before reaching
  Forge.

### Requirement: System schemas are gated
Reading `system.<schema>.*` SHALL require the schema to be enabled and the
caller to be admin or hold `SELECT` (directly or inherited) on
`system.<schema>`; `information_schema` SHALL be readable by every
authenticated user but SHALL only return rows for securables the caller can
see.

#### Scenario: Non-admin without grant
- **WHEN** `bob` runs `SELECT * FROM system.access.audit`
- **THEN** `PERMISSION_DENIED`.

#### Scenario: Non-admin with grant
- **GIVEN** `GRANT SELECT ON SCHEMA system.access TO bob`
- **WHEN** `bob` runs the same query
- **THEN** rows are returned.

#### Scenario: Disabled schema
- **GIVEN** `DELETE …/systemschemas/billing`
- **WHEN** an admin queries `system.billing.list_prices`
- **THEN** the statement fails with a "schema not enabled" error.

### Requirement: System tables are read-only
`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, `ALTER`, `CREATE` targeting
anything in catalog `system` SHALL fail with `PERMISSION_DENIED` for every
principal, including admins.

#### Scenario: Admin cannot drop
- **WHEN** an admin runs `DROP TABLE system.access.audit`
- **THEN** `PERMISSION_DENIED`.

### Requirement: Column definitions match Databricks
Each registered table SHALL expose the Databricks-documented column names and
compatible types (`STRING`, `TIMESTAMP`, `DATE`, `BIGINT`, `BOOLEAN`,
`MAP<STRING,STRING>` rendered as JSON string), and `DESCRIBE system.<s>.<t>`
SHALL list exactly those columns.

#### Scenario: Describe history
- **WHEN** `DESCRIBE system.query.history`
- **THEN** columns include `statement_id`, `executed_by`, `statement_text`,
  `execution_status`, `start_time`, `end_time`, `total_duration_ms`,
  `read_rows`, `produced_rows`, `statement_type`.

### Requirement: information_schema reflects the metastore and grants
`information_schema.tables/columns/schemata/catalogs/routines/volumes` SHALL
list only objects the caller can see (owner, any privilege, or admin);
`*_privileges` SHALL list direct grants (`grantor`, `grantee`, `privilege_type`,
`is_grantable = NO`, `inherited_from`); `row_filters`/`column_masks` SHALL
reflect attached policies.

#### Scenario: Grants visible in information_schema
- **GIVEN** `GRANT SELECT ON TABLE main.s.t TO bob`
- **WHEN** an admin runs `SELECT grantee, privilege_type FROM
  main.information_schema.table_privileges WHERE table_name = 't'`
- **THEN** a row `(bob, SELECT)` is returned.

### Requirement: Listing the system catalog
`SHOW SCHEMAS IN system`, `SHOW TABLES IN system.access`, and the UC REST
list endpoints SHALL return the virtual schemas/tables without requiring
materialisation; after a cluster starts, all system tables SHALL be
materialised once so Forge's own `SHOW TABLES` agrees.

#### Scenario: REST listing
- **WHEN** `GET /api/2.1/unity-catalog/tables?catalog_name=system&schema_name=lakebase`
- **THEN** `instances` and `synced_tables` are listed with `table_type =
  SYSTEM`.

## Negative Cases

- Enabling an unknown schema → 400.
- Disabling `information_schema` → 400.
- `GET /lakeforge/system-tables/{schema}/{name}` for a disabled schema → 403.
- Materialisation failure (storage error) → statement fails with
  `INTERNAL_ERROR`; nothing partially registered.
- `information_schema` in a catalog the caller cannot `USE` → 404.

## Tests

- Smoke: `uc-lakebase-smoke.sh` — `system schemas`, `information_schema
  tables`, `system.access.audit` via SQL, `system.query.history` via SQL,
  `system table write denied`.
- Unit (planned, LF-010/011): column set per table vs. a fixture copied from
  Databricks docs; virtual doc synthesis; visibility filtering.

## Parity Boundaries

- Full re-materialisation on every referencing statement; cost is O(rows)
  per query and the 200k caps bound it. Incremental/append-only refresh is
  proposed (LF-010).
- `billing.usage`, `billing.account_prices`, `compute.node_timeline`,
  `access.outbound_network`, `marketplace.*`, `storage.*`, `query.history`
  metrics columns are missing or empty (LF-010).
- `information_schema` is one metastore-wide set filtered per catalog; some
  Databricks columns (`is_grantable` semantics, `data_type` details for
  complex types, `routine_privileges` on non-SQL routines) are approximated
  (LF-011).
- System tables are Parquet snapshots, not Delta; time travel is not
  available on them.
- No streaming/`readStream` on system tables.
