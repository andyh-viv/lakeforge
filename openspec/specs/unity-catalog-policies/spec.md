# Unity Catalog Policies Specification

Row filters, column masks, SQL user-defined functions and session functions.

## Purpose

Databricks lets a table owner attach a *row filter* function and per-column
*mask* functions so that every reader — including the owner — sees only the
rows and values the functions allow, evaluated in the reader's session
context (`current_user()`, `is_account_group_member(g)`). Lakeforge
implements the same contract by rewriting SQL inside the choke point before
it reaches Forge, and by inlining SQL UDF bodies so Forge (DataFusion) never
needs to know about UC functions.

## Scope

In scope: policy attachment (REST), storage on the UC table document, query
rewrite semantics, SQL UDF creation/drop/inlining, session function
substitution, `EXECUTE` checks on policy functions.
Out of scope: Python UDFs, UDAF/UDTF, policy SQL syntax (`ALTER TABLE … SET
ROW FILTER`, proposed in `changes/integrate-uc-sql-enforcement`).

## Data Model

Table document (`kind = uc_table`) fields:

```json
"row_filter": { "function_name": "main.s.region_ok", "input_column_names": ["region"] },
"columns": [ { "name": "email", "type_name": "STRING", …,
               "mask": { "function_name": "main.s.mask_email", "using_column_names": [] } } ]
```

Function document (`kind = uc_function`): Databricks `FunctionInfo` shape —
`name`, `catalog_name`, `schema_name`, `full_name`, `input_params.parameters[]
{ name, type_text, type_name, position }`, `data_type`, `full_data_type`,
`routine_body = "SQL"`, `routine_definition` (expression body),
`is_deterministic`, `sql_data_access`, `owner`, `created_at`, …

Policy functions MUST be SQL-bodied (`routine_body = SQL`) with a body that
is a single scalar expression over the parameters.

## API

| Route | Body | Auth |
| --- | --- | --- |
| `PUT /api/2.0/lakeforge/unity-catalog/tables/{table}/row-filter` | `{ "function_name", "input_columns": [..] }` | table owner/admin; `EXECUTE` on function |
| `DELETE …/tables/{table}/row-filter` | – | table owner/admin |
| `PUT …/tables/{table}/column-masks` | `{ "column", "function_name", "using_columns": [..] }` | table owner/admin; `EXECUTE` on function |
| `DELETE …/tables/{table}/column-masks/{column}` | – | table owner/admin |
| `POST /api/2.1/unity-catalog/functions` | `FunctionInfo` | `CREATE_FUNCTION` on schema |
| `DELETE /api/2.1/unity-catalog/functions/{name}` | – | owner/admin |
| SQL `CREATE [OR REPLACE] FUNCTION c.s.f(p TYPE, …) RETURNS TYPE RETURN <expr>` | – | metastore op; `CREATE_FUNCTION` |
| SQL `DROP FUNCTION [IF EXISTS] c.s.f` | – | metastore op; owner/admin |
| `GET /api/2.1/unity-catalog/tables/{name}` | – | returns `row_filter` and column `mask` as Databricks does |

Session functions substituted in every statement: `current_user()`,
`session_user()`, `user()`, `current_catalog()`, `current_schema()`,
`current_database()`, `is_account_group_member(g)`, `is_member(g)`.

## Requirements

### Requirement: Row filters restrict every reader
When a table has a row filter, every `SELECT`/`INSERT … SELECT`/`CREATE TABLE
AS` that reads it SHALL be rewritten so the table reference becomes a
subquery `(SELECT * FROM t WHERE <filter body with params bound to input
columns>) AS t`, with session functions replaced by literals for the calling
principal. Owners and admins are NOT exempt.

#### Scenario: Group-based filter
- **GIVEN** function `region_ok(r STRING) RETURN is_account_group_member('eu')
  OR r <> 'EU'` is the row filter on `main.s.users(region)`
- **WHEN** `bob` (not in `eu`) runs `SELECT count(*) FROM main.s.users`
- **THEN** the count excludes rows with `region = 'EU'`; the SQL received by
  Forge contains `false OR region <> 'EU'` (no `is_account_group_member`).

#### Scenario: Filter applies to admins
- **GIVEN** the filter above
- **WHEN** an admin not in `eu` runs the same query
- **THEN** EU rows are excluded as well.

### Requirement: Column masks rewrite projections
When a column has a mask, the table reference SHALL be rewritten to a
subquery projecting every column, with the masked column replaced by
`<mask body with param bound to the column and using-columns> AS <column>`.
`SELECT *` and explicit references SHALL both see the masked value; `WHERE`,
`GROUP BY` and `JOIN` predicates SHALL evaluate on the masked value.

#### Scenario: Email mask
- **GIVEN** `mask_email(e STRING) RETURN CASE WHEN
  is_account_group_member('pii') THEN e ELSE '***' END` on
  `main.s.users.email`
- **WHEN** `bob` (not in `pii`) runs `SELECT email FROM main.s.users`
- **THEN** every value is `***`.

#### Scenario: Mask with using-columns
- **GIVEN** mask `redact(v STRING, tier STRING)` with `using_columns:
  ["tier"]`
- **WHEN** the table is read
- **THEN** the mask is invoked as `redact(v, tier)` with both columns bound
  from the same row.

### Requirement: Policy functions must be executable and SQL-bodied
Attaching a row filter or mask SHALL require the caller to hold `EXECUTE` on
the function (or own it) and the function SHALL have `routine_body = SQL`;
otherwise `INVALID_PARAMETER_VALUE`.

#### Scenario: Python function rejected
- **WHEN** `PUT …/row-filter` names a function with `routine_body = PYTHON`
- **THEN** 400 `INVALID_PARAMETER_VALUE`.

### Requirement: Input columns must exist
`input_columns` and `using_columns` SHALL name existing columns of the table;
`column` in a mask SHALL exist. Unknown names → 400.

#### Scenario: Unknown input column
- **WHEN** `PUT …/row-filter` with `input_columns: ["nope"]`
- **THEN** 400 and the table document is unchanged.

### Requirement: SQL UDFs are metastore objects, inlined at query time
`CREATE [OR REPLACE] FUNCTION` SHALL create/replace the UC function document
without contacting Forge; `DROP FUNCTION` SHALL delete it. Any statement
referencing a UC SQL function by 1–3-part name SHALL have the call replaced by
the function body with parameters substituted (nested calls resolved
recursively), so Forge receives plain SQL.

#### Scenario: Create and call
- **WHEN** `CREATE FUNCTION main.s.double_it(x INT) RETURNS INT RETURN x * 2`
  then `SELECT main.s.double_it(21)`
- **THEN** the result is `42` and Forge received `SELECT 21 * 2`.

#### Scenario: Replace keeps name
- **WHEN** `CREATE OR REPLACE FUNCTION main.s.double_it(x INT) RETURNS INT
  RETURN x + x`
- **THEN** `GET /functions/main.s.double_it` shows the new body and the same
  owner.

### Requirement: Session functions reflect the caller
`current_user()` SHALL become the caller's user name; `is_account_group_member(g)`
SHALL become `true`/`false` from the caller's SCIM group memberships
(`admins` for workspace admins, `users`/`account users` for everyone);
`current_catalog()`/`current_schema()` SHALL come from the session defaults.

#### Scenario: current_user in a view
- **WHEN** `bob` runs `SELECT current_user()`
- **THEN** the result is `bob@example.com` and Forge received `SELECT
  'bob@example.com'`.

## Negative Cases

- Row filter whose function has a parameter count ≠ `input_columns.len()` →
  400 (arity mismatch).
- Attaching a policy by a non-owner → `PERMISSION_DENIED`.
- Dropping a policy that is not set → 200 no-op (idempotent).
- Calling a UC SQL function without `EXECUTE` → `PERMISSION_DENIED`
  (currently only enforced when the function is referenced by a policy — see
  Parity Boundaries).
- `CREATE FUNCTION` in a schema without `CREATE_FUNCTION` →
  `PERMISSION_DENIED`.
- Non-SQL routine body (`PYTHON`) is stored via REST but never inlined; calls
  to it fail in Forge with an unknown-function error.

## Tests

- Unit: `uc::sqlguard::tests` (`rewrite` with `region_ok`/`mask_email`,
  session facts, nested UDF inlining).
- Smoke: `uc-lakebase-smoke.sh` — `session functions`, `sql udf create`,
  `column mask`, `row filter applied for non-admin`.
- Planned (LF-003, LF-004): mask type-check; policy prevents `DROP FUNCTION`;
  `EXECUTE` denial on direct call; `SHOW FUNCTIONS`/`DESCRIBE FUNCTION`.

## Parity Boundaries

- No `ALTER TABLE … SET ROW FILTER / SET MASK` SQL syntax (LF-003).
- No `CREATE TABLE … WITH ROW FILTER` / `MASK` column clause.
- Return-type compatibility of masks is not checked (LF-003).
- `EXECUTE` is checked when attaching policies but not yet on every direct
  UDF call in queries (LF-004).
- Dropping a function still referenced by a policy is not blocked (LF-003).
- Only expression-bodied SQL UDFs (`RETURN <expr>`); no `RETURNS TABLE`, no
  `BEGIN … END`, no default parameter values.
- Policies are applied to Delta tables by name; tables read through paths
  (`delta.\`/path\``) are not policed.
- **Unparsable SQL bypasses policies.** When `sqlparser` rejects a
  statement, `sqlguard::analyze_fallback` still authorizes the table names it
  can extract but performs no rewrite, so row filters and column masks are
  not applied to that statement (`Analysis.parsed == false`). LF-003 decides
  between failing closed for policy-protected tables and widening the
  grammar.
