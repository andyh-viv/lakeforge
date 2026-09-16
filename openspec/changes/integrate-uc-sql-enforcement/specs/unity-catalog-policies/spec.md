# Delta: unity-catalog-policies (integrate-uc-sql-enforcement)

Applies to `openspec/specs/unity-catalog-policies/spec.md`. **Proposed**;
implement under LF-003/LF-004/LF-005, then merge into the capability spec.

## ADDED Requirements

### Requirement: Policies are settable from SQL
The system SHALL parse, as metastore operations:

```sql
ALTER TABLE <t> SET ROW FILTER <fn> ON (<col>[, <col>…])
ALTER TABLE <t> DROP ROW FILTER
ALTER TABLE <t> ALTER COLUMN <c> SET MASK <fn> [USING COLUMNS (<col>[, …])]
ALTER TABLE <t> ALTER COLUMN <c> DROP MASK
CREATE TABLE <t> (… <c> <type> MASK <fn> …) WITH ROW FILTER <fn> ON (<cols>)
```

with the same authorization and validation as the REST routes (table owner
or `MANAGE`; function exists, is SQL-bodied and executable by the caller;
columns exist). (LF-003)

#### Scenario: Databricks docs example
- **WHEN** the owner runs
  `ALTER TABLE main.s.sales SET ROW FILTER main.s.us_filter ON (region)`
- **THEN** `GET /api/2.0/lakeforge/unity-catalog/tables/main.s.sales/row-filter`
  returns `{ function_name: "main.s.us_filter", input_column_names: ["region"] }`
  and a non-admin's `SELECT` is rewritten.

#### Scenario: Drop mask
- **WHEN** `ALTER TABLE main.s.users ALTER COLUMN email DROP MASK`
- **THEN** the mask is removed and subsequent selects return clear values.

### Requirement: Mask return type must match the column
Attaching a mask SHALL fail with `[INVALID_PARAMETER_VALUE] mask function
return type <r> does not match column type <t>` when the UDF's `return_type`
differs from the column's `type_name` (case-insensitive; `STRING` ≡
`VARCHAR`). (LF-003)

#### Scenario: Type mismatch
- **GIVEN** `mask_to_int(s STRING) RETURNS INT`
- **WHEN** it is attached to `email STRING`
- **THEN** 400.

### Requirement: Referenced functions cannot be dropped
`DROP FUNCTION` (SQL or REST) on a function referenced by any row filter or
column mask SHALL fail with 409 `RESOURCE_CONFLICT` listing the tables,
unless `FORCE` (SQL) / `force=true` (REST) is given, in which case the
policies are detached first. (LF-003)

#### Scenario: Blocked drop
- **GIVEN** `main.s.mask_email` masks `main.s.users.email`
- **WHEN** `DROP FUNCTION main.s.mask_email`
- **THEN** 409 mentioning `main.s.users`.

### Requirement: EXECUTE is checked on invocation
Every UDF inlined into a statement SHALL require `EXECUTE` (or ownership) by
the caller; the check SHALL happen even when the UDF is nested in another
UDF's body. (LF-004)

#### Scenario: No EXECUTE
- **GIVEN** `bob` lacks `EXECUTE` on `main.s.f`
- **WHEN** `bob` runs `SELECT main.s.f(1)`
- **THEN** 403 `PERMISSION_DENIED` before Forge is contacted.

### Requirement: Additional session functions
`is_member(g)` SHALL behave like `is_account_group_member(g)`;
`session_user()` SHALL equal `current_user()`; `current_metastore()` SHALL
return `METASTORE_ID`; group membership SHALL resolve nested groups
transitively. (LF-005)

#### Scenario: Nested group
- **GIVEN** `bob ∈ eng`, `eng ∈ all-staff`
- **WHEN** `bob` runs `SELECT is_account_group_member('all-staff')`
- **THEN** `true`.

## MODIFIED Requirements

### Requirement: Row filters restrict every reader
(Modified to add negative coverage.) A row filter SHALL be applied to every
read of the table by every principal, including owners and admins, and SHALL
be verified by a smoke check in which a non-admin principal sees only rows
for which the policy function returns true, while a second principal in a
different group sees a different subset.

#### Scenario: Two principals
- **GIVEN** `region_ok(region)` returns `region = 'EU'` for `eu-team` and
  `region = 'US'` for `us-team`
- **WHEN** an `eu-team` member and a `us-team` member each `SELECT count(*)`
- **THEN** the counts equal the EU and US row counts respectively.

## REMOVED Requirements

None.
