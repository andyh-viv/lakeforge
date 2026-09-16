# Delta: unity-catalog-authorization (integrate-uc-sql-enforcement)

Applies to `openspec/specs/unity-catalog-authorization/spec.md`. Requirements
below are **proposed** (not on the branch); after implementation copy them
into the capability spec.

## ADDED Requirements

### Requirement: Unparsable statements fail closed on policy-protected tables
When `sqlguard::analyze` cannot parse a statement (`Analysis.parsed ==
false`) and any table extracted by the fallback carries a row filter or
column mask, the system MUST reject the statement with
`[UNSUPPORTED_FEATURE] statement could not be analysed for policy
enforcement` unless the caller owns the table or is admin. (LF-003)

#### Scenario: Time-travel syntax against a masked table
- **GIVEN** `main.s.users.email` has a mask and `bob` has `SELECT`
- **WHEN** `bob` runs `SELECT * FROM main.s.users VERSION AS OF 0`
- **THEN** the statement is rejected and an `sqlStatementDenied` audit event
  is recorded.

#### Scenario: Owner is exempt
- **WHEN** the owner runs the same statement
- **THEN** it is forwarded to Forge unrewritten (Databricks semantics: owner
  still sees masked values on Databricks; Lakeforge documents this as a
  boundary until the grammar covers time travel).

### Requirement: SHOW GRANTS TO principal
`SHOW GRANTS TO <principal>` and `SHOW GRANTS <principal>` without `ON`
SHALL list every direct grant the principal holds on any securable in the
metastore, as rows `(principal, action_type, object_type, object_key)`.
Non-admins MAY only query themselves or groups they belong to. (LF-002)

#### Scenario: Self
- **WHEN** `bob` runs `SHOW GRANTS TO bob`
- **THEN** rows for every securable where `bob` appears in `privileges`.

#### Scenario: Other user
- **WHEN** `bob` runs `SHOW GRANTS TO alice`
- **THEN** 403.

### Requirement: Schema-wide grants
`GRANT <priv> ON ALL TABLES IN SCHEMA c.s TO p` SHALL be accepted and stored
as an ordinary grant on the schema (Databricks semantics: inherited by all
current and future tables). `REVOKE … ON ALL TABLES IN SCHEMA` SHALL revoke
the schema-level grant. (LF-002)

#### Scenario: Grant on all tables
- **WHEN** `GRANT SELECT ON ALL TABLES IN SCHEMA main.s TO bob`
- **THEN** `SHOW GRANTS ON SCHEMA main.s` lists `SELECT` for `bob` and `bob`
  can select from a table created afterwards.

### Requirement: Privilege cache per request
`Authorizer` SHALL memoize grant documents and ownership lookups for the
lifetime of one `prepare_sql`/REST call so that a statement touching N tables
in one schema performs O(N + depth) store reads, not O(N × depth). (LF-001)

#### Scenario: Multi-table join
- **WHEN** a statement joins 20 tables in `main.s`
- **THEN** the schema and catalog grant documents are read once each
  (assert via store read counter in a unit test).

## MODIFIED Requirements

### Requirement: Privilege SQL executes as a metastore operation
(Modified to add the `MODEL` securable keyword and `SHOW GRANTS TO`.)
`GRANT`, `REVOKE`, `SHOW GRANTS [TO]`, and `ALTER … OWNER TO` SHALL be
recognised before ordinary analysis, parsed by `uc/grant_sql.rs`, authorized
by the grant-mutation rules, applied to the grants document, and SHALL never
be sent to Forge. Securable keywords: `METASTORE`, `CATALOG`, `SCHEMA`,
`TABLE`, `VIEW`, `VOLUME`, `FUNCTION`, `MODEL`, `EXTERNAL LOCATION`,
`STORAGE CREDENTIAL`, `CONNECTION`, `SHARE`, `RECIPIENT`, `PROVIDER`.

#### Scenario: Model grant
- **WHEN** `GRANT EXECUTE ON MODEL main.ml.churn TO bob`
- **THEN** `GET /api/2.1/unity-catalog/permissions/model/main.ml.churn`
  lists `EXECUTE` for `bob`.

## REMOVED Requirements

None.
