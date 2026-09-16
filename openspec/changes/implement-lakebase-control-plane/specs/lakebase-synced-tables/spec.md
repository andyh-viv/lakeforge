# Delta: lakebase-synced-tables (implement-lakebase-control-plane)

Applies to `openspec/specs/lakebase-synced-tables/spec.md`. **Proposed**
(LF-018); merge when data movement exists.

## ADDED Requirements

### Requirement: Synced tables are backed by a pipeline
Creating a synced table SHALL create (or link) a Lakeflow pipeline of kind
`synced_table` whose id is `data_synchronization_status.pipeline_id`;
`existing_pipeline_id` MUST reference such a pipeline owned by the caller.

#### Scenario: Pipeline created
- **WHEN** a synced table is created without `existing_pipeline_id`
- **THEN** `GET /api/2.0/pipelines/{pipeline_id}` returns `kind =
  synced_table`, `target = <synced table name>`.

### Requirement: SNAPSHOT copies the full source
A `SNAPSHOT` synced table SHALL, on creation, read the source through
`execute_sql` as the creator (so UC policies apply), write all rows into a
staging PostgreSQL table, atomically swap it in, and set
`last_sync.delta_table_version` to the Delta version read and
`provisioning_status.initial_pipeline_sync_progress.synced_row_count` to
the row count.

#### Scenario: Snapshot rows
- **GIVEN** source has 1 000 rows at Delta version 7
- **WHEN** the snapshot completes
- **THEN** `SELECT count(*)` on the PostgreSQL table is 1 000 and
  `last_sync.delta_table_version == 7`.

### Requirement: TRIGGERED refresh upserts by primary key
`POST /api/2.0/database/synced_tables/{name}/refresh` SHALL require
`MODIFY` on the mirror table, upsert rows by `primary_key_columns`
(`INSERT … ON CONFLICT (pk) DO UPDATE`), delete rows absent from the
source, and update `triggered_update_status { last_processed_commit_version,
timestamp, triggered_update_progress }`. Concurrent refreshes SHALL be
rejected with 409.

#### Scenario: Refresh
- **GIVEN** the source gained 10 rows and changed 5 since the last sync
- **WHEN** refresh runs
- **THEN** PostgreSQL reflects the 15 changes and `detailed_state` returns
  to `ONLINE_TRIGGERED_UPDATE`.

### Requirement: CONTINUOUS polls Delta versions
A `CONTINUOUS` synced table SHALL be refreshed by the background worker
whenever the source Delta version advances (checked every `tick_secs`),
updating `continuous_update_status`; failures SHALL set `detailed_state =
ONLINE_UPDATING_PIPELINE_RESOURCES` or `OFFLINE_FAILED` with `message` and
be retried with backoff.

#### Scenario: Auto refresh
- **WHEN** `INSERT INTO <source> VALUES (…)` commits
- **THEN** within `2 × tick_secs` the row is present in PostgreSQL.

### Requirement: Policies flow through
Rows and columns written to PostgreSQL SHALL be those the synced table's
creator can see after row filters and masks; the spec SHALL record
`executed_as = <creator>`.

#### Scenario: Masked source
- **GIVEN** `email` is masked for everyone except `admins`
- **WHEN** a non-admin creates a synced table from the source
- **THEN** the PostgreSQL copy contains masked emails.

## MODIFIED Requirements

### Requirement: Status lifecycle
(Modified: real transitions replace the timer.) Creation SHALL return a
`PROVISIONING_*` state; the pipeline SHALL drive transitions to `ONLINE_*`
on success or `OFFLINE_FAILED` on error; `message` SHALL carry the last
error; the 3-second simulated settle SHALL apply only on the emulated
backend.

#### Scenario: Failure
- **WHEN** the PostgreSQL write fails
- **THEN** `detailed_state == OFFLINE_FAILED` and `message` contains the
  SQLSTATE.

## REMOVED Requirements

None.
