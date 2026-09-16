# Models in Unity Catalog Specification

## Purpose

Databricks stores registered models as three-level UC securables
(`catalog.schema.model`) with versions and aliases, governed by UC privileges
(`CREATE_MODEL`, `EXECUTE`, `MANAGE`, ownership) and surfaced through both
the UC REST API and the MLflow client (`registry_uri = databricks-uc`).
Lakeforge has a workspace-scoped MLflow model registry (`mlflow_model` kind)
and a **placeholder** for UC models; this spec defines the target behaviour
and marks what exists.

## Current State (branch `devin/1789466312-unity-catalog-lakebase`)

Implemented:
- `uc::privileges::KIND_MODEL = "uc_registered_model"` and
  `uc::models::KIND_MODEL_VERSION = "uc_model_version"` constants.
- Schema listing (`GET /api/2.0/lakeforge/catalog/schema/{name}` details)
  includes a `models` array; catalog/schema deletion cascades to `KIND_MODEL`
  children.
- `information_schema.models`, `model_versions`, `model_version_aliases`
  registered with Databricks columns (rows come from `KIND_MODEL` /
  `KIND_MODEL_VERSION` documents, which nothing creates yet).
- `CREATE_MODEL` privilege exists in the vocabulary.

Not implemented: every REST route below, `Securable::Model`, MLflow
`databricks-uc` registry mapping, aliases, version storage. Work item:
`docs/issues.md` LF-012.

## Scope

In scope: UC registered models, versions, aliases, privileges, MLflow
registry bridge, information_schema rows. Out of scope: model serving
(`api/serving.rs` consumes versions by URI and is unchanged), model
signatures/validation, model lineage to training runs beyond `run_id`.

## Data Model

`RegisteredModelInfo` (`kind = uc_registered_model`, id = full name):

```
name, catalog_name, schema_name, full_name, owner, comment?,
storage_location, created_at, created_by, updated_at, updated_by,
aliases: [{ alias_name, version_num }], browse_only = false,
metastore_id, securable_type = "MODEL"
```

`ModelVersionInfo` (`kind = uc_model_version`, parent = model id, id =
`<full_name>/<version>`):

```
model_name, catalog_name, schema_name, version: int, status: PENDING_REGISTRATION|FAILED_REGISTRATION|READY,
source?, run_id?, run_workspace_id?, storage_location, comment?, aliases: [..],
created_at, created_by, updated_at, updated_by, metastore_id
```

Version storage: `<schema storage_root or storage_root>/models/<model>/<version>/`
under the Lakeforge storage backend (Files API paths).

Privileges: `CREATE_MODEL` on schema to create; owner/`MANAGE` to update,
delete, set aliases and register versions; `EXECUTE` to read/download a
version (Databricks semantics: `EXECUTE` on model = can load it);
`USE_CATALOG`/`USE_SCHEMA` chain applies.

## API

| Route | Auth |
| --- | --- |
| `GET /api/2.1/unity-catalog/models?catalog_name&schema_name&max_results&page_token` | visible models |
| `POST /api/2.1/unity-catalog/models` `{ name, catalog_name, schema_name, comment?, storage_location? }` | `CREATE_MODEL` on schema |
| `GET /api/2.1/unity-catalog/models/{full_name}?include_aliases` | any privilege |
| `PATCH /api/2.1/unity-catalog/models/{full_name}` `{ new_name?, comment?, owner? }` | owner/`MANAGE`; owner change needs owner/admin |
| `DELETE /api/2.1/unity-catalog/models/{full_name}?force` | owner/admin; fails if versions exist unless `force` |
| `GET /api/2.1/unity-catalog/models/{full_name}/versions` | `EXECUTE` |
| `GET /api/2.1/unity-catalog/models/{full_name}/versions/{version}` | `EXECUTE` |
| `PATCH …/versions/{version}` `{ comment }` | owner/`MANAGE` |
| `DELETE …/versions/{version}` | owner/`MANAGE` |
| `GET …/models/{full_name}/versions/{version}/download-uri` (Lakeforge ext.) | `EXECUTE` |
| `PUT /api/2.1/unity-catalog/models/{full_name}/aliases/{alias}` `{ version_num }` | owner/`MANAGE` |
| `DELETE …/aliases/{alias}` | owner/`MANAGE` |
| `GET …/models/{full_name}/versions/by-alias/{alias}` | `EXECUTE` |
| MLflow `/api/2.0/mlflow/registered-models/*` and `model-versions/*` with three-part names | mapped onto UC models when `name` contains two dots |
| SQL `GRANT EXECUTE ON MODEL main.ml.churn TO …`, `SHOW GRANTS ON MODEL …`, `ALTER MODEL … OWNER TO` | via `grant_sql` (add `MODEL` securable) |

## Requirements

### Requirement: Models are UC securables
The system SHALL treat `catalog.schema.model` as a securable of type
`MODEL` in `Authorizer`, inheriting from its schema and catalog, owned by its
creator, with grants stored in the standard grants document and reported by
`permissions`/`effective-permissions`.

#### Scenario: Create requires CREATE_MODEL
- **GIVEN** `bob` has `USE CATALOG`, `USE SCHEMA` but not `CREATE_MODEL` on
  `main.ml`
- **WHEN** `POST /models { name: churn, catalog_name: main, schema_name: ml }`
- **THEN** 403 `PERMISSION_DENIED`.

#### Scenario: Inherited EXECUTE
- **GIVEN** `GRANT EXECUTE ON SCHEMA main.ml TO bob`
- **WHEN** `bob` `GET`s `/models/main.ml.churn/versions/1`
- **THEN** 200 with the version document.

### Requirement: Versions are immutable, monotonically numbered
Creating a version SHALL assign `max(version)+1` per model, start in
`PENDING_REGISTRATION`, and transition to `READY` once the artifact upload is
finalised (`POST …/versions/{v}/finalize` or on first read of the artifacts).
Version fields other than `comment` and `status` SHALL be immutable.

#### Scenario: Numbering
- **GIVEN** versions 1 and 2 exist and version 2 is deleted
- **WHEN** a new version is created
- **THEN** it is version 3.

### Requirement: Aliases
An alias SHALL map to exactly one version per model; setting an existing
alias SHALL move it; deleting a version SHALL remove its aliases; the alias
list SHALL appear on both the model (`aliases`) and the version.

#### Scenario: Move alias
- **GIVEN** alias `champion → 1`
- **WHEN** `PUT …/aliases/champion { version_num: 2 }`
- **THEN** `GET …/versions/by-alias/champion` returns version 2 and version
  1 no longer lists `champion`.

### Requirement: MLflow bridge
MLflow registry calls whose model name has the form `c.s.m` SHALL be routed
to UC models (create → UC model; `create-model-version` → UC version with
`source`/`run_id`; `get-latest-versions`/`search` → UC lookups; aliases via
`registered-models/alias`), so `mlflow.register_model("main.ml.churn",
run_uri)` and `mlflow.pyfunc.load_model("models:/main.ml.churn@champion")`
work with `MLFLOW_TRACKING_URI=lakeforge` and registry URI `databricks-uc`.
Two-part or bare names SHALL continue to use the workspace registry.

#### Scenario: register_model
- **WHEN** the Python client calls `mlflow.register_model("main.ml.churn",
  "runs:/<run>/model")`
- **THEN** `GET /api/2.1/unity-catalog/models/main.ml.churn/versions/1`
  exists with `run_id = <run>` and `status = READY` after artifacts copy.

### Requirement: Serving consumes UC versions
Serving endpoint `served_entities[].entity_name` of the form `c.s.m` with
`entity_version` SHALL resolve to a UC version's `storage_location`; the
caller SHALL need `EXECUTE` on the model.

#### Scenario: Endpoint from UC model
- **WHEN** an endpoint is created with `entity_name = main.ml.churn`,
  `entity_version = "2"`
- **THEN** the served worker loads artifacts from version 2's storage
  location.

### Requirement: information_schema and system tables
`information_schema.models`, `model_versions`, `model_version_aliases` SHALL
list visible UC models/versions/aliases; `system.mlflow.*` remains
experiment/run telemetry.

#### Scenario: Listing
- **WHEN** `SELECT model_name, version FROM main.information_schema.model_versions`
- **THEN** one row per visible version.

## Negative Cases

- Creating a model whose schema does not exist → 404.
- Duplicate model name → 409 `RESOURCE_ALREADY_EXISTS`.
- Alias pointing at a missing version → 404.
- Deleting a model with versions without `force` → 400
  `INVALID_STATE`.
- MLflow `create-model-version` with `source` the caller cannot read → 403.
- Version number in path not an integer → 400.

## Tests

- Unit: version numbering; alias move/delete; MLflow name routing.
- Smoke (extend `uc-lakebase-smoke.sh`): create model, grant EXECUTE, create
  version via MLflow API, set alias, resolve by alias, non-grantee denied,
  `information_schema.model_versions` row present.
- Python: `mlflow.register_model` with a three-part name against a local API.

## Parity Boundaries

- Placeholder today; none of the routes exist (LF-012).
- After LF-012: no model signature validation, no `browse_only`
  (marketplace), no cross-workspace `run_workspace_id`, no lineage to
  feature tables, no online model monitoring.
- Serving is process-based local inference (see `parity.md`), unaffected by
  where the version lives.
