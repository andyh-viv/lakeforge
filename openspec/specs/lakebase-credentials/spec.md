# Lakebase Credentials Specification

## Purpose

Databricks issues short-lived OAuth tokens (`POST /api/2.0/database/credentials`)
that clients use as the PostgreSQL password for a database instance. Lakeforge
implements the same call, mints a short-lived token, checks the caller's
access to every named instance, and labels the response with the backend so
clients can tell whether a PostgreSQL endpoint is real.

## Scope

In scope: credential request/response, authorization, TTL, token storage and
redaction, audit. Out of scope: PostgreSQL authentication hooks (proposed in
`changes/implement-lakebase-control-plane`, LF-014/LF-017).

## Data Model

Credential = a standard Lakeforge personal access token (`kind = token`) with
`comment = "lakebase credential for <instances>"`,
`expiry_time = now + CREDENTIAL_TTL_SECS (3600) s`, stored as a hash only.

Response:

```json
{ "token": "<opaque>", "expiration_time": "<RFC3339>", "request_id": "<uuid|client>", "backend": "emulated|external" }
```

## API

| Route | Body | Behaviour |
| --- | --- | --- |
| `POST /api/2.0/database/credentials` | `{ "instance_names": [..], "request_id"?: str, "claims"?: [..] }` | mint token; `instance_names` non-empty |

## Requirements

### Requirement: Access is required for every named instance
The caller MUST be admin, the creator of each instance, or hold a role on it
whose `name` equals the caller's user name or one of the caller's groups;
otherwise 403 `PERMISSION_DENIED` naming the first failing instance and no
token is created.

#### Scenario: Role holder
- **GIVEN** role `data-eng` (GROUP) on `db1` and `bob ∈ data-eng`
- **WHEN** `bob` posts `{ instance_names: ["db1"] }`
- **THEN** 200 with a token.

#### Scenario: No role
- **GIVEN** `bob` has no role on `db2`
- **WHEN** `bob` posts `{ instance_names: ["db1", "db2"] }`
- **THEN** 403 and `GET /api/2.0/token/list` for bob shows no new token.

### Requirement: Instances must be usable
Each named instance MUST exist and be in `AVAILABLE`, `STARTING` or
`UPDATING`; `STOPPED`/`DELETING` → 400 `INVALID_STATE`; unknown → 404.

#### Scenario: Stopped instance
- **WHEN** credentials are requested for a `STOPPED` instance
- **THEN** 400 `INVALID_STATE`.

### Requirement: Tokens are short-lived and opaque
The token SHALL expire after `CREDENTIAL_TTL_SECS`; `expiration_time` SHALL
be RFC3339; the token value SHALL never be persisted in clear (hash only) and
SHALL be redacted from audit (`request_params` contain `instance_names` and
`request_id` only).

#### Scenario: Expiry
- **WHEN** a credential is minted at `T`
- **THEN** `expiration_time == T + 1h` and the token authenticates as the
  caller against the Lakeforge API until then.

### Requirement: Backend is disclosed
The response MUST include `backend`; on `emulated` the token is a Lakeforge
API token only and no PostgreSQL server accepts it.

#### Scenario: Emulated
- **WHEN** credentials are minted with no `LAKEFORGE_LAKEBASE_POSTGRES_URL`
- **THEN** `backend == "emulated"`.

### Requirement: Audit
A `generateDatabaseCredential` event (`service_name = database`) SHALL be
recorded with `instance_names` and `request_id`, never the token.

#### Scenario: Audited
- **WHEN** a credential is minted
- **THEN** `system.access.audit` has the event and no column contains the
  token value.

## Negative Cases

- Empty `instance_names` → 400.
- `request_id` provided → echoed; otherwise a uuid is generated.
- `claims` are accepted and ignored (Databricks uses them for
  permission-set scoping; see Parity Boundaries).
- Token revoked via `POST /api/2.0/token/delete` → credential stops working
  immediately.

## Tests

- Smoke: `uc-lakebase-smoke.sh` — `lakebase credential` (token present,
  `backend` reported).
- Planned (LF-014): non-role denial; stopped instance; audit redaction; token
  scoped so it cannot call non-Lakebase APIs.

## Parity Boundaries

- The token is a Lakeforge API bearer token that also grants API access as
  the caller; Databricks credentials are OAuth tokens usable only as a
  PostgreSQL password. Scoping is LF-014.
- No PostgreSQL server validates the token (LF-017 adds a `pg_hba`/auth hook
  or password sync for the external backend).
- `claims`/permission sets are ignored.
- No `GET /api/2.0/database/credentials/*` introspection (Databricks has
  none either).
