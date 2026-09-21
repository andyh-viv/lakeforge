# OpenSpec Instructions

This directory follows the OpenSpec convention: `specs/` holds the
**current truth** (one capability per folder, `spec.md`), `changes/` holds
**proposed changes** (proposal, tasks, optional design, and spec deltas).

## Layout

```
openspec/
  project.md                  project context and conventions
  specs/<capability>/spec.md  current behaviour (Requirements + Scenarios)
  changes/<change-id>/
    proposal.md               Why / What Changes / Impact
    tasks.md                  ordered checklist, tick as you go
    design.md                 optional technical decisions
    specs/<capability>/spec.md  deltas: ## ADDED | MODIFIED | REMOVED Requirements
  changes/archive/            merged changes (move here after deployment)
```

## Spec format

```markdown
# <Capability> Specification

## Purpose
## Scope
## Data Model
## API
## Requirements
### Requirement: <name>
The system SHALL/MUST …

#### Scenario: <name>
- **GIVEN** …
- **WHEN** …
- **THEN** …
## Negative Cases
## Tests
## Parity Boundaries
```

Every requirement needs at least one scenario. Deltas in `changes/*/specs`
copy the full requirement text under `## ADDED Requirements`,
`## MODIFIED Requirements` or `## REMOVED Requirements`.

## Workflow

1. Before changing behaviour, find the capability in `specs/`; if the
   requirement is missing, add a delta in `changes/<change-id>/specs/`.
2. Implement; tick `tasks.md`.
3. In the PR, update `docs/uc-lakebase-status.md` / `docs/parity.md`.
4. After merge, apply deltas to `specs/` and move the change to
   `changes/archive/YYYY-MM-DD-<change-id>/`.

## Active changes

| Change | Status | Issues |
| --- | --- | --- |
| `integrate-uc-sql-enforcement` | partially implemented on branch; remaining tasks open | LF-001..LF-012, LF-020, LF-021, LF-025..LF-028 |
| `implement-lakebase-control-plane` | control plane implemented (emulated); data plane open | LF-013..LF-019, LF-022, LF-023 |

Delta Sharing (LF-024) and PostgreSQL federation (LF-019) get their own change
folders when picked up; see `changes/README.md`.
