# Changes index

| Change | Status | Issues | Capability specs touched |
| --- | --- | --- | --- |
| [`integrate-uc-sql-enforcement`](integrate-uc-sql-enforcement/proposal.md) | partially implemented; tasks open | LF-001..LF-012, LF-020, LF-021, LF-025..LF-028 | unity-catalog-authorization, unity-catalog-policies, unity-catalog-audit-lineage, unity-catalog-system-tables, unity-catalog-models |
| [`implement-lakebase-control-plane`](implement-lakebase-control-plane/proposal.md) | control plane done (emulated); data plane open | LF-013..LF-019, LF-022, LF-023 | lakebase-instances, lakebase-credentials, lakebase-catalogs, lakebase-synced-tables, lakebase-emulation |

## Archived changes

| Change | Archived | Issue | Live spec |
| --- | --- | --- | --- |
| [`2026-09-21-fix-cluster-liveness-portability`](archive/2026-09-21-fix-cluster-liveness-portability/proposal.md) | 2026-09-21 | LF-029 | `openspec/specs/cluster-lifecycle/spec.md` |

## Starting a new change

Copy this skeleton to `changes/<change-id>/` (kebab-case verb phrase, e.g.
`add-delta-sharing-server`, `enforce-workspace-object-acls`):

```
changes/<change-id>/
  proposal.md      # Why / What Changes / Impact / Risks
  tasks.md         # numbered checklist grouped by layer; cite LF-issues
  design.md        # optional: decisions, alternatives, data flow
  specs/<capability>/spec.md   # ADDED / MODIFIED / REMOVED Requirements
```

`proposal.md` skeleton:

```markdown
# Change: <change-id>

Status: proposed

## Why
<problem, evidence from docs/uc-lakebase-status.md or docs/issues.md>

## What Changes
1. …

## Impact
- **Behaviour**: …
- **Code**: crates/…, web/…, python/…
- **Specs**: <capabilities> (deltas in specs/ here)
- **Docs**: docs/uc-lakebase-status.md, docs/parity.md, docs/api-surface.md
- **Tests**: …

## Risks
- …
```

Capabilities that do not have a `specs/<capability>/spec.md` yet and are
expected to be created by their first change:

| Capability | Issue | Notes |
| --- | --- | --- |
| `delta-sharing` | LF-024 | provider-side shares/recipients/providers exist as UC docs; open protocol server missing |
| `workspace-object-acls` | LF-027 | `api/permissions.rs` stores ACLs; enforcement on object routes missing |
| `postgres-federation` | LF-019 | may live as a delta on `lakebase-catalogs` instead |

## Archiving

After a change merges and its deltas are folded into `openspec/specs/`,
`git mv changes/<change-id> changes/archive/YYYY-MM-DD-<change-id>` and
update the table above.
