# Tasks: fix-cluster-liveness-portability

Tick items as they land. `[x]` = on branch
`fix/lf-029-cluster-liveness` and covered by
`cargo test -p lakeforge-cluster-manager`. Issue: LF-029.

## 1. Portable liveness

- [x] 1.1 Keep the `/proc`-based `pid_alive` (zombie-aware) under
      `#[cfg(target_os = "linux")]`
- [x] 1.2 Add a portable `kill -0` fallback under
      `#[cfg(all(unix, not(target_os = "linux")))]` (exit 0 = alive)
- [x] 1.3 Keep the non-Unix `true` fallback; treat pid `0` as never alive
- [x] 1.4 Regression test `pid_alive_tracks_liveness_portably` (live child →
      alive; killed+reaped → dead; pid 0 → dead)
- [x] 1.5 Verify the test fails on the old `/proc`-only form on macOS, then
      passes on the fix

## 2. Docs and spec

- [x] 2.1 Add LF-029 to `docs/issues.md` section F (after LF-028) with
      Problem / Evidence / Scope / Proposed implementation / Dependencies /
      Acceptance criteria / Focused tests / Docs-parity / OpenSpec
- [x] 2.2 Add LF-029 to the Wave 0 row of the "Index by dependency order" table
- [x] 2.3 Add the `cluster-lifecycle` delta in `specs/cluster-lifecycle/spec.md`
      (RUNNING-while-alive and TERMINATED-when-reaped scenarios)
- [x] 2.4 Record the orphan-process-leak supervision gap as a known limitation
      in `proposal.md`

## 3. Verification

- [x] 3.1 `cargo test -p lakeforge-cluster-manager` passes
- [x] 3.2 `cargo clippy --workspace --all-targets -- -D warnings` is clean
- [x] 3.3 Commit in small `LF-029: …` commits ending `(refs #LF-029)`; no push
