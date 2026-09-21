# Tasks: fix-cluster-liveness-portability

Tick items as they land. `[x]` = on branch
`fix/lf-029-cluster-liveness` and covered by
`cargo test -p lakeforge-cluster-manager`. Issue: LF-029.

## 1. Portable liveness

- [x] 1.1 Keep the `/proc`-based `pid_alive` (zombie-aware) under
      `#[cfg(target_os = "linux")]`
- [x] 1.2 Add a portable `kill -0` fallback under
      `#[cfg(all(unix, not(target_os = "linux")))]` (exit 0 = alive)
- [x] 1.3 Non-Unix fallback reports pid `0` as dead (the pid-0 invariant holds on
      every target); real pids are assumed alive (documented stub)
- [x] 1.4 Regression test `pid_alive_tracks_liveness_portably` (live child →
      alive; killed+reaped → dead; pid 0 → dead)
- [x] 1.5 Verify the test fails on the old `/proc`-only form on macOS, then
      passes on the fix

## 2. Review remediation (adversarial review on PR #3, gpt-5.6-luna)

- [x] 2.1 Retain spawned `tokio::process::Child` handles and answer liveness for
      our own children with `Child::try_wait` (reports *and reaps*), so an exited
      driver reads as dead even before it is reaped
- [x] 2.2 Regression test `exited_but_unreaped_tracked_child_reports_dead`;
      verified to FAIL against the `kill -0`-only implementation
- [x] 2.3 Non-Unix `pid_alive` honours the pid-0 invariant (`pid != 0`)
- [x] 2.4 `terminate` reaps the handles it holds, bounded (≤1s), so a torn-down
      cluster leaves no zombies
- [x] 2.5 Regression test `pid_zero_is_never_live` (both paths) and a RAII
      cleanup guard so a failing assertion cannot leak a child
- [x] 2.6 Correct `proposal.md` to match the implementation (TERMINATED once the
      driver exits *and is reaped*; untracked-pid residual; non-Unix stub)
- [x] 2.7 Scope the `cluster-lifecycle` delta to what is implemented and tested;
      add the exited-but-unreaped scenario
- [x] 2.8 Add the change to the active-change table in
      `openspec/changes/README.md`
- [x] 2.9 Document the `kill -0` residual (zombie/EPERM) and `PATH`-resolved
      `kill` in the module docs rather than leaving them implied

## 2b. Review remediation, round 2 (same reviewer, after round-1 fixes)

- [x] 2b.1 Stop falling back to a probe when `try_wait` errors: `pid_live` now
      returns `Result<bool>` and reports the failure instead of guessing
- [x] 2b.2 Add `reap_exited()`, a registry-wide sweep called from `status()` and
      `terminate()`, so handles whose pids left cluster state are collected
- [x] 2b.3 `resize()` reaps the executors it removes instead of leaking a handle
      (and a zombie) per scale-down
- [x] 2b.4 `terminate()` reaps its own pids (bounded) then sweeps the registry, so
      a process that exits after the cluster handle is cleared is still collected
- [x] 2b.5 Tolerate a poisoned registry mutex instead of panicking the monitor loop
- [x] 2b.6 `spawn()` returns a launch error if the child has no pid, instead of
      storing pid `0`
- [x] 2b.7 Regression test `reap_exited_collects_handles_whose_pids_left_cluster_state`;
      verified to FAIL when the sweep is removed
- [x] 2b.8 Make `exited_but_unreaped_tracked_child_reports_dead` robust (child
      `sleep 1`, no immediate-assert race) and drop the stale-guard nit in favour
      of a bounded child lifetime, documented in the test
- [x] 2b.9 Qualify the "exited ⇒ dead" claim in `proposal.md` to retained handles,
      and add the sweep requirement/scenarios to the spec delta

## 3. Docs and spec

- [x] 3.1 Add LF-029 to `docs/issues.md` section F (after LF-028) with
      Problem / Evidence / Scope / Proposed implementation / Dependencies /
      Acceptance criteria / Focused tests / Docs-parity / OpenSpec
- [x] 3.2 Add LF-029 to the Wave 0 row of the "Index by dependency order" table
- [x] 3.3 Add the `cluster-lifecycle` delta in `specs/cluster-lifecycle/spec.md`
- [x] 3.4 Record the orphan-process-leak supervision gap as a known limitation
      in `proposal.md`

## 4. Verification

- [x] 4.1 `cargo test -p lakeforge-cluster-manager` passes (3 tests)
- [x] 4.2 `cargo clippy --workspace --all-targets -- -D warnings` is clean
- [x] 4.3 `openspec validate --changes` passes for this change
- [x] 4.4 `tests/smoke/platform-smoke.sh` reaches `passed=39 failed=0` on macOS
- [x] 4.5 Commit in small `LF-029: …` commits ending `(refs #LF-029)`; no push
