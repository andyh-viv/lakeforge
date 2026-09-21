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
- [x] 2.4 `terminate` reaps the handles it holds, bounded (a 20×50 ms polite
      grace, then SIGKILL, then a 20×50 ms wait), so a torn-down cluster leaves
      no zombies
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
- [x] 2b.2 Add `sweep_exited()`, a registry-wide sweep (driven by the control
      plane via `reap_orphans` since round 5, 2e.1), so handles whose pids left
      cluster state are collected
- [x] 2b.3 `resize()` reaps the executors it removes instead of leaking a handle
      (and a zombie) per scale-down
- [x] 2b.4 `terminate()` reaps its own pids (bounded); the registry sweep moved
      off `terminate()` to the control-plane `reap_orphans` in round 5 (2e.1), so
      orphaned executors from earlier scale-downs and children whose state was
      already dropped are still collected there
- [x] 2b.5 Tolerate a poisoned registry mutex instead of panicking the monitor loop
- [x] 2b.6 `spawn()` returns a launch error if the child has no pid, instead of
      storing pid `0`
- [x] 2b.7 Regression test `sweep_exited_collects_handles_whose_pids_left_cluster_state`;
      verified to FAIL when the sweep is removed
- [x] 2b.8 Make `exited_but_unreaped_tracked_child_reports_dead` deterministic:
      the child is stopped through its retained handle instead of being raced with
      a short sleep, and it is long-lived so a failing assertion cannot linger

## 2c. Review remediation, round 3 (gpt-5.6-sol, the governance-permitted reviewer)

- [x] 2c.1 Isolate sweep failures per child: `sweep_exited()` logs each failing
      pid and never returns an error, so one unqueryable child cannot stop
      reconciliation for every other cluster (the sweep runs once per tick via
      `reap_orphans`); a cluster that owns such a child still gets the error from
      `pid_live`
- [x] 2c.2 Preserve the authoritative answer across the sweep: pids observed
      exiting are kept in a bounded ring, and `pid_live` consults it rather than
      falling through to the probe when the handle is already gone
- [x] 2c.3 `status()` queries only its own pids (the sweep moved off `status()`
      to the control-plane `reap_orphans` in round 5, 2e.1), and queries
      executors even when the driver is already dead, so their exits are reaped
      too
- [x] 2c.4 Escalate cleanup: `reap_pids()` force-kills a child that survives the
      polite signal and returns an error rather than reporting a false success, so
      a cluster handle is not cleared while a live process remains
- [x] 2c.5 Regression test for the `status()` wiring (not just the sweep
      mechanism) — replaced in round 5 (2e.2) because it asserted the defective
      caller-pids-only sweep
- [x] 2c.6 Regression test `recorded_exit_takes_precedence_over_the_probe` —
      verified to FAIL when the precedence is removed
- [x] 2c.7 Regression test `reap_pids_force_kills_a_child_that_ignores_the_polite_signal`,
      with a readiness handshake so the signal cannot land before the child
      installs its ignore-trap — verified to FAIL when the escalation is removed
      (without the handshake it passed for the wrong reason)
- [x] 2c.8 Correct the remaining overclaims: the live spec's `SUSPENDED` state
      (neither `BackendState` nor `ClusterState` defines it), `resize` listed as
      non-scope while it was changed, the post-handle-clear claim, the test count
      and the stale scope/OpenSpec path in `docs/issues.md`
- [x] 2b.9 Qualify the "exited ⇒ dead" claim in `proposal.md` to retained handles,
      and add the sweep requirement/scenarios to the spec delta

## 2d. Review remediation, round 4 (gpt-5.6-sol, same reviewer after round-3 fixes)

- [x] 2d.1 Replace the bounded recently-exited retention ring with an
      ownership-aware sweep: `sweep_exited(referenced)` skips any pid in the
      reference set so the owner consumes the authoritative exit itself and the
      guarantee no longer depends on ring capacity (the ring's
      capacity-dependence is removed)
- [x] 2d.2 Two new regression tests replace the white-box ring test:
      `sweep_exited_retains_pids_its_cluster_still_references` (a referenced pid is
      never collected) and
      `sweep_exited_collects_many_unreferenced_handles_in_one_pass` (300 children
      in one pass)
- [x] 2d.3 Three API-layer fixes in `crates/lakeforge-api/src/api/clusters.rs`,
      building on `terminate_cluster` keeping the handle and a
      `cleanup incomplete` `state_message` on failure: `start_cluster` refuses a
      `Terminating` cluster that still holds a handle, `permanent_delete`
      propagates a cleanup failure instead of deleting the record (a missing
      cluster stays an idempotent success), and `monitor_clusters` retries a
      `Terminating` cluster
- [x] 2d.4 Residual: the API-layer injected-failure path is not unit-tested — it
      needs the LF-025 integration harness — and is recorded rather than implied
      as covered

## 2e. Review remediation, round 5 (gpt-5.6-sol, same reviewer after round-4 fixes)

- [x] 2e.1 The sweep protects the reference set of state-referenced pids supplied
      by the control plane (collected from every cluster), not the calling
      cluster's own pids: `status()` no longer sweeps, `terminate()` no longer
      sweeps with an empty set, and `monitor_clusters` collects every cluster's
      pids (via `referenced_pids`) and calls `reap_orphans` once per tick
- [x] 2e.2 Replaced the round-3 caller-pids-only `status()` regression (which
      asserted the defective behaviour) with the two-cluster regression
      `reap_orphans_protects_another_clusters_referenced_exit`
- [x] 2e.3 The startup-timeout path clears the handle only after successful
      cleanup and persists a retryable `Terminating` state (with the handle and a
      `cleanup incomplete` `state_message`) on failure; `start_cluster` refuses ANY
      inactive cluster holding an unresolved handle via `ApiError::InvalidState`;
      a post-launch persistence failure rolls the launch back
- [x] 2e.4 Driver loss runs retryable cleanup before clearing the handle: on
      success the cluster is `Terminated` and the handle cleared, on failure the
      handle is retained and the cluster persisted `Terminating`
- [x] 2e.5 API-layer regression tests with a scripted mock backend:
      `settle_failed_launch_clears_handle_only_after_successful_cleanup`,
      `start_cluster_refuses_any_inactive_cluster_holding_a_handle`, and
      `monitor_driver_loss_cleans_up_before_clearing_the_handle`

## 2f. Review remediation, round 6 (gpt-5.6-sol, final round)

- [x] 2f.1 `terminate()` no longer reports success for an untracked-but-alive pid
      (state recorded before a control-plane restart): `reap_round` probes a pid it
      does not hold a handle for, `force_kill` escalates it with a direct
      `kill -KILL`, and cleanup returns an error if the pid still cannot be
      confirmed gone
- [x] 2f.2 Regression test `terminate_never_reports_success_for_an_untracked_alive_pid`
      — verified to FAIL when the untracked pid is skipped (mutation check)
- [x] 2f.3 Preserve the initiating termination reason across `Terminating` retries:
      the monitor's driver-loss path persists `DRIVER_UNREACHABLE` on a failed
      cleanup, and `terminate_cluster` reads it back on a retry instead of
      overwriting it with `USER_REQUEST`
- [x] 2f.4 Regression test `retried_driver_loss_cleanup_preserves_the_reason`
      (fail-once mock backend)
- [x] 2f.5 Correct the artifact overclaims and stale statements in the live spec,
      the archived change, and `docs/issues.md` (retained referenced exits, the
      fetched reference-set snapshot, the untracked-pid probe fallback, the
      removal of the per-`status()` sweep, and the provenance fix)

## 3. Docs and spec

- [x] 3.1 Add LF-029 to `docs/issues.md` section F (after LF-028) with
      Problem / Evidence / Scope / Proposed implementation / Dependencies /
      Acceptance criteria / Focused tests / Docs-parity / OpenSpec
- [x] 3.2 Add LF-029 to the Wave 0 row of the "Index by dependency order" table
- [x] 3.3 Add the `cluster-lifecycle` delta in `specs/cluster-lifecycle/spec.md`
- [x] 3.4 Record the orphan-process-leak supervision gap as a known limitation
      in `proposal.md`

## 4. Verification

- [x] 4.1 `cargo test -p lakeforge-cluster-manager` passes (9 tests)
- [x] 4.2 `cargo clippy --workspace --all-targets -- -D warnings` is clean
- [x] 4.3 `openspec validate --changes` passes for this change
- [x] 4.4 `tests/smoke/platform-smoke.sh` reaches `passed=39 failed=0` on macOS
- [x] 4.5 Commit in small `LF-029: …` commits ending `(refs #LF-029)`; no push
