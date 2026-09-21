# Change: fix-cluster-liveness-portability

Status: **implemented** on branch `fix/lf-029-cluster-liveness`. Issue LF-029
(Wave 0). Bug fix, not a new capability.

## Why

`crates/lakeforge-cluster-manager/src/local.rs::pid_alive` decides whether a
cluster is alive by probing `/proc/<pid>`, but it is gated `#[cfg(unix)]`.
`/proc` is Linux-only; macOS is Unix without `/proc`, so
`Path::new("/proc/<pid>").exists()` is always false and `pid_alive` always
returns false.

Consequences on macOS (`main` @ 993cbd7):

- `LocalProcessBackend::status()` returns `Terminated` ("driver process
  exited") immediately after launch, so the API reports **every** cluster
  TERMINATED while the `forge driver`/`forge executor` processes are alive and
  healthy (`driver.log` shows `driver listening` + `executor registered`).
- Because `status()` says terminated, the API respawns clusters and leaks
  orphaned `forge driver`/`forge executor` processes (10+ observed for one
  cluster).
- Baseline: `tests/smoke/platform-smoke.sh` = `passed=36 failed=3`, and
  `tests/smoke/uc-lakebase-smoke.sh` = `passed=49 failed=1`. On Linux CI the
  same code reports 39/39 and 50/50 because `/proc` exists there.

## What Changes

1. **Liveness for the pids we spawn is answered from the child handle, not a
   probe.** `LocalProcessBackend` now retains the `tokio::process::Child` handles
   it spawns (keyed by pid) and `status()` asks `LocalProcessBackend::pid_live`,
   which calls `Child::try_wait`. `try_wait` *reports and reaps* an exited child,
   so a driver that has exited — even one not yet reaped (a zombie) — reads as
   dead, and no zombie is left behind. This is platform-independent: it does not
   depend on `/proc` at all. If `try_wait` itself fails, `pid_live` returns the
   error instead of guessing: a probe cannot describe our own child, and falling
   back to one is exactly the zombie-blind answer this change removes.
2. **`pid_alive` becomes portable for pids we do not hold.** Keep the existing
   `/proc`-based implementation (zombie-aware: `State:\tZ` is dead) under
   `#[cfg(target_os = "linux")]`. Other Unix targets get a `kill -0` probe
   (`std::process::Command::new("kill").arg("-0")`), treated as best-effort; the
   non-Unix fallback assumes real pids are alive but still reports pid `0` dead.
   A pid of `0` is never alive on any target, so `terminate`/`kill` stay
   reachable. No new crate dependency (`libc` is not added).
3. **The handle registry is swept by the control plane, so nothing is leaked or
   stranded.** A retained handle for an exited child is a zombie nothing else will
   reap, and it grows the map. The backend exposes `reap_orphans(referenced)`,
   which sweeps the whole registry, and `monitor_clusters` calls it once per tick
   with the set of pids referenced by every cluster's state it fetched that tick.
   `status()` and `terminate()` deliberately do NOT sweep — a single cluster cannot
   see the other clusters' state, so sweeping there could only use an incomplete set
   and collect another cluster's still-referenced exit. The sweep collects children
   whose pids have left cluster state: executors removed by `resize()` and a dead
   driver's executors; an exited child whose pid a cluster's state still references
   is deliberately retained, and the owner consumes that exit on its next liveness
   query. A failing `try_wait` during a sweep is logged with its pid and isolated to
   that child — it is not returned as an error, because the sweep runs once per tick
   and one unqueryable child must not stop reconciliation for every other cluster. A
   sweep never collects a pid in the reference set: the owner consumes that exit
   through its own liveness query, so a pid whose handle is still retained is never
   handed to the best-effort probe (only an untracked pid — state recorded before a
   control-plane restart — reaches that probe), and the guarantee does not depend on
   any retention window or capacity. `resize()` reaps the executors it removes
   directly, and `terminate()` reaps its own pids, including untracked ones: a pid
   it does not hold a handle for is probed, force-killed with a direct SIGKILL if
   still alive, and reported as an error if it still cannot be confirmed gone, so a
   cluster handle is never cleared while a live process remains — for a tracked
   child that confirmation is the authoritative handle, for an untracked pid it is
   the best-effort probe. Sweeps are bounded and never block on an unresponsive
   process. The registry lock tolerates poisoning (a panic while held would
   otherwise take down the API's cluster monitor loop).
4. **Cleanup failure is retryable across the API layer** in
   `crates/lakeforge-api/src/api/clusters.rs`. When `backend.terminate` cannot
   reap every process, `terminate_cluster` keeps the handle and a
   `cleanup incomplete: …` `state_message` and returns an error rather than
   clearing the handle (which would strand a live process with nothing pointing
   at it), and a cluster left in `Terminating` is retried instead of
   early-returning. `start_cluster` refuses ANY inactive cluster that still holds
   a handle (`ApiError::InvalidState`, not `InvalidParameterValue`), so a launch
   cannot overwrite the only handle to a process that survived cleanup. The
   startup-timeout path clears the handle only after successful cleanup and
   otherwise persists a retryable `Terminating` state, and a post-launch
   persistence failure rolls the launch back. Driver loss runs cleanup before
   clearing the handle, so a dead driver's live executors are reaped rather than
   stranded. `permanent_delete` propagates a cleanup failure instead of
   deleting the record (the last reference), while a cluster that is already gone
   stays an idempotent success. `monitor_clusters` retries a `Terminating`
   cluster, so the "a later reconcile retries" claim is true rather than
   aspirational, and the retry preserves the reason that initiated the
   termination — read back from `termination_reason` (e.g. `DRIVER_UNREACHABLE`)
   rather than overwritten with `USER_REQUEST`. The cleanup-failure-then-retry
   path is unit-tested with a fail-once mock backend, not just the LF-025
   integration harness.
5. **Regression tests** in `local.rs`'s `#[cfg(test)] mod tests`:
   - `pid_alive_tracks_liveness_portably` — a live child reports alive, a killed
     *and reaped* child reports dead, pid `0` reports dead. Does not touch
     `/proc`, so it fails on the original implementation on macOS.
   - `exited_but_unreaped_tracked_child_reports_dead` — a retained child that is
     stopped and left unreaped must report dead and be dropped from the handle
     map. This is the first review finding: a bare `kill -0` probe reports a
     zombie as alive. Verified to fail against that probe.
   - `sweep_exited_collects_handles_whose_pids_left_cluster_state` — three exited
     children whose pids appear in no cluster state must all be collected, and
     must still read dead afterwards. This is the second review finding (the
     `resize` leak); verified to fail when the sweep is removed.
   - `reap_orphans_protects_another_clusters_referenced_exit` — a two-cluster
     regression: cluster A's sweep (with the reference set the control plane
     collected) cannot collect cluster B's still-referenced exit, and B still
     learns of it authoritatively.
   - `sweep_exited_retains_pids_its_cluster_still_references` — a sweep never
     collects a pid its cluster's state still references; the owner consumes the
     exit itself. Verified to fail when the ownership guard is removed.
   - `sweep_exited_collects_many_unreferenced_handles_in_one_pass` — 300
     unreferenced exited children are all collected in one pass, so no exit is
     lost to a capacity limit.
   - `reap_pids_force_kills_a_child_that_ignores_the_polite_signal` — a child that
     ignores SIGTERM is force-killed rather than left running; the child signals
     readiness first so the test cannot pass for the wrong reason, and it is
     verified to fail when the escalation is removed.
   - `terminate_never_reports_success_for_an_untracked_alive_pid` — a
     SIGTERM-ignoring child spawned with plain `std::process::Command` (so it is
     NOT tracked) must be probed, force-killed, and reported as an error if it
     still cannot be confirmed gone; `terminate()` must never return `Ok` while it
     is alive. Verified to fail when the untracked pid is skipped (the restart
     regression).
   - `pid_zero_is_never_live` — the pid-0 invariant holds through both paths.

API-layer regression tests in `clusters.rs` (scripted mock backend):
   - `settle_failed_launch_clears_handle_only_after_successful_cleanup` — the
     startup-timeout path clears the handle only after cleanup succeeds and keeps
     it (persisting a retryable `Terminating` state) when cleanup fails.
   - `start_cluster_refuses_any_inactive_cluster_holding_a_handle` — an inactive
     cluster that still holds a handle is refused with `ApiError::InvalidState`.
   - `monitor_driver_loss_cleans_up_before_clearing_the_handle` — driver loss runs
     cleanup before clearing the handle; a failed cleanup keeps the handle and
     persists a retryable `Terminating` state.
   - `retried_driver_loss_cleanup_preserves_the_reason` — a driver-loss cleanup
     that fails once and succeeds on the retry is still recorded as
     `DRIVER_UNREACHABLE`, not `USER_REQUEST`.

The required cases hold on Linux **and** macOS: (a) a live process → alive;
(b) an exited process → dead whether or not it has been reaped, **for a pid the
backend holds a child handle for** (the untracked case is best-effort and can read
alive for a dead-but-unreaped pid; see LF-030); (c) pid `0` →
dead.

## Scope

- `crates/lakeforge-cluster-manager/src/local.rs`: `pid_alive` (the portable
  probe), the retained-handle registry and its ownership-aware sweep
  (`sweep_exited`), `pid_live`, `reap_pids`, and `status`/`resize`/`terminate`,
  plus the test module.
- `crates/lakeforge-api/src/api/clusters.rs`: cleanup-failure handling in
  `terminate_cluster`, `start_cluster`, `permanent_delete`, and
  `monitor_clusters`.

## Non-scope

- The smoke scripts (`tests/smoke/*.sh`) — that is LF-028, a separate PR.
- `launch`, port allocation, and the Kubernetes backend. (`resize` **is** touched:
  it now reaps the executors it removes instead of leaking their handles.)
- Fixing the orphan-process leak with process supervision — recorded below as
  a known limitation of this change.
- No new crates, no rustfmt sweep, no unrelated refactors.

## Impact

- **Behaviour**: on macOS/BSD the local backend now reports a cluster RUNNING
  while its driver is alive, and TERMINATED once the driver exits — reported and
  reaped on the same `status()` call that observes it, so an exited driver never
  reads as RUNNING. This holds for the driver and executors the backend spawned
  and still holds a handle for. No exited child is leaked or stranded: a child
  whose pid no cluster state references is collected by a registry sweep, and a
  child whose pid is still referenced is consumed by its owner's own liveness
  query (the sweep never collects a referenced pid). Linux behaviour is unchanged
  for the `/proc` path; spawned children are now handle-based there too.
- **API cleanup**: a failed `backend.terminate` no longer clears the handle or
  claims `Terminated`. `terminate_cluster` keeps the handle and a
  `cleanup incomplete: …` `state_message` and returns an error, `start_cluster`
  refuses ANY inactive cluster that still holds a handle (`InvalidState`),
  `permanent_delete` propagates the failure instead of deleting the record (a
  missing cluster stays an idempotent success), and `monitor_clusters` retries a
  `Terminating` cluster and runs cleanup before clearing the handle on driver
  loss. A retry preserves the reason that initiated the termination (e.g.
  `DRIVER_UNREACHABLE`) rather than overwriting it with `USER_REQUEST`. The
  startup-timeout path clears the handle only after successful cleanup.
- **Code**: `crates/lakeforge-cluster-manager/src/local.rs` and
  `crates/lakeforge-api/src/api/clusters.rs`.
- **Specs**: new `cluster-lifecycle` capability spec, delta in
  `specs/cluster-lifecycle/spec.md` here.
- **Docs**: `docs/issues.md` (LF-029 entry, Wave 0 index row); `docs/handoff.md`
  (UC/Lakebase smoke recorded as 49/1, not 50/50).
- **Tests**: `cargo test -p lakeforge-cluster-manager` (9 tests:
  `pid_alive_tracks_liveness_portably`,
  `exited_but_unreaped_tracked_child_reports_dead`,
  `sweep_exited_collects_handles_whose_pids_left_cluster_state`,
  `reap_orphans_protects_another_clusters_referenced_exit`,
  `sweep_exited_retains_pids_its_cluster_still_references`,
  `sweep_exited_collects_many_unreferenced_handles_in_one_pass`,
  `reap_pids_force_kills_a_child_that_ignores_the_polite_signal`,
  `terminate_never_reports_success_for_an_untracked_alive_pid`,
  `pid_zero_is_never_live`), plus 4 API-layer tests in `clusters.rs` (the
  cleanup-failure-then-retry path included, via a fail-once mock backend).

## Traceability

- Issue: `docs/issues.md` → **LF-029**.
- Branch: `fix/lf-029-cluster-liveness`; commits prefixed `LF-029: …`.
- Review: independent reviews on PR #3 — `gpt-5.6-luna` (rounds 1–2) and
  `gpt-5.6-sol` (rounds 3–6). Round 1: zombie handling, pid-0 on non-Unix.
  Round 2: swallowed `try_wait` error, `resize` handle leak, unswept executor
  handles. Round 3: sweep-failure isolation, recorded-exit precedence, force-kill
  escalation. Round 4: the recorded-exit guarantee was capacity-dependent, and
  cleanup failure was not safely retryable across the API. Round 5: the sweep
  protected only the calling cluster's pids (not the reference set collected from
  every cluster), the startup-timeout path ignored cleanup failure, and driver
  death cleared the handle while executors were still alive. Round 6 (final):
  `terminate()` reported success for an untracked-but-alive pid after a restart,
  a retried driver-loss cleanup lost the DRIVER_UNREACHABLE classification, and
  the artifacts overclaimed the sweep/retention/cleanup guarantees. All rounds are
  addressed here.

## Risks

- **The "exited ⇒ dead" guarantee is scoped to handles we retain.** That covers
  every driver and executor this backend spawns. For a pid we do **not** hold — a
  cluster recorded before the control plane restarted, so the handle is gone —
  `pid_alive` is used: exact on Linux (including zombies), best-effort elsewhere.
  On a platform without `/proc` such a record can read RUNNING until the pid is
  released. Callers must not read the fallback as a guarantee.
- **`kill -0` false negatives.** If the process exists but is not ours, `kill -0`
  fails with EPERM and the pid reads dead. Not applicable to our own children.
- **`kill` resolution.** `Command::new("kill")` resolves through `PATH` rather
  than pinning `/bin/kill`; this matches the pre-existing terminate path.
- **PID reuse / pid 1** remain limitations of any pid-based liveness check; not
  introduced or worsened here.
- **Orphan-process leak (narrowed, not eliminated).** With liveness correct the
  API stops respawning clusters, and retained handles are reaped; there is still
  no process *supervision*, so a driver that dies while the control plane is
  itself down can leave executors orphaned. Out of scope.
- **Non-Unix targets.** Liveness there is a stub (real pids assumed alive, pid 0
  dead) and is covered by inspection only, not by tests.
