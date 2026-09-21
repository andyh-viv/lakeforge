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
3. **The handle registry is swept, so nothing is leaked or stranded.** A retained
   handle for an exited child is a zombie nothing else will reap, and it grows the
   map. `reap_exited()` sweeps the *whole* registry and is called from `status()`
   and `terminate()`, which is what collects children whose pids have already left
   cluster state: executors removed by `resize()` (whose pids are dropped from
   state) and a dead driver's executors. `resize()` also reaps the executors it
   removes directly, and `terminate()` reaps its own pids with a bounded wait
   before sweeping, so a torn-down cluster is collected even if a process exits
   after the handle is cleared. Sweeps are bounded and never block on an
   unresponsive process. The registry lock tolerates poisoning (a panic while held
   would otherwise take down the API's cluster monitor loop).
4. **Regression tests** in `local.rs`'s `#[cfg(test)] mod tests`:
   - `pid_alive_tracks_liveness_portably` — a live child reports alive, a killed
     *and reaped* child reports dead, pid `0` reports dead. Does not touch
     `/proc`, so it fails on the original implementation on macOS.
   - `exited_but_unreaped_tracked_child_reports_dead` — a retained child that
     exits on its own, never `wait()`ed by the test, must report dead and be
     dropped from the handle map. This is the first review finding: a bare
     `kill -0` probe reports a zombie as alive. Verified to fail against that probe.
   - `reap_exited_collects_handles_whose_pids_left_cluster_state` — three exited
     children whose pids appear in no cluster state must all be collected by the
     registry sweep. This is the second review finding (the `resize` leak);
     verified to fail when the sweep is removed.
   - `pid_zero_is_never_live` — the pid-0 invariant holds through both paths.

The required cases hold on Linux **and** macOS: (a) a live process → alive;
(b) an exited process → dead whether or not it has been reaped; (c) pid `0` →
dead.

## Scope

- `pid_alive` in `crates/lakeforge-cluster-manager/src/local.rs` and its test
  module.

## Non-scope

- The smoke scripts (`tests/smoke/*.sh`) — that is LF-028, a separate PR.
- `resize`, `launch`, port allocation, and the Kubernetes backend.
- Fixing the orphan-process leak with process supervision — recorded below as
  a known limitation of this change.
- No new crates, no rustfmt sweep, no unrelated refactors.

## Impact

- **Behaviour**: on macOS/BSD the local backend now reports a cluster RUNNING
  while its driver is alive, and TERMINATED once the driver exits — reported and
  reaped on the same `status()` call that observes it, so an exited driver never
  reads as RUNNING. This holds for the driver and executors the backend spawned
  and still holds a handle for. Exited children are collected by a registry sweep
  whether or not their pids remain in cluster state, so no handle is leaked and no
  zombie is stranded. Linux behaviour is unchanged for the `/proc` path; spawned
  children are now handle-based there too.
- **Code**: `crates/lakeforge-cluster-manager/src/local.rs` only.
- **Specs**: new `cluster-lifecycle` capability spec, delta in
  `specs/cluster-lifecycle/spec.md` here.
- **Docs**: `docs/issues.md` (LF-029 entry, Wave 0 index row).
- **Tests**: `cargo test -p lakeforge-cluster-manager` (4 tests:
  `pid_alive_tracks_liveness_portably`,
  `exited_but_unreaped_tracked_child_reports_dead`,
  `reap_exited_collects_handles_whose_pids_left_cluster_state`,
  `pid_zero_is_never_live`).

## Traceability

- Issue: `docs/issues.md` → **LF-029**.
- Branch: `fix/lf-029-cluster-liveness`; commits prefixed `LF-029: …`.
- Review: independent `gpt-5.6-luna` reviews on PR #3. Round 1 returned
  REQUEST_CHANGES (zombie handling, pid-0 on non-Unix); round 2 returned
  REQUEST_CHANGES again (swallowed `try_wait` error, `resize` handle leak,
  unswept executor handles). Both rounds are addressed here.

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
