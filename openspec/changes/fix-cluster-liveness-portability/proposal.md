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

1. **`pid_alive` becomes portable.** Keep the existing `/proc`-based
   implementation (zombie-aware: `State:\tZ` is dead) under
   `#[cfg(target_os = "linux")]`. Add a portable fallback for other Unix
   targets under `#[cfg(all(unix, not(target_os = "linux")))]` that runs
   `std::process::Command::new("kill").arg("-0").arg(pid).status()` and treats
   exit status 0 as alive. Keep the non-Unix `true` fallback. A pid of `0` is
   never alive. No new crate dependency (`libc` is not added).
2. **Regression test** in `local.rs`'s `#[cfg(test)] mod tests`: spawn a
   portable long-lived child (`sleep 30`), assert `pid_alive(child.id())`;
   kill + `wait()` to reap, assert `pid_alive(pid)` is false (bounded poll);
   assert `pid_alive(0)` is false. It does not touch `/proc`, so it fails on
   the old implementation on macOS.

The three required cases now hold on Linux **and** macOS: (a) a live process →
alive; (b) an exited and reaped process → dead; (c) pid `0` → dead.

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
  while its driver is alive and TERMINATED only after the driver exits.
  Linux behaviour is unchanged.
- **Code**: `crates/lakeforge-cluster-manager/src/local.rs` only.
- **Specs**: new `cluster-lifecycle` capability spec, delta in
  `specs/cluster-lifecycle/spec.md` here.
- **Docs**: `docs/issues.md` (LF-029 entry, Wave 0 index row).
- **Tests**: `cargo test -p lakeforge-cluster-manager`
  (`pid_alive_tracks_liveness_portably`).

## Traceability

- Issue: `docs/issues.md` → **LF-029**.
- Branch: `fix/lf-029-cluster-liveness`; commits prefixed `LF-029: …`.

## Risks

- **Orphan-process leak (known limitation, not fixed here).** Once `pid_alive`
  is correct the API stops respawning, so the leak is dormant; but there is
  still no process supervision, so a driver that dies while the control plane
  is down leaves executors orphaned. Recorded for a future
  reconciliation/supervision change, out of scope for this bug fix.
- **`kill -0` semantics.** `kill -0` succeeds while a pid is a zombie not yet
  reaped, and could in principle match a recycled pid. This matches the
  behaviour expected for the smoke tests and is the standard portable probe;
  the test reaps the child before asserting death.
