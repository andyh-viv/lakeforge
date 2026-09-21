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
   depend on `/proc` at all.
2. **`pid_alive` becomes portable for pids we do not hold.** Keep the existing
   `/proc`-based implementation (zombie-aware: `State:\tZ` is dead) under
   `#[cfg(target_os = "linux")]`. Other Unix targets get a `kill -0` probe
   (`std::process::Command::new("kill").arg("-0")`), treated as best-effort; the
   non-Unix fallback assumes real pids are alive but still reports pid `0` dead.
   A pid of `0` is never alive on any target, so `terminate`/`kill` stay
   reachable. No new crate dependency (`libc` is not added).
3. **`terminate` reaps what we hold.** After signalling, it polls the retained
   handles for a bounded 1s and reaps them, so a torn-down cluster leaves no
   zombies behind. It never blocks termination on an unresponsive process.
4. **Regression tests** in `local.rs`'s `#[cfg(test)] mod tests`:
   - `pid_alive_tracks_liveness_portably` — a live child reports alive, a killed
     *and reaped* child reports dead, pid `0` reports dead. Does not touch
     `/proc`, so it fails on the original implementation on macOS.
   - `exited_but_unreaped_tracked_child_reports_dead` — a tracked child that
     exits on its own, never `wait()`ed by the test, must report dead and be
     dropped from the handle map. This is the review finding: a bare `kill -0`
     probe reports a zombie as alive. Verified to fail against that probe.
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
  while its driver is alive, and TERMINATED once the driver exits **and is
  reaped** (reaping happens on the same `status()` call that observes the exit).
  Linux behaviour is unchanged for the `/proc` path; for spawned children it is
  now also handle-based. A cluster record whose driver pid the backend does not
  hold falls back to the platform probe, which is best-effort (see Risks).
- **Code**: `crates/lakeforge-cluster-manager/src/local.rs` only.
- **Specs**: new `cluster-lifecycle` capability spec, delta in
  `specs/cluster-lifecycle/spec.md` here.
- **Docs**: `docs/issues.md` (LF-029 entry, Wave 0 index row).
- **Tests**: `cargo test -p lakeforge-cluster-manager` (3 tests:
  `pid_alive_tracks_liveness_portably`,
  `exited_but_unreaped_tracked_child_reports_dead`, `pid_zero_is_never_live`).

## Traceability

- Issue: `docs/issues.md` → **LF-029**.
- Branch: `fix/lf-029-cluster-liveness`; commits prefixed `LF-029: …`.
- Review: independent `gpt-5.6-luna` review on PR #3 (REQUEST_CHANGES); this
  revision addresses both blocking findings (zombie handling, pid-0 on non-Unix)
  and the recorded nits.

## Risks

- **Untracked pids are still best-effort.** The `kill -0` / `/proc` probe is only
  reached for a pid the backend does not hold (e.g. a state record that outlived
  its handle). On platforms without `/proc` that probe cannot distinguish a
  zombie from a live process, so such a record can read RUNNING until the pid is
  released. The pids the backend actually spawns are not in this class.
- **`kill -0` false negatives.** If the process exists but is not ours, `kill -0`
  fails with EPERM and the pid reads dead. Not applicable to our own children.
- **`kill`/`ps` resolution.** `Command::new("kill")` resolves through `PATH`
  rather than pinning `/bin/kill`; this matches the pre-existing terminate path.
- **PID reuse / pid 1** remain limitations of any pid-based liveness check; not
  introduced or worsened here.
- **Orphan-process leak (narrowed, not eliminated).** With liveness correct the
  API stops respawning clusters, and `terminate` now reaps the handles it holds;
  there is still no process *supervision*, so a driver that dies while the control
  plane is itself down can leave executors orphaned. That remains out of scope.
- **Non-Unix targets.** Liveness there is a stub (real pids assumed alive, pid 0
  dead) and is covered by inspection only, not by tests.
