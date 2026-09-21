# cluster-lifecycle Specification

## Purpose

How a cluster's lifecycle is reported to callers: what `status()` promises about
liveness, and which processes the local backend owns. Contract: a cluster reads
`RUNNING` while its driver is alive and `TERMINATED` once the driver has exited
or is unknown-dead — on every supported platform, not only the one with `/proc`.
Exited processes this backend spawned are reaped, so a dead driver can never read
as live, and no handle is retained past its process.

Scope note: this capability covers *reporting* the lifecycle. Suspension, idle
timeout and restart policy live in `crates/lakeforge-api/src/api/clusters.rs`.

## Requirements

### Requirement: Cluster liveness is portable across supported platforms

The local-process cluster backend SHALL report a cluster as `RUNNING` whenever
its driver process is alive, on every supported platform (Linux and macOS/BSD).
Liveness for the driver and executors the backend spawns MUST be determined from
the child handle (`Child::try_wait`, which also reaps), so that an exited process
SHALL be reported dead whether or not it has been reaped yet; liveness MUST NOT
depend on Linux-only facilities such as `/proc`. For a pid the backend does not
hold, liveness SHALL be probed portably on Unix targets without `/proc` (e.g. a
`kill -0` signal-0 probe) and that probe is best-effort. A pid of `0` SHALL never
be considered alive on any target, so `terminate` and `kill` stay reachable.
Non-Unix targets are out of scope: liveness there is a documented stub that
assumes a non-zero pid is alive. (LF-029)

#### Scenario: A cluster whose driver process is alive reports RUNNING on every supported platform
- **GIVEN** a `LocalProcessBackend` launched cluster whose `forge driver`
  process is alive
- **WHEN** `status()` is called on any supported platform (Linux or macOS)
- **THEN** it returns `BackendState::Running` with the count of live executors
  as `ready_workers`, and not `Terminated`

#### Scenario: A driver that has exited is reported TERMINATED even before it is reaped
- **GIVEN** a cluster whose driver process has exited but has not been reaped yet
  (a zombie), and whose handle the backend retains
- **WHEN** `status()` is called
- **THEN** it returns `BackendState::Terminated` with message
  `driver process exited`, and the child is reaped in the process
- **AND** this holds on platforms without `/proc`, where a bare `kill -0` probe
  would still report the zombie as alive

#### Scenario: pid 0 is never alive
- **WHEN** the liveness check is applied to pid `0` on any target
- **THEN** it reports dead, so a cluster record with a `0` driver pid is
  `Terminated` and can still be terminated

### Requirement: Retained child handles are never leaked or stranded

The backend SHALL NOT retain the handle of a child process that has already
exited: every sweep SHALL reap exited children and drop their handles, including
children whose pids no longer appear in any cluster's state (executors removed by
`resize`, and a dead driver's executors). Reaping SHALL be bounded, and a process
that survives the polite signal SHALL be force-killed, so an unresponsive process
cannot block or silently outlive `resize`, `terminate` or `status`. (LF-029)

#### Scenario: An exited child whose pid left cluster state is still collected
- **GIVEN** a retained handle for a child that has exited and whose pid appears in
  no cluster state (e.g. an executor removed by `resize`)
- **WHEN** the registry is swept
- **THEN** the child is reaped and its handle removed

#### Scenario: A child that ignores the polite signal does not outlive its cluster
- **GIVEN** a retained child that ignores the polite termination signal
- **WHEN** its pids are reaped as the cluster is scaled down or terminated
- **THEN** the reaper escalates to an uncatchable signal and reaps it, and if a
  process still survives it reports an error rather than a false success, so the
  cluster's handle is not cleared while that process is still running

### Requirement: A recorded exit is authoritative for the cluster that owns it

A pid the backend observed exiting SHALL be reported dead for every cluster whose
state refers to it, even if the retained handle has since been collected by
another cluster's sweep. The backend SHALL NOT pass such a pid to the best-effort
platform probe, which cannot distinguish a zombie from a live process and, on
non-Unix targets, assumes a non-zero pid is alive. (LF-029)

#### Scenario: Another cluster's sweep does not cost the owner its answer
- **GIVEN** a cluster whose driver process has exited, and whose handle was
  collected by a sweep run on behalf of a different cluster
- **WHEN** `status()` is called for the owning cluster
- **THEN** it returns `BackendState::Terminated`, not a probe-derived answer

#### Scenario: A failing sweep does not stop reconciliation for other clusters
- **GIVEN** one registered child whose liveness query fails during a sweep
- **WHEN** `status()` is called for a cluster that does not own that child
- **THEN** the sweep logs the failure with its pid and continues, and the call
  still succeeds; the error is surfaced only to the cluster that owns that child
