# Delta: cluster-lifecycle (fix-cluster-liveness-portability)

Applies to a new capability spec `openspec/specs/cluster-lifecycle/spec.md`.
**ADDED** requirements for the local-process backend liveness check. Issue
LF-029.

## ADDED Requirements

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
