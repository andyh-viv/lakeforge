# Delta: cluster-lifecycle (fix-cluster-liveness-portability)

Applies to a new capability spec `openspec/specs/cluster-lifecycle/spec.md`.
**ADDED** requirements for the local-process backend liveness check. Issue
LF-029.

## ADDED Requirements

### Requirement: Cluster liveness is portable across supported platforms

The local-process cluster backend SHALL report a cluster as `RUNNING` whenever
its driver process is alive, on every supported platform (Linux and macOS/
BSD). Liveness MUST NOT depend on Linux-only facilities such as `/proc`. On
Unix targets without `/proc`, liveness SHALL be probed portably (e.g. a
`kill -0` signal-0 probe treating exit status 0 as alive). A pid of `0` SHALL
never be considered alive, so `terminate` and `kill` stay reachable. (LF-029)

#### Scenario: A cluster whose driver process is alive reports RUNNING on every supported platform
- **GIVEN** a `LocalProcessBackend` launched cluster whose `forge driver`
  process is alive
- **WHEN** `status()` is called on any supported platform (Linux or macOS)
- **THEN** it returns `BackendState::Running` with the count of live executors
  as `ready_workers`, and not `Terminated`

#### Scenario: A reaped driver process reports TERMINATED
- **GIVEN** a cluster whose driver process has exited and been reaped
- **WHEN** `status()` is called
- **THEN** it returns `BackendState::Terminated` with message
  `driver process exited`

#### Scenario: pid 0 is never alive
- **WHEN** the liveness check is applied to pid `0`
- **THEN** it reports dead, so a cluster record with a `0` driver pid is
  `Terminated` and can still be terminated
