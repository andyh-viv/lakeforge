//! Local-process backend: runs the Forge driver and executors as child
//! processes of the control plane using the `forge` binary.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::{BackendState, BackendStatus, ClusterBackend, ClusterError, ClusterHandle, LaunchSpec, Result};

#[derive(Debug, Clone)]
pub struct LocalProcessBackend {
    pub forge_bin: PathBuf,
    pub work_root: PathBuf,
    pub host: String,
    /// Live driver/executor children, keyed by pid.
    ///
    /// Holding the handles (instead of letting them drop) makes liveness
    /// deterministic on every platform: `Child::try_wait` reports *and reaps* an
    /// exited child, so a process that has exited but has not been reaped yet (a
    /// zombie) is never mistaken for a live one — the false positive a bare
    /// `kill -0` probe produces where there is no `/proc`.
    children: Arc<Mutex<HashMap<u32, tokio::process::Child>>>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct LocalState {
    driver_pid: u32,
    driver_port: u16,
    executor_pids: Vec<u32>,
    executor_ports: Vec<u16>,
    work_dir: String,
}

impl LocalProcessBackend {
    pub fn from_env() -> Self {
        let forge_bin = std::env::var("LAKEFORGE_FORGE_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|_| locate_forge_bin());
        let work_root = std::env::var("LAKEFORGE_CLUSTER_WORK_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/lakeforge/clusters"));
        Self { forge_bin, work_root, host: "127.0.0.1".into(), children: Arc::new(Mutex::new(HashMap::new())) }
    }

    fn spawn(&self, args: &[String], env: &std::collections::BTreeMap<String, String>, log: &Path) -> Result<u32> {
        let out = std::fs::File::create(log)?;
        let err = out.try_clone()?;
        let mut cmd = Command::new(&self.forge_bin);
        cmd.args(args)
            .envs(env)
            .env("RUST_LOG", std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()))
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .kill_on_drop(false);
        let child = cmd.spawn().map_err(|e| {
            ClusterError::Launch(format!("spawn {}: {e}", self.forge_bin.display()))
        })?;
        // Retain the handle so liveness can be answered with `try_wait` (which also
        // reaps the child) rather than a zombie-tolerant probe.
        let pid = child.id().ok_or_else(|| {
            ClusterError::Launch(format!(
                "spawn {}: child exited before its pid could be recorded",
                self.forge_bin.display()
            ))
        })?;
        self.children().insert(pid, child);
        Ok(pid)
    }

    /// The child registry, tolerating a poisoned mutex.
    ///
    /// A panic while the lock was held poisons it. The registry is a plain map of
    /// handles with no cross-entry invariant, so recovering the guard is strictly
    /// better than panicking the caller — a panic here would take down the control
    /// plane's cluster monitor loop.
    fn children(&self) -> std::sync::MutexGuard<'_, HashMap<u32, tokio::process::Child>> {
        self.children.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reap every retained child that has already exited, dropping its handle.
    ///
    /// A retained handle for an exited child is a zombie nothing else will reap,
    /// and it keeps the map growing. Sweeping the *whole* registry (rather than
    /// only the pids currently in a cluster's state) is what collects children
    /// whose pids have already left state — executors removed by [`Self::resize`],
    /// and a driver's executors after the driver itself has exited. `try_wait`
    /// errors are returned rather than swallowed. Returns the number reaped.
    fn reap_exited(&self) -> Result<usize> {
        let mut reaped = 0usize;
        let mut first_err: Option<std::io::Error> = None;
        let mut done: Vec<u32> = Vec::new();
        {
            let mut children = self.children();
            for (pid, child) in children.iter_mut() {
                match child.try_wait() {
                    Ok(Some(_)) => {
                        done.push(*pid);
                        reaped += 1;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            for pid in done {
                children.remove(&pid);
            }
        }
        match first_err {
            Some(e) => Err(ClusterError::Backend(format!("reaping child processes: {e}"))),
            None => Ok(reaped),
        }
    }

    /// Give pids that are leaving cluster state a bounded chance to exit, then
    /// reap and forget their handles.
    ///
    /// Bounded on purpose: an unresponsive process must not block `resize` or
    /// `terminate`. Anything still running after the budget keeps its handle and is
    /// collected later by [`Self::reap_exited`], so no zombie is stranded.
    async fn reap_pids(&self, pids: &[u32]) {
        for _ in 0..20 {
            let mut still_running = false;
            let mut done: Vec<u32> = Vec::new();
            {
                let mut children = self.children();
                for pid in pids {
                    if let Some(child) = children.get_mut(pid) {
                        match child.try_wait() {
                            Ok(Some(_)) => done.push(*pid),
                            Ok(None) => still_running = true,
                            // Do not remove on error: leave it for `reap_exited`,
                            // which reports errors instead of discarding them.
                            Err(_) => still_running = true,
                        }
                    }
                }
                for pid in done {
                    children.remove(&pid);
                }
            }
            if !still_running {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Whether a process this backend spawned is still running.
    ///
    /// For a child we hold this is authoritative *and* it reaps: `try_wait` reports
    /// an exited process whether or not it has been reaped yet, so a zombie can
    /// never read as live. If `try_wait` itself fails there is no answer we can
    /// trust — a probe cannot describe our own child, and falling back to one is
    /// precisely the zombie-blind behaviour this change removes — so the error is
    /// returned for the caller to report rather than guessed at.
    ///
    /// A pid we do **not** hold (a cluster recorded before the control plane
    /// restarted has state but no handle) is resolved with [`pid_alive`]: exact on
    /// Linux, best-effort elsewhere. That residual is called out in the change
    /// proposal and must not be read as a guarantee.
    ///
    /// A pid of `0` is never live, so `terminate`/`kill` stay reachable.
    fn pid_live(&self, pid: u32) -> Result<bool> {
        if pid == 0 {
            return Ok(false);
        }
        let mut exited = false;
        let mut child_err: Option<std::io::Error> = None;
        {
            let mut children = self.children();
            if let Some(child) = children.get_mut(&pid) {
                match child.try_wait() {
                    Ok(None) => return Ok(true),   // still running
                    Ok(Some(_)) => exited = true,  // exited; try_wait has reaped it
                    Err(e) => child_err = Some(e),
                }
            }
            if exited {
                children.remove(&pid);
            }
        }
        if let Some(e) = child_err {
            return Err(ClusterError::Backend(format!("querying child process {pid}: {e}")));
        }
        if exited {
            return Ok(false);
        }
        Ok(pid_alive(pid))
    }

    fn spawn_executor(
        &self,
        spec: &LaunchSpec,
        idx: usize,
        driver_port: u16,
        work_dir: &Path,
    ) -> Result<(u32, u16)> {
        let port = free_port()?;
        let id = format!("{}-exec-{idx}", &spec.cluster_id[..spec.cluster_id.len().min(8)]);
        let args = vec![
            "executor".to_string(),
            "--bind".into(),
            format!("{}:{port}", self.host),
            "--advertise-host".into(),
            self.host.clone(),
            "--driver".into(),
            format!("http://{}:{driver_port}", self.host),
            "--id".into(),
            id.clone(),
            "--slots".into(),
            spec.slots_per_worker.to_string(),
            "--work-dir".into(),
            work_dir.join(&id).to_string_lossy().into_owned(),
        ];
        let mut env = spec.env.clone();
        env.insert("FORGE_MEMORY_LIMIT_MB".into(), spec.worker_memory_mb.to_string());
        let pid = self.spawn(&args, &env, &work_dir.join(format!("{id}.log")))?;
        Ok((pid, port))
    }
}

#[async_trait]
impl ClusterBackend for LocalProcessBackend {
    fn name(&self) -> &'static str {
        "local"
    }

    async fn launch(&self, spec: &LaunchSpec) -> Result<ClusterHandle> {
        let work_dir = self.work_root.join(&spec.cluster_id);
        std::fs::create_dir_all(&work_dir)?;
        let driver_port = free_port()?;
        let mut env = spec.env.clone();
        for (k, v) in &spec.conf {
            env.insert(format!("FORGE_CONF_{}", k.replace('.', "_").to_uppercase()), v.clone());
        }
        let driver_args = vec![
            "driver".to_string(),
            "--bind".into(),
            format!("{}:{driver_port}", self.host),
            "--advertise-host".into(),
            self.host.clone(),
            "--work-dir".into(),
            work_dir.join("driver").to_string_lossy().into_owned(),
        ];
        let driver_pid = self.spawn(&driver_args, &env, &work_dir.join("driver.log"))?;

        let mut state = LocalState {
            driver_pid,
            driver_port,
            work_dir: work_dir.to_string_lossy().into_owned(),
            ..Default::default()
        };
        for i in 0..spec.num_workers as usize {
            let (pid, port) = self.spawn_executor(spec, i, driver_port, &work_dir)?;
            state.executor_pids.push(pid);
            state.executor_ports.push(port);
        }
        Ok(ClusterHandle {
            backend: "local".into(),
            driver_addr: format!("http://{}:{driver_port}", self.host),
            state: serde_json::to_value(state).unwrap_or_default(),
        })
    }

    async fn status(&self, handle: &ClusterHandle) -> Result<BackendStatus> {
        // Sweep first: this keeps the handle registry bounded and collects children
        // whose pids have already left cluster state (executors removed by
        // `resize`, and a dead driver's executors).
        self.reap_exited()?;
        let st: LocalState = serde_json::from_value(handle.state.clone()).unwrap_or_default();
        if !self.pid_live(st.driver_pid)? {
            return Ok(BackendStatus {
                state: BackendState::Terminated,
                message: Some("driver process exited".into()),
                ready_workers: 0,
            });
        }
        let mut ready = 0u32;
        for p in &st.executor_pids {
            if self.pid_live(*p)? {
                ready += 1;
            }
        }
        Ok(BackendStatus { state: BackendState::Running, message: None, ready_workers: ready })
    }

    async fn resize(&self, spec: &LaunchSpec, handle: &ClusterHandle) -> Result<ClusterHandle> {
        let mut st: LocalState = serde_json::from_value(handle.state.clone()).unwrap_or_default();
        let target = spec.num_workers as usize;
        let work_dir = PathBuf::from(&st.work_dir);
        let mut removed: Vec<u32> = Vec::new();
        while st.executor_pids.len() > target {
            if let Some(pid) = st.executor_pids.pop() {
                kill(pid);
                st.executor_ports.pop();
                removed.push(pid);
            }
        }
        // These pids are leaving cluster state, so nothing else would collect their
        // handles: reap them here instead of leaking a map entry (and a zombie) per
        // scale-down.
        self.reap_pids(&removed).await;
        let mut idx = st.executor_pids.len();
        while st.executor_pids.len() < target {
            let (pid, port) = self.spawn_executor(spec, idx, st.driver_port, &work_dir)?;
            st.executor_pids.push(pid);
            st.executor_ports.push(port);
            idx += 1;
        }
        Ok(ClusterHandle {
            backend: handle.backend.clone(),
            driver_addr: handle.driver_addr.clone(),
            state: serde_json::to_value(st).unwrap_or_default(),
        })
    }

    async fn terminate(&self, handle: &ClusterHandle) -> Result<()> {
        let st: LocalState = serde_json::from_value(handle.state.clone()).unwrap_or_default();
        let pids: Vec<u32> = st.executor_pids.iter().copied().chain(std::iter::once(st.driver_pid)).collect();
        for pid in &pids {
            kill(*pid);
        }
        // Reap this cluster's processes, then sweep the registry: a child that exits
        // after this cluster's handle is cleared (or after the bounded wait) is still
        // collected, so it cannot linger as a zombie or a stale map entry.
        self.reap_pids(&pids).await;
        self.reap_exited()?;
        Ok(())
    }
}

fn locate_forge_bin() -> PathBuf {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join("forge");
            if sibling.exists() {
                return sibling;
            }
        }
    }
    PathBuf::from("forge")
}

fn free_port() -> Result<u16> {
    let l = TcpListener::bind("127.0.0.1:0")?;
    Ok(l.local_addr()?.port())
}

/// Whether `pid` refers to a live process.
///
/// Linux uses `/proc` (which correctly treats a zombie, `State:\tZ`, as dead).
/// Every other Unix (macOS, *BSD) has no `/proc`, so it falls back to the
/// portable `kill -0` probe: exit status 0 means the process exists. A pid of
/// `0` is never a live process.
///
/// This is deliberately the *fallback*: it cannot distinguish a zombie from a
/// live process, and it false-negatives if the process exists but is not ours
/// (`kill -0` then fails with EPERM). Cluster status therefore goes through
/// [`LocalProcessBackend::pid_live`], which answers for the children the backend
/// spawns (via `try_wait`, which reaps) and only uses this probe for pids it does
/// not hold. `kill` is resolved through `PATH`, as the terminate path was before.
#[cfg(target_os = "linux")]
fn pid_alive(pid: u32) -> bool {
    pid != 0 && std::path::Path::new(&format!("/proc/{pid}")).exists()
        && !std::fs::read_to_string(format!("/proc/{pid}/status"))
            .map(|s| s.contains("State:\tZ"))
            .unwrap_or(true)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn pid_alive(pid: u32) -> bool {
    // Best-effort only: there is no portable liveness probe here, so real pids are
    // assumed alive. Pid 0 is still reported dead so that the "pid 0 is never
    // alive" invariant — which keeps `terminate`/`kill` reachable — holds on every
    // target rather than being an Unix-only property.
    pid != 0
}

#[cfg(unix)]
fn kill(pid: u32) {
    if pid == 0 {
        return;
    }
    let _ = std::process::Command::new("kill").arg("-TERM").arg(pid.to_string()).status();
}

#[cfg(not(unix))]
fn kill(_pid: u32) {}

#[cfg(all(test, unix))]
mod tests {
    use super::{pid_alive, LocalProcessBackend};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::process::Stdio;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    // NOTE: this module is Unix-only, so the non-Unix `pid_alive` fallback is
    // covered by inspection rather than by these tests. It is a documented
    // best-effort stub; only the "pid 0 is never alive" part is load-bearing.

    fn backend() -> LocalProcessBackend {
        LocalProcessBackend {
            forge_bin: PathBuf::from("forge"),
            work_root: PathBuf::from("/tmp"),
            host: "127.0.0.1".into(),
            children: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Kills and reaps a child on drop, so a failing assertion cannot leak a live
    /// process (or leave a zombie) for the rest of the test run.
    struct Guard(std::process::Child);

    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn spawn_guarded(prog: &str, args: &[&str]) -> (u32, Guard) {
        let child = std::process::Command::new(prog)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");
        let pid = child.id();
        (pid, Guard(child))
    }

    // Regression test for LF-029: on macOS the old `/proc`-only `pid_alive`
    // always returned `false`, so a live `forge driver` was reported as
    // TERMINATED. This test does not touch `/proc`, so it fails on the old
    // implementation for every Unix target, including macOS.
    #[test]
    fn pid_alive_tracks_liveness_portably() {
        // (a) a live child process is alive.
        let (pid, mut guard) = spawn_guarded("sleep", &["30"]);
        assert!(pid != 0, "spawned child should have a pid");
        assert!(pid_alive(pid), "a live child process must report alive");

        // (b) once killed and reaped, the pid is dead.
        guard.0.kill().expect("kill sleep");
        guard.0.wait().expect("reap sleep");

        // A probe can transiently succeed until the pid is fully released, so
        // poll with a bounded budget rather than assuming instant death.
        let mut dead = false;
        for _ in 0..50 {
            if !pid_alive(pid) {
                dead = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(dead, "a reaped child process must report dead");

        // (c) pid 0 is never alive.
        assert!(!pid_alive(0), "pid 0 must never report alive");
    }

    // Review finding: a driver that has exited but has NOT been reaped yet (a
    // zombie) must read as DEAD. A bare `kill -0` probe says "alive" for a zombie
    // on platforms without `/proc`, which would report a dead cluster as RUNNING —
    // so this test pins the retained-handle behaviour (`try_wait`, which also reaps)
    // rather than the fallback probe.
    //
    // The child is `sleep 1` (not a long sleep) so that if an assertion fails the
    // process is gone within a second rather than lingering; no RAII guard is used
    // because the handle's owner is the map under test.
    #[tokio::test]
    async fn exited_but_unreaped_tracked_child_reports_dead() {
        let be = backend();

        let mut cmd = tokio::process::Command::new("sleep");
        cmd.arg("1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        let child = cmd.spawn().expect("spawn sleep 1");
        let pid = child.id().expect("child pid");
        be.children().insert(pid, child);

        assert!(
            be.pid_live(pid).expect("pid_live"),
            "a tracked, still-running child must report alive"
        );

        // `sleep 1` exits on its own. We never call wait() ourselves, so only
        // pid_live's `try_wait` can observe the exit and reap the zombie.
        let deadline = Instant::now() + Duration::from_secs(10);
        while be.pid_live(pid).expect("pid_live") && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(
            !be.pid_live(pid).expect("pid_live"),
            "an exited (unreaped) tracked child must report dead"
        );
        assert!(
            be.children().get(&pid).is_none(),
            "an exited child must be reaped and dropped from the handle map"
        );
    }

    // Review finding: a retained handle whose pid has left cluster state would never
    // be scanned again — leaking a map entry and stranding a zombie. `resize` does
    // exactly that (it drops executor pids from state), and a dead driver's
    // executors are in the same position. `reap_exited` sweeps the whole registry,
    // which is what must collect them.
    #[tokio::test]
    async fn reap_exited_collects_handles_whose_pids_left_cluster_state() {
        let be = backend();
        for _ in 0..3 {
            let mut cmd = tokio::process::Command::new("sleep");
            cmd.arg("0.3")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(false);
            let child = cmd.spawn().expect("spawn sleep 0.3");
            let pid = child.id().expect("pid");
            be.children().insert(pid, child);
        }
        assert_eq!(be.children().len(), 3, "three handles retained");

        // No pid is probed through any cluster state here, so only the registry
        // sweep can collect them.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut reaped = 0usize;
        while Instant::now() < deadline {
            reaped += be.reap_exited().expect("reap_exited");
            if reaped >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(reaped >= 3, "the sweep must reap every exited child, got {reaped}");
        assert!(be.children().is_empty(), "no handle may be retained after reaping");
    }

    // The pid-0 invariant keeps `terminate`/`kill` reachable for a cluster record
    // whose driver pid is 0, so it must hold through both paths.
    #[tokio::test]
    async fn pid_zero_is_never_live() {
        let be = backend();
        assert!(!be.pid_live(0).expect("pid_live"), "pid 0 must never be live via pid_live");
        assert!(!pid_alive(0), "pid 0 must never be live via pid_alive");
    }
}
