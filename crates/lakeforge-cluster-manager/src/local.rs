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
        let pid = child.id().unwrap_or_default();
        if pid != 0 {
            // Retain the handle so liveness can be answered with `try_wait`
            // (which also reaps the child) rather than a zombie-tolerant probe.
            self.children.lock().unwrap().insert(pid, child);
        }
        Ok(pid)
    }

    /// Whether a process this backend spawned is still running.
    ///
    /// Children we hold are probed with [`tokio::process::Child::try_wait`], which
    /// *reaps* the child when it has exited: an exited — even not-yet-reaped —
    /// driver therefore reads as dead, and no zombie is left behind. This is the
    /// platform-independent answer, and it is the one `status()` uses for the pids
    /// we spawned.
    ///
    /// A pid we do not hold (e.g. a work-dir/state record that outlived its
    /// handle) falls back to [`pid_alive`], which is best-effort: on platforms
    /// without `/proc` a probe cannot distinguish a zombie from a live process.
    /// A pid of `0` is never live, so `terminate`/`kill` stay reachable.
    fn pid_live(&self, pid: u32) -> bool {
        if pid == 0 {
            return false;
        }
        let mut children = self.children.lock().unwrap();
        let mut exited = false;
        if let Some(child) = children.get_mut(&pid) {
            match child.try_wait() {
                Ok(None) => return true,      // still running
                Ok(Some(_)) => exited = true, // exited; try_wait has now reaped it
                Err(_) => {}                  // unknown: fall back to the probe
            }
        }
        if exited {
            children.remove(&pid);
            return false;
        }
        drop(children);
        pid_alive(pid)
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
        let st: LocalState = serde_json::from_value(handle.state.clone()).unwrap_or_default();
        if !self.pid_live(st.driver_pid) {
            return Ok(BackendStatus {
                state: BackendState::Terminated,
                message: Some("driver process exited".into()),
                ready_workers: 0,
            });
        }
        let ready = st.executor_pids.iter().filter(|p| self.pid_live(**p)).count() as u32;
        Ok(BackendStatus { state: BackendState::Running, message: None, ready_workers: ready })
    }

    async fn resize(&self, spec: &LaunchSpec, handle: &ClusterHandle) -> Result<ClusterHandle> {
        let mut st: LocalState = serde_json::from_value(handle.state.clone()).unwrap_or_default();
        let target = spec.num_workers as usize;
        let work_dir = PathBuf::from(&st.work_dir);
        while st.executor_pids.len() > target {
            if let Some(pid) = st.executor_pids.pop() {
                kill(pid);
                st.executor_ports.pop();
            }
        }
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
        // Give the signalled processes a moment to exit, then reap the handles we
        // hold so a terminated cluster leaves no zombies behind. Bounded: we never
        // block termination on an unresponsive process.
        for _ in 0..20 {
            let mut alive = false;
            let mut done: Vec<u32> = Vec::new();
            {
                let mut children = self.children.lock().unwrap();
                for pid in &pids {
                    if let Some(child) = children.get_mut(pid) {
                        match child.try_wait() {
                            Ok(Some(_)) => done.push(*pid),
                            _ => alive = true,
                        }
                    }
                }
                for pid in done {
                    children.remove(&pid);
                }
            }
            if !alive {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
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
    // so this test pins the tracked-child behaviour (`try_wait`, which also reaps)
    // rather than the fallback probe.
    #[tokio::test]
    async fn exited_but_unreaped_tracked_child_reports_dead() {
        let be = backend();

        let mut cmd = tokio::process::Command::new("sleep");
        cmd.arg("0.2")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        let child = cmd.spawn().expect("spawn sleep 0.2");
        let pid = child.id().expect("child pid");
        be.children.lock().unwrap().insert(pid, child);

        assert!(be.pid_live(pid), "a tracked, still-running child must report alive");

        // `sleep 0.2` exits on its own. We never call wait() ourselves, so only
        // pid_live's `try_wait` can observe the exit and reap the zombie.
        let deadline = Instant::now() + Duration::from_secs(10);
        while be.pid_live(pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(!be.pid_live(pid), "an exited (unreaped) tracked child must report dead");
        assert!(
            be.children.lock().unwrap().get(&pid).is_none(),
            "an exited child must be reaped and dropped from the handle map"
        );
    }

    // The pid-0 invariant keeps `terminate`/`kill` reachable for a cluster record
    // whose driver pid is 0, so it must hold through both paths.
    #[tokio::test]
    async fn pid_zero_is_never_live() {
        let be = backend();
        assert!(!be.pid_live(0), "pid 0 must never be live via pid_live");
        assert!(!pid_alive(0), "pid 0 must never be live via pid_alive");
    }
}
