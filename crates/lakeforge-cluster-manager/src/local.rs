//! Local-process backend: runs the Forge driver and executors as child
//! processes of the control plane using the `forge` binary.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::{BackendState, BackendStatus, ClusterBackend, ClusterError, ClusterHandle, LaunchSpec, Result};

#[derive(Debug, Clone)]
pub struct LocalProcessBackend {
    pub forge_bin: PathBuf,
    pub work_root: PathBuf,
    pub host: String,
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
        Self { forge_bin, work_root, host: "127.0.0.1".into() }
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
        Ok(child.id().unwrap_or_default())
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
        if !pid_alive(st.driver_pid) {
            return Ok(BackendStatus {
                state: BackendState::Terminated,
                message: Some("driver process exited".into()),
                ready_workers: 0,
            });
        }
        let ready = st.executor_pids.iter().filter(|p| pid_alive(**p)).count() as u32;
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
        for pid in st.executor_pids.iter().chain(std::iter::once(&st.driver_pid)) {
            kill(*pid);
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
fn pid_alive(_pid: u32) -> bool {
    true
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
    use super::pid_alive;
    use std::process::{Command, Stdio};

    // Regression test for LF-029: on macOS the old `/proc`-only `pid_alive`
    // always returned `false`, so a live `forge driver` was reported as
    // TERMINATED. This test does not touch `/proc`, so it fails on the old
    // implementation for every Unix target, including macOS.
    #[test]
    fn pid_alive_tracks_liveness_portably() {
        // (a) a live child process is alive.
        let mut child = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        assert!(pid != 0, "spawned child should have a pid");
        assert!(pid_alive(pid), "a live child process must report alive");

        // (b) once killed and reaped, the pid is dead.
        child.kill().expect("kill sleep");
        child.wait().expect("reap sleep");

        // `kill -0` can transiently succeed until the pid is fully reaped, so
        // poll with a bounded budget rather than assuming instant death.
        let mut dead = false;
        for _ in 0..50 {
            if !pid_alive(pid) {
                dead = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(dead, "a reaped child process must report dead");

        // (c) pid 0 is never alive.
        assert!(!pid_alive(0), "pid 0 must never report alive");
    }
}
