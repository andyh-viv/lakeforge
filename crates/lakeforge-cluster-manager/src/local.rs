//! Local-process backend: runs the Forge driver and executors as child
//! processes of the control plane using the `forge` binary.

use std::collections::{HashMap, HashSet};
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
    /// Keeping the handles (rather than letting them drop) makes liveness
    /// deterministic: `Child::try_wait` reports *and reaps* an exited child, so a
    /// process that has exited but not yet been reaped (a zombie) is never
    /// mistaken for a live one — the mistake that a bare `kill -0` probe makes on
    /// platforms without `/proc`.
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
        Self {
            forge_bin,
            work_root,
            host: "127.0.0.1".into(),
            children: Arc::new(Mutex::new(HashMap::new())),
        }
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

    /// Poll interval while waiting for a signalled child to exit, and the number
    /// of polls in the polite (SIGTERM) and escalated (SIGKILL) phases. Both
    /// phases are ~1s, so cleanup is bounded and cannot block termination.
    const REAP_POLL_MS: u64 = 50;
    const REAP_GRACE_POLLS: usize = 20;
    const REAP_FORCE_POLLS: usize = 20;

    /// The child registry, tolerating a poisoned mutex.
    ///
    /// A panic while the lock was held poisons it. The registry is a plain map of
    /// handles with no cross-entry invariant, so recovering the guard is strictly
    /// better than panicking the caller — a panic here would take down the control
    /// plane's cluster monitor loop.
    fn children(&self) -> std::sync::MutexGuard<'_, HashMap<u32, tokio::process::Child>> {
        self.children.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reap every retained child that has already exited and that no cluster's
    /// state refers to any more, dropping its handle.
    ///
    /// A retained handle for an exited child is a zombie nothing else will reap,
    /// and it keeps the map growing. Sweeping the *whole* registry is what collects
    /// children whose pids have already left state — executors removed by
    /// [`Self::resize`], and a dead driver's executors.
    ///
    /// `referenced` is the COMPLETE set of pids referenced by every cluster's
    /// state, collected by the control plane once per reconcile tick and passed
    /// through [`ClusterBackend::reap_orphans`]. Pids in `referenced` are
    /// deliberately NOT collected: those belong to a cluster that still holds a
    /// handle for them, and that cluster will consume the exit itself through
    /// [`Self::pid_live`]. Collecting them here would discard an authoritative
    /// answer and hand the pid back to the best-effort probe. The set is copied
    /// into a `HashSet` so membership is O(1) rather than a linear scan per child.
    /// This is why the guarantee does not depend on any retention window: a
    /// referenced pid is never evicted, however many children exited at once.
    ///
    /// A `try_wait` failure is isolated to that child: it is logged with its pid,
    /// the handle is kept, and the sweep continues. It is deliberately NOT
    /// returned as an error, because the sweep runs once per reconcile tick and one
    /// unqueryable child must not stop reconciliation for every other cluster; a
    /// cluster that actually owns such a child still receives the error from
    /// [`Self::pid_live`]. Returns the number of children reaped.
    fn sweep_exited(&self, referenced: &[u32]) -> usize {
        let referenced: HashSet<u32> = referenced.iter().copied().collect();
        let mut reaped = 0usize;
        let mut done: Vec<u32> = Vec::new();
        {
            let mut children = self.children();
            for (pid, child) in children.iter_mut() {
                if referenced.contains(pid) {
                    continue;
                }
                match child.try_wait() {
                    Ok(Some(_)) => {
                        done.push(*pid);
                        reaped += 1;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(pid = *pid, error = %e, "child liveness query failed during sweep");
                    }
                }
            }
            for pid in &done {
                children.remove(pid);
            }
        }
        reaped
    }

    /// Poll `pids` once: reap the ones that have exited, return the ones still
    /// running or unqueryable.
    ///
    /// For a pid this backend does NOT hold a handle for (a cluster recorded
    /// before a control-plane restart has state but no child), it is NOT assumed
    /// gone: the pid is probed with [`pid_alive`], and a pid that still reads
    /// alive is returned as still-running so the caller can escalate. A pid that
    /// reads dead is silently dropped from the set — it is already gone.
    fn reap_round(&self, pids: &[u32]) -> Vec<u32> {
        let mut still: Vec<u32> = Vec::new();
        let mut done: Vec<u32> = Vec::new();
        {
            let mut children = self.children();
            for pid in pids {
                match children.get_mut(pid) {
                    Some(child) => match child.try_wait() {
                        Ok(Some(_)) => done.push(*pid),
                        Ok(None) => still.push(*pid),
                        Err(e) => {
                            tracing::warn!(pid = *pid, error = %e, "liveness query failed while reaping");
                            still.push(*pid);
                        }
                    },
                    // No retained handle: probe it rather than assume it is gone.
                    None => {
                        if pid_alive(*pid) {
                            still.push(*pid);
                        }
                    }
                }
            }
            for pid in &done {
                children.remove(pid);
            }
        }
        still
    }

    /// Send an uncatchable signal to retained children, and to pids this backend
    /// does not hold a handle for.
    fn force_kill(&self, pids: &[u32]) {
        let mut children = self.children();
        for pid in pids {
            match children.get_mut(pid) {
                Some(child) => {
                    // `start_kill` is SIGKILL on Unix: a child that ignores SIGTERM
                    // cannot survive it, so cleanup cannot silently leave a live
                    // process behind after the cluster handle is cleared.
                    if let Err(e) = child.start_kill() {
                        tracing::warn!(pid = *pid, error = %e, "force-kill failed");
                    }
                }
                None => {
                    // No retained handle: escalate with a direct SIGKILL, the only
                    // uncatchable signal we can send to a pid we do not own.
                    if let Err(e) = force_kill_untracked(*pid) {
                        tracing::warn!(pid = *pid, error = %e, "force-kill (untracked) failed");
                    }
                }
            }
        }
    }

    /// Give pids that are leaving cluster state a bounded chance to exit, escalate
    /// to an uncatchable signal, then reap and forget their handles.
    ///
    /// Covers both tracked children and untracked pids (a cluster recorded before
    /// a control-plane restart has state but no handle): a tracked child is reaped
    /// through its handle, an untracked pid is verified through the probe and
    /// force-killed with a direct SIGKILL, and either way an unconfirmed survivor
    /// is an error, never a success.
    ///
    /// Bounded on purpose: an unresponsive process must not block `resize` or
    /// `terminate`. If a child is still running after the polite grace period it
    /// is force-killed, and if it is *still* there afterwards this returns an
    /// error rather than reporting success — the caller can then keep the state
    /// and retry, and the handle is retained for a later sweep.
    async fn reap_pids(&self, pids: &[u32]) -> Result<()> {
        let mut still = self.reap_round(pids);
        for _ in 0..Self::REAP_GRACE_POLLS {
            if still.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(Self::REAP_POLL_MS)).await;
            still = self.reap_round(pids);
        }
        if still.is_empty() {
            return Ok(());
        }
        self.force_kill(&still);
        for _ in 0..Self::REAP_FORCE_POLLS {
            tokio::time::sleep(std::time::Duration::from_millis(Self::REAP_POLL_MS)).await;
            still = self.reap_round(pids);
            if still.is_empty() {
                return Ok(());
            }
        }
        Err(ClusterError::Backend(format!(
            "child processes still running after SIGKILL; handles retained for a later sweep: {still:?}"
        )))
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
        // No handle: this is either a pid we never spawned (the probe is the right
        // answer) or one whose handle was already collected because no cluster state
        // referenced it. `sweep_exited` never collects a referenced pid, so a cluster
        // that still refers to this pid has already consumed its exit above.
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
        let st: LocalState = serde_json::from_value(handle.state.clone()).unwrap_or_default();
        // Query this cluster's own pids. Each query is authoritative and
        // consumes the exit. Executors are queried even when the driver is
        // already dead, so their exits are reaped too.
        //
        // NOTE: `status()` deliberately does NOT sweep the registry here. A
        // sweep needs the COMPLETE set of state-referenced pids, and a single
        // cluster cannot see the other clusters' state; sweeping with only this
        // cluster's pids would let it collect another cluster's referenced exit.
        // The control plane sweeps instead, via [`ClusterBackend::reap_orphans`]
        // with the full reference set.
        let driver_alive = self.pid_live(st.driver_pid)?;
        let mut ready = 0u32;
        for p in &st.executor_pids {
            if self.pid_live(*p)? {
                ready += 1;
            }
        }
        if !driver_alive {
            return Ok(BackendStatus {
                state: BackendState::Terminated,
                message: Some("driver process exited".into()),
                ready_workers: 0,
            });
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
        // scale-down. A child that survives even the escalated kill returns an error
        // rather than reporting success.
        self.reap_pids(&removed).await?;
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
        // Reap this cluster's processes (escalating to an uncatchable signal if they
        // ignore the polite one). This covers untracked pids too: a cluster recorded
        // before a control-plane restart has state but no child handle, and such a
        // pid is verified through the probe, force-killed with a direct SIGKILL if
        // still alive, and reported as an error if it still cannot be confirmed
        // gone — never a false success that would clear the last handle to a live
        // process.
        //
        // NOTE: `terminate()` reaps only this cluster's own pids. It deliberately
        // does NOT sweep the registry with an empty reference set — doing so would
        // collect another cluster's still-referenced child. Orphan collection is the
        // control plane's job, via [`ClusterBackend::reap_orphans`] with the full
        // reference set.
        self.reap_pids(&pids).await?;
        Ok(())
    }

    async fn reap_orphans(&self, referenced: &[u32]) -> Result<usize> {
        Ok(self.sweep_exited(referenced))
    }

    fn referenced_pids(&self, handle: &ClusterHandle) -> Vec<u32> {
        let st: LocalState = serde_json::from_value(handle.state.clone()).unwrap_or_default();
        let mut pids = st.executor_pids;
        pids.push(st.driver_pid);
        pids
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

/// Send an uncatchable SIGKILL to a pid this backend does NOT hold a handle for
/// (a cluster recorded before a control-plane restart has state but no child).
/// `Child::start_kill` is unavailable for such a pid, so this shells out to
/// `kill -KILL`, exactly as the SIGTERM path shells out to `kill -TERM`.
#[cfg(unix)]
fn force_kill_untracked(pid: u32) -> std::io::Result<()> {
    if pid == 0 {
        return Ok(());
    }
    let status = std::process::Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!("kill -KILL {pid} exited with {status}")))
    }
}

#[cfg(not(unix))]
fn force_kill_untracked(_pid: u32) -> std::io::Result<()> {
    // No signal to send on this target: the pid stays unconfirmed, so the
    // caller reports an error rather than a false success.
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::{pid_alive, LocalProcessBackend};
    use crate::{ClusterBackend, ClusterHandle};
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
    // so this pins the retained-handle behaviour (`try_wait`, which also reaps).
    //
    // The child is long-lived (`sleep 30`) and is stopped through its retained
    // handle, so the opening "alive" assertion cannot race with a self-exit; if an
    // assertion fails the process is gone within 30s rather than never.
    #[tokio::test]
    async fn exited_but_unreaped_tracked_child_reports_dead() {
        let be = backend();

        let mut cmd = tokio::process::Command::new("sleep");
        cmd.arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        let child = cmd.spawn().expect("spawn sleep 30");
        let pid = child.id().expect("child pid");
        be.children().insert(pid, child);

        assert!(
            be.pid_live(pid).expect("pid_live"),
            "a tracked, still-running child must report alive"
        );

        // Stop it through the handle and then leave it UNREAPED: only pid_live's
        // `try_wait` can observe the exit, and it must reap it too.
        {
            let mut children = be.children();
            let held = children.get_mut(&pid).expect("retained handle");
            held.start_kill().expect("stop the child");
        }

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
    // executors are in the same position. The registry sweep (driven by
    // `reap_orphans`) is what collects them.
    //
    // This test covers the sweep mechanism itself; the control-plane wiring around
    // it (collecting the complete reference set) is covered by
    // `reap_orphans_protects_another_clusters_referenced_exit`.
    #[tokio::test]
    async fn sweep_exited_collects_handles_whose_pids_left_cluster_state() {
        let be = backend();
        let mut pids = Vec::new();
        for _ in 0..3 {
            let mut cmd = tokio::process::Command::new("sleep");
            cmd.arg("0.3")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(false);
            let child = cmd.spawn().expect("spawn sleep 0.3");
            let pid = child.id().expect("pid");
            pids.push(pid);
            be.children().insert(pid, child);
        }
        assert_eq!(be.children().len(), 3, "three handles retained");

        // No pid is probed through any cluster state here, so only the registry
        // sweep can collect them.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut reaped = 0usize;
        while Instant::now() < deadline {
            reaped += be.sweep_exited(&[]);
            if reaped >= 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(reaped >= 3, "the sweep must reap every exited child, got {reaped}");
        assert!(be.children().is_empty(), "no handle may be retained after reaping");

        // And the reaped pids must still read DEAD for their owner: collecting the
        // handle must not downgrade the answer to the best-effort probe.
        for pid in &pids {
            assert!(
                !be.pid_live(*pid).expect("pid_live"),
                "a pid reaped by the sweep must still read dead"
            );
        }
    }

    // Round-5 finding (blocking): the sweep must protect the COMPLETE set of
    // state-referenced pids, not just the caller's own. If cluster A's sweep
    // collects cluster B's still-referenced exit, B loses its authoritative
    // answer and falls through to the best-effort probe (which cannot tell a
    // zombie from a live process, and on non-Unix assumes alive).
    //
    // This pins the fix: the control plane passes every cluster's pids to
    // `reap_orphans`, so A's sweep cannot consume B's referenced exit, and B
    // still learns of it authoritatively through `pid_live`.
    #[tokio::test]
    async fn reap_orphans_protects_another_clusters_referenced_exit() {
        let be = backend();

        // Cluster A's driver is long-lived and still referenced; cluster B's
        // driver exits on its own while B still references it.
        let driver_a = spawn_tracked(&be, "30").await;
        let driver_b = spawn_tracked(&be, "0.1").await;

        // The control plane builds the COMPLETE reference set from every
        // cluster's state: A's driver and B's driver.
        let complete = vec![driver_a, driver_b];

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut reaped_by_a = 0usize;
        let mut b_learned_exit = false;
        while Instant::now() < deadline {
            // A's `reap_orphans` runs with the complete set. It must never
            // collect B's (or A's) still-referenced pid.
            reaped_by_a += be.reap_orphans(&complete).await.expect("reap_orphans");
            assert!(
                be.children().get(&driver_b).is_some(),
                "B's referenced driver must never be collected by A's sweep"
            );
            assert!(
                be.children().get(&driver_a).is_some(),
                "A's own driver must never be collected by its sweep"
            );
            // B consumes its own exit authoritatively.
            if !be.pid_live(driver_b).expect("pid_live") {
                b_learned_exit = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(b_learned_exit, "B must authoritatively learn its driver exited");
        assert_eq!(
            reaped_by_a, 0,
            "the complete reference set protects every referenced pid from A's sweep"
        );
        assert!(
            be.children().get(&driver_b).is_none(),
            "B's own liveness query is what reaped and forgot its handle"
        );
        // Clean up A's still-live driver.
        be.reap_pids(&[driver_a]).await.expect("clean up A's driver");
    }

    // The recorded exit must take precedence over the platform probe. This is what
    // keeps a pid we watched exit from being handed back to a probe that cannot
    // tell a zombie from a live process (and that on non-Unix assumes alive).
    //
    // Review finding: the guarantee must not depend on capacity. A sweep must never
    // collect a pid that a cluster state still refers to, however many children
    // exited at once — the owner consumes that exit itself.
    async fn spawn_tracked(be: &LocalProcessBackend, secs: &str) -> u32 {
        let mut cmd = tokio::process::Command::new("sleep");
        cmd.arg(secs)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        let child = cmd.spawn().expect("spawn tracked child");
        let pid = child.id().expect("pid");
        be.children().insert(pid, child);
        pid
    }

    #[tokio::test]
    async fn sweep_exited_retains_pids_its_cluster_still_references() {
        let be = backend();
        let pid = spawn_tracked(&be, "0.3").await;

        // The child exits on its own while this cluster still refers to its pid.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut collected_referenced = 0usize;
        while Instant::now() < deadline {
            collected_referenced += be.sweep_exited(&[pid]);
            assert!(
                be.children().get(&pid).is_some(),
                "a referenced pid must never be collected by a sweep"
            );
            if !be.pid_live(pid).expect("pid_live") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            collected_referenced, 0,
            "the sweep must not collect a pid its cluster still references"
        );
        assert!(
            !be.pid_live(pid).expect("pid_live"),
            "the owning cluster must still learn the child exited"
        );
        assert!(
            be.children().get(&pid).is_none(),
            "the owner's own query is what reaps and forgets it"
        );
    }

    // Review finding: with a capacity-bounded retention window, a pid's
    // authoritative exit could be evicted before its owner read it. This pins that
    // no such window exists: every unreferenced exited child is collected in one
    // pass, and a referenced one is held, however many exit at once.
    #[tokio::test]
    async fn sweep_exited_collects_many_unreferenced_handles_in_one_pass() {
        let be = backend();
        const N: usize = 300;

        for _ in 0..N {
            spawn_tracked(&be, "0.05").await;
        }
        assert_eq!(be.children().len(), N, "all children are registered");

        let deadline = Instant::now() + Duration::from_secs(60);
        let mut reaped = 0usize;
        while reaped < N && Instant::now() < deadline {
            reaped += be.sweep_exited(&[]);
            if reaped < N {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        assert_eq!(reaped, N, "every unreferenced exited child must be collected, got {reaped} of {N}");
        assert!(be.children().is_empty(), "no handle may be retained after the sweep");
    }

    // Review finding: a child that ignores the polite signal must not be left
    // running (and unreaped) once its cluster handle is cleared — a SIGTERM-ignoring
    // driver would otherwise leak, because the API clears the handle as soon as
    // `terminate` reports success. The reaper must escalate to an uncatchable signal.
    #[tokio::test]
    async fn reap_pids_force_kills_a_child_that_ignores_the_polite_signal() {
        let be = backend();

        // The shell must PROVE it has installed the ignore-trap before we signal it;
        // otherwise the TERM can land first and the test passes without ever
        // exercising the escalation (it did, until this handshake was added).
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let ready = std::env::temp_dir().join(format!("lf-reap-ready-{stamp}"));

        // A shell that ignores SIGTERM and keeps itself alive. SIGKILL still ends
        // it, so this can be cleaned up; the in-flight `sleep 1` it leaves behind
        // exits on its own within a second.
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(format!(
                "trap '' TERM; : > {}; while true; do sleep 1; done",
                ready.display()
            ))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        let child = cmd.spawn().expect("spawn term-ignoring shell");
        let pid = child.id().expect("pid");
        be.children().insert(pid, child);
        assert!(be.pid_live(pid).expect("pid_live"), "the child starts alive");

        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready.exists(), "the child must report that its trap is installed");

        // The polite signal, exactly as `terminate()` sends it first.
        super::kill(pid);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            be.pid_live(pid).expect("pid_live"),
            "the child must have survived the polite signal, or this proves nothing"
        );

        // It ignored that, so the reaper must escalate rather than report success.
        be.reap_pids(&[pid])
            .await
            .expect("reap_pids must escalate to an uncatchable signal, not give up");
        assert!(
            be.children().get(&pid).is_none(),
            "the straggler must be reaped, not retained"
        );
        assert!(!be.pid_live(pid).expect("pid_live"), "the straggler must be dead");
        let _ = std::fs::remove_file(&ready);
    }

    // Final-round finding (blocking): after a control-plane restart the `children`
    // registry is empty, so a cluster recorded before the restart has state but no
    // handle. `terminate()` used to send one SIGTERM and then reap only the pids it
    // held a handle for — an untracked pid was silently skipped, so a
    // SIGTERM-ignoring process read as "gone" and cleanup reported success while the
    // process was still alive, clearing the last persisted handle.
    //
    // This pins the fix: an untracked-but-alive pid is probed, escalated to SIGKILL,
    // and — if it still cannot be confirmed gone — reported as an error, never as
    // success. The child is spawned with plain `std::process::Command` so it is NOT
    // in `children`, and it ignores SIGTERM so the polite signal alone must not
    // count as cleanup. On Linux the SIGKILLed (dead) child reads dead and
    // `terminate` succeeds; on non-Linux the best-effort probe cannot distinguish a
    // zombie, so `terminate` must conservatively error. Either outcome is allowed —
    // "Ok while still alive" is not.
    #[tokio::test]
    async fn terminate_never_reports_success_for_an_untracked_alive_pid() {
        let be = backend();

        // A SIGTERM-ignoring shell, spawned with std::process::Command (NOT tracked).
        // The readiness stamp proves the ignore-trap is installed before we signal.
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let ready = std::env::temp_dir().join(format!("lf-untracked-ready-{stamp}"));
        let child = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "trap '' TERM; : > {}; while true; do sleep 1; done",
                ready.display()
            ))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn untracked SIGTERM-ignoring shell");
        let pid = child.id();
        assert!(pid != 0, "the spawned shell must have a pid");
        let mut guard = Guard(child);

        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "the shell must report that its trap is installed");

        // A cluster record as it would look after a control-plane restart: state
        // points at a pid the backend holds no handle for.
        let handle = ClusterHandle {
            backend: "local".into(),
            driver_addr: "http://127.0.0.1:1".into(),
            state: serde_json::json!({
                "driver_pid": pid,
                "driver_port": 1,
                "executor_pids": [],
                "executor_ports": [],
                "work_dir": "/tmp"
            }),
        };

        let result = be.terminate(&handle).await;
        match result {
            Ok(()) => {
                let gone = guard.0.try_wait().expect("try_wait");
                assert!(
                    gone.is_some(),
                    "terminate reported success while the untracked child is still alive"
                );
            }
            Err(_) => {
                // Conservative: the process could not be confirmed gone. Acceptable —
                // the guard reaps it on drop.
            }
        }

        let _ = std::fs::remove_file(&ready);
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
