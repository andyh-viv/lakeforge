//! Python notebook kernels: one subprocess per execution context, speaking a
//! JSON-lines protocol over stdin/stdout. The kernel source is embedded in the
//! binary (`python/lakeforge_kernel.py`) and materialised on first use.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, Mutex};

use crate::error::{ApiError, ApiResult};

const KERNEL_SOURCE: &str = include_str!("../python/lakeforge_kernel.py");

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KernelEvent {
    Stdout { text: String },
    Stderr { text: String },
    /// A rich output: `{"mime": "text/plain" | "text/html" | "application/json" | "image/png", "data": ...}`.
    Display { mime: String, data: Value },
    /// Tabular display of a DataFrame / SQL result.
    Table { columns: Vec<String>, rows: Vec<Vec<Value>>, truncated: bool },
    Result { text: String },
    Error { ename: String, evalue: String, traceback: Vec<String> },
    Exit { value: Option<String> },
    Done { status: String },
}

struct Kernel {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    pending: Arc<DashMap<String, mpsc::UnboundedSender<KernelEvent>>>,
}

pub struct KernelManager {
    python: String,
    public_url: String,
    kernels: DashMap<String, Arc<Kernel>>,
    script_path: std::path::PathBuf,
}

#[derive(Debug, Serialize)]
struct KernelRequest<'a> {
    id: &'a str,
    op: &'a str,
    code: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<&'a str>,
}

impl KernelManager {
    pub fn new(python: String, public_url: String) -> Self {
        let dir = std::path::PathBuf::from(".lakeforge/kernel");
        let _ = std::fs::create_dir_all(&dir);
        let script_path = dir.join("lakeforge_kernel.py");
        if std::fs::read_to_string(&script_path).map(|s| s != KERNEL_SOURCE).unwrap_or(true) {
            let _ = std::fs::write(&script_path, KERNEL_SOURCE);
        }
        Self { python, public_url, kernels: DashMap::new(), script_path }
    }

    pub fn has(&self, context_id: &str) -> bool {
        self.kernels.contains_key(context_id)
    }

    pub async fn start(&self, context_id: &str, env: HashMap<String, String>) -> ApiResult<()> {
        if self.kernels.contains_key(context_id) {
            return Ok(());
        }
        let mut cmd = Command::new(&self.python);
        cmd.arg("-u")
            .arg(&self.script_path)
            .env("LAKEFORGE_URL", &self.public_url)
            .env("LAKEFORGE_CONTEXT_ID", context_id)
            .env("PYTHONUNBUFFERED", "1")
            .envs(env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| ApiError::Unavailable(format!("cannot start python kernel ({}): {e}", self.python)))?;
        let stdin = child.stdin.take().ok_or_else(|| ApiError::internal("kernel stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| ApiError::internal("kernel stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| ApiError::internal("kernel stderr"))?;
        let pending: Arc<DashMap<String, mpsc::UnboundedSender<KernelEvent>>> = Arc::new(DashMap::new());

        let p = Arc::clone(&pending);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(v) = serde_json::from_str::<Value>(&line) else {
                    tracing::debug!(%line, "kernel noise");
                    continue;
                };
                let id = v.get("id").and_then(|x| x.as_str()).unwrap_or_default().to_string();
                let Ok(ev) = serde_json::from_value::<KernelEvent>(v.clone()) else {
                    tracing::warn!(%line, "unparseable kernel event");
                    continue;
                };
                let done = matches!(ev, KernelEvent::Done { .. });
                if let Some(tx) = p.get(&id) {
                    let _ = tx.send(ev);
                }
                if done {
                    p.remove(&id);
                }
            }
        });
        let ctx = context_id.to_string();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::debug!(context = %ctx, "kernel stderr: {line}");
            }
        });

        self.kernels.insert(context_id.to_string(), Arc::new(Kernel { child: Mutex::new(child), stdin: Mutex::new(stdin), pending }));
        Ok(())
    }

    /// Submit code; returns a receiver of events ending with `Done`.
    pub async fn execute(&self, context_id: &str, command_id: &str, language: &str, code: &str) -> ApiResult<mpsc::UnboundedReceiver<KernelEvent>> {
        let k = self.kernels.get(context_id).map(|k| Arc::clone(&k)).ok_or_else(|| ApiError::not_found("Context", context_id))?;
        let (tx, rx) = mpsc::unbounded_channel();
        k.pending.insert(command_id.to_string(), tx);
        let req = serde_json::to_string(&KernelRequest { id: command_id, op: "execute", code, language: Some(language) })?;
        let mut stdin = k.stdin.lock().await;
        stdin.write_all(req.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(rx)
    }

    pub async fn interrupt(&self, context_id: &str, command_id: &str) -> ApiResult<()> {
        let k = self.kernels.get(context_id).map(|k| Arc::clone(&k)).ok_or_else(|| ApiError::not_found("Context", context_id))?;
        let req = serde_json::to_string(&KernelRequest { id: command_id, op: "interrupt", code: "", language: None })?;
        let mut stdin = k.stdin.lock().await;
        stdin.write_all(req.as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        #[cfg(unix)]
        {
            let child = k.child.lock().await;
            if let Some(pid) = child.id() {
                let _ = std::process::Command::new("kill").arg("-INT").arg(pid.to_string()).status();
            }
        }
        Ok(())
    }

    pub async fn stop(&self, context_id: &str) {
        if let Some((_, k)) = self.kernels.remove(context_id) {
            let mut child = k.child.lock().await;
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        }
    }

    pub fn list(&self) -> Vec<String> {
        self.kernels.iter().map(|e| e.key().clone()).collect()
    }
}
