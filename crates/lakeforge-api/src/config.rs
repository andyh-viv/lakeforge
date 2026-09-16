use std::net::SocketAddr;

use clap::Parser;

/// Lakeforge control-plane configuration (CLI flags or `LAKEFORGE_*` env).
#[derive(Debug, Clone, Parser)]
#[command(name = "lakeforge-api", version, about = "Lakeforge control plane")]
pub struct Config {
    #[arg(long, env = "LAKEFORGE_BIND", default_value = "0.0.0.0:8080")]
    pub bind: SocketAddr,

    /// `sqlite://path/to/db.sqlite?mode=rwc` or `postgres://user:pw@host/db`.
    #[arg(long, env = "LAKEFORGE_DATABASE_URL", default_value = "sqlite://.lakeforge/lakeforge.db?mode=rwc")]
    pub database_url: String,

    /// Root of the workspace object storage (DBFS root, notebooks, MLflow artifacts).
    /// Local path, `file:///...`, `s3://bucket/prefix`, `gs://bucket/prefix`, `az://container/prefix`.
    #[arg(long, env = "LAKEFORGE_STORAGE_ROOT", default_value = ".lakeforge/storage")]
    pub storage_root: String,

    #[arg(long, env = "LAKEFORGE_JWT_SECRET")]
    pub jwt_secret: Option<String>,

    /// Initial admin user (created if no users exist).
    #[arg(long, env = "LAKEFORGE_ADMIN_USER", default_value = "admin@lakeforge.local")]
    pub admin_user: String,
    #[arg(long, env = "LAKEFORGE_ADMIN_PASSWORD", default_value = "admin")]
    pub admin_password: String,

    /// Public URL of this deployment (used in links).
    #[arg(long, env = "LAKEFORGE_PUBLIC_URL", default_value = "http://localhost:8080")]
    pub public_url: String,

    /// Directory with the built web UI (served at `/`). Optional.
    #[arg(long, env = "LAKEFORGE_UI_DIR")]
    pub ui_dir: Option<String>,

    /// Cloud the deployment runs on: `local` | `aws` | `gcp` | `azure`.
    #[arg(long, env = "LAKEFORGE_CLOUD", default_value = "local")]
    pub cloud: String,

    /// Python interpreter used for notebook kernels.
    #[arg(long, env = "LAKEFORGE_PYTHON", default_value = "python3")]
    pub python: String,

    /// Local scratch directory (git checkouts for Repos, kernel temp files).
    #[arg(long, env = "LAKEFORGE_WORK_DIR", default_value = ".lakeforge/work")]
    pub work_dir: String,

    /// Interval for the job scheduler / cluster monitor loops.
    #[arg(long, env = "LAKEFORGE_TICK_SECS", default_value_t = 5)]
    pub tick_secs: u64,

    /// PostgreSQL server backing Lakebase database instances
    /// (`postgres://user:pw@host:5432/`). When unset, Lakebase instances are
    /// metadata-only emulations with no reachable Postgres endpoint.
    #[arg(long, env = "LAKEFORGE_LAKEBASE_POSTGRES_URL")]
    pub lakebase_postgres_url: Option<String>,
}

impl Config {
    pub fn workspace_id(&self) -> &'static str {
        "default"
    }
}
