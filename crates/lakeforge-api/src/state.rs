use std::sync::Arc;

use dashmap::DashMap;
use lakeforge_cluster_manager::ClusterBackend;

use crate::auth::Auth;
use crate::config::Config;
use crate::error::ApiResult;
use crate::forge::ForgeRegistry;
use crate::kernel::KernelManager;
use crate::storage::Storage;
use crate::store::Store;

pub struct AppState {
    pub config: Config,
    pub store: Store,
    pub storage: Storage,
    pub auth: Auth,
    pub backend: Arc<dyn ClusterBackend>,
    pub forge: ForgeRegistry,
    pub kernels: KernelManager,
    /// In-flight SQL statements (statement_id -> cancellation + job id).
    pub statements: DashMap<String, crate::api::sql::StatementHandle>,
    /// Interactive execution contexts (kernels) and their commands.
    pub contexts: crate::api::commands::Contexts,
    /// context_id -> (run_id, task_key) for kernels started by job tasks.
    pub jobs_task_context: DashMap<String, TaskScope>,
    /// Active job runs (run_id -> abort handle).
    pub runs: DashMap<i64, tokio::task::AbortHandle>,
    pub started_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct TaskScope {
    pub run_id: i64,
    pub task_key: String,
}

impl AppState {
    pub async fn new(config: Config) -> ApiResult<Arc<Self>> {
        let store = Store::connect(&config.database_url).await?;
        let storage = Storage::open(&config.storage_root)?;
        let secret = match &config.jwt_secret {
            Some(s) => s.clone(),
            None => match store.kv_get("jwt_secret").await? {
                Some(s) => s,
                None => {
                    let s = crate::auth::random_secret();
                    store.kv_set("jwt_secret", &s).await?;
                    s
                }
            },
        };
        let backend: Arc<dyn ClusterBackend> = Arc::from(lakeforge_cluster_manager::backend_from_env().await?);
        let kernels = KernelManager::new(config.python.clone(), config.public_url.clone());
        let state = Arc::new(Self {
            auth: Auth::new(secret.as_bytes()),
            store,
            storage,
            backend,
            forge: ForgeRegistry::default(),
            kernels,
            statements: DashMap::new(),
            contexts: Default::default(),
            jobs_task_context: DashMap::new(),
            runs: DashMap::new(),
            started_at: chrono::Utc::now(),
            config,
        });
        state.bootstrap_auth().await?;
        state.ensure_default_catalog().await?;
        Ok(state)
    }

    pub fn ws(&self) -> &str {
        self.config.workspace_id()
    }
}
