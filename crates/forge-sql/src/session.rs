//! Construction of DataFusion sessions configured for Forge.

use std::sync::Arc;

use datafusion::catalog::CatalogProviderList;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::Result as DFResult;
use deltalake::delta_datafusion::planner::DeltaPlanner;
use deltalake::delta_datafusion::DeltaTableFactory;
use forge_common::config::SessionSettings;

pub const DEFAULT_CATALOG: &str = "main";
pub const DEFAULT_SCHEMA: &str = "default";

/// Builder producing a [`SessionContext`] wired with Forge defaults: the
/// `DELTA` table factory, information schema, and settings translated from
/// [`SessionSettings`].
#[derive(Clone, Default)]
pub struct ForgeSessionBuilder {
    settings: SessionSettings,
    runtime: Option<Arc<RuntimeEnv>>,
    catalogs: Option<Arc<dyn CatalogProviderList>>,
}

impl std::fmt::Debug for ForgeSessionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForgeSessionBuilder")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl ForgeSessionBuilder {
    pub fn new(settings: SessionSettings) -> Self {
        Self {
            settings,
            runtime: None,
            catalogs: None,
        }
    }

    pub fn settings(&self) -> &SessionSettings {
        &self.settings
    }

    pub fn with_runtime(mut self, runtime: Arc<RuntimeEnv>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Share a catalog list (tables, schemas) across sessions.
    pub fn with_catalog_list(mut self, catalogs: Arc<dyn CatalogProviderList>) -> Self {
        self.catalogs = Some(catalogs);
        self
    }

    pub fn session_config(&self) -> SessionConfig {
        let s = &self.settings;
        let mut cfg = SessionConfig::new()
            .with_target_partitions(s.target_partitions.max(1))
            .with_batch_size(s.batch_size.max(1))
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA)
            .with_create_default_catalog_and_schema(true)
            .with_information_schema(true)
            .with_repartition_joins(true)
            .with_repartition_aggregations(true)
            .with_repartition_windows(true)
            .with_repartition_file_scans(true)
            .with_parquet_pruning(true);
        cfg.options_mut().execution.parquet.pushdown_filters = true;
        cfg.options_mut().execution.parquet.reorder_filters = true;
        cfg.options_mut().optimizer.enable_round_robin_repartition = true;
        cfg.options_mut().catalog.default_catalog = s.default_catalog.clone().unwrap_or_else(|| DEFAULT_CATALOG.into());
        cfg.options_mut().catalog.default_schema = s.default_schema.clone().unwrap_or_else(|| DEFAULT_SCHEMA.into());
        cfg
    }

    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        if let Some(rt) = &self.runtime {
            return rt.clone();
        }
        let mut b = RuntimeEnvBuilder::new();
        if let Some(limit) = self.settings.memory_limit_bytes {
            b = b.with_memory_limit(limit as usize, 0.9);
        }
        Arc::new(b.build().expect("runtime env"))
    }

    pub fn build_state(&self) -> SessionState {
        let mut config = self.session_config();
        if let Some(c) = &self.catalogs {
            if c.catalog(DEFAULT_CATALOG).is_some() {
                config = config.with_create_default_catalog_and_schema(false);
            }
        }
        let mut b = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(self.runtime_env())
            .with_default_features()
            .with_query_planner(DeltaPlanner::new())
            .with_table_factory("DELTA".into(), Arc::new(DeltaTableFactory {}));
        if let Some(c) = &self.catalogs {
            b = b.with_catalog_list(Arc::clone(c));
        }
        b.build()
    }

    pub fn build(&self) -> SessionContext {
        SessionContext::new_with_state(self.build_state())
    }
}

/// Extra helpers on a session context.
pub trait ForgeSessionExt {
    /// Make sure every file scan in `plan` can resolve its object store.
    fn ensure_plan_object_stores(&self, plan: &Arc<dyn ExecutionPlan>) -> DFResult<()>;
}

impl ForgeSessionExt for SessionContext {
    fn ensure_plan_object_stores(&self, plan: &Arc<dyn ExecutionPlan>) -> DFResult<()> {
        ensure_plan_object_stores(&self.runtime_env(), plan)
    }
}

/// Walk `plan`, registering an object store for each `DataSourceExec` whose
/// store is not yet known to `runtime`.
pub fn ensure_plan_object_stores(
    runtime: &RuntimeEnv,
    plan: &Arc<dyn ExecutionPlan>,
) -> DFResult<()> {
    if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some(cfg) = exec.data_source().as_any().downcast_ref::<FileScanConfig>() {
            let url: &url::Url = cfg.object_store_url.as_ref();
            crate::object_store::ensure_object_store(runtime, url)?;
        }
    }
    for child in plan.children() {
        ensure_plan_object_stores(runtime, child)?;
    }
    Ok(())
}
