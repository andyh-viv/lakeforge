//! Per-client SQL sessions sharing one catalog and runtime.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::CatalogProviderList;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use forge_common::config::SessionSettings;
use forge_common::now_ms;
use forge_sql::ForgeSessionBuilder;
use parking_lot::Mutex;

pub struct Session {
    pub id: String,
    pub settings: Mutex<SessionSettings>,
    pub ctx: Mutex<SessionContext>,
    pub last_used_ms: Mutex<u64>,
}

/// Owns the shared catalog + runtime and vends per-session contexts. Session
/// contexts differ only by their settings; tables registered in any session
/// are visible to all of them.
pub struct SessionManager {
    catalogs: Arc<dyn CatalogProviderList>,
    runtime: Arc<RuntimeEnv>,
    defaults: SessionSettings,
    sessions: DashMap<String, Arc<Session>>,
}

impl SessionManager {
    pub fn new(defaults: SessionSettings) -> Self {
        let base = ForgeSessionBuilder::new(defaults.clone()).build();
        let catalogs = base.state().catalog_list().clone();
        let runtime = base.runtime_env();
        Self {
            catalogs,
            runtime,
            defaults,
            sessions: DashMap::new(),
        }
    }

    pub fn defaults(&self) -> &SessionSettings {
        &self.defaults
    }

    pub fn catalogs(&self) -> Arc<dyn CatalogProviderList> {
        Arc::clone(&self.catalogs)
    }

    pub fn runtime(&self) -> Arc<RuntimeEnv> {
        Arc::clone(&self.runtime)
    }

    fn build_ctx(&self, settings: &SessionSettings) -> SessionContext {
        ForgeSessionBuilder::new(settings.clone())
            .with_runtime(self.runtime())
            .with_catalog_list(self.catalogs())
            .build()
    }

    /// Fetch or create the session `id` (empty => anonymous session), applying
    /// `overrides` on top of its current settings.
    pub fn session(&self, id: &str, overrides: &HashMap<String, String>) -> Arc<Session> {
        let id = if id.is_empty() { "default".to_string() } else { id.to_string() };
        let s = self
            .sessions
            .entry(id.clone())
            .or_insert_with(|| {
                Arc::new(Session {
                    id: id.clone(),
                    settings: Mutex::new(self.defaults.clone()),
                    ctx: Mutex::new(self.build_ctx(&self.defaults)),
                    last_used_ms: Mutex::new(now_ms()),
                })
            })
            .clone();
        *s.last_used_ms.lock() = now_ms();
        if !overrides.is_empty() {
            let mut settings = s.settings.lock();
            for (k, v) in overrides {
                settings.apply(k, v);
            }
            *s.ctx.lock() = self.build_ctx(&settings);
        }
        s
    }

    /// Apply `SET key = value` to a session and rebuild its context.
    pub fn set(&self, session: &Session, key: &str, value: &str) {
        let mut settings = session.settings.lock();
        settings.apply(key, value);
        *session.ctx.lock() = self.build_ctx(&settings);
    }

    pub fn context(&self, session: &Session) -> SessionContext {
        session.ctx.lock().clone()
    }

    pub fn settings(&self, session: &Session) -> SessionSettings {
        session.settings.lock().clone()
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}
