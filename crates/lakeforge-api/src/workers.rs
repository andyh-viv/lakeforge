//! Background loops: cluster health/auto-termination, the job scheduler tick
//! and cleanup of interactive contexts whose cluster has gone away.

use std::sync::Arc;
use std::time::Duration;

use crate::api::clusters::ClusterState;
use crate::state::AppState;

pub fn spawn_all(state: Arc<AppState>) {
    let period = Duration::from_secs(state.config.tick_secs.max(1));

    let st = Arc::clone(&state);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            if let Err(e) = st.monitor_clusters().await {
                tracing::warn!(error = %e, "cluster monitor tick failed");
            }
        }
    });

    let st = Arc::clone(&state);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            if let Err(e) = st.tick_jobs().await {
                tracing::warn!(error = %e, "job scheduler tick failed");
            }
        }
    });

    let st = state;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tick.tick().await;
            reap_orphan_contexts(&st).await;
        }
    });
}

/// Destroy execution contexts whose cluster is no longer running.
async fn reap_orphan_contexts(st: &Arc<AppState>) {
    let ctxs: Vec<(String, String)> = st.contexts.contexts.iter().map(|c| (c.key().clone(), c.cluster_id.clone())).collect();
    for (id, cluster_id) in ctxs {
        let alive = match st.get_cluster(&cluster_id).await {
            Ok(c) => matches!(c.data.state, ClusterState::Running | ClusterState::Pending | ClusterState::Resizing | ClusterState::Restarting),
            Err(_) => false,
        };
        if !alive {
            tracing::info!(context = %id, cluster = %cluster_id, "reaping context of stopped cluster");
            st.destroy_context(&id).await;
        }
    }
}
