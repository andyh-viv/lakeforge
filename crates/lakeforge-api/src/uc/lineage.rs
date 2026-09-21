//! Table- and column-level lineage captured from executed SQL. Each statement
//! that reads tables/paths and writes a table produces one
//! `system.access.table_lineage` row per (source, target) pair and one
//! `system.access.column_lineage` row per column edge. Read-only queries are
//! recorded with a null target so "who reads this table" works too.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::auth::Principal;
use crate::error::ApiResult;
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};
use crate::uc::sqlguard::Analysis;

pub const KIND_TABLE_LINEAGE: &str = "uc_table_lineage";
pub const KIND_COLUMN_LINEAGE: &str = "uc_column_lineage";
pub const MAX_ROWS: i64 = 200_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub entity_type: String,
    pub entity_id: String,
    pub entity_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableLineage {
    pub event_id: String,
    pub event_time: i64,
    pub workspace_id: String,
    pub statement_id: String,
    pub created_by: String,
    pub entity: Entity,
    pub source_table_full_name: Option<String>,
    pub source_path: Option<String>,
    pub source_type: Option<String>,
    pub target_table_full_name: Option<String>,
    pub target_path: Option<String>,
    pub target_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineage {
    pub event_id: String,
    pub event_time: i64,
    pub workspace_id: String,
    pub statement_id: String,
    pub created_by: String,
    pub entity: Entity,
    pub source_table_full_name: String,
    pub source_column_name: String,
    pub target_table_full_name: String,
    pub target_column_name: String,
}

/// Where a statement came from (notebook / job / pipeline / SQL editor).
#[derive(Debug, Clone, Default)]
pub struct LineageContext {
    pub entity_type: Option<String>,
    pub entity_id: Option<String>,
    pub entity_run_id: Option<String>,
}

impl LineageContext {
    pub fn from_conf(conf: &std::collections::HashMap<String, String>) -> Self {
        Self { entity_type: conf.get("lakeforge.entity_type").cloned(), entity_id: conf.get("lakeforge.entity_id").cloned(), entity_run_id: conf.get("lakeforge.entity_run_id").cloned() }
    }
}

fn split3(full: &str) -> (Option<&str>, Option<&str>, Option<&str>) {
    let mut it = full.split('.');
    let a = it.next();
    let b = it.next();
    let c = it.next();
    match (a, b, c) {
        (Some(a), Some(b), Some(c)) => (Some(a), Some(b), Some(c)),
        _ => (None, None, a),
    }
}

impl TableLineage {
    pub fn to_row(&self, metastore_id: &str) -> Value {
        let (sc, ss, sn) = self.source_table_full_name.as_deref().map(split3).unwrap_or((None, None, None));
        let (tc, ts, tn) = self.target_table_full_name.as_deref().map(split3).unwrap_or((None, None, None));
        json!({
            "account_id": "lakeforge",
            "metastore_id": metastore_id,
            "workspace_id": self.workspace_id,
            "entity_type": self.entity.entity_type,
            "entity_id": self.entity.entity_id,
            "entity_run_id": self.entity.entity_run_id,
            "statement_id": self.statement_id,
            "source_table_full_name": self.source_table_full_name,
            "source_table_catalog": sc,
            "source_table_schema": ss,
            "source_table_name": sn,
            "source_path": self.source_path,
            "source_type": self.source_type,
            "target_table_full_name": self.target_table_full_name,
            "target_table_catalog": tc,
            "target_table_schema": ts,
            "target_table_name": tn,
            "target_path": self.target_path,
            "target_type": self.target_type,
            "created_by": self.created_by,
            "event_time": crate::uc::system_tables::ts(self.event_time),
            "event_date": crate::uc::system_tables::date(self.event_time),
            "event_id": self.event_id,
        })
    }
}

impl ColumnLineage {
    pub fn to_row(&self, metastore_id: &str) -> Value {
        let (sc, ss, sn) = split3(&self.source_table_full_name);
        let (tc, ts, tn) = split3(&self.target_table_full_name);
        json!({
            "account_id": "lakeforge",
            "metastore_id": metastore_id,
            "workspace_id": self.workspace_id,
            "entity_type": self.entity.entity_type,
            "entity_id": self.entity.entity_id,
            "entity_run_id": self.entity.entity_run_id,
            "statement_id": self.statement_id,
            "source_table_full_name": self.source_table_full_name,
            "source_table_catalog": sc,
            "source_table_schema": ss,
            "source_table_name": sn,
            "source_column_name": self.source_column_name,
            "source_type": "TABLE",
            "target_table_full_name": self.target_table_full_name,
            "target_table_catalog": tc,
            "target_table_schema": ts,
            "target_table_name": tn,
            "target_column_name": self.target_column_name,
            "target_type": "TABLE",
            "created_by": self.created_by,
            "event_time": crate::uc::system_tables::ts(self.event_time),
            "event_date": crate::uc::system_tables::date(self.event_time),
            "event_id": self.event_id,
        })
    }
}

impl AppState {
    /// Persist lineage for one successfully executed statement.
    pub async fn record_lineage(&self, p: &Principal, statement_id: &str, an: &Analysis, ctx: &LineageContext, table_types: &std::collections::HashMap<String, String>) -> ApiResult<usize> {
        let now = now_ms();
        let entity = Entity {
            entity_type: ctx.entity_type.clone().unwrap_or_else(|| "QUERY".into()),
            entity_id: ctx.entity_id.clone().unwrap_or_else(|| statement_id.to_string()),
            entity_run_id: ctx.entity_run_id.clone(),
        };
        let ty = |t: &str| table_types.get(t).cloned().unwrap_or_else(|| "TABLE".into());
        let mut rows: Vec<TableLineage> = vec![];
        let targets: Vec<String> = an.writes.iter().cloned().collect();
        let sources: BTreeSet<&String> = an.reads.iter().filter(|r| !an.writes.contains(*r) || an.kind == crate::uc::sqlguard::StmtKind::Merge).collect();
        let base = |src_t: Option<String>, src_p: Option<String>, tgt: Option<String>| TableLineage {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_time: now,
            workspace_id: self.ws().to_string(),
            statement_id: statement_id.to_string(),
            created_by: p.user_name.clone(),
            entity: entity.clone(),
            source_type: src_t.as_deref().map(ty).or(src_p.as_ref().map(|_| "PATH".to_string())),
            source_table_full_name: src_t,
            source_path: src_p,
            target_type: tgt.as_deref().map(ty),
            target_table_full_name: tgt,
            target_path: None,
        };
        if targets.is_empty() {
            for s in &sources {
                rows.push(base(Some((*s).clone()), None, None));
            }
            for pth in &an.paths {
                rows.push(base(None, Some(pth.clone()), None));
            }
        } else {
            for t in &targets {
                if sources.is_empty() && an.paths.is_empty() {
                    rows.push(base(None, None, Some(t.clone())));
                }
                for s in &sources {
                    rows.push(base(Some((*s).clone()), None, Some(t.clone())));
                }
                for pth in &an.paths {
                    rows.push(base(None, Some(pth.clone()), Some(t.clone())));
                }
            }
        }
        let mut n = 0;
        for r in &rows {
            self.store.insert(KIND_TABLE_LINEAGE, self.ws(), &r.event_id, r.target_table_full_name.as_deref(), r.source_table_full_name.as_deref(), r).await?;
            n += 1;
        }
        if let Some(target) = targets.first() {
            for e in &an.column_edges {
                let r = ColumnLineage {
                    event_id: uuid::Uuid::new_v4().to_string(),
                    event_time: now,
                    workspace_id: self.ws().to_string(),
                    statement_id: statement_id.to_string(),
                    created_by: p.user_name.clone(),
                    entity: entity.clone(),
                    source_table_full_name: e.source_table.clone(),
                    source_column_name: e.source_column.clone(),
                    target_table_full_name: target.clone(),
                    target_column_name: e.target_column.clone(),
                };
                self.store.insert(KIND_COLUMN_LINEAGE, self.ws(), &r.event_id, Some(target), Some(&e.source_table), &r).await?;
                n += 1;
            }
        }
        if now % 53 == 0 {
            for kind in [KIND_TABLE_LINEAGE, KIND_COLUMN_LINEAGE] {
                let c = self.store.count(kind, self.ws(), None).await?;
                if c > MAX_ROWS {
                    let _ = self.store.delete_oldest(kind, self.ws(), (c - MAX_ROWS) as u64).await;
                }
            }
        }
        Ok(n)
    }

    pub async fn table_lineage_rows(&self) -> ApiResult<Vec<TableLineage>> {
        let docs: Vec<Doc<TableLineage>> = self.store.list(KIND_TABLE_LINEAGE, self.ws(), Filter { newest_first: true, limit: Some(50_000), ..Default::default() }).await?;
        Ok(docs.into_iter().map(|d| d.data).collect())
    }

    pub async fn column_lineage_rows(&self) -> ApiResult<Vec<ColumnLineage>> {
        let docs: Vec<Doc<ColumnLineage>> = self.store.list(KIND_COLUMN_LINEAGE, self.ws(), Filter { newest_first: true, limit: Some(50_000), ..Default::default() }).await?;
        Ok(docs.into_iter().map(|d| d.data).collect())
    }

    /// Databricks lineage-tracking API shape: upstream/downstream tables for one table.
    pub async fn table_lineage_for(&self, full: &str) -> ApiResult<Value> {
        let all = self.table_lineage_rows().await?;
        let mut upstreams: Vec<Value> = vec![];
        let mut downstreams: Vec<Value> = vec![];
        let mut seen_up = BTreeSet::new();
        let mut seen_down = BTreeSet::new();
        for r in all {
            if r.target_table_full_name.as_deref() == Some(full) {
                if let Some(s) = &r.source_table_full_name {
                    if seen_up.insert(s.clone()) {
                        let (c, sch, n) = split3(s);
                        upstreams.push(json!({ "tableInfo": { "name": n, "catalog_name": c, "schema_name": sch, "table_type": r.source_type, "lineage_timestamp": crate::uc::system_tables::ts(r.event_time) }, "queryInfos": [{ "statement_id": r.statement_id, "user": r.created_by, "entity_type": r.entity.entity_type, "entity_id": r.entity.entity_id }] }));
                    }
                } else if let Some(p) = &r.source_path {
                    if seen_up.insert(p.clone()) {
                        upstreams.push(json!({ "fileInfo": { "path": p, "has_permission": true, "securable_type": "PATH" }, "queryInfos": [{ "statement_id": r.statement_id, "user": r.created_by }] }));
                    }
                }
            }
            if r.source_table_full_name.as_deref() == Some(full) {
                if let Some(t) = &r.target_table_full_name {
                    if seen_down.insert(t.clone()) {
                        let (c, sch, n) = split3(t);
                        downstreams.push(json!({ "tableInfo": { "name": n, "catalog_name": c, "schema_name": sch, "table_type": r.target_type, "lineage_timestamp": crate::uc::system_tables::ts(r.event_time) }, "queryInfos": [{ "statement_id": r.statement_id, "user": r.created_by, "entity_type": r.entity.entity_type, "entity_id": r.entity.entity_id }] }));
                    }
                } else if let Some(ctx_id) = seen_down.get(&format!("__q:{}", r.entity.entity_id)).cloned().or_else(|| Some(format!("__q:{}", r.entity.entity_id))) {
                    // read-only consumer (notebook / job / query)
                    if seen_down.insert(ctx_id) {
                        downstreams.push(json!({ "notebookInfos": if r.entity.entity_type == "NOTEBOOK" { json!([{ "notebook_id": r.entity.entity_id, "workspace_id": r.workspace_id }]) } else { json!([]) }, "jobInfos": if r.entity.entity_type == "JOB" { json!([{ "job_id": r.entity.entity_id, "run_id": r.entity.entity_run_id }]) } else { json!([]) }, "queryInfos": [{ "statement_id": r.statement_id, "user": r.created_by, "entity_type": r.entity.entity_type, "entity_id": r.entity.entity_id }] }));
                    }
                }
            }
        }
        Ok(json!({ "upstreams": upstreams, "downstreams": downstreams }))
    }

    pub async fn column_lineage_for(&self, full: &str, column: &str) -> ApiResult<Value> {
        let all = self.column_lineage_rows().await?;
        let mut upstream_cols: Vec<Value> = vec![];
        let mut downstream_cols: Vec<Value> = vec![];
        let mut seen = BTreeSet::new();
        for r in all {
            if r.target_table_full_name == full && r.target_column_name == column && seen.insert(format!("u:{}:{}", r.source_table_full_name, r.source_column_name)) {
                let (c, s, n) = split3(&r.source_table_full_name);
                upstream_cols.push(json!({ "name": r.source_column_name, "catalog_name": c, "schema_name": s, "table_name": n, "table_type": "TABLE", "lineage_timestamp": crate::uc::system_tables::ts(r.event_time) }));
            }
            if r.source_table_full_name == full && r.source_column_name == column && seen.insert(format!("d:{}:{}", r.target_table_full_name, r.target_column_name)) {
                let (c, s, n) = split3(&r.target_table_full_name);
                downstream_cols.push(json!({ "name": r.target_column_name, "catalog_name": c, "schema_name": s, "table_name": n, "table_type": "TABLE", "lineage_timestamp": crate::uc::system_tables::ts(r.event_time) }));
            }
        }
        Ok(json!({ "upstream_cols": upstream_cols, "downstream_cols": downstream_cols }))
    }
}
