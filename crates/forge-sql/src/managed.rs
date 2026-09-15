//! Managed (warehouse-owned) Delta tables.
//!
//! `CREATE TABLE t (...)` / `CREATE TABLE t AS SELECT ...` are parsed by
//! DataFusion as in-memory tables. When a session has a warehouse directory
//! configured, Forge instead materialises them as Delta tables under
//! `<warehouse>/<catalog>/<schema>/<table>` so they survive driver restarts
//! and are visible to every cluster in the workspace.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{CreateMemoryTable, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::{ResolvedTableReference, TableReference};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::operations::write::SchemaMode;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;

use crate::object_store::{ensure_object_store, parse_location, root_url};
use crate::tables::{register_table, TableFormat, TableSpec};

/// Resolve `name` against the session's default catalog/schema.
pub fn resolve(ctx: &SessionContext, name: &TableReference) -> ResolvedTableReference {
    let state = ctx.state();
    let opts = &state.config().options().catalog;
    name.clone().resolve(&opts.default_catalog, &opts.default_schema)
}

/// Storage location of a managed table.
pub fn managed_location(warehouse_dir: &str, r: &ResolvedTableReference) -> String {
    format!(
        "{}/{}/{}/{}",
        warehouse_dir.trim_end_matches('/'),
        r.catalog,
        r.schema,
        r.table
    )
}

/// Outcome of a managed `CREATE TABLE`.
#[derive(Debug, Clone)]
pub enum CreateOutcome {
    /// Table written and registered.
    Created(TableSpec),
    /// `IF NOT EXISTS` and the table was already there.
    AlreadyExists,
}

/// Materialise a `CreateMemoryTable` plan as a Delta table under
/// `warehouse_dir` and register it in `ctx`.
pub async fn create_managed_table(
    ctx: &SessionContext,
    cmt: &CreateMemoryTable,
    warehouse_dir: &str,
) -> DFResult<CreateOutcome> {
    let resolved = resolve(ctx, &cmt.name);
    let full = format!("{}.{}.{}", resolved.catalog, resolved.schema, resolved.table);
    let exists = ctx.table_exist(TableReference::from(resolved.clone()))?;
    if exists && cmt.if_not_exists {
        return Ok(CreateOutcome::AlreadyExists);
    }
    if exists && !cmt.or_replace {
        return Err(DataFusionError::Plan(format!("table '{full}' already exists")));
    }

    let location = managed_location(warehouse_dir, &resolved);
    let url = parse_location(&location)?;
    ensure_object_store(&ctx.runtime_env(), &url)?;
    if url.scheme() == "file" {
        if let Ok(path) = url.to_file_path() {
            std::fs::create_dir_all(&path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
    }

    let mut table = DeltaTable::try_from_url(url.clone())
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let on_disk = table
        .verify_deltatable_existence()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if on_disk && !cmt.or_replace {
        let spec = TableSpec {
            name: full.clone(),
            format: TableFormat::Delta,
            location,
            options: Default::default(),
        };
        if cmt.if_not_exists {
            register_table(ctx, &spec).await?;
            return Ok(CreateOutcome::AlreadyExists);
        }
        return Err(DataFusionError::Plan(format!("table '{full}' already exists")));
    }
    if on_disk {
        table.load().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
    }
    let save_mode = if cmt.or_replace { SaveMode::Overwrite } else { SaveMode::ErrorIfExists };

    let schema_only = matches!(cmt.input.as_ref(), LogicalPlan::EmptyRelation(_));
    if schema_only {
        let arrow_schema = Arc::new(cmt.input.schema().as_arrow().clone());
        let kernel: StructType = arrow_schema
            .as_ref()
            .try_into_kernel()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        table
            .create()
            .with_table_name(resolved.table.to_string())
            .with_columns(kernel.fields().cloned())
            .with_save_mode(save_mode)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    } else {
        let mut w = table
            .write(Vec::new())
            .with_input_plan(cmt.input.as_ref().clone())
            .with_session_state(Arc::new(ctx.state()))
            .with_table_name(resolved.table.to_string())
            .with_save_mode(save_mode);
        if cmt.or_replace {
            w = w.with_schema_mode(SchemaMode::Overwrite);
        }
        w.await.map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    let spec = TableSpec { name: full, format: TableFormat::Delta, location, options: Default::default() };
    register_table(ctx, &spec).await?;
    Ok(CreateOutcome::Created(spec))
}

/// If a Delta table exists at the managed location for `r`, register it in
/// `ctx` and return its spec. Lets a driver discover tables created by other
/// clusters sharing the same warehouse directory.
pub async fn probe_managed_table(
    ctx: &SessionContext,
    warehouse_dir: &str,
    r: &ResolvedTableReference,
) -> DFResult<Option<TableSpec>> {
    let location = managed_location(warehouse_dir, r);
    let url = parse_location(&location)?;
    if url.scheme() == "file" {
        match url.to_file_path() {
            Ok(p) if p.join("_delta_log").is_dir() => {}
            _ => return Ok(None),
        }
    }
    ensure_object_store(&ctx.runtime_env(), &url)?;
    let table = DeltaTable::try_from_url(url)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let on_disk = table
        .verify_deltatable_existence()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if !on_disk {
        return Ok(None);
    }
    let spec = TableSpec {
        name: format!("{}.{}.{}", r.catalog, r.schema, r.table),
        format: TableFormat::Delta,
        location,
        options: Default::default(),
    };
    register_table(ctx, &spec).await?;
    Ok(Some(spec))
}

/// Delete the files of a managed Delta table (used by `DROP TABLE`).
pub async fn drop_managed_table(ctx: &SessionContext, location: &str) -> DFResult<()> {
    let url = parse_location(location)?;
    let runtime = ctx.runtime_env();
    ensure_object_store(&runtime, &url)?;
    let store_url = if url.scheme() == "file" {
        ObjectStoreUrl::local_filesystem()
    } else {
        ObjectStoreUrl::parse(root_url(&url)?.as_str())?
    };
    let store = runtime.object_store(&store_url)?;
    let prefix = object_store::path::Path::from(url.path());
    let mut listing = store.list(Some(&prefix));
    let mut paths = Vec::new();
    while let Some(meta) = listing.try_next().await? {
        paths.push(meta.location);
    }
    for p in paths {
        store.delete(&p).await?;
    }
    if url.scheme() == "file" {
        if let Ok(path) = url.to_file_path() {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
    Ok(())
}
