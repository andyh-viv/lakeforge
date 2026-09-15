//! DBFS API (`/api/2.0/dbfs/*`) and Files API (`/api/2.0/fs/*`) over the
//! workspace object store. `dbfs:/x` maps to `/dbfs/x`; `/Volumes/c/s/v/x`
//! maps to the volume's storage location.

use axum::body::Bytes;
use axum::extract::{FromRequest, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Value};

use super::catalog::KIND_VOLUME;
use super::{empty, Body, S};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::storage::Entry;

fn norm(p: &str) -> String {
    let p = p.trim();
    let p = p.strip_prefix("dbfs:").unwrap_or(p);
    let mut parts: Vec<&str> = vec![];
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    format!("/{}", parts.join("/"))
}

/// Storage path of a dbfs path (without the `dbfs:` scheme).
pub fn dbfs_path(p: &str) -> String {
    let n = norm(p);
    if n.starts_with("/Volumes/") {
        return n;
    }
    format!("/dbfs{n}")
}

impl AppState {
    /// Resolve a user-facing path (`dbfs:/..`, `/Volumes/..`, `/..`) to a storage path.
    pub async fn resolve_fs_path(&self, p: &str) -> ApiResult<String> {
        let n = norm(p);
        if let Some(rest) = n.strip_prefix("/Volumes/") {
            let parts: Vec<&str> = rest.splitn(4, '/').collect();
            if parts.len() < 3 {
                return Err(ApiError::invalid("Volume paths must be /Volumes/<catalog>/<schema>/<volume>/..."));
            }
            let full = format!("{}.{}.{}", parts[0], parts[1], parts[2]);
            let vol = self.store.get::<Value>(KIND_VOLUME, &full).await?.ok_or_else(|| ApiError::NotFound(format!("Volume {full} does not exist.")))?;
            let base = vol.data["storage_location"].as_str().and_then(|l| self.storage.path_of(l)).unwrap_or_else(|| format!("/Volumes/{}/{}/{}", parts[0], parts[1], parts[2]));
            let tail = parts.get(3).copied().unwrap_or("");
            return Ok(format!("{}/{}", base.trim_end_matches('/'), tail).trim_end_matches('/').to_string());
        }
        Ok(format!("/dbfs{n}"))
    }
}

fn to_dbfs(e: &Entry, root: &str, user_root: &str) -> Value {
    let rel = e.path.strip_prefix(root).unwrap_or(&e.path);
    let user_path = format!("{}/{}", user_root.trim_end_matches('/'), rel.trim_start_matches('/'));
    json!({ "path": user_path, "is_dir": e.is_dir, "file_size": e.size, "modification_time": e.modified_ms })
}

#[derive(Debug, Deserialize)]
struct PathQ {
    path: String,
}

async fn list(State(st): State<S>, Query(q): Query<PathQ>) -> ApiResult<Json<Value>> {
    let sp = st.resolve_fs_path(&q.path).await?;
    let user = norm(&q.path);
    if let Some(meta) = st.storage.head(&sp).await? {
        return Ok(Json(json!({ "files": [ { "path": user, "is_dir": false, "file_size": meta.size, "modification_time": meta.last_modified.timestamp_millis() } ] })));
    }
    if !st.storage.is_dir(&sp).await? {
        return Err(ApiError::NotFound(format!("No file or directory exists on path {}.", q.path)));
    }
    let entries = st.storage.list_dir(&sp).await?;
    Ok(Json(json!({ "files": entries.iter().map(|e| to_dbfs(e, &sp, &user)).collect::<Vec<_>>() })))
}

async fn get_status(State(st): State<S>, Query(q): Query<PathQ>) -> ApiResult<Json<Value>> {
    let sp = st.resolve_fs_path(&q.path).await?;
    if let Some(meta) = st.storage.head(&sp).await? {
        return Ok(Json(json!({ "path": norm(&q.path), "is_dir": false, "file_size": meta.size, "modification_time": meta.last_modified.timestamp_millis() })));
    }
    if st.storage.is_dir(&sp).await? {
        return Ok(Json(json!({ "path": norm(&q.path), "is_dir": true, "file_size": 0, "modification_time": 0 })));
    }
    Err(ApiError::NotFound(format!("No file or directory exists on path {}.", q.path)))
}

async fn mkdirs(State(st): State<S>, Body(b): Body<PathQ>) -> ApiResult<Json<Value>> {
    let sp = st.resolve_fs_path(&b.path).await?;
    if st.storage.head(&sp).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("A file already exists at {}", b.path)));
    }
    st.storage.mkdirs(&sp).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct DeleteBody {
    path: String,
    #[serde(default)]
    recursive: bool,
}

async fn delete(State(st): State<S>, Body(b): Body<DeleteBody>) -> ApiResult<Json<Value>> {
    let sp = st.resolve_fs_path(&b.path).await?;
    if st.storage.head(&sp).await?.is_some() {
        st.storage.delete(&sp).await?;
        return Ok(empty());
    }
    if st.storage.is_dir(&sp).await? {
        let children = st.storage.list_dir(&sp).await?;
        if !children.is_empty() && !b.recursive {
            return Err(ApiError::InvalidState(format!("{} is a non-empty directory; set recursive=true", b.path)));
        }
        st.storage.delete_prefix(&format!("{}/", sp.trim_end_matches('/'))).await?;
    }
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct MoveBody {
    source_path: String,
    destination_path: String,
    #[serde(default)]
    #[allow(dead_code)]
    recursive: bool,
}

async fn move_h(State(st): State<S>, Body(b): Body<MoveBody>) -> ApiResult<Json<Value>> {
    let from = st.resolve_fs_path(&b.source_path).await?;
    let to = st.resolve_fs_path(&b.destination_path).await?;
    copy_or_move(&st, &from, &to, true).await?;
    Ok(empty())
}

async fn copy_h(State(st): State<S>, Body(b): Body<MoveBody>) -> ApiResult<Json<Value>> {
    let from = st.resolve_fs_path(&b.source_path).await?;
    let to = st.resolve_fs_path(&b.destination_path).await?;
    copy_or_move(&st, &from, &to, false).await?;
    Ok(empty())
}

async fn copy_or_move(st: &AppState, from: &str, to: &str, mv: bool) -> ApiResult<()> {
    if st.storage.head(to).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("Destination {to} already exists")));
    }
    if st.storage.head(from).await?.is_some() {
        if mv {
            st.storage.rename(from, to).await?;
        } else {
            st.storage.copy(from, to).await?;
        }
        return Ok(());
    }
    if st.storage.is_dir(from).await? {
        let prefix = format!("{}/", from.trim_end_matches('/'));
        for e in st.storage.list_all(&prefix).await? {
            let rel = e.path.strip_prefix(&prefix).unwrap_or(&e.path);
            let dest = format!("{}/{rel}", to.trim_end_matches('/'));
            if mv {
                st.storage.rename(&e.path, &dest).await?;
            } else {
                st.storage.copy(&e.path, &dest).await?;
            }
        }
        st.storage.mkdirs(to).await?;
        if mv {
            st.storage.delete_prefix(&prefix).await?;
        }
        return Ok(());
    }
    Err(ApiError::NotFound(format!("No file or directory exists on path {from}.")))
}

#[derive(Debug, Deserialize)]
struct PutBody {
    path: String,
    #[serde(default)]
    contents: Option<String>,
    #[serde(default)]
    overwrite: bool,
}

async fn put_json(st: &AppState, b: PutBody) -> ApiResult<()> {
    let sp = st.resolve_fs_path(&b.path).await?;
    if !b.overwrite && st.storage.head(&sp).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("A file or directory already exists at path {}.", b.path)));
    }
    let bytes = match &b.contents {
        Some(c) => base64::engine::general_purpose::STANDARD.decode(c).map_err(|e| ApiError::invalid(format!("contents is not valid base64: {e}")))?,
        None => vec![],
    };
    st.storage.put(&sp, Bytes::from(bytes)).await
}

/// `dbfs/put` accepts JSON or multipart (`path`, `overwrite`, `contents` file field).
async fn put(State(st): State<S>, req: axum::extract::Request) -> ApiResult<Json<Value>> {
    let ct = req.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    if ct.starts_with("multipart/form-data") {
        let mut mp = axum::extract::Multipart::from_request(req, &()).await.map_err(|e| ApiError::invalid(e.to_string()))?;
        let mut path = None;
        let mut overwrite = false;
        let mut data: Option<Vec<u8>> = None;
        while let Some(field) = mp.next_field().await.map_err(|e| ApiError::invalid(e.to_string()))? {
            match field.name().unwrap_or("") {
                "path" => path = Some(field.text().await.map_err(|e| ApiError::invalid(e.to_string()))?),
                "overwrite" => overwrite = field.text().await.map(|t| t == "true").unwrap_or(false),
                "contents" | "file" => data = Some(field.bytes().await.map_err(|e| ApiError::invalid(e.to_string()))?.to_vec()),
                _ => {}
            }
        }
        let path = path.ok_or_else(|| ApiError::invalid("path is required"))?;
        let sp = st.resolve_fs_path(&path).await?;
        if !overwrite && st.storage.head(&sp).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("A file or directory already exists at path {path}.")));
        }
        st.storage.put(&sp, Bytes::from(data.unwrap_or_default())).await?;
        return Ok(empty());
    }
    let bytes = axum::body::to_bytes(req.into_body(), 64 * 1024 * 1024).await.map_err(|e| ApiError::invalid(e.to_string()))?;
    let b: PutBody = serde_json::from_slice(&bytes).map_err(|e| ApiError::invalid(format!("invalid JSON body: {e}")))?;
    put_json(&st, b).await?;
    Ok(empty())
}


#[derive(Debug, Deserialize)]
struct ReadQ {
    path: String,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    length: Option<u64>,
}

async fn read(State(st): State<S>, Query(q): Query<ReadQ>) -> ApiResult<Json<Value>> {
    let sp = st.resolve_fs_path(&q.path).await?;
    if st.storage.head(&sp).await?.is_none() {
        return Err(ApiError::NotFound(format!("No file exists on path {}.", q.path)));
    }
    let len = q.length.unwrap_or(1024 * 1024).min(1024 * 1024);
    let (bytes, _) = st.storage.get_range(&sp, q.offset.unwrap_or(0), len).await?;
    Ok(Json(json!({ "bytes_read": bytes.len(), "data": base64::engine::general_purpose::STANDARD.encode(&bytes) })))
}

// Streaming upload handles: create -> add-block* -> close.

#[derive(Debug, Deserialize)]
struct CreateBody {
    path: String,
    #[serde(default)]
    overwrite: bool,
}

async fn create(State(st): State<S>, Body(b): Body<CreateBody>) -> ApiResult<Json<Value>> {
    let sp = st.resolve_fs_path(&b.path).await?;
    if !b.overwrite && st.storage.head(&sp).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("A file or directory already exists at path {}.", b.path)));
    }
    let handle = st.store.next_seq("dbfs_handle").await?;
    st.store.kv_set(&format!("dbfs_handle:{handle}"), &sp).await?;
    st.storage.put(&format!("/.uploads/{handle}"), Bytes::new()).await?;
    Ok(Json(json!({ "handle": handle })))
}

#[derive(Debug, Deserialize)]
struct BlockBody {
    handle: i64,
    data: String,
}

async fn add_block(State(st): State<S>, Body(b): Body<BlockBody>) -> ApiResult<Json<Value>> {
    st.store.kv_get(&format!("dbfs_handle:{}", b.handle)).await?.ok_or_else(|| ApiError::NotFound(format!("Handle {} does not exist", b.handle)))?;
    let chunk = base64::engine::general_purpose::STANDARD.decode(&b.data).map_err(|e| ApiError::invalid(format!("data is not valid base64: {e}")))?;
    let tmp = format!("/.uploads/{}", b.handle);
    let mut cur = st.storage.get(&tmp).await?.to_vec();
    cur.extend_from_slice(&chunk);
    st.storage.put(&tmp, Bytes::from(cur)).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct HandleBody {
    handle: i64,
}

async fn close(State(st): State<S>, Body(b): Body<HandleBody>) -> ApiResult<Json<Value>> {
    let key = format!("dbfs_handle:{}", b.handle);
    let dest = st.store.kv_get(&key).await?.ok_or_else(|| ApiError::NotFound(format!("Handle {} does not exist", b.handle)))?;
    let tmp = format!("/.uploads/{}", b.handle);
    let data = st.storage.get(&tmp).await?;
    st.storage.put(&dest, data).await?;
    st.storage.delete(&tmp).await?;
    st.store.kv_delete(&key).await?;
    Ok(empty())
}

// ------------------------------------------------------------- Files API

fn files_path(p: &str) -> String {
    format!("/{}", p.trim_start_matches('/'))
}

async fn files_get(State(st): State<S>, Path(p): Path<String>) -> ApiResult<Response> {
    let sp = st.resolve_fs_path(&files_path(&p)).await?;
    let meta = st.storage.head(&sp).await?.ok_or_else(|| ApiError::NotFound(format!("File {p} not found")))?;
    let bytes = st.storage.get(&sp).await?;
    let mime = mime_guess::from_path(&p).first_or_octet_stream();
    Ok((
        [(header::CONTENT_TYPE, mime.to_string()), (header::CONTENT_LENGTH, meta.size.to_string()), (header::LAST_MODIFIED, meta.last_modified.to_rfc2822())],
        bytes,
    )
        .into_response())
}

async fn files_head(State(st): State<S>, Path(p): Path<String>) -> ApiResult<Response> {
    let sp = st.resolve_fs_path(&files_path(&p)).await?;
    let meta = st.storage.head(&sp).await?.ok_or_else(|| ApiError::NotFound(format!("File {p} not found")))?;
    Ok(([(header::CONTENT_LENGTH, meta.size.to_string()), (header::LAST_MODIFIED, meta.last_modified.to_rfc2822())], StatusCode::OK).into_response())
}

#[derive(Debug, Deserialize)]
struct OverwriteQ {
    #[serde(default)]
    overwrite: Option<bool>,
}

async fn files_put(State(st): State<S>, Path(p): Path<String>, Query(q): Query<OverwriteQ>, body: Bytes) -> ApiResult<Response> {
    let sp = st.resolve_fs_path(&files_path(&p)).await?;
    if q.overwrite != Some(true) && st.storage.head(&sp).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("File {p} already exists (use ?overwrite=true)")));
    }
    st.storage.put(&sp, body).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn files_delete(State(st): State<S>, Path(p): Path<String>) -> ApiResult<Response> {
    let sp = st.resolve_fs_path(&files_path(&p)).await?;
    if st.storage.head(&sp).await?.is_none() {
        return Err(ApiError::NotFound(format!("File {p} not found")));
    }
    st.storage.delete(&sp).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Debug, Deserialize)]
struct DirQ {
    #[serde(default)]
    page_size: Option<usize>,
    #[serde(default)]
    page_token: Option<String>,
}

async fn dir_list(State(st): State<S>, Path(p): Path<String>, Query(q): Query<DirQ>) -> ApiResult<Json<Value>> {
    let user = files_path(&p);
    let sp = st.resolve_fs_path(&user).await?;
    if !st.storage.is_dir(&sp).await? {
        return Err(ApiError::NotFound(format!("Directory {p} not found")));
    }
    let entries = st.storage.list_dir(&sp).await?;
    let start: usize = q.page_token.as_deref().and_then(|t| t.parse().ok()).unwrap_or(0);
    let size = q.page_size.unwrap_or(1000).clamp(1, 1000);
    let page: Vec<Value> = entries
        .iter()
        .skip(start)
        .take(size)
        .map(|e| {
            let rel = e.path.strip_prefix(&sp).unwrap_or(&e.path).trim_start_matches('/');
            let mut v = json!({ "path": format!("{}/{rel}", user.trim_end_matches('/')), "is_directory": e.is_dir, "name": rel.rsplit('/').next().unwrap_or(rel) });
            if !e.is_dir {
                v["file_size"] = json!(e.size);
                v["last_modified"] = json!(e.modified_ms);
            }
            v
        })
        .collect();
    let mut out = json!({ "contents": page });
    if start + size < entries.len() {
        out["next_page_token"] = json!((start + size).to_string());
    }
    Ok(Json(out))
}

async fn dir_create(State(st): State<S>, Path(p): Path<String>) -> ApiResult<Response> {
    let sp = st.resolve_fs_path(&files_path(&p)).await?;
    st.storage.mkdirs(&sp).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn dir_delete(State(st): State<S>, Path(p): Path<String>) -> ApiResult<Response> {
    let sp = st.resolve_fs_path(&files_path(&p)).await?;
    let children = st.storage.list_dir(&sp).await?;
    if !children.is_empty() {
        return Err(ApiError::InvalidState(format!("Directory {p} is not empty")));
    }
    st.storage.delete(&format!("{}/.dir", sp.trim_end_matches('/'))).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn dir_head(State(st): State<S>, Path(p): Path<String>) -> ApiResult<Response> {
    let sp = st.resolve_fs_path(&files_path(&p)).await?;
    if st.storage.is_dir(&sp).await? {
        Ok(StatusCode::OK.into_response())
    } else {
        Err(ApiError::NotFound(format!("Directory {p} not found")))
    }
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/dbfs/list", get(list))
        .route("/api/2.0/dbfs/get-status", get(get_status))
        .route("/api/2.0/dbfs/mkdirs", post(mkdirs))
        .route("/api/2.0/dbfs/delete", post(delete))
        .route("/api/2.0/dbfs/move", post(move_h))
        .route("/api/2.0/dbfs/copy", post(copy_h))
        .route("/api/2.0/dbfs/put", post(put))
        .route("/api/2.0/dbfs/read", get(read))
        .route("/api/2.0/dbfs/create", post(create))
        .route("/api/2.0/dbfs/add-block", post(add_block))
        .route("/api/2.0/dbfs/close", post(close))
        .route("/api/2.0/fs/files/{*path}", get(files_get).head(files_head).put(files_put).delete(files_delete))
        .route("/api/2.0/fs/directories/{*path}", get(dir_list).put(dir_create).delete(dir_delete).head(dir_head))
}
