//! Workspace API (`/api/2.0/workspace/*`): directories, notebooks and files.
//!
//! Objects are docs of kind `ws_object` keyed by normalized path. Notebook
//! cells live inline in the doc; file bytes live in object storage under
//! `/.workspace/files/<object_id>`.

use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{empty, Body, S};
use crate::auth::Who;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND: &str = "ws_object";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObjectType {
    Notebook,
    Directory,
    File,
    Repo,
    Library,
    Dashboard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Language {
    Python,
    Sql,
    Scala,
    R,
}

impl Language {
    pub fn cell_name(self) -> &'static str {
        match self {
            Language::Python => "python",
            Language::Sql => "sql",
            Language::Scala => "scala",
            Language::R => "r",
        }
    }
    pub fn comment_prefix(self) -> &'static str {
        match self {
            Language::Sql => "-- ",
            Language::R => "# ",
            Language::Scala => "// ",
            Language::Python => "# ",
        }
    }
    pub fn from_cell(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "python" | "py" => Some(Language::Python),
            "sql" => Some(Language::Sql),
            "scala" => Some(Language::Scala),
            "r" => Some(Language::R),
            _ => None,
        }
    }
    pub fn extension(self) -> &'static str {
        match self {
            Language::Python => "py",
            Language::Sql => "sql",
            Language::Scala => "scala",
            Language::R => "r",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Cell {
    pub id: String,
    /// `python` | `sql` | `markdown` | `shell` | `scala` | `r`
    pub language: String,
    pub source: String,
    #[serde(default)]
    pub outputs: Vec<Value>,
    #[serde(default)]
    pub collapsed: bool,
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Notebook {
    pub default_language: String,
    pub cells: Vec<Cell>,
    #[serde(default)]
    pub widgets: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsObject {
    pub object_type: ObjectType,
    pub path: String,
    #[serde(default)]
    pub language: Option<Language>,
    pub object_id: i64,
    pub created_at: i64,
    pub modified_at: i64,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub notebook: Option<Notebook>,
    #[serde(default)]
    pub created_by: String,
    #[serde(default)]
    pub repo_id: Option<String>,
}

impl WsObject {
    pub fn status(&self) -> Value {
        let mut v = json!({
            "object_type": self.object_type,
            "path": self.path,
            "object_id": self.object_id,
            "created_at": self.created_at,
            "modified_at": self.modified_at,
            "resource_id": self.object_id.to_string(),
        });
        if let Some(l) = self.language {
            v["language"] = json!(l);
        }
        if self.object_type == ObjectType::File {
            v["size"] = json!(self.size);
        }
        v
    }
}

pub fn normalize(path: &str) -> ApiResult<String> {
    let p = path.trim();
    if p.is_empty() || !p.starts_with('/') {
        return Err(ApiError::invalid(format!("Path ({path}) must be absolute.")));
    }
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
    Ok(format!("/{}", parts.join("/")))
}

pub fn parent_of(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    match path.rfind('/') {
        Some(0) => Some("/".into()),
        Some(i) => Some(path[..i].to_string()),
        None => None,
    }
}

pub fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

impl AppState {
    pub async fn ws_get(&self, path: &str) -> ApiResult<Option<WsObject>> {
        let path = normalize(path)?;
        if path == "/" {
            return Ok(Some(WsObject { object_type: ObjectType::Directory, path, language: None, object_id: 0, created_at: 0, modified_at: 0, size: 0, notebook: None, created_by: String::new(), repo_id: None }));
        }
        Ok(self.store.get::<WsObject>(KIND, &path).await?.map(|d| d.data))
    }

    pub async fn ws_require(&self, path: &str) -> ApiResult<WsObject> {
        self.ws_get(path).await?.ok_or_else(|| ApiError::NotFound(format!("Path ({path}) doesn't exist.")))
    }

    pub async fn ws_list(&self, path: &str) -> ApiResult<Vec<WsObject>> {
        let path = normalize(path)?;
        let docs: Vec<Doc<WsObject>> = self.store.list(KIND, self.ws(), Filter { parent_id: Some(&path), ..Default::default() }).await?;
        let mut items: Vec<WsObject> = docs.into_iter().map(|d| d.data).collect();
        items.sort_by(|a, b| (a.object_type != ObjectType::Directory).cmp(&(b.object_type != ObjectType::Directory)).then_with(|| a.path.to_ascii_lowercase().cmp(&b.path.to_ascii_lowercase())));
        Ok(items)
    }

    pub async fn ws_mkdirs(&self, path: &str, user: &str) -> ApiResult<()> {
        let path = normalize(path)?;
        if path == "/" {
            return Ok(());
        }
        let mut cur = String::new();
        for seg in path.trim_matches('/').split('/') {
            cur.push('/');
            cur.push_str(seg);
            match self.store.get::<WsObject>(KIND, &cur).await? {
                Some(d) if d.data.object_type == ObjectType::Directory || d.data.object_type == ObjectType::Repo => {}
                Some(_) => return Err(ApiError::AlreadyExists(format!("Path ({cur}) already exists and is not a directory."))),
                None => {
                    let obj = WsObject {
                        object_type: ObjectType::Directory,
                        path: cur.clone(),
                        language: None,
                        object_id: self.store.next_seq("ws_object_id").await?,
                        created_at: now_ms(),
                        modified_at: now_ms(),
                        size: 0,
                        notebook: None,
                        created_by: user.to_string(),
                        repo_id: None,
                    };
                    self.store.insert(KIND, self.ws(), &cur, parent_of(&cur).as_deref(), Some(&cur), &obj).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn ws_put_notebook(&self, path: &str, language: Language, nb: Notebook, user: &str, overwrite: bool) -> ApiResult<WsObject> {
        let path = normalize(path)?;
        let parent = parent_of(&path).ok_or_else(|| ApiError::invalid("cannot write to root"))?;
        self.ws_mkdirs(&parent, user).await?;
        let existing = self.store.get::<WsObject>(KIND, &path).await?;
        if let Some(e) = &existing {
            if e.data.object_type != ObjectType::Notebook {
                return Err(ApiError::AlreadyExists(format!("Path ({path}) exists and is not a notebook.")));
            }
            if !overwrite {
                return Err(ApiError::AlreadyExists(format!("Path ({path}) already exists.")));
            }
        }
        let size = nb.cells.iter().map(|c| c.source.len() as u64).sum();
        let obj = WsObject {
            object_type: ObjectType::Notebook,
            path: path.clone(),
            language: Some(language),
            object_id: match &existing {
                Some(e) => e.data.object_id,
                None => self.store.next_seq("ws_object_id").await?,
            },
            created_at: existing.as_ref().map(|e| e.data.created_at).unwrap_or_else(now_ms),
            modified_at: now_ms(),
            size,
            notebook: Some(nb),
            created_by: existing.as_ref().map(|e| e.data.created_by.clone()).unwrap_or_else(|| user.to_string()),
            repo_id: existing.as_ref().and_then(|e| e.data.repo_id.clone()),
        };
        self.store.upsert(KIND, self.ws(), &path, Some(&parent), Some(&path), &obj).await?;
        Ok(obj)
    }

    pub async fn ws_put_file(&self, path: &str, bytes: &[u8], user: &str, overwrite: bool) -> ApiResult<WsObject> {
        let path = normalize(path)?;
        let parent = parent_of(&path).ok_or_else(|| ApiError::invalid("cannot write to root"))?;
        self.ws_mkdirs(&parent, user).await?;
        let existing = self.store.get::<WsObject>(KIND, &path).await?;
        if let Some(e) = &existing {
            if e.data.object_type == ObjectType::Directory {
                return Err(ApiError::AlreadyExists(format!("Path ({path}) is a directory.")));
            }
            if !overwrite {
                return Err(ApiError::AlreadyExists(format!("Path ({path}) already exists.")));
            }
        }
        let object_id = match &existing {
            Some(e) => e.data.object_id,
            None => self.store.next_seq("ws_object_id").await?,
        };
        self.storage.put(&format!("/.workspace/files/{object_id}"), bytes.to_vec().into()).await?;
        let obj = WsObject {
            object_type: ObjectType::File,
            path: path.clone(),
            language: None,
            object_id,
            created_at: existing.as_ref().map(|e| e.data.created_at).unwrap_or_else(now_ms),
            modified_at: now_ms(),
            size: bytes.len() as u64,
            notebook: None,
            created_by: user.to_string(),
            repo_id: existing.as_ref().and_then(|e| e.data.repo_id.clone()),
        };
        self.store.upsert(KIND, self.ws(), &path, Some(&parent), Some(&path), &obj).await?;
        Ok(obj)
    }

    pub async fn ws_read_file(&self, obj: &WsObject) -> ApiResult<Vec<u8>> {
        Ok(self.storage.get(&format!("/.workspace/files/{}", obj.object_id)).await?.to_vec())
    }

    pub async fn ws_delete(&self, path: &str, recursive: bool) -> ApiResult<()> {
        let path = normalize(path)?;
        let obj = self.ws_require(&path).await?;
        if matches!(obj.object_type, ObjectType::Directory | ObjectType::Repo) {
            let children = self.ws_list(&path).await?;
            if !children.is_empty() && !recursive {
                return Err(ApiError::InvalidState(format!("Folder ({path}) is not empty; set recursive=true.")));
            }
            for c in children {
                Box::pin(self.ws_delete(&c.path, true)).await?;
            }
        } else if obj.object_type == ObjectType::File {
            let _ = self.storage.delete(&format!("/.workspace/files/{}", obj.object_id)).await;
        }
        self.store.delete(KIND, &path).await?;
        Ok(())
    }

    pub async fn ws_move(&self, from: &str, to: &str, user: &str) -> ApiResult<()> {
        let from = normalize(from)?;
        let to = normalize(to)?;
        if self.store.get::<WsObject>(KIND, &to).await?.is_some() {
            return Err(ApiError::AlreadyExists(format!("Path ({to}) already exists.")));
        }
        let obj = self.ws_require(&from).await?;
        let parent = parent_of(&to).ok_or_else(|| ApiError::invalid("cannot move to root"))?;
        self.ws_mkdirs(&parent, user).await?;
        let mut moved = obj.clone();
        moved.path = to.clone();
        moved.modified_at = now_ms();
        self.store.upsert(KIND, self.ws(), &to, Some(&parent), Some(&to), &moved).await?;
        if matches!(obj.object_type, ObjectType::Directory | ObjectType::Repo) {
            for c in self.ws_list(&from).await? {
                let child_to = format!("{to}/{}", basename(&c.path));
                Box::pin(self.ws_move(&c.path, &child_to, user)).await?;
            }
        }
        self.store.delete(KIND, &from).await?;
        Ok(())
    }

    /// All descendants (used for export / repo sync).
    pub async fn ws_walk(&self, path: &str) -> ApiResult<Vec<WsObject>> {
        let mut out = vec![];
        let mut stack = vec![normalize(path)?];
        while let Some(p) = stack.pop() {
            for c in self.ws_list(&p).await? {
                if matches!(c.object_type, ObjectType::Directory | ObjectType::Repo) {
                    stack.push(c.path.clone());
                }
                out.push(c);
            }
        }
        Ok(out)
    }
}

// ------------------------------------------------------ notebook formats

pub fn parse_source(text: &str, language: Language) -> Notebook {
    let prefix = language.comment_prefix();
    let header = format!("{prefix}Databricks notebook source");
    let sep = format!("{prefix}COMMAND ----------");
    let magic = format!("{prefix}MAGIC ");
    let body = text.strip_prefix(&header).unwrap_or(text);
    let mut cells = vec![];
    for raw in body.split(&sep) {
        let chunk = raw.trim_matches('\n');
        if chunk.trim().is_empty() && !cells.is_empty() {
            continue;
        }
        let mut lines: Vec<String> = vec![];
        let mut is_magic = true;
        for l in chunk.lines() {
            if let Some(rest) = l.strip_prefix(&magic) {
                lines.push(rest.to_string());
            } else if l.trim() == format!("{}MAGIC", prefix.trim_end()) {
                lines.push(String::new());
            } else {
                is_magic = false;
                lines.push(l.to_string());
            }
        }
        let src = lines.join("\n");
        let (lang, source) = if is_magic && !src.is_empty() { split_magic(&src, language) } else { (language.cell_name().to_string(), src) };
        cells.push(Cell { id: uuid::Uuid::new_v4().simple().to_string(), language: lang, source, ..Default::default() });
    }
    if cells.is_empty() {
        cells.push(Cell { id: uuid::Uuid::new_v4().simple().to_string(), language: language.cell_name().into(), ..Default::default() });
    }
    Notebook { default_language: language.cell_name().into(), cells, widgets: json!({}) }
}

/// `%sql select 1` -> ("sql", "select 1"); unknown magics stay in source.
pub fn split_magic(src: &str, default: Language) -> (String, String) {
    let first = src.lines().next().unwrap_or("").trim();
    let rest = src.split_once('\n').map(|x| x.1).unwrap_or("");
    let magic = first.strip_prefix('%').map(|m| m.split_whitespace().next().unwrap_or("")).unwrap_or("");
    match magic {
        "sql" => ("sql".into(), rest.to_string()),
        "md" | "md-sandbox" | "markdown" => ("markdown".into(), rest.to_string()),
        "sh" => ("shell".into(), rest.to_string()),
        "python" | "py" => ("python".into(), rest.to_string()),
        "scala" => ("scala".into(), rest.to_string()),
        "r" => ("r".into(), rest.to_string()),
        _ => (default.cell_name().into(), src.to_string()),
    }
}

pub fn to_source(nb: &Notebook, language: Language) -> String {
    let prefix = language.comment_prefix();
    let mut out = format!("{prefix}Databricks notebook source\n");
    for (i, c) in nb.cells.iter().enumerate() {
        if i > 0 {
            out.push_str(&format!("\n{prefix}COMMAND ----------\n\n"));
        }
        if c.language == language.cell_name() {
            out.push_str(&c.source);
            out.push('\n');
        } else {
            let magic = match c.language.as_str() {
                "markdown" => "%md",
                "shell" => "%sh",
                "sql" => "%sql",
                "python" => "%python",
                "scala" => "%scala",
                "r" => "%r",
                other => other,
            };
            out.push_str(&format!("{prefix}MAGIC {magic}\n"));
            for l in c.source.lines() {
                if l.is_empty() {
                    out.push_str(&format!("{}MAGIC\n", prefix.trim_end()));
                } else {
                    out.push_str(&format!("{prefix}MAGIC {l}\n"));
                }
            }
        }
    }
    out
}

pub fn to_ipynb(nb: &Notebook) -> Value {
    let cells: Vec<Value> = nb
        .cells
        .iter()
        .map(|c| {
            if c.language == "markdown" {
                json!({ "cell_type": "markdown", "metadata": {}, "source": c.source })
            } else {
                let src = if c.language == nb.default_language { c.source.clone() } else { format!("%{}\n{}", if c.language == "shell" { "sh" } else { &c.language }, c.source) };
                let outputs: Vec<Value> = c
                    .outputs
                    .iter()
                    .filter_map(|o| match o.get("type").and_then(|t| t.as_str()) {
                        Some("stdout") => Some(json!({ "output_type": "stream", "name": "stdout", "text": o["text"] })),
                        Some("stderr") => Some(json!({ "output_type": "stream", "name": "stderr", "text": o["text"] })),
                        Some("result") => Some(json!({ "output_type": "execute_result", "execution_count": null, "metadata": {}, "data": { "text/plain": o["text"] } })),
                        Some("display") => Some(json!({ "output_type": "display_data", "metadata": {}, "data": { o["mime"].as_str().unwrap_or("text/plain"): o["data"] } })),
                        Some("error") => Some(json!({ "output_type": "error", "ename": o["ename"], "evalue": o["evalue"], "traceback": o["traceback"] })),
                        _ => None,
                    })
                    .collect();
                json!({ "cell_type": "code", "execution_count": null, "metadata": {}, "outputs": outputs, "source": src })
            }
        })
        .collect();
    json!({
        "cells": cells,
        "metadata": { "application/vnd.databricks.v1+notebook": { "language": nb.default_language, "notebookMetadata": {} }, "kernelspec": { "display_name": "Python 3", "language": "python", "name": "python3" }, "language_info": { "name": nb.default_language } },
        "nbformat": 4,
        "nbformat_minor": 5
    })
}

pub fn from_ipynb(v: &Value, language: Language) -> Notebook {
    let mut cells = vec![];
    for c in v["cells"].as_array().cloned().unwrap_or_default() {
        let source = match &c["source"] {
            Value::Array(a) => a.iter().filter_map(|s| s.as_str()).collect::<Vec<_>>().join(""),
            Value::String(s) => s.clone(),
            _ => String::new(),
        };
        let (lang, src) = if c["cell_type"] == "markdown" { ("markdown".to_string(), source) } else { split_magic(&source, language) };
        cells.push(Cell { id: uuid::Uuid::new_v4().simple().to_string(), language: lang, source: src, ..Default::default() });
    }
    Notebook { default_language: language.cell_name().into(), cells, widgets: json!({}) }
}

pub fn to_html(nb: &Notebook, path: &str) -> String {
    let mut body = String::new();
    for c in &nb.cells {
        let esc = html_escape(&c.source);
        if c.language == "markdown" {
            body.push_str(&format!("<section class=\"cell md\"><pre>{esc}</pre></section>\n"));
        } else {
            body.push_str(&format!("<section class=\"cell code\"><div class=\"lang\">{}</div><pre><code>{esc}</code></pre>", c.language));
            for o in &c.outputs {
                if let Some(t) = o.get("text").and_then(|t| t.as_str()) {
                    body.push_str(&format!("<pre class=\"out\">{}</pre>", html_escape(t)));
                }
            }
            body.push_str("</section>\n");
        }
    }
    format!("<!doctype html><html><head><meta charset=\"utf-8\"><title>{}</title><style>body{{font-family:system-ui;margin:2rem auto;max-width:900px}}.cell{{border:1px solid #ddd;border-radius:6px;margin:1rem 0;padding:.5rem 1rem}}.lang{{color:#888;font-size:.75rem}}.out{{background:#f6f8fa;padding:.5rem}}</style></head><body><h1>{}</h1>{}</body></html>", html_escape(path), html_escape(basename(path)), body)
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

// ---------------------------------------------------------------- handlers

#[derive(Debug, Deserialize)]
struct PathQ {
    path: String,
}

async fn list(State(st): State<S>, Query(q): Query<PathQ>) -> ApiResult<Json<Value>> {
    let obj = st.ws_require(&q.path).await?;
    if !matches!(obj.object_type, ObjectType::Directory | ObjectType::Repo) {
        return Ok(Json(json!({ "objects": [obj.status()] })));
    }
    let items = st.ws_list(&q.path).await?;
    Ok(Json(json!({ "objects": items.iter().map(|o| o.status()).collect::<Vec<_>>() })))
}

async fn get_status(State(st): State<S>, Query(q): Query<PathQ>) -> ApiResult<Json<Value>> {
    Ok(Json(st.ws_require(&q.path).await?.status()))
}

async fn mkdirs(State(st): State<S>, Who(p): Who, Body(b): Body<PathQ>) -> ApiResult<Json<Value>> {
    st.ws_mkdirs(&b.path, &p.user_name).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct DeleteBody {
    path: String,
    #[serde(default)]
    recursive: bool,
}

async fn delete(State(st): State<S>, Body(b): Body<DeleteBody>) -> ApiResult<Json<Value>> {
    st.ws_delete(&b.path, b.recursive).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
pub struct ImportBody {
    pub path: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub language: Option<Language>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub overwrite: bool,
}

fn guess_language(path: &str) -> Option<Language> {
    match path.rsplit('.').next() {
        Some("py") | Some("ipynb") => Some(Language::Python),
        Some("sql") => Some(Language::Sql),
        Some("scala") => Some(Language::Scala),
        Some("r") | Some("R") => Some(Language::R),
        _ => None,
    }
}

fn strip_notebook_ext(path: &str) -> String {
    for ext in [".py", ".sql", ".scala", ".r", ".ipynb", ".html"] {
        if let Some(s) = path.strip_suffix(ext) {
            return s.to_string();
        }
    }
    path.to_string()
}

pub async fn import_object(st: &AppState, user: &str, b: ImportBody) -> ApiResult<WsObject> {
    let bytes = match &b.content {
        Some(c) => base64::engine::general_purpose::STANDARD.decode(c).map_err(|e| ApiError::invalid(format!("content is not valid base64: {e}")))?,
        None => vec![],
    };
    let format = b.format.clone().unwrap_or_else(|| "AUTO".into()).to_ascii_uppercase();
    let text = String::from_utf8_lossy(&bytes).to_string();
    let lang = b.language.or_else(|| guess_language(&b.path));
    let is_db_source = text.contains("Databricks notebook source");
    match format.as_str() {
        "SOURCE" => {
            let lang = lang.ok_or_else(|| ApiError::invalid("language is required for SOURCE imports"))?;
            let nb = parse_source(&text, lang);
            st.ws_put_notebook(&strip_notebook_ext(&b.path), lang, nb, user, b.overwrite).await
        }
        "JUPYTER" => {
            let v: Value = serde_json::from_slice(&bytes).map_err(|e| ApiError::invalid(format!("invalid ipynb: {e}")))?;
            let lang = lang.unwrap_or(Language::Python);
            st.ws_put_notebook(&strip_notebook_ext(&b.path), lang, from_ipynb(&v, lang), user, b.overwrite).await
        }
        "HTML" | "DBC" | "R_MARKDOWN" => Err(ApiError::invalid(format!("Import format {format} is not supported; use SOURCE, JUPYTER, RAW or AUTO."))),
        "RAW" => st.ws_put_file(&b.path, &bytes, user, b.overwrite).await,
        _ => {
            // AUTO: Databricks-source text or .ipynb becomes a notebook, everything else a file.
            if b.path.ends_with(".ipynb") {
                let v: Value = serde_json::from_slice(&bytes).map_err(|e| ApiError::invalid(format!("invalid ipynb: {e}")))?;
                let lang = lang.unwrap_or(Language::Python);
                st.ws_put_notebook(&strip_notebook_ext(&b.path), lang, from_ipynb(&v, lang), user, b.overwrite).await
            } else if let (true, Some(lang)) = (is_db_source, lang) {
                st.ws_put_notebook(&strip_notebook_ext(&b.path), lang, parse_source(&text, lang), user, b.overwrite).await
            } else {
                st.ws_put_file(&b.path, &bytes, user, b.overwrite).await
            }
        }
    }
}

async fn import(State(st): State<S>, Who(p): Who, Body(b): Body<ImportBody>) -> ApiResult<Json<Value>> {
    import_object(&st, &p.user_name, b).await?;
    Ok(empty())
}

#[derive(Debug, Deserialize)]
struct ExportQ {
    path: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    direct_download: Option<bool>,
}

pub async fn export_bytes(st: &AppState, path: &str, format: &str) -> ApiResult<(Vec<u8>, String)> {
    let obj = st.ws_require(path).await?;
    match obj.object_type {
        ObjectType::Notebook => {
            let nb = obj.notebook.clone().unwrap_or_default();
            let lang = obj.language.unwrap_or(Language::Python);
            match format {
                "JUPYTER" => Ok((serde_json::to_vec_pretty(&to_ipynb(&nb))?, "ipynb".into())),
                "HTML" => Ok((to_html(&nb, &obj.path).into_bytes(), "html".into())),
                _ => Ok((to_source(&nb, lang).into_bytes(), lang.extension().into())),
            }
        }
        ObjectType::File => Ok((st.ws_read_file(&obj).await?, obj.path.rsplit('.').next().unwrap_or("bin").to_string())),
        _ => Err(ApiError::invalid(format!("Cannot export {:?} ({path}).", obj.object_type))),
    }
}

async fn export(State(st): State<S>, Query(q): Query<ExportQ>) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    let format = q.format.clone().unwrap_or_else(|| "SOURCE".into()).to_ascii_uppercase();
    let (bytes, file_type) = export_bytes(&st, &q.path, &format).await?;
    if q.direct_download == Some(true) {
        let name = format!("{}.{}", basename(&q.path), file_type);
        return Ok(([(axum::http::header::CONTENT_DISPOSITION, format!("attachment; filename=\"{name}\""))], bytes).into_response());
    }
    Ok(Json(json!({ "content": base64::engine::general_purpose::STANDARD.encode(&bytes), "file_type": file_type })).into_response())
}

#[derive(Debug, Deserialize)]
struct MoveBody {
    source_path: String,
    destination_path: String,
}

async fn move_h(State(st): State<S>, Who(p): Who, Body(b): Body<MoveBody>) -> ApiResult<Json<Value>> {
    st.ws_move(&b.source_path, &b.destination_path, &p.user_name).await?;
    Ok(empty())
}

/// Recursive listing used by the UI tree and search.
#[derive(Debug, Deserialize)]
struct SearchQ {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    q: Option<String>,
}

async fn search(State(st): State<S>, Query(q): Query<SearchQ>) -> ApiResult<Json<Value>> {
    let root = q.path.unwrap_or_else(|| "/".into());
    let needle = q.q.unwrap_or_default().to_ascii_lowercase();
    let items = st.ws_walk(&root).await?;
    let out: Vec<Value> = items.iter().filter(|o| needle.is_empty() || o.path.to_ascii_lowercase().contains(&needle)).map(|o| o.status()).collect();
    Ok(Json(json!({ "objects": out })))
}

/// Files stored in the workspace (`/api/2.0/workspace-files/...`).
async fn read_ws_file(State(st): State<S>, axum::extract::Path(path): axum::extract::Path<String>) -> ApiResult<axum::response::Response> {
    use axum::response::IntoResponse;
    let obj = st.ws_require(&format!("/{path}")).await?;
    if obj.object_type != ObjectType::File {
        return Err(ApiError::invalid("not a file"));
    }
    let bytes = st.ws_read_file(&obj).await?;
    let mime = mime_guess::from_path(&obj.path).first_or_octet_stream();
    Ok(([(axum::http::header::CONTENT_TYPE, mime.to_string())], bytes).into_response())
}

async fn write_ws_file(State(st): State<S>, Who(p): Who, axum::extract::Path(path): axum::extract::Path<String>, body: axum::body::Bytes) -> ApiResult<Json<Value>> {
    st.ws_put_file(&format!("/{path}"), &body, &p.user_name, true).await?;
    Ok(empty())
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/workspace/list", get(list))
        .route("/api/2.0/workspace/get-status", get(get_status))
        .route("/api/2.0/workspace/mkdirs", post(mkdirs))
        .route("/api/2.0/workspace/delete", post(delete))
        .route("/api/2.0/workspace/import", post(import))
        .route("/api/2.0/workspace/export", get(export))
        .route("/api/2.0/lakeforge/workspace/move", post(move_h))
        .route("/api/2.0/lakeforge/workspace/search", get(search))
        .route("/api/2.0/workspace-files/{*path}", get(read_ws_file).put(write_ws_file).post(write_ws_file))
}
