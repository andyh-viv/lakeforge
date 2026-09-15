//! Repos (Git folders) API — `/api/2.0/repos`.
//!
//! A repo is a git checkout kept under `work_dir/repos/<id>` and mirrored into
//! the workspace tree at `/Repos/<user>/<name>` (or the requested path).
//! `update` checks out a branch/tag and pulls, then re-imports; the Lakeforge
//! extension endpoints `commit`/`push` export the workspace tree back to the
//! checkout and run git.

use std::path::{Path as FsPath, PathBuf};

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::process::Command;

use super::workspace::{import_object, normalize, to_source, ImportBody, Language, ObjectType, WsObject};
use super::{empty, Body, S};
use crate::auth::{Principal, Who};
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND_REPO: &str = "repo";

const IGNORED_DIRS: &[&str] = &[".git", "__pycache__", ".ipynb_checkpoints", "node_modules", ".venv", "target"];

async fn git(dir: &FsPath, args: &[&str], env: &[(&str, &str)]) -> ApiResult<String> {
    let mut cmd = Command::new("git");
    cmd.args(args).current_dir(dir);
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = tokio::time::timeout(std::time::Duration::from_secs(300), cmd.output()).await.map_err(|_| ApiError::Unavailable("git operation timed out".into()))?.map_err(|e| ApiError::internal(format!("failed to run git: {e}")))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr).to_string();
        return Err(ApiError::invalid(format!("git {} failed: {}", args.first().unwrap_or(&""), redact(&err))));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Strip any `scheme://user:token@` credentials from git error output.
fn redact(s: &str) -> String {
    regex::Regex::new(r"://[^/\s]*@").map(|re| re.replace_all(s, "://***@").to_string()).unwrap_or_else(|_| s.to_string())
}

fn with_credentials(url: &str, creds: Option<&(String, String)>) -> String {
    let Some((user, token)) = creds else { return url.to_string() };
    if let Some(rest) = url.strip_prefix("https://") {
        if rest.contains('@') {
            return url.to_string();
        }
        return format!("https://{}:{}@{rest}", urlencoding(user), urlencoding(token));
    }
    url.to_string()
}

fn urlencoding(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn provider_of(url: &str) -> &'static str {
    let u = url.to_ascii_lowercase();
    if u.contains("github.com") {
        "gitHub"
    } else if u.contains("gitlab") {
        "gitLab"
    } else if u.contains("bitbucket") {
        "bitbucketCloud"
    } else if u.contains("dev.azure.com") || u.contains("visualstudio.com") {
        "azureDevOpsServices"
    } else if u.contains("aws") && u.contains("codecommit") {
        "awsCodeCommit"
    } else {
        "gitHubEnterprise"
    }
}

fn repo_name(url: &str) -> String {
    url.trim_end_matches('/').rsplit('/').next().unwrap_or("repo").trim_end_matches(".git").to_string()
}

fn head_commit(json: &Value) -> Option<&str> {
    json["head_commit_id"].as_str()
}

impl AppState {
    fn repo_dir(&self, id: &str) -> PathBuf {
        PathBuf::from(&self.config.work_dir).join("repos").join(id)
    }

    async fn repo_credentials(&self, p: &Principal, url: &str) -> Option<(String, String)> {
        self.git_token_for(p, Some(provider_of(url))).await.ok().flatten().or(self.git_token_for(p, None).await.ok().flatten())
    }

    /// Import a checkout directory into the workspace tree at `ws_root`.
    async fn import_checkout(&self, user: &str, dir: &FsPath, ws_root: &str, repo_id: &str, patterns: &[String]) -> ApiResult<usize> {
        let mut count = 0usize;
        let mut stack = vec![(dir.to_path_buf(), ws_root.to_string())];
        self.ws_mkdirs(ws_root, user).await?;
        while let Some((d, ws)) = stack.pop() {
            let mut rd = tokio::fs::read_dir(&d).await.map_err(|e| ApiError::internal(format!("read_dir {}: {e}", d.display())))?;
            while let Some(entry) = rd.next_entry().await.map_err(ApiError::internal)? {
                let name = entry.file_name().to_string_lossy().to_string();
                let ft = entry.file_type().await.map_err(ApiError::internal)?;
                let child_ws = format!("{}/{}", ws.trim_end_matches('/'), name);
                if ft.is_dir() {
                    if IGNORED_DIRS.contains(&name.as_str()) {
                        continue;
                    }
                    let rel = entry.path().strip_prefix(dir).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    if !patterns.is_empty() && !patterns.iter().any(|p| p.trim_end_matches('/') == rel || rel.starts_with(p.trim_end_matches('/'))) && !patterns.iter().any(|p| p.starts_with(&rel)) {
                        continue;
                    }
                    self.ws_mkdirs(&child_ws, user).await?;
                    stack.push((entry.path(), child_ws));
                } else if ft.is_file() {
                    let rel = entry.path().strip_prefix(dir).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    if !patterns.is_empty() && !patterns.iter().any(|p| rel.starts_with(p.trim_end_matches('/'))) {
                        continue;
                    }
                    let meta = entry.metadata().await.map_err(ApiError::internal)?;
                    if meta.len() > 10 * 1024 * 1024 {
                        continue;
                    }
                    let bytes = tokio::fs::read(entry.path()).await.map_err(ApiError::internal)?;
                    let content = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
                    let obj = import_object(self, user, ImportBody { path: child_ws.clone(), format: Some("AUTO".into()), language: None, content: Some(content), overwrite: true }).await?;
                    let mut obj = obj;
                    obj.repo_id = Some(repo_id.to_string());
                    self.store.upsert(super::workspace::KIND, self.ws(), &obj.path, super::workspace::parent_of(&obj.path).as_deref(), Some(&obj.path), &obj).await?;
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    /// Export the workspace tree under `ws_root` into `dir` (notebooks as source files).
    async fn export_to_checkout(&self, ws_root: &str, dir: &FsPath) -> ApiResult<usize> {
        let objs: Vec<WsObject> = self.ws_walk(ws_root).await?;
        let mut count = 0;
        for o in objs {
            let rel = o.path.strip_prefix(ws_root).unwrap_or(&o.path).trim_start_matches('/');
            if rel.is_empty() {
                continue;
            }
            let target = dir.join(rel);
            match o.object_type {
                ObjectType::Directory => {
                    tokio::fs::create_dir_all(&target).await.map_err(ApiError::internal)?;
                }
                ObjectType::Notebook => {
                    let lang = o.language.unwrap_or(Language::Python);
                    let nb = o.notebook.clone().unwrap_or_default();
                    let src = to_source(&nb, lang);
                    let file = if target.extension().is_some() { target.clone() } else { target.with_extension(lang.extension()) };
                    if let Some(parent) = file.parent() {
                        tokio::fs::create_dir_all(parent).await.map_err(ApiError::internal)?;
                    }
                    tokio::fs::write(&file, src).await.map_err(ApiError::internal)?;
                    count += 1;
                }
                ObjectType::File => {
                    let bytes = self.ws_read_file(&o).await?;
                    if let Some(parent) = target.parent() {
                        tokio::fs::create_dir_all(parent).await.map_err(ApiError::internal)?;
                    }
                    tokio::fs::write(&target, bytes).await.map_err(ApiError::internal)?;
                    count += 1;
                }
                _ => {}
            }
        }
        Ok(count)
    }

    async fn refresh_repo_meta(&self, id: &str) -> ApiResult<Value> {
        let dir = self.repo_dir(id);
        let head = git(&dir, &["rev-parse", "HEAD"], &[]).await.unwrap_or_default();
        let branch = git(&dir, &["rev-parse", "--abbrev-ref", "HEAD"], &[]).await.unwrap_or_default();
        let doc = self.store.update::<Value, _>(KIND_REPO, id, "Repo", |r| {
            r["head_commit_id"] = json!(head);
            if branch != "HEAD" {
                r["branch"] = json!(branch);
            }
            r["updated_at"] = json!(now_ms());
            Ok(())
        }).await?;
        Ok(doc.data)
    }
}

fn public_view(v: &Value) -> Value {
    json!({ "id": v["id"], "url": v["url"], "provider": v["provider"], "path": v["path"], "branch": v["branch"], "head_commit_id": v["head_commit_id"], "sparse_checkout": v["sparse_checkout"] })
}

#[derive(Debug, Deserialize)]
struct CreateRepo {
    url: String,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    sparse_checkout: Option<Value>,
    #[serde(default)]
    branch: Option<String>,
}

async fn create(State(st): State<S>, Who(p): Who, Body(b): Body<CreateRepo>) -> ApiResult<Json<Value>> {
    let id = st.store.next_seq("repo_id").await?;
    let id_s = id.to_string();
    let path = match &b.path {
        Some(p) => normalize(p)?,
        None => format!("/Repos/{}/{}", p.user_name, repo_name(&b.url)),
    };
    if !path.starts_with("/Repos/") && !path.starts_with("/Workspace/") && !path.starts_with("/Users/") {
        return Err(ApiError::invalid("Repo path must be under /Repos/<user>/ or /Users/<user>/"));
    }
    if st.ws_get(&path).await?.is_some() {
        return Err(ApiError::AlreadyExists(format!("Path ({path}) already exists.")));
    }
    let dir = st.repo_dir(&id_s);
    if let Some(parent) = dir.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(ApiError::internal)?;
    }
    let creds = st.repo_credentials(&p, &b.url).await;
    let url = with_credentials(&b.url, creds.as_ref());
    let mut args = vec!["clone", "--quiet"];
    if let Some(br) = &b.branch {
        args.extend(["--branch", br.as_str()]);
    }
    let patterns: Vec<String> = b.sparse_checkout.as_ref().and_then(|s| s["patterns"].as_array()).map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect()).unwrap_or_default();
    if !patterns.is_empty() {
        args.extend(["--filter=blob:none", "--sparse"]);
    }
    let dir_s = dir.to_string_lossy().to_string();
    args.extend([url.as_str(), dir_s.as_str()]);
    if let Err(e) = git(FsPath::new("."), &args, &[]).await {
        let _ = tokio::fs::remove_dir_all(&dir).await;
        return Err(e);
    }
    if !patterns.is_empty() {
        let mut a = vec!["sparse-checkout", "set", "--no-cone"];
        a.extend(patterns.iter().map(|s| s.as_str()));
        git(&dir, &a, &[]).await?;
    }
    let _ = git(&dir, &["config", "user.email", &p.user_name], &[]).await;
    let _ = git(&dir, &["config", "user.name", &p.user_name], &[]).await;
    let v = json!({ "id": id, "url": b.url, "provider": b.provider.clone().unwrap_or_else(|| provider_of(&b.url).to_string()), "path": path, "branch": b.branch, "head_commit_id": "", "sparse_checkout": if patterns.is_empty() { Value::Null } else { json!({ "patterns": patterns }) }, "creator_user_name": p.user_name, "created_at": now_ms() });
    if let Err(e) = st.store.insert(KIND_REPO, st.ws(), &id_s, Some(&p.user_name), Some(&path), &v).await {
        let _ = tokio::fs::remove_dir_all(&dir).await;
        return Err(e);
    }
    // Repo root object in the workspace.
    st.ws_mkdirs(&path, &p.user_name).await?;
    let mut root = st.ws_require(&path).await?;
    root.object_type = ObjectType::Repo;
    root.repo_id = Some(id_s.clone());
    st.store.upsert(super::workspace::KIND, st.ws(), &root.path, super::workspace::parent_of(&root.path).as_deref(), Some(&root.path), &root).await?;
    let n = st.import_checkout(&p.user_name, &dir, &path, &id_s, &patterns).await?;
    tracing::info!(repo = id, files = n, %path, "repo cloned");
    let v = st.refresh_repo_meta(&id_s).await?;
    Ok(Json(public_view(&v)))
}

#[derive(Debug, Deserialize)]
struct ListQ {
    #[serde(default)]
    path_prefix: Option<String>,
}

async fn list(State(st): State<S>, Query(q): Query<ListQ>) -> ApiResult<Json<Value>> {
    let docs: Vec<Doc<Value>> = st.store.list(KIND_REPO, st.ws(), Filter::default()).await?;
    let repos: Vec<Value> = docs.iter().filter(|d| q.path_prefix.as_ref().map(|pp| d.data["path"].as_str().map(|p| p.starts_with(pp.as_str())).unwrap_or(false)).unwrap_or(true)).map(|d| public_view(&d.data)).collect();
    Ok(Json(json!({ "repos": repos })))
}

async fn get_one(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let v = st.refresh_repo_meta(&id).await?;
    Ok(Json(public_view(&v)))
}

#[derive(Debug, Deserialize)]
struct UpdateRepo {
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    sparse_checkout: Option<Value>,
}

async fn update(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<UpdateRepo>) -> ApiResult<Json<Value>> {
    let doc: Doc<Value> = st.store.require(KIND_REPO, &id, "Repo").await?;
    let dir = st.repo_dir(&id);
    let url = doc.data["url"].as_str().unwrap_or("").to_string();
    let creds = st.repo_credentials(&p, &url).await;
    let auth_url = with_credentials(&url, creds.as_ref());
    let _ = git(&dir, &["remote", "set-url", "origin", &auth_url], &[]).await;
    git(&dir, &["fetch", "--quiet", "--all", "--tags", "--prune"], &[]).await?;
    if let Some(t) = &b.tag {
        git(&dir, &["checkout", "--quiet", &format!("tags/{t}")], &[]).await?;
    } else if let Some(br) = &b.branch {
        if git(&dir, &["rev-parse", "--verify", &format!("refs/heads/{br}")], &[]).await.is_ok() {
            git(&dir, &["checkout", "--quiet", br], &[]).await?;
            git(&dir, &["pull", "--quiet", "--ff-only", "origin", br], &[]).await.ok();
        } else if git(&dir, &["rev-parse", "--verify", &format!("refs/remotes/origin/{br}")], &[]).await.is_ok() {
            git(&dir, &["checkout", "--quiet", "-b", br, "--track", &format!("origin/{br}")], &[]).await?;
        } else {
            git(&dir, &["checkout", "--quiet", "-b", br], &[]).await?;
        }
    } else {
        git(&dir, &["pull", "--quiet", "--ff-only"], &[]).await.ok();
    }
    let _ = git(&dir, &["remote", "set-url", "origin", &url], &[]).await;
    let patterns: Vec<String> = b.sparse_checkout.as_ref().and_then(|s| s["patterns"].as_array()).map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect()).unwrap_or_else(|| doc.data["sparse_checkout"]["patterns"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect()).unwrap_or_default());
    if b.sparse_checkout.is_some() && !patterns.is_empty() {
        let mut a = vec!["sparse-checkout", "set", "--no-cone"];
        a.extend(patterns.iter().map(|s| s.as_str()));
        git(&dir, &a, &[]).await?;
    }
    let path = doc.data["path"].as_str().unwrap_or("").to_string();
    // Re-sync workspace tree: remove children then re-import.
    for c in st.ws_list(&path).await? {
        st.ws_delete(&c.path, true).await?;
    }
    st.import_checkout(&p.user_name, &dir, &path, &id, &patterns).await?;
    st.store.update::<Value, _>(KIND_REPO, &id, "Repo", |r| {
        if let Some(t) = &b.tag {
            r["tag"] = json!(t);
        }
        if b.sparse_checkout.is_some() {
            r["sparse_checkout"] = json!({ "patterns": patterns });
        }
        Ok(())
    }).await?;
    let v = st.refresh_repo_meta(&id).await?;
    Ok(Json(public_view(&v)))
}

async fn delete(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let doc: Doc<Value> = st.store.require(KIND_REPO, &id, "Repo").await?;
    if let Some(path) = doc.data["path"].as_str() {
        let _ = st.ws_delete(path, true).await;
    }
    let _ = tokio::fs::remove_dir_all(st.repo_dir(&id)).await;
    st.store.delete(KIND_REPO, &id).await?;
    Ok(empty())
}

// ------------------------------------------------- Lakeforge git extensions

async fn status(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let doc: Doc<Value> = st.store.require(KIND_REPO, &id, "Repo").await?;
    let dir = st.repo_dir(&id);
    let path = doc.data["path"].as_str().unwrap_or("").to_string();
    st.export_to_checkout(&path, &dir).await?;
    let porcelain = git(&dir, &["status", "--porcelain"], &[]).await?;
    let changes: Vec<Value> = porcelain.lines().filter(|l| l.len() > 3).map(|l| json!({ "status": l[..2].trim(), "path": l[3..].trim() })).collect();
    let branches = git(&dir, &["branch", "--format=%(refname:short)", "-a"], &[]).await.unwrap_or_default();
    let log = git(&dir, &["log", "--pretty=format:%H%x1f%an%x1f%at%x1f%s", "-n", "20"], &[]).await.unwrap_or_default();
    let commits: Vec<Value> = log.lines().filter_map(|l| {
        let parts: Vec<&str> = l.split('\u{1f}').collect();
        (parts.len() == 4).then(|| json!({ "sha": parts[0], "author": parts[1], "time": parts[2].parse::<i64>().unwrap_or(0) * 1000, "message": parts[3] }))
    }).collect();
    Ok(Json(json!({ "id": doc.data["id"], "branch": doc.data["branch"], "head_commit_id": head_commit(&doc.data), "changes": changes, "branches": branches.lines().map(|b| b.trim()).filter(|b| !b.is_empty()).collect::<Vec<_>>(), "commits": commits })))
}

#[derive(Debug, Deserialize)]
struct CommitBody {
    message: String,
    #[serde(default)]
    push: bool,
    #[serde(default)]
    files: Vec<String>,
}

async fn commit(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<CommitBody>) -> ApiResult<Json<Value>> {
    let doc: Doc<Value> = st.store.require(KIND_REPO, &id, "Repo").await?;
    let dir = st.repo_dir(&id);
    let path = doc.data["path"].as_str().unwrap_or("").to_string();
    st.export_to_checkout(&path, &dir).await?;
    let _ = git(&dir, &["config", "user.email", &p.user_name], &[]).await;
    let _ = git(&dir, &["config", "user.name", &p.user_name], &[]).await;
    if b.files.is_empty() {
        git(&dir, &["add", "-A"], &[]).await?;
    } else {
        let mut a = vec!["add", "--"];
        a.extend(b.files.iter().map(|s| s.as_str()));
        git(&dir, &a, &[]).await?;
    }
    let staged = git(&dir, &["diff", "--cached", "--name-only"], &[]).await?;
    if staged.is_empty() {
        return Err(ApiError::invalid("Nothing to commit"));
    }
    git(&dir, &["commit", "--quiet", "-m", &b.message], &[]).await?;
    let mut pushed = false;
    if b.push {
        let url = doc.data["url"].as_str().unwrap_or("").to_string();
        let creds = st.repo_credentials(&p, &url).await;
        let auth_url = with_credentials(&url, creds.as_ref());
        let branch = git(&dir, &["rev-parse", "--abbrev-ref", "HEAD"], &[]).await?;
        let res = git(&dir, &["push", "--quiet", &auth_url, &format!("HEAD:refs/heads/{branch}")], &[]).await;
        res?;
        pushed = true;
    }
    let v = st.refresh_repo_meta(&id).await?;
    Ok(Json(json!({ "head_commit_id": v["head_commit_id"], "branch": v["branch"], "pushed": pushed, "files": staged.lines().collect::<Vec<_>>() })))
}

#[derive(Debug, Deserialize)]
struct BranchBody {
    name: String,
}

async fn create_branch(State(st): State<S>, Who(p): Who, Path(id): Path<String>, Body(b): Body<BranchBody>) -> ApiResult<Json<Value>> {
    let doc: Doc<Value> = st.store.require(KIND_REPO, &id, "Repo").await?;
    let dir = st.repo_dir(&id);
    let path = doc.data["path"].as_str().unwrap_or("").to_string();
    st.export_to_checkout(&path, &dir).await?;
    git(&dir, &["checkout", "--quiet", "-b", &b.name], &[]).await?;
    let _ = p;
    let v = st.refresh_repo_meta(&id).await?;
    Ok(Json(public_view(&v)))
}

async fn repo_by_path(State(st): State<S>, Query(q): Query<ListQ>) -> ApiResult<Json<Value>> {
    let path = q.path_prefix.ok_or_else(|| ApiError::invalid("path_prefix is required"))?;
    let docs: Vec<Doc<Value>> = st.store.list(KIND_REPO, st.ws(), Filter::default()).await?;
    let found = docs.into_iter().find(|d| d.data["path"].as_str().map(|p| path == p || path.starts_with(&format!("{p}/"))).unwrap_or(false)).ok_or_else(|| ApiError::NotFound(format!("No repo contains {path}")))?;
    Ok(Json(public_view(&found.data)))
}

async fn repo_permissions(State(st): State<S>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    let acl = st.get_permissions("repos", &id).await?;
    Ok(Json(json!({ "object_id": format!("/repos/{id}"), "object_type": "repo", "access_control_list": acl })))
}

pub fn router() -> Router<S> {
    Router::new()
        .route("/api/2.0/repos", get(list).post(create))
        .route("/api/2.0/repos/{id}", get(get_one).patch(update).delete(delete))
        .route("/api/2.0/repos/{id}/permissions", get(repo_permissions))
        .route("/api/2.0/lakeforge/repos/{id}/status", get(status))
        .route("/api/2.0/lakeforge/repos/{id}/commit", post(commit))
        .route("/api/2.0/lakeforge/repos/{id}/branches", post(create_branch))
        .route("/api/2.0/lakeforge/repos/by-path", get(repo_by_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_providers() {
        assert_eq!(repo_name("https://github.com/andyh-viv/lakeforge.git"), "lakeforge");
        assert_eq!(provider_of("https://github.com/x/y"), "gitHub");
        assert_eq!(with_credentials("https://github.com/x/y.git", Some(&("u".into(), "t k".into()))), "https://u:t%20k@github.com/x/y.git");
        assert_eq!(redact("fatal: https://u:tok@github.com/x"), "fatal: https://***@github.com/x");
    }
}
