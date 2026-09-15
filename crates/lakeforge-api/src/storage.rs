//! Workspace object storage (DBFS root, Files API, notebook exports, MLflow
//! artifacts). Backed by `object_store`, so the same code targets a local
//! directory, S3, GCS or Azure Blob depending on `LAKEFORGE_STORAGE_ROOT`.

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};

use crate::error::ApiResult;

#[derive(Clone)]
pub struct Storage {
    store: Arc<dyn ObjectStore>,
    prefix: ObjPath,
    pub root_url: String,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub path: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified_ms: i64,
}

impl Storage {
    pub fn open(root: &str) -> ApiResult<Self> {
        if root.contains("://") {
            let url = url::Url::parse(root).map_err(|e| crate::error::ApiError::invalid(format!("bad storage root: {e}")))?;
            if url.scheme() == "file" {
                let dir = url.to_file_path().map_err(|_| crate::error::ApiError::invalid("bad file:// storage root"))?;
                std::fs::create_dir_all(&dir)?;
                let store = LocalFileSystem::new_with_prefix(&dir)?;
                return Ok(Self { store: Arc::new(store), prefix: ObjPath::default(), root_url: root.to_string() });
            }
            let (store, prefix) = object_store::parse_url_opts(&url, std::env::vars())?;
            Ok(Self { store: Arc::from(store), prefix, root_url: root.to_string() })
        } else {
            std::fs::create_dir_all(root)?;
            let store = LocalFileSystem::new_with_prefix(std::fs::canonicalize(root)?)?;
            Ok(Self { store: Arc::new(store), prefix: ObjPath::default(), root_url: format!("file://{}", std::fs::canonicalize(root)?.display()) })
        }
    }

    /// Absolute URL of `path` (e.g. `file:///…/tables/x` or `s3://bucket/prefix/tables/x`)
    /// usable as a table location by the Forge engine.
    pub fn url_for(&self, path: &str) -> String {
        let clean = path.trim_matches('/');
        let root = self.root_url.trim_end_matches('/');
        if clean.is_empty() {
            root.to_string()
        } else {
            format!("{root}/{clean}")
        }
    }

    /// Inverse of [`url_for`]: relative storage path if `url` lives under this root.
    pub fn path_of(&self, url: &str) -> Option<String> {
        let root = self.root_url.trim_end_matches('/');
        url.strip_prefix(root).map(|r| format!("/{}", r.trim_start_matches('/')))
    }

    fn full(&self, path: &str) -> ObjPath {
        let clean = path.trim_matches('/');
        if self.prefix.as_ref().is_empty() {
            ObjPath::from(clean)
        } else if clean.is_empty() {
            self.prefix.clone()
        } else {
            ObjPath::from(format!("{}/{}", self.prefix.as_ref(), clean))
        }
    }

    fn strip(&self, p: &ObjPath) -> String {
        let s = p.as_ref();
        let pre = self.prefix.as_ref();
        let rel = if pre.is_empty() { s } else { s.strip_prefix(pre).unwrap_or(s).trim_start_matches('/') };
        format!("/{rel}")
    }

    pub async fn put(&self, path: &str, data: Bytes) -> ApiResult<()> {
        self.store.put(&self.full(path), PutPayload::from(data)).await?;
        Ok(())
    }

    pub async fn get(&self, path: &str) -> ApiResult<Bytes> {
        Ok(self.store.get(&self.full(path)).await?.bytes().await?)
    }

    pub async fn get_range(&self, path: &str, offset: u64, len: u64) -> ApiResult<(Bytes, u64)> {
        let p = self.full(path);
        let meta = self.store.head(&p).await?;
        let size = meta.size;
        if offset >= size {
            return Ok((Bytes::new(), size));
        }
        let end = (offset + len).min(size);
        Ok((self.store.get_range(&p, offset..end).await?, size))
    }

    pub async fn head(&self, path: &str) -> ApiResult<Option<ObjectMeta>> {
        match self.store.head(&self.full(path)).await {
            Ok(m) => Ok(Some(m)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn delete(&self, path: &str) -> ApiResult<()> {
        match self.store.delete(&self.full(path)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn delete_prefix(&self, path: &str) -> ApiResult<u64> {
        let p = self.full(path);
        let paths: Vec<ObjPath> = self.store.list(Some(&p)).map_ok(|m| m.location).try_collect().await?;
        let n = paths.len() as u64;
        for chunk in paths.chunks(256) {
            let owned: Vec<ObjPath> = chunk.to_vec();
            let stream = futures::stream::iter(owned.into_iter().map(Ok));
            self.store.delete_stream(Box::pin(stream)).try_collect::<Vec<_>>().await?;
        }
        Ok(n)
    }

    pub async fn copy(&self, from: &str, to: &str) -> ApiResult<()> {
        self.store.copy(&self.full(from), &self.full(to)).await?;
        Ok(())
    }

    pub async fn rename(&self, from: &str, to: &str) -> ApiResult<()> {
        self.store.rename(&self.full(from), &self.full(to)).await?;
        Ok(())
    }

    /// Directory-style listing (immediate children).
    pub async fn list_dir(&self, path: &str) -> ApiResult<Vec<Entry>> {
        let p = self.full(path);
        let prefix = if p.as_ref().is_empty() { None } else { Some(&p) };
        let res = self.store.list_with_delimiter(prefix).await?;
        let mut out: Vec<Entry> = res
            .common_prefixes
            .iter()
            .map(|d| Entry { path: self.strip(d), is_dir: true, size: 0, modified_ms: 0 })
            .collect();
        for o in res.objects {
            if o.location.filename() == Some(".dir") {
                continue;
            }
            out.push(Entry {
                path: self.strip(&o.location),
                is_dir: false,
                size: o.size,
                modified_ms: o.last_modified.timestamp_millis(),
            });
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    /// Recursive listing of all objects under a prefix.
    pub async fn list_all(&self, path: &str) -> ApiResult<Vec<Entry>> {
        let p = self.full(path);
        let prefix = if p.as_ref().is_empty() { None } else { Some(&p) };
        let metas: Vec<ObjectMeta> = self.store.list(prefix).try_collect().await?;
        Ok(metas
            .into_iter()
            .filter(|o| o.location.filename() != Some(".dir"))
            .map(|o| Entry { path: self.strip(&o.location), is_dir: false, size: o.size, modified_ms: o.last_modified.timestamp_millis() })
            .collect())
    }

    /// Object stores have no directories; we materialise one with a marker.
    pub async fn mkdirs(&self, path: &str) -> ApiResult<()> {
        let marker = format!("{}/.dir", path.trim_end_matches('/'));
        self.put(&marker, Bytes::new()).await
    }

    pub async fn is_dir(&self, path: &str) -> ApiResult<bool> {
        let p = self.full(path);
        if p.as_ref().is_empty() {
            return Ok(true);
        }
        let res = self.store.list_with_delimiter(Some(&p)).await?;
        Ok(!res.common_prefixes.is_empty() || !res.objects.is_empty())
    }
}
