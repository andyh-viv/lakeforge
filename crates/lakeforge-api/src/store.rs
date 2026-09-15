//! Document store over SQLite or Postgres (`sqlx::Any`).
//!
//! Every control-plane resource is a JSON document with a handful of indexed
//! columns (`kind`, `workspace_id`, `parent_id`, `name`). This keeps the schema
//! portable between the embedded SQLite used for single-node installs and the
//! managed Postgres used in cloud deployments.

use serde::de::DeserializeOwned;
use serde::Serialize;
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, Row};

use crate::error::{ApiError, ApiResult};

#[derive(Clone)]
pub struct Store {
    pool: AnyPool,
    dialect: Dialect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
}

#[derive(Debug, Clone)]
pub struct Doc<T> {
    pub id: String,
    pub kind: String,
    pub workspace_id: String,
    pub parent_id: Option<String>,
    pub name: Option<String>,
    pub data: T,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Default, Clone)]
pub struct Filter<'a> {
    pub parent_id: Option<&'a str>,
    pub name: Option<&'a str>,
    pub name_prefix: Option<&'a str>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub newest_first: bool,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS docs (
        id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        workspace_id TEXT NOT NULL,
        parent_id TEXT,
        name TEXT,
        data TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS docs_kind_ws ON docs (kind, workspace_id, parent_id, name)",
    "CREATE INDEX IF NOT EXISTS docs_kind_created ON docs (kind, workspace_id, created_at)",
    "CREATE TABLE IF NOT EXISTS kv (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL,
        updated_at BIGINT NOT NULL
    )",
];

impl Store {
    pub async fn connect(url: &str) -> ApiResult<Self> {
        sqlx::any::install_default_drivers();
        let dialect = if url.starts_with("postgres") { Dialect::Postgres } else { Dialect::Sqlite };
        if dialect == Dialect::Sqlite {
            if let Some(path) = url.strip_prefix("sqlite://").or_else(|| url.strip_prefix("sqlite:")) {
                let path = path.split('?').next().unwrap_or(path);
                if path != ":memory:" {
                    if let Some(dir) = std::path::Path::new(path).parent() {
                        if !dir.as_os_str().is_empty() {
                            std::fs::create_dir_all(dir)?;
                        }
                    }
                }
            }
        }
        let pool = AnyPoolOptions::new()
            .max_connections(if dialect == Dialect::Sqlite { 1 } else { 16 })
            .connect(url)
            .await?;
        let store = Self { pool, dialect };
        store.migrate().await?;
        Ok(store)
    }

    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    async fn migrate(&self) -> ApiResult<()> {
        if self.dialect == Dialect::Sqlite {
            sqlx::query("PRAGMA journal_mode=WAL").execute(&self.pool).await.ok();
            sqlx::query("PRAGMA busy_timeout=5000").execute(&self.pool).await.ok();
        }
        for stmt in SCHEMA {
            sqlx::query(stmt).execute(&self.pool).await?;
        }
        Ok(())
    }

    fn row_to_doc<T: DeserializeOwned>(row: AnyRow) -> ApiResult<Doc<T>> {
        let data: String = row.try_get("data")?;
        Ok(Doc {
            id: row.try_get("id")?,
            kind: row.try_get("kind")?,
            workspace_id: row.try_get("workspace_id")?,
            parent_id: row.try_get("parent_id")?,
            name: row.try_get("name")?,
            data: serde_json::from_str(&data).map_err(|e| ApiError::Internal(format!("corrupt document: {e}")))?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    pub async fn insert<T: Serialize + DeserializeOwned>(
        &self,
        kind: &str,
        workspace_id: &str,
        id: &str,
        parent_id: Option<&str>,
        name: Option<&str>,
        data: &T,
    ) -> ApiResult<Doc<T>> {
        let now = now_ms();
        let json = serde_json::to_string(data)?;
        let res = sqlx::query(
            "INSERT INTO docs (id, kind, workspace_id, parent_id, name, data, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(id)
        .bind(kind)
        .bind(workspace_id)
        .bind(parent_id)
        .bind(name)
        .bind(&json)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => {}
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                return Err(ApiError::AlreadyExists(format!("{kind} {id} already exists.")));
            }
            Err(e) => return Err(e.into()),
        }
        Ok(Doc {
            id: id.to_string(),
            kind: kind.to_string(),
            workspace_id: workspace_id.to_string(),
            parent_id: parent_id.map(str::to_string),
            name: name.map(str::to_string),
            data: serde_json::from_str(&json)?,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn get<T: DeserializeOwned>(&self, kind: &str, id: &str) -> ApiResult<Option<Doc<T>>> {
        let row = sqlx::query("SELECT * FROM docs WHERE kind = $1 AND id = $2")
            .bind(kind)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(Self::row_to_doc).transpose()
    }

    pub async fn require<T: DeserializeOwned>(&self, kind: &str, id: &str, what: &str) -> ApiResult<Doc<T>> {
        self.get(kind, id).await?.ok_or_else(|| ApiError::not_found(what, id))
    }

    pub async fn find_by_name<T: DeserializeOwned>(
        &self,
        kind: &str,
        workspace_id: &str,
        parent_id: Option<&str>,
        name: &str,
    ) -> ApiResult<Option<Doc<T>>> {
        let row = sqlx::query(
            "SELECT * FROM docs WHERE kind = $1 AND workspace_id = $2 AND name = $3
             AND (($4 IS NULL AND parent_id IS NULL) OR parent_id = $4) LIMIT 1",
        )
        .bind(kind)
        .bind(workspace_id)
        .bind(name)
        .bind(parent_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Self::row_to_doc).transpose()
    }

    pub async fn list<T: DeserializeOwned>(&self, kind: &str, workspace_id: &str, f: Filter<'_>) -> ApiResult<Vec<Doc<T>>> {
        let mut sql = String::from("SELECT * FROM docs WHERE kind = $1 AND workspace_id = $2");
        if f.parent_id.is_some() {
            sql.push_str(" AND parent_id = $3");
        } else {
            sql.push_str(" AND ($3 IS NULL OR parent_id = $3)");
        }
        sql.push_str(" AND ($4 IS NULL OR name = $4)");
        sql.push_str(" AND ($5 IS NULL OR name LIKE $5)");
        sql.push_str(if f.newest_first { " ORDER BY created_at DESC, id DESC" } else { " ORDER BY created_at ASC, id ASC" });
        sql.push_str(" LIMIT $6 OFFSET $7");
        let prefix = f.name_prefix.map(|p| format!("{}%", p.replace('%', "\\%").replace('_', "\\_")));
        let rows = sqlx::query(&sql)
            .bind(kind)
            .bind(workspace_id)
            .bind(f.parent_id)
            .bind(f.name)
            .bind(prefix.as_deref())
            .bind(f.limit.unwrap_or(10_000))
            .bind(f.offset.unwrap_or(0))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Self::row_to_doc).collect()
    }

    pub async fn count(&self, kind: &str, workspace_id: &str, parent_id: Option<&str>) -> ApiResult<i64> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS n FROM docs WHERE kind = $1 AND workspace_id = $2 AND ($3 IS NULL OR parent_id = $3)",
        )
        .bind(kind)
        .bind(workspace_id)
        .bind(parent_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get::<i64, _>("n")?)
    }

    pub async fn put<T: Serialize>(&self, kind: &str, id: &str, parent_id: Option<&str>, name: Option<&str>, data: &T) -> ApiResult<()> {
        let json = serde_json::to_string(data)?;
        let n = sqlx::query("UPDATE docs SET data = $1, parent_id = $2, name = $3, updated_at = $4 WHERE kind = $5 AND id = $6")
            .bind(&json)
            .bind(parent_id)
            .bind(name)
            .bind(now_ms())
            .bind(kind)
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if n == 0 {
            return Err(ApiError::not_found(kind, id));
        }
        Ok(())
    }

    pub async fn upsert<T: Serialize + DeserializeOwned>(&self, kind: &str, workspace_id: &str, id: &str, parent_id: Option<&str>, name: Option<&str>, data: &T) -> ApiResult<()> {
        match self.put(kind, id, parent_id, name, data).await {
            Ok(()) => Ok(()),
            Err(ApiError::NotFound(_)) => self.insert(kind, workspace_id, id, parent_id, name, data).await.map(|_| ()),
            Err(e) => Err(e),
        }
    }

    /// Read-modify-write helper. `f` returns `false` to abort without writing.
    pub async fn update<T, F>(&self, kind: &str, id: &str, what: &str, f: F) -> ApiResult<Doc<T>>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(&mut T) -> ApiResult<()>,
    {
        let mut doc = self.require::<T>(kind, id, what).await?;
        f(&mut doc.data)?;
        self.put(kind, id, doc.parent_id.as_deref(), doc.name.as_deref(), &doc.data).await?;
        doc.updated_at = now_ms();
        Ok(doc)
    }

    pub async fn delete(&self, kind: &str, id: &str) -> ApiResult<bool> {
        let n = sqlx::query("DELETE FROM docs WHERE kind = $1 AND id = $2")
            .bind(kind)
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(n > 0)
    }

    pub async fn delete_children(&self, kind: &str, parent_id: &str) -> ApiResult<u64> {
        Ok(sqlx::query("DELETE FROM docs WHERE kind = $1 AND parent_id = $2")
            .bind(kind)
            .bind(parent_id)
            .execute(&self.pool)
            .await?
            .rows_affected())
    }

    pub async fn kv_get(&self, key: &str) -> ApiResult<Option<String>> {
        let row = sqlx::query("SELECT v FROM kv WHERE k = $1").bind(key).fetch_optional(&self.pool).await?;
        Ok(row.map(|r| r.try_get::<String, _>("v")).transpose()?)
    }

    pub async fn kv_set(&self, key: &str, value: &str) -> ApiResult<()> {
        let sql = match self.dialect {
            Dialect::Sqlite | Dialect::Postgres => {
                "INSERT INTO kv (k, v, updated_at) VALUES ($1, $2, $3)
                 ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = EXCLUDED.updated_at"
            }
        };
        sqlx::query(sql).bind(key).bind(value).bind(now_ms()).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn kv_delete(&self, key: &str) -> ApiResult<()> {
        sqlx::query("DELETE FROM kv WHERE k = $1").bind(key).execute(&self.pool).await?;
        Ok(())
    }

    /// Monotonic per-kind sequence used for Databricks-style numeric ids.
    pub async fn next_seq(&self, name: &str) -> ApiResult<i64> {
        let key = format!("seq:{name}");
        let cur: i64 = self.kv_get(&key).await?.and_then(|v| v.parse().ok()).unwrap_or(1000);
        let next = cur + 1;
        self.kv_set(&key, &next.to_string()).await?;
        Ok(next)
    }
}
