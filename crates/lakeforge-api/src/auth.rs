//! Authentication: personal access tokens (`dapi...`), UI session JWTs and
//! HTTP basic auth; users, groups and service principals as documents.

use std::sync::Arc;

use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use crate::store::{now_ms, Doc, Filter};

pub const KIND_USER: &str = "user";
pub const KIND_GROUP: &str = "group";
pub const KIND_TOKEN: &str = "token";
pub const ADMINS_GROUP: &str = "admins";
pub const USERS_GROUP: &str = "users";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_name: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub password_hash: Option<String>,
    #[serde(default = "default_true")]
    pub active: bool,
    #[serde(default)]
    pub groups: Vec<String>,
    /// Service principals are users with an application id and no password.
    #[serde(default)]
    pub application_id: Option<String>,
    #[serde(default)]
    pub entitlements: Vec<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    pub display_name: String,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default)]
    pub entitlements: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub token_id: String,
    pub comment: String,
    pub created_by_id: String,
    pub created_by_username: String,
    pub creation_time: i64,
    pub expiry_time: i64,
    pub hash: String,
    #[serde(default)]
    pub owner_id: String,
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub user_id: String,
    pub user_name: String,
    pub is_admin: bool,
    pub groups: Vec<String>,
}

impl Principal {
    pub fn require_admin(&self) -> ApiResult<()> {
        if self.is_admin {
            Ok(())
        } else {
            Err(ApiError::PermissionDenied("This operation requires workspace admin privileges.".into()))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    name: String,
    exp: usize,
    iat: usize,
}

pub struct Auth {
    enc: EncodingKey,
    dec: DecodingKey,
    seal_key: [u8; 32],
}

/// Authenticated encryption for secrets at rest (XChaCha20-Poly1305).
pub struct Sealer {
    key: [u8; 32],
}

impl Sealer {
    pub fn seal(&self, plain: &[u8]) -> ApiResult<String> {
        use chacha20poly1305::aead::{Aead, KeyInit};
        let cipher = chacha20poly1305::XChaCha20Poly1305::new((&self.key).into());
        let mut nonce = [0u8; 24];
        rand::rng().fill_bytes(&mut nonce);
        let ct = cipher.encrypt((&nonce).into(), plain).map_err(|_| ApiError::internal("encryption failed"))?;
        let mut out = nonce.to_vec();
        out.extend(ct);
        Ok(format!("v1:{}", base64::engine::general_purpose::STANDARD.encode(out)))
    }

    pub fn open(&self, sealed: &str) -> ApiResult<Vec<u8>> {
        use chacha20poly1305::aead::{Aead, KeyInit};
        let raw = sealed.strip_prefix("v1:").ok_or_else(|| ApiError::internal("unknown secret envelope"))?;
        let bytes = base64::engine::general_purpose::STANDARD.decode(raw).map_err(ApiError::internal)?;
        if bytes.len() < 24 {
            return Err(ApiError::internal("corrupt secret envelope"));
        }
        let (nonce, ct) = bytes.split_at(24);
        let cipher = chacha20poly1305::XChaCha20Poly1305::new((&self.key).into());
        cipher.decrypt(nonce.into(), ct).map_err(|_| ApiError::internal("secret decryption failed (key changed?)"))
    }
}

impl Auth {
    pub fn new(secret: &[u8]) -> Self {
        let seal_key: [u8; 32] = Sha256::digest([b"lakeforge-seal:".as_slice(), secret].concat()).into();
        Self { enc: EncodingKey::from_secret(secret), dec: DecodingKey::from_secret(secret), seal_key }
    }

    pub fn sealer(&self) -> Sealer {
        Sealer { key: self.seal_key }
    }

    pub fn issue_jwt(&self, user_id: &str, user_name: &str, ttl_secs: i64) -> ApiResult<String> {
        let now = chrono::Utc::now().timestamp();
        let claims = Claims { sub: user_id.into(), name: user_name.into(), exp: (now + ttl_secs) as usize, iat: now as usize };
        jsonwebtoken::encode(&Header::default(), &claims, &self.enc).map_err(ApiError::internal)
    }

    fn verify_jwt(&self, token: &str) -> Option<String> {
        jsonwebtoken::decode::<Claims>(token, &self.dec, &Validation::default()).ok().map(|d| d.claims.sub)
    }
}

pub fn hash_password(pw: &str) -> ApiResult<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default().hash_password(pw.as_bytes(), &salt).map(|h| h.to_string()).map_err(ApiError::internal)
}

pub fn verify_password(pw: &str, hash: &str) -> bool {
    PasswordHash::new(hash).map(|h| Argon2::default().verify_password(pw.as_bytes(), &h).is_ok()).unwrap_or(false)
}

pub fn hash_token(tok: &str) -> String {
    format!("{:x}", Sha256::digest(tok.as_bytes()))
}

pub fn new_token_value() -> String {
    let mut b = [0u8; 32];
    rand::rng().fill_bytes(&mut b);
    format!("dapi{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>())
}

pub fn random_secret() -> String {
    let mut b = [0u8; 48];
    rand::rng().fill_bytes(&mut b);
    base64::engine::general_purpose::STANDARD.encode(b)
}

impl AppState {
    pub async fn user_by_name(&self, name: &str) -> ApiResult<Option<Doc<User>>> {
        self.store.find_by_name(KIND_USER, self.ws(), None, name).await
    }

    pub async fn principal_for_user(&self, doc: &Doc<User>) -> ApiResult<Principal> {
        if !doc.data.active {
            return Err(ApiError::Unauthenticated("User is deactivated.".into()));
        }
        let mut groups = doc.data.groups.clone();
        // Groups also record their members; union both directions.
        let all: Vec<Doc<Group>> = self.store.list(KIND_GROUP, self.ws(), Filter::default()).await?;
        for g in &all {
            if g.data.members.contains(&doc.id) && !groups.contains(&g.data.display_name) {
                groups.push(g.data.display_name.clone());
            }
        }
        Ok(Principal {
            user_id: doc.id.clone(),
            user_name: doc.data.user_name.clone(),
            is_admin: groups.iter().any(|g| g == ADMINS_GROUP),
            groups,
        })
    }

    /// Principal for background work (schedulers) acting as `user_name`;
    /// falls back to the bootstrap admin.
    pub async fn system_principal(&self, user_name: &str) -> Principal {
        if let Ok(Some(doc)) = self.user_by_name(user_name).await {
            if let Ok(p) = self.principal_for_user(&doc).await {
                return p;
            }
        }
        if let Ok(Some(doc)) = self.user_by_name(&self.config.admin_user).await {
            if let Ok(p) = self.principal_for_user(&doc).await {
                return p;
            }
        }
        Principal { user_id: "system".into(), user_name: "system".into(), is_admin: true, groups: vec![ADMINS_GROUP.into()] }
    }

    pub async fn authenticate_password(&self, user_name: &str, password: &str) -> ApiResult<Principal> {
        let doc = self.user_by_name(user_name).await?.ok_or_else(|| ApiError::Unauthenticated("Invalid credentials.".into()))?;
        let ok = doc.data.password_hash.as_deref().map(|h| verify_password(password, h)).unwrap_or(false);
        if !ok {
            return Err(ApiError::Unauthenticated("Invalid credentials.".into()));
        }
        self.principal_for_user(&doc).await
    }

    pub async fn authenticate_bearer(&self, token: &str) -> ApiResult<Principal> {
        if token.starts_with("dapi") {
            let hash = hash_token(token);
            let tok: Doc<Token> = self
                .store
                .find_by_name(KIND_TOKEN, self.ws(), None, &hash)
                .await?
                .ok_or_else(|| ApiError::Unauthenticated("Invalid access token.".into()))?;
            if tok.data.expiry_time > 0 && tok.data.expiry_time < now_ms() {
                return Err(ApiError::Unauthenticated("Access token expired.".into()));
            }
            let user: Doc<User> = self.store.require(KIND_USER, &tok.data.owner_id, "User").await?;
            return self.principal_for_user(&user).await;
        }
        let uid = self.auth.verify_jwt(token).ok_or_else(|| ApiError::Unauthenticated("Invalid or expired session.".into()))?;
        let user: Doc<User> = self.store.require(KIND_USER, &uid, "User").await?;
        self.principal_for_user(&user).await
    }

    pub async fn create_token(&self, p: &Principal, comment: &str, lifetime_seconds: Option<i64>) -> ApiResult<(String, Token)> {
        let value = new_token_value();
        let now = now_ms();
        let tok = Token {
            token_id: uuid::Uuid::new_v4().simple().to_string(),
            comment: comment.to_string(),
            created_by_id: p.user_id.clone(),
            created_by_username: p.user_name.clone(),
            creation_time: now,
            expiry_time: lifetime_seconds.filter(|s| *s > 0).map(|s| now + s * 1000).unwrap_or(-1),
            hash: hash_token(&value),
            owner_id: p.user_id.clone(),
        };
        let hash = tok.hash.clone();
        self.store.insert(KIND_TOKEN, self.ws(), &tok.token_id, None, Some(&hash), &tok).await?;
        Ok((value, tok))
    }

    /// Create the initial admin user and default groups on first boot.
    pub async fn bootstrap_auth(&self) -> ApiResult<()> {
        for (name, id) in [(ADMINS_GROUP, "group-admins"), (USERS_GROUP, "group-users")] {
            if self.store.get::<Group>(KIND_GROUP, id).await?.is_none() {
                self.store
                    .insert(KIND_GROUP, self.ws(), id, None, Some(name), &Group { display_name: name.into(), members: vec![], entitlements: vec!["workspace-access".into(), "databricks-sql-access".into(), "allow-cluster-create".into()] })
                    .await?;
            }
        }
        let users: Vec<Doc<User>> = self.store.list(KIND_USER, self.ws(), Filter { limit: Some(1), ..Default::default() }).await?;
        if users.is_empty() {
            let id = uuid::Uuid::new_v4().simple().to_string();
            let user = User {
                user_name: self.config.admin_user.clone(),
                display_name: "Workspace Admin".into(),
                password_hash: Some(hash_password(&self.config.admin_password)?),
                active: true,
                groups: vec![ADMINS_GROUP.into(), USERS_GROUP.into()],
                application_id: None,
                entitlements: vec!["workspace-access".into(), "databricks-sql-access".into(), "allow-cluster-create".into()],
            };
            let name = user.user_name.clone();
            self.store.insert(KIND_USER, self.ws(), &id, None, Some(&name), &user).await?;
            tracing::warn!(user = %name, "created initial admin user; change the password via LAKEFORGE_ADMIN_PASSWORD");
        }
        Ok(())
    }

    /// Principal for the configured workspace admin (used for first-boot provisioning).
    pub async fn admin_principal(&self) -> ApiResult<Principal> {
        let doc = self
            .user_by_name(&self.config.admin_user)
            .await?
            .ok_or_else(|| ApiError::internal("admin user missing"))?;
        self.principal_for_user(&doc).await
    }
}

/// Paths that never require authentication.
fn is_public(path: &str) -> bool {
    path == "/health"
        || path == "/api/2.0/auth/login"
        || path == "/api/2.0/lakeforge/login"
        || path == "/api/2.0/lakeforge/info"
        || !path.starts_with("/api/")
}

pub async fn auth_middleware(State(state): State<Arc<AppState>>, mut req: Request, next: Next) -> Result<Response, ApiError> {
    if is_public(req.uri().path()) {
        return Ok(next.run(req).await);
    }
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| cookie_token(&req));
    let Some(header) = header else {
        return Err(ApiError::Unauthenticated("Missing Authorization header.".into()));
    };
    let principal = if let Some(tok) = header.strip_prefix("Bearer ").or_else(|| header.strip_prefix("bearer ")) {
        state.authenticate_bearer(tok.trim()).await?
    } else if let Some(b64) = header.strip_prefix("Basic ") {
        let raw = base64::engine::general_purpose::STANDARD.decode(b64.trim()).map_err(|_| ApiError::Unauthenticated("Malformed basic auth.".into()))?;
        let raw = String::from_utf8_lossy(&raw);
        let (u, p) = raw.split_once(':').ok_or_else(|| ApiError::Unauthenticated("Malformed basic auth.".into()))?;
        if u == "token" {
            state.authenticate_bearer(p).await?
        } else {
            state.authenticate_password(u, p).await?
        }
    } else {
        return Err(ApiError::Unauthenticated("Unsupported Authorization scheme.".into()));
    };
    req.extensions_mut().insert(principal);
    Ok(next.run(req).await)
}

fn cookie_token(req: &Request) -> Option<String> {
    let cookies = req.headers().get(axum::http::header::COOKIE)?.to_str().ok()?;
    cookies.split(';').map(str::trim).find_map(|c| c.strip_prefix("lakeforge_session=")).map(|v| format!("Bearer {v}"))
}

/// Axum extractor for the authenticated principal.
pub struct Who(pub Principal);

impl<S: Send + Sync> FromRequestParts<S> for Who {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Principal>()
            .cloned()
            .map(Who)
            .ok_or_else(|| ApiError::Unauthenticated("Not authenticated.".into()))
    }
}
