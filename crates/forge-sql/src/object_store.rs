//! Lazily register cloud object stores (S3 / GCS / Azure / HTTP) with a
//! DataFusion runtime based on a table URL. Credentials come from the
//! environment (the same variables the `object_store` crate documents), which
//! is how every Lakeforge deployment target injects them.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use object_store::ObjectStore;
use url::Url;

/// Ensure the object store for `url` is available in the runtime. The store is
/// keyed by scheme + authority, so all tables in the same bucket share one
/// client. Local `file://` paths need no registration.
pub fn ensure_object_store(runtime: &RuntimeEnv, url: &Url) -> DFResult<()> {
    let scheme = url.scheme();
    if matches!(scheme, "file" | "") {
        return Ok(());
    }
    let root = root_url(url)?;
    if runtime.object_store_registry.get_store(&root).is_ok() {
        return Ok(());
    }
    let store = build_store(&root)?;
    runtime.register_object_store(&root, store);
    tracing::info!(%root, "registered object store");
    Ok(())
}

/// The `scheme://authority/` prefix under which DataFusion keys object stores.
pub fn root_url(url: &Url) -> DFResult<Url> {
    let authority = url
        .host_str()
        .ok_or_else(|| DataFusionError::Plan(format!("url {url} has no host/bucket")))?;
    Url::parse(&format!("{}://{}/", url.scheme(), authority))
        .map_err(|e| DataFusionError::Plan(format!("invalid object store url {url}: {e}")))
}

fn build_store(root: &Url) -> DFResult<Arc<dyn ObjectStore>> {
    let bucket = root.host_str().unwrap_or_default();
    let store: Arc<dyn ObjectStore> = match root.scheme() {
        "s3" | "s3a" => Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ),
        "gs" | "gcs" => Arc::new(
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ),
        "az" | "abfs" | "abfss" | "azure" | "wasb" | "wasbs" | "adl" => Arc::new(
            MicrosoftAzureBuilder::from_env()
                .with_url(root.as_str())
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ),
        "http" | "https" => Arc::new(
            HttpBuilder::new()
                .with_url(root.as_str())
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ),
        other => {
            return Err(DataFusionError::Plan(format!(
                "unsupported object store scheme '{other}' in {root}"
            )))
        }
    };
    Ok(store)
}

/// Parse a user supplied location into a URL. Bare paths become `file://`
/// URLs; directories are normalised to end in `/`.
pub fn parse_location(location: &str) -> DFResult<Url> {
    if let Ok(url) = Url::parse(location) {
        if url.scheme().len() > 1 {
            return Ok(url);
        }
    }
    let path = std::path::Path::new(location);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let is_dir = abs.is_dir();
    let url = if is_dir {
        Url::from_directory_path(&abs)
    } else {
        Url::from_file_path(&abs)
    };
    url.map_err(|_| DataFusionError::Plan(format!("invalid local path {location}")))
}
