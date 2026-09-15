//! Engine configuration shared by driver and executors.

use serde::{Deserialize, Serialize};

/// Keys understood by the Forge session configuration. These mirror the Spark
/// configuration names Databricks users are familiar with where sensible.
pub mod keys {
    pub const SHUFFLE_PARTITIONS: &str = "forge.sql.shuffle.partitions";
    pub const BATCH_SIZE: &str = "forge.sql.execution.batchSize";
    pub const TARGET_PARTITIONS: &str = "forge.sql.files.targetPartitions";
    pub const TASK_MAX_RETRIES: &str = "forge.task.maxRetries";
    pub const ADAPTIVE_ENABLED: &str = "forge.sql.adaptive.enabled";
    pub const ADAPTIVE_COALESCE_TARGET_BYTES: &str = "forge.sql.adaptive.coalesceTargetBytes";
    pub const SPECULATION_ENABLED: &str = "forge.speculation.enabled";
    pub const MEMORY_LIMIT_BYTES: &str = "forge.memory.limitBytes";
    pub const DEFAULT_CATALOG: &str = "forge.sql.defaultCatalog";
    pub const DEFAULT_SCHEMA: &str = "forge.sql.defaultSchema";

    /// Spark aliases mapped to Forge keys.
    pub fn normalize(key: &str) -> &str {
        match key {
            "spark.sql.shuffle.partitions" => SHUFFLE_PARTITIONS,
            "spark.sql.adaptive.enabled" => ADAPTIVE_ENABLED,
            "spark.task.maxFailures" => TASK_MAX_RETRIES,
            "spark.speculation" => SPECULATION_ENABLED,
            "spark.sql.defaultCatalog" | "spark.databricks.sql.initial.catalog.name" => DEFAULT_CATALOG,
            "spark.sql.defaultSchema" => DEFAULT_SCHEMA,
            other => other,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSettings {
    pub shuffle_partitions: usize,
    pub batch_size: usize,
    pub target_partitions: usize,
    pub task_max_retries: u32,
    pub adaptive_enabled: bool,
    pub adaptive_coalesce_target_bytes: u64,
    pub speculation_enabled: bool,
    pub memory_limit_bytes: Option<u64>,
    pub default_catalog: Option<String>,
    pub default_schema: Option<String>,
}

impl Default for SessionSettings {
    fn default() -> Self {
        Self {
            shuffle_partitions: 16,
            batch_size: 8192,
            target_partitions: 8,
            task_max_retries: 3,
            adaptive_enabled: true,
            adaptive_coalesce_target_bytes: 64 * 1024 * 1024,
            speculation_enabled: false,
            memory_limit_bytes: None,
            default_catalog: None,
            default_schema: None,
        }
    }
}

impl SessionSettings {
    /// Defaults overridden by `FORGE_CONF_<KEY>` environment variables, where
    /// `KEY` is the setting name upper-cased with dots replaced by `_`
    /// (e.g. `FORGE_CONF_FORGE_SQL_SHUFFLE_PARTITIONS=32`).
    pub fn from_env() -> Self {
        let mut s = Self::default();
        for (k, v) in std::env::vars() {
            if let Some(rest) = k.strip_prefix("FORGE_CONF_") {
                let key = rest.to_ascii_lowercase().replace('_', ".");
                let key = match key.as_str() {
                    "forge.sql.shuffle.partitions" => keys::SHUFFLE_PARTITIONS,
                    "forge.sql.execution.batchsize" => keys::BATCH_SIZE,
                    "forge.sql.files.targetpartitions" => keys::TARGET_PARTITIONS,
                    "forge.task.maxretries" => keys::TASK_MAX_RETRIES,
                    "forge.sql.adaptive.enabled" => keys::ADAPTIVE_ENABLED,
                    "forge.sql.adaptive.coalescetargetbytes" => keys::ADAPTIVE_COALESCE_TARGET_BYTES,
                    "forge.speculation.enabled" => keys::SPECULATION_ENABLED,
                    "forge.memory.limitbytes" => keys::MEMORY_LIMIT_BYTES,
                    "forge.sql.defaultcatalog" => keys::DEFAULT_CATALOG,
                    "forge.sql.defaultschema" => keys::DEFAULT_SCHEMA,
                    other => other,
                };
                s.apply(key, &v);
            }
        }
        s
    }

    /// Build from a key/value map (Spark aliases accepted).
    pub fn from_map<'a, I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut s = Self::default();
        for (k, v) in pairs {
            s.apply(k, v);
        }
        s
    }

    pub fn apply(&mut self, key: &str, value: &str) {
        match keys::normalize(key) {
            keys::SHUFFLE_PARTITIONS => {
                if let Ok(v) = value.parse() {
                    self.shuffle_partitions = v
                }
            }
            keys::BATCH_SIZE => {
                if let Ok(v) = value.parse() {
                    self.batch_size = v
                }
            }
            keys::TARGET_PARTITIONS => {
                if let Ok(v) = value.parse() {
                    self.target_partitions = v
                }
            }
            keys::TASK_MAX_RETRIES => {
                if let Ok(v) = value.parse() {
                    self.task_max_retries = v
                }
            }
            keys::ADAPTIVE_ENABLED => self.adaptive_enabled = value.eq_ignore_ascii_case("true"),
            keys::ADAPTIVE_COALESCE_TARGET_BYTES => {
                if let Ok(v) = value.parse() {
                    self.adaptive_coalesce_target_bytes = v
                }
            }
            keys::SPECULATION_ENABLED => {
                self.speculation_enabled = value.eq_ignore_ascii_case("true")
            }
            keys::MEMORY_LIMIT_BYTES => self.memory_limit_bytes = value.parse().ok(),
            keys::DEFAULT_CATALOG => self.default_catalog = Some(value.to_string()).filter(|v| !v.is_empty()),
            keys::DEFAULT_SCHEMA => self.default_schema = Some(value.to_string()).filter(|v| !v.is_empty()),
            _ => {}
        }
    }

    pub fn to_map(&self) -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert(keys::SHUFFLE_PARTITIONS.into(), self.shuffle_partitions.to_string());
        m.insert(keys::BATCH_SIZE.into(), self.batch_size.to_string());
        m.insert(keys::TARGET_PARTITIONS.into(), self.target_partitions.to_string());
        m.insert(keys::TASK_MAX_RETRIES.into(), self.task_max_retries.to_string());
        m.insert(keys::ADAPTIVE_ENABLED.into(), self.adaptive_enabled.to_string());
        m.insert(
            keys::ADAPTIVE_COALESCE_TARGET_BYTES.into(),
            self.adaptive_coalesce_target_bytes.to_string(),
        );
        m.insert(keys::SPECULATION_ENABLED.into(), self.speculation_enabled.to_string());
        if let Some(c) = &self.default_catalog {
            m.insert(keys::DEFAULT_CATALOG.into(), c.clone());
        }
        if let Some(c) = &self.default_schema {
            m.insert(keys::DEFAULT_SCHEMA.into(), c.clone());
        }
        if let Some(b) = self.memory_limit_bytes {
            m.insert(keys::MEMORY_LIMIT_BYTES.into(), b.to_string());
        }
        m
    }
}
