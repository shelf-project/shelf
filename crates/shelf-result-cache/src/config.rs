//! Configuration for shelf-result-cache.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for the result cache proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Listen address for the proxy (e.g., "0.0.0.0:8080").
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    /// Upstream Trino coordinator URL (e.g., "http://trino:8080").
    pub trino_url: String,

    /// Maximum cache size in bytes.
    #[serde(default = "default_max_cache_bytes")]
    pub max_cache_bytes: u64,

    /// Maximum entries in the cache.
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,

    /// TTL for cached results.
    #[serde(with = "humantime_serde", default = "default_ttl")]
    pub ttl: Duration,

    /// Whether to enable the cache (can be toggled at runtime).
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Minimum query latency to cache (don't cache fast queries).
    #[serde(with = "humantime_serde", default = "default_min_latency")]
    pub min_latency_to_cache: Duration,

    /// Maximum result size to cache in bytes.
    #[serde(default = "default_max_result_bytes")]
    pub max_result_bytes: u64,

    /// User patterns to cache (regex; empty = all users).
    #[serde(default)]
    pub cache_users: Vec<String>,

    /// Query patterns to exclude from caching (regex).
    #[serde(default)]
    pub exclude_patterns: Vec<String>,

    /// Shelfd endpoint for snapshot lookups.
    #[serde(default)]
    pub shelfd_url: Option<String>,

    /// Metrics listen address.
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,
}

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_max_cache_bytes() -> u64 {
    10 * 1024 * 1024 * 1024 // 10 GiB
}

fn default_max_entries() -> usize {
    10_000
}

fn default_ttl() -> Duration {
    Duration::from_secs(24 * 60 * 60) // 24 hours
}

fn default_true() -> bool {
    true
}

fn default_min_latency() -> Duration {
    Duration::from_millis(100) // Don't cache queries faster than 100ms
}

fn default_max_result_bytes() -> u64 {
    100 * 1024 * 1024 // 100 MiB max result size
}

fn default_metrics_addr() -> String {
    "0.0.0.0:9090".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            trino_url: "http://localhost:8080".to_string(),
            max_cache_bytes: default_max_cache_bytes(),
            max_entries: default_max_entries(),
            ttl: default_ttl(),
            enabled: default_true(),
            min_latency_to_cache: default_min_latency(),
            max_result_bytes: default_max_result_bytes(),
            cache_users: Vec::new(),
            exclude_patterns: Vec::new(),
            shelfd_url: None,
            metrics_addr: default_metrics_addr(),
        }
    }
}

mod humantime_serde {
    use std::time::Duration;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        humantime::format_duration(*duration)
            .to_string()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}
