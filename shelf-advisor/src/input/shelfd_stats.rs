//! shelfd `/stats` reader.
//!
//! `PinListRecommender` (SHELF-53) and `MaterializedViewRecommender`
//! (SHELF-65 follow-up) both want a per-pod capacity / used-bytes
//! snapshot to ground their scoring (the pin-list score's
//! `1 + total_bytes / pool_capacity` denominator; SHELF-65's
//! `nvme_quota * pin_fraction` cap). Rather than forcing every
//! recommender to learn the shelfd HTTP wire format, we expose a
//! small reader contract here.
//!
//! The wire format we read mirrors `shelfd::http::Stats`
//! (see `shelfd/src/http.rs:849` for the canonical definition):
//!
//! ```json
//! {
//!   "pod_id": "shelf-2",
//!   "capacity_bytes": 12884901888,
//!   "used_bytes":      3221225472,
//!   "metadata_pool": {"capacity_bytes": ..., "used_bytes": ...},
//!   "rowgroup_pool": {"capacity_bytes": ..., "used_bytes": ...},
//!   "pinned_bytes": ...,
//!   "pinned_count": ...,
//!   "draining": false
//! }
//! ```
//!
//! We parse the subset the recommenders actually use; unknown
//! fields are tolerated (forward-compat with later shelfd
//! revisions that grow the payload). Auth is intentionally absent
//! — same-cluster shelfd `/stats` is unauthenticated by design,
//! per the SHELF-20 control-plane contract.

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Per-pool capacity / used-bytes pair. Mirrors the shelfd
/// `PoolStats` shape but only carries the two numbers the advisor
/// reads.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolStats {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
}

/// One pod's `/stats` response, slimmed to the fields the
/// recommenders consume. Missing fields default to zero so that a
/// future shelfd that drops a numeric field for any reason still
/// parses cleanly here.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PodStats {
    pub pod_id: String,
    #[serde(default)]
    pub capacity_bytes: u64,
    #[serde(default)]
    pub used_bytes: u64,
    #[serde(default)]
    pub metadata_pool: PoolStats,
    #[serde(default)]
    pub rowgroup_pool: PoolStats,
    #[serde(default)]
    pub pinned_bytes: u64,
    #[serde(default)]
    pub pinned_count: u64,
    #[serde(default)]
    pub draining: bool,
}

/// Reader contract for shelfd `/stats`.
///
/// Production callers pass the configured pod URL list (one
/// `/stats` poll per pod). Tests + the `dry-run` CLI mode replay
/// a frozen JSON array via [`FixtureShelfdStatsReader`].
///
/// Implementations should not bubble up transport errors when one
/// pod is briefly unreachable — the recommenders reason over the
/// *available* sample. Return `Ok(vec![])` if nothing is reachable
/// and let the recommender decide how to degrade. If a recommender
/// needs hard-fail semantics it can check `is_empty()` itself.
pub trait ShelfdStatsReader: Send + Sync {
    /// Pull the latest snapshot from every configured pod. The
    /// returned vector is sorted by `pod_id` for deterministic
    /// downstream consumption.
    fn read_all(&self) -> Result<Vec<PodStats>>;
}

/// JSON-fixture reader. Reads a `Vec<PodStats>` from a path and
/// returns it on every call. Used by the integration test suite
/// and the `dry-run` CLI mode.
pub struct FixtureShelfdStatsReader {
    pods: Vec<PodStats>,
}

impl FixtureShelfdStatsReader {
    pub fn new(mut pods: Vec<PodStats>) -> Self {
        pods.sort_by(|a, b| a.pod_id.cmp(&b.pod_id));
        Self { pods }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        let pods: Vec<PodStats> = serde_json::from_slice(&bytes)?;
        Ok(Self::new(pods))
    }

    pub fn pod_count(&self) -> usize {
        self.pods.len()
    }
}

impl ShelfdStatsReader for FixtureShelfdStatsReader {
    fn read_all(&self) -> Result<Vec<PodStats>> {
        Ok(self.pods.clone())
    }
}

/// Production reader: HTTP GET against each configured shelfd
/// `/stats` URL via `reqwest`. One blocking-on-tokio call per pod;
/// failures degrade per the trait contract (warn + skip).
///
/// The reader is constructed once at advisor startup so the
/// internal `reqwest::Client` is shared (TCP + TLS reuse). The
/// per-pod timeout caps how long a single unhealthy pod can stall
/// a run.
pub struct HttpShelfdStatsReader {
    client: reqwest::Client,
    urls: Vec<String>,
    timeout: Duration,
}

impl HttpShelfdStatsReader {
    /// Build a reader against the listed `/stats` URLs.
    /// `per_pod_timeout` defaults to 2s when unset.
    pub fn new(urls: Vec<String>, per_pod_timeout: Option<Duration>) -> Result<Self> {
        let timeout = per_pod_timeout.unwrap_or_else(|| Duration::from_secs(2));
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(concat!("shelf-advisor/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            urls,
            timeout,
        })
    }

    /// Number of pods this reader is configured to scrape.
    pub fn pod_count(&self) -> usize {
        self.urls.len()
    }
}

impl ShelfdStatsReader for HttpShelfdStatsReader {
    fn read_all(&self) -> Result<Vec<PodStats>> {
        // The advisor binary is a `#[tokio::main]` shop, so we
        // hop on the current Tokio handle rather than spinning a
        // private runtime. If we are called outside a runtime
        // (synchronous integration test), fall back to a tiny
        // single-thread runtime; this branch is hit only by
        // ad-hoc CLI smoke runs.
        let urls = self.urls.clone();
        let client = self.client.clone();
        let timeout = self.timeout;
        let fut = async move {
            let mut out: Vec<PodStats> = Vec::with_capacity(urls.len());
            for u in &urls {
                match client
                    .get(u)
                    .timeout(timeout)
                    .send()
                    .await
                    .and_then(|r| r.error_for_status())
                {
                    Ok(resp) => match resp.json::<PodStats>().await {
                        Ok(p) => out.push(p),
                        Err(e) => {
                            tracing::warn!(url = %u, error = %e, "shelfd /stats decode failed")
                        }
                    },
                    Err(e) => {
                        tracing::warn!(url = %u, error = %e, "shelfd /stats fetch failed");
                    }
                }
            }
            out.sort_by(|a, b| a.pod_id.cmp(&b.pod_id));
            Ok::<Vec<PodStats>, anyhow::Error>(out)
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(fut)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_sorts_by_pod_id() {
        let r = FixtureShelfdStatsReader::new(vec![
            PodStats {
                pod_id: "shelf-2".into(),
                ..Default::default()
            },
            PodStats {
                pod_id: "shelf-0".into(),
                ..Default::default()
            },
            PodStats {
                pod_id: "shelf-1".into(),
                ..Default::default()
            },
        ]);
        let pods = r.read_all().unwrap();
        let ids: Vec<_> = pods.iter().map(|p| p.pod_id.as_str()).collect();
        assert_eq!(ids, vec!["shelf-0", "shelf-1", "shelf-2"]);
    }

    #[test]
    fn pod_stats_tolerates_unknown_fields() {
        let payload = r#"{
            "pod_id": "shelf-x",
            "capacity_bytes": 100,
            "used_bytes": 10,
            "metadata_pool": {"capacity_bytes": 50, "used_bytes": 5},
            "rowgroup_pool": {"capacity_bytes": 50, "used_bytes": 5},
            "future_field_we_havent_modelled_yet": [1, 2, 3]
        }"#;
        let p: PodStats = serde_json::from_str(payload).expect("forward-compat");
        assert_eq!(p.pod_id, "shelf-x");
        assert_eq!(p.metadata_pool.capacity_bytes, 50);
    }
}
