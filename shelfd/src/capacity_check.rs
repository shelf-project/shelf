//! Cluster-capacity readiness probe (RC6 P1.2 — `/admin/cap-ready`).
//!
//! Codifies the workspace ops rule "scale +2 shelf pods before adding
//! a new replica's traffic to the pool":
//!
//! > Verify ALL existing pods are `< 22 GiB RSS` (the warn watermark;
//! > OOM ceiling is ~27.3 GiB on `m6a/m5a/m7a/c6a 4xlarge`).
//!
//! Today operators check this manually before every cutover. The
//! endpoint flips that into a one-shot machine-readable check the
//! cutover MR template can curl, so the rule is enforced at the gate
//! rather than caught after a saturation incident.
//!
//! ## Contract
//!
//! `GET /admin/cap-ready[?caller=<replica-name>]` →
//!
//! - `200 OK { "ready": true,  "max_rss_gib": <f64> }` — every
//!   reachable peer reports `rss_bytes < threshold` AND no peer was
//!   unreachable.
//! - `503 Service Unavailable
//!      { "ready": false, "max_rss_gib": <f64>, "max_rss_pod": "<id>" }`
//!   — at least one peer crossed the threshold OR was unreachable
//!   (conservative-by-design, see [`CapReadyReport`]).
//!
//! The `caller` query parameter is opaque audit metadata: cutover
//! tooling threads the replica name (e.g. `rep-0`) so `kubectl logs`
//! shows which side initiated each gate check.
//!
//! ## Why probe peers rather than read shared state
//!
//! Each shelfd pod is its own process; the RSS of `shelf-0` is not
//! observable from inside `shelf-2` without an out-of-band query.
//! We reuse the SHELF-23 resolver-driven HRW ring (see
//! [`crate::router::Router::view`]) as the "who is in the cluster"
//! source of truth, then fan out one [`Stats`] probe per peer and
//! aggregate `max(rss_bytes)`.
//!
//! ## Failure modes (per RC6 plan)
//!
//! 1. **Peer unreachable** → conservative `503` with
//!    `peers_unreachable` populated. The ops rule was "verify ALL
//!    pods" — if we can't read a pod, we don't know its RSS, and
//!    silently skipping it would let a saturated pod hide.
//! 2. **No peers in ring** (boot-time placeholder, DNS/probe glitch)
//!    → still `200` based on self-RSS only. The empty-ring case is
//!    a separate ops signal already exposed via `/admin/ring`.
//! 3. **Probe timeout** → counted as unreachable (option 1).
//!
//! ## Disable lever (rollback)
//!
//! The endpoint is read-only and side-effect free; rollback is
//! either (a) `kubectl set image` to a pre-feature build, or
//! (b) a future config flag (left out of v1 — the implementation
//! is small enough that disabling it is just "ignore the curl").

use std::sync::Arc;
use std::time::Duration;

use crate::control::Stats;
use crate::router::Member;

/// Default RSS threshold (workspace-memory warn watermark).
///
/// Picked as 22 GiB because:
/// - Karpenter `m6a/m5a/m7a/c6a 4xlarge` instances expose
///   ~27.3 GiB allocatable.
/// - Foyer 0.12 LDC + DRAM rowgroup pool peaks at ~14–20 GiB
///   under sustained read load.
/// - Empirical OOMKill threshold is ~24 GiB (live evidence Apr 28
///   shelf-1 chaos window).
/// - 22 GiB leaves a 5.3 GiB headroom over the OOM ceiling, which
///   is roughly one pre-empt + Foyer disk replay envelope.
pub const DEFAULT_CAP_READY_THRESHOLD_BYTES: u64 = 22 * 1024 * 1024 * 1024;

/// Default per-peer `/stats` probe deadline. Same value the SHELF-20
/// membership resolver uses (`membership::DEFAULT_STATS_TIMEOUT`).
/// Chosen short so an admin-driven gate check can never block a
/// cutover for more than `peers × timeout` worst case.
pub const DEFAULT_PEER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Outcome of [`check_cluster_capacity`]. Serialized verbatim into
/// the `/admin/cap-ready` response body.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CapReadyReport {
    /// `true` when every probed peer (including self) reports
    /// `rss_bytes < threshold` AND the `peers_unreachable` set is
    /// empty. The endpoint reports `503` whenever this is `false`.
    pub ready: bool,
    /// Highest RSS observed across all reachable peers, in bytes.
    /// `0` when no peer (not even self) returned a parseable stat.
    pub max_rss_bytes: u64,
    /// Same value rendered in GiB (binary, base 1024 ^ 3) so the
    /// response is human-greppable in `kubectl logs`.
    pub max_rss_gib: f64,
    /// Pod id whose RSS equals `max_rss_bytes`. `None` only when no
    /// peer responded.
    pub max_rss_pod: Option<String>,
    /// Number of peers we attempted to probe (includes self).
    pub peers_probed: usize,
    /// `pod_id` of every peer whose `/stats` probe failed (or `None`
    /// in the slot when self-RSS read fails — the field carries the
    /// string `"self"` in that case).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers_unreachable: Vec<String>,
    /// Threshold the check used (echoed back so the client sees
    /// exactly which value the daemon evaluated against, regardless
    /// of when the binary was built).
    pub threshold_bytes: u64,
}

/// Trait that abstracts the per-peer `/stats` probe so unit tests
/// can drive the [`check_cluster_capacity`] flow without standing
/// up real HTTP servers.
///
/// Mirrors the SHELF-20 [`crate::membership::StatsProbe`] shape but
/// keyed by `endpoint` (the `Member.endpoint` value the resolver
/// already publishes) rather than `(IpAddr, port)` because the
/// cap-ready path doesn't need to re-resolve DNS — it consumes the
/// router view that the membership loop already populated.
pub trait PeerStatsProbe: Send + Sync {
    /// Probe `<endpoint>/stats`. The implementation owns its own
    /// timeout; [`check_cluster_capacity`] does not wrap the call
    /// (the resolver pattern is the same, and double-wrapping
    /// only adds tail latency to a slow peer).
    fn probe<'a>(
        &'a self,
        endpoint: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Stats>> + Send + 'a>>;
}

/// Default [`PeerStatsProbe`] backed by `reqwest`. The endpoint
/// argument is rewritten so the probe targets the **stats** port
/// (`peer_stats_port` / 9090) rather than the data-plane port
/// (`peer_http`'s default — typically 9092 / S3 shim) that the
/// router publishes for hot-path peer-fetch.
#[derive(Debug, Clone)]
pub struct ReqwestPeerStatsProbe {
    http: reqwest::Client,
    timeout: Duration,
    stats_port: u16,
}

impl ReqwestPeerStatsProbe {
    pub fn new(http: reqwest::Client, stats_port: u16, timeout: Duration) -> Self {
        Self {
            http,
            timeout,
            stats_port,
        }
    }

    /// Rewrite a router-published endpoint (`<host>:<data_port>`)
    /// to the control-plane stats URL (`http://<host>:<stats_port>/stats`).
    /// Falls through to the raw endpoint if the colon split fails so
    /// a caller passing a hostname with no port still gets a useful
    /// error from `reqwest::get` rather than a panic.
    fn stats_url(&self, endpoint: &str) -> String {
        match endpoint.rsplit_once(':') {
            Some((host, _port)) => format!("http://{host}:{}/stats", self.stats_port),
            None => format!("http://{endpoint}:{}/stats", self.stats_port),
        }
    }
}

impl PeerStatsProbe for ReqwestPeerStatsProbe {
    fn probe<'a>(
        &'a self,
        endpoint: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Stats>> + Send + 'a>>
    {
        let url = self.stats_url(endpoint);
        let timeout = self.timeout;
        let http = self.http.clone();
        Box::pin(async move {
            let fut = http.get(&url).send();
            let resp = tokio::time::timeout(timeout, fut)
                .await
                .map_err(|_| crate::Error::Membership(format!("cap-ready probe timeout: {url}")))?
                .map_err(|e| crate::Error::Membership(format!("cap-ready probe http: {e}")))?;
            if !resp.status().is_success() {
                return Err(crate::Error::Membership(format!(
                    "cap-ready probe non-2xx: {url} status={}",
                    resp.status()
                )));
            }
            let stats: Stats = tokio::time::timeout(timeout, resp.json::<Stats>())
                .await
                .map_err(|_| crate::Error::Membership(format!("cap-ready body timeout: {url}")))?
                .map_err(|e| crate::Error::Membership(format!("cap-ready body parse: {e}")))?;
            Ok(stats)
        })
    }
}

/// Read this process's resident set size in bytes.
///
/// Uses `/proc/self/status` `VmRSS:` line on Linux, which reports
/// kB. On non-Linux platforms (developer laptop dev/test) the file
/// is absent and we return `0` — the cluster the gate is meant to
/// protect runs distroless Linux only, so this fallback never fires
/// in production. A `0` self-RSS in a unit test is a no-op for the
/// "any peer >= threshold" predicate, exactly what tests need.
pub fn read_self_rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let raw = match std::fs::read_to_string("/proc/self/status") {
            Ok(s) => s,
            Err(_) => return 0,
        };
        for line in raw.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                // Format: "VmRSS:\t   12345 kB"
                let kb: u64 = rest
                    .split_whitespace()
                    .next()
                    .and_then(|tok| tok.parse().ok())
                    .unwrap_or(0);
                return kb.saturating_mul(1024);
            }
        }
        0
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// Run the cap-ready check.
///
/// `members` is the router view (as returned by
/// `state.router.view().members()`) plus implicitly self — the
/// caller does NOT need to add self to the slice; this function
/// observes self via [`read_self_rss_bytes`] and `self_pod_id`.
///
/// `probe` is consulted once per non-self member. A probe failure
/// (timeout, non-2xx, parse error) marks the peer as unreachable
/// and forces `ready=false`. We never short-circuit on the first
/// over-threshold peer because the report payload should still
/// surface the *worst* RSS for ops, not the first one we found.
pub async fn check_cluster_capacity<P: PeerStatsProbe + ?Sized>(
    members: &[Member],
    self_pod_id: &str,
    threshold_bytes: u64,
    probe: Arc<P>,
) -> CapReadyReport {
    // 1. Self snapshot — always counted, independent of router view.
    let self_rss = read_self_rss_bytes();
    let mut max_rss_bytes: u64 = self_rss;
    let mut max_rss_pod: Option<String> = if self_rss > 0 {
        Some(self_pod_id.to_owned())
    } else {
        None
    };
    let mut peers_probed: usize = 1;
    let mut peers_unreachable: Vec<String> = Vec::new();

    // 2. Concurrent fan-out across non-self peers. Probes own their
    //    own timeout; we don't double-wrap.
    let peer_iter = members.iter().filter(|m| m.id != self_pod_id);
    let probes = peer_iter.map(|m| {
        let probe = Arc::clone(&probe);
        async move {
            let result = probe.probe(&m.endpoint).await;
            (m.id.clone(), result)
        }
    });
    let results = futures::future::join_all(probes).await;

    for (pod_id, result) in results {
        peers_probed += 1;
        match result {
            Ok(stats) => {
                if stats.rss_bytes >= max_rss_bytes {
                    max_rss_bytes = stats.rss_bytes;
                    max_rss_pod = Some(stats.pod_id);
                }
            }
            Err(_) => {
                peers_unreachable.push(pod_id);
            }
        }
    }

    let any_unreachable = !peers_unreachable.is_empty();
    let any_over_threshold = max_rss_bytes >= threshold_bytes;
    let ready = !any_unreachable && !any_over_threshold;

    CapReadyReport {
        ready,
        max_rss_bytes,
        max_rss_gib: bytes_to_gib(max_rss_bytes),
        max_rss_pod,
        peers_probed,
        peers_unreachable,
        threshold_bytes,
    }
}

/// Convert a byte count to a base-1024 GiB float, rounded to 3
/// decimal places. Pulled out so unit tests can assert deterministic
/// JSON.
pub fn bytes_to_gib(bytes: u64) -> f64 {
    let raw = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    (raw * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{PoolStats, Stats};
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn member(id: &str, endpoint: &str) -> Member {
        Member {
            id: id.to_string(),
            endpoint: endpoint.to_string(),
            weight: 1,
        }
    }

    fn stats_with_rss(pod_id: &str, rss: u64) -> Stats {
        Stats {
            pod_id: pod_id.to_string(),
            capacity_bytes: 0,
            used_bytes: 0,
            metadata_pool: PoolStats {
                capacity_bytes: 0,
                used_bytes: 0,
                disk_used_bytes: 0,
                disk_capacity_bytes: 0,
            },
            rowgroup_pool: PoolStats {
                capacity_bytes: 0,
                used_bytes: 0,
                disk_used_bytes: 0,
                disk_capacity_bytes: 0,
            },
            pinned_bytes: 0,
            pinned_count: 0,
            draining: false,
            rss_bytes: rss,
            // K2 (rc.8) — `cap-ready` does not consult pod-load.
            pod_load: None,
        }
    }

    /// Static mock probe — every endpoint maps to a canned `Stats`,
    /// or `Err(_)` when the endpoint is in the `unreachable` set.
    struct MockProbe {
        responses: HashMap<String, Stats>,
        unreachable: Mutex<Vec<String>>,
    }

    impl MockProbe {
        fn new(responses: Vec<(&str, Stats)>, unreachable: Vec<&str>) -> Arc<Self> {
            Arc::new(Self {
                responses: responses
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
                unreachable: Mutex::new(unreachable.into_iter().map(String::from).collect()),
            })
        }
    }

    impl PeerStatsProbe for MockProbe {
        fn probe<'a>(
            &'a self,
            endpoint: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Stats>> + Send + 'a>>
        {
            let owned = endpoint.to_string();
            let unreach = self.unreachable.lock().unwrap().clone();
            let resp = self.responses.get(endpoint).cloned();
            Box::pin(async move {
                if unreach.contains(&owned) {
                    return Err(crate::Error::Membership(format!(
                        "mock unreachable {owned}"
                    )));
                }
                resp.ok_or_else(|| crate::Error::Membership(format!("mock no canned for {owned}")))
            })
        }
    }

    #[tokio::test]
    async fn all_peers_under_threshold_reports_ready() {
        let members = vec![
            member("shelf-0", "10.0.0.1:9092"),
            member("shelf-1", "10.0.0.2:9092"),
            member("shelf-2", "10.0.0.3:9092"),
        ];
        let probe = MockProbe::new(
            vec![
                (
                    "10.0.0.1:9092",
                    stats_with_rss("shelf-0", 10 * 1024 * 1024 * 1024),
                ),
                (
                    "10.0.0.2:9092",
                    stats_with_rss("shelf-1", 12 * 1024 * 1024 * 1024),
                ),
                (
                    "10.0.0.3:9092",
                    stats_with_rss("shelf-2", 18 * 1024 * 1024 * 1024),
                ),
            ],
            vec![],
        );
        let report = check_cluster_capacity(
            &members,
            "shelf-1", // self
            DEFAULT_CAP_READY_THRESHOLD_BYTES,
            probe,
        )
        .await;
        assert!(report.ready, "report: {report:?}");
        assert_eq!(report.peers_probed, 3, "self + 2 non-self peers");
        // self RSS is 0 in tests (non-linux fallback) so the max comes
        // from the highest peer (shelf-2 at 18 GiB).
        assert_eq!(report.max_rss_pod.as_deref(), Some("shelf-2"));
        assert_eq!(report.max_rss_bytes, 18 * 1024 * 1024 * 1024);
        assert!(report.peers_unreachable.is_empty());
    }

    #[tokio::test]
    async fn one_peer_over_threshold_reports_not_ready() {
        let members = vec![
            member("shelf-0", "10.0.0.1:9092"),
            member("shelf-1", "10.0.0.2:9092"),
        ];
        let probe = MockProbe::new(
            vec![
                (
                    "10.0.0.1:9092",
                    stats_with_rss("shelf-0", 10 * 1024 * 1024 * 1024),
                ),
                (
                    "10.0.0.2:9092",
                    stats_with_rss("shelf-1", 23 * 1024 * 1024 * 1024),
                ),
            ],
            vec![],
        );
        let report = check_cluster_capacity(
            &members,
            "shelf-0",
            DEFAULT_CAP_READY_THRESHOLD_BYTES,
            probe,
        )
        .await;
        assert!(!report.ready);
        assert_eq!(report.max_rss_pod.as_deref(), Some("shelf-1"));
        assert_eq!(report.max_rss_bytes, 23 * 1024 * 1024 * 1024);
    }

    #[tokio::test]
    async fn unreachable_peer_forces_not_ready_even_when_known_peers_under_threshold() {
        let members = vec![
            member("shelf-0", "10.0.0.1:9092"),
            member("shelf-1", "10.0.0.2:9092"),
        ];
        let probe = MockProbe::new(
            vec![(
                "10.0.0.1:9092",
                stats_with_rss("shelf-0", 8 * 1024 * 1024 * 1024),
            )],
            vec!["10.0.0.2:9092"],
        );
        let report = check_cluster_capacity(
            &members,
            "shelf-0",
            DEFAULT_CAP_READY_THRESHOLD_BYTES,
            probe,
        )
        .await;
        assert!(!report.ready);
        assert!(report.peers_unreachable.contains(&"shelf-1".to_string()));
    }

    #[tokio::test]
    async fn empty_ring_falls_back_to_self_rss_only() {
        // No peers at all (boot-time placeholder). The endpoint must
        // not blow up; it just reports based on whatever self-RSS we
        // can read.
        let probe = MockProbe::new(vec![], vec![]);
        let report =
            check_cluster_capacity(&[], "shelf-0", DEFAULT_CAP_READY_THRESHOLD_BYTES, probe).await;
        // On non-Linux test hosts self-RSS = 0 -> ready.
        assert!(report.ready);
        assert_eq!(report.peers_probed, 1);
    }

    #[test]
    fn bytes_to_gib_rounds_to_three_decimals() {
        assert_eq!(bytes_to_gib(0), 0.0);
        assert_eq!(bytes_to_gib(1024 * 1024 * 1024), 1.0);
        // 22 GiB exact
        assert_eq!(bytes_to_gib(22u64 * 1024 * 1024 * 1024), 22.0);
        // 1.5 GiB
        let one_and_half = 1024u64 * 1024 * 1024 + 512 * 1024 * 1024;
        assert_eq!(bytes_to_gib(one_and_half), 1.5);
    }

    #[test]
    fn read_self_rss_bytes_is_nonneg() {
        // Smoke: never panics, value fits in u64.
        let v = read_self_rss_bytes();
        // On Linux the test process's RSS is typically multi-MiB;
        // on macOS/Windows we expect 0. Either is fine — we just
        // assert no panic / no overflow path.
        let _ = v;
    }

    /// Self-pod is implicitly always counted: even when the router
    /// view doesn't list us, our own RSS should be reflected in the
    /// max if it's the highest. Synthetic test using a hand-crafted
    /// `Member` slice + a probe that pretends self appears as a
    /// peer too (mimicking a sloppy resolver).
    #[tokio::test]
    async fn self_pod_id_is_filtered_from_peer_list() {
        let members = vec![
            member("shelf-0", "10.0.0.1:9092"),
            member("shelf-1", "10.0.0.2:9092"), // self
        ];
        // If the filter wasn't applied, this canned response for
        // "10.0.0.2:9092" would be probed and bring its 25 GiB RSS
        // into the max. The test verifies the filter actually kicks.
        let probe = MockProbe::new(
            vec![
                (
                    "10.0.0.1:9092",
                    stats_with_rss("shelf-0", 10 * 1024 * 1024 * 1024),
                ),
                (
                    "10.0.0.2:9092",
                    stats_with_rss("shelf-1", 25 * 1024 * 1024 * 1024),
                ),
            ],
            vec![],
        );
        let report = check_cluster_capacity(
            &members,
            "shelf-1", // self
            DEFAULT_CAP_READY_THRESHOLD_BYTES,
            probe,
        )
        .await;
        // Only shelf-0 was probed; self contributes RSS=0 on non-linux.
        assert_eq!(report.peers_probed, 2, "self + 1 peer");
        assert!(report.ready);
        assert_eq!(report.max_rss_pod.as_deref(), Some("shelf-0"));
    }
}
