//! DNS / headless-service membership resolver and lameduck drain
//! signal.
//!
//! Ticket ownership:
//! - SHELF-20 — resolve `shelf.shelf.svc.cluster.local` every
//!   `dns_refresh` seconds, poll each pod's `/stats` for capacity
//!   weights and drain state, push results into `Router::update`.
//!   No Raft per ADR-0001.
//! - Phase 3 (SHELF-3x) — chaos-drill robustness, KEDA rotation
//!   test, eventual SRV-record support for AZ awareness.
//!
//! References:
//! - `agents/out/adr/0001-no-embedded-raft.md`
//! - `agents/out/adr/0002-hrw-hashing-over-vnode-ring.md`
//! - `shelfd/docs/design-notes/SHELF-20-membership-and-drain.md`
//!
//! ## Design at a glance
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────┐
//!   │ Resolver loop (one per pod)                                  │
//!   │                                                              │
//!   │   tick = interval(cfg.dns_refresh)                           │
//!   │   loop {                                                     │
//!   │     select { tick | shutdown }                               │
//!   │     ips    <- lookup_host(headless:stats_port)               │
//!   │     stats  <- join_all(GET /stats with per-peer timeout)     │
//!   │     members<- build {id, endpoint, weight}                   │
//!   │              filter !draining                                │
//!   │     router.update(members)                                   │
//!   │   }                                                          │
//!   └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! The per-peer probe uses a hard wall-clock timeout, so any one slow
//! peer cannot stall the whole refresh — its IP is simply omitted from
//! this round's ring view. Next round the missing peer is given a
//! fresh chance: there is no failure budget. A pod that flaps in and
//! out of the ring still serves correctly because HRW only displaces
//! `~1/N` of the keys per membership change.
//!
//! ## Drain (lameduck) protocol
//!
//! Local drain is a one-bit signal owned by the process:
//!
//! 1. `main` receives `SIGTERM`.
//! 2. `DrainSignal::begin()` flips the bit. The next `GET /stats`
//!    response from this pod carries `draining: true`.
//! 3. Peers' resolvers see `draining: true` on their next refresh
//!    (≤ `cfg.dns_refresh` later) and drop us from their rings.
//! 4. `Resolver::wait_drained()` sleeps `cfg.drain_grace` (or until
//!    `shutdown` cancels — hard kill case). After this point all
//!    peers have refreshed at least once, so no fresh traffic should
//!    arrive.
//! 5. `main` cancels the shutdown token. The resolver loop exits.
//!    The data plane stops accepting new connections.
//!
//! `drain_grace` is intentionally a constant rather than a calibrated
//! quantity: the data plane idles within `dns_refresh + max(p99 stats
//! probe latency)` after the bit flips, and a few extra seconds of
//! lameduck is a much smaller cost than letting a misrouted request
//! fall through to S3 because a peer's ring was stale.

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::control::Stats;
use crate::router::{Member, Router};

/// Default control-plane (`/stats`) port. Mirrors `charts/shelf/values.yaml`.
pub const DEFAULT_STATS_PORT: u16 = 9090;

/// Default data-plane (S3-shim) port.
pub const DEFAULT_DATA_PORT: u16 = 9092;

/// Default DNS refresh cadence. Trades off "ring is stale" vs "DNS
/// load on coredns". 5s lines up with kube-proxy iptables sync.
pub const DEFAULT_DNS_REFRESH: Duration = Duration::from_secs(5);

/// Default per-peer `/stats` probe deadline. SHELF-08 jitter data
/// puts same-AZ HTTP/2 probe p99 < 5 ms; 1 s absorbs GC pauses.
pub const DEFAULT_STATS_TIMEOUT: Duration = Duration::from_secs(1);

/// Default lameduck grace before the resolver lets shutdown proceed.
/// Must be ≥ 2× `dns_refresh` so every peer has had at least one
/// refresh window to observe `draining: true`.
pub const DEFAULT_DRAIN_GRACE: Duration = Duration::from_secs(15);

/// Default capacity unit for HRW weight computation. A pod with
/// 1 GiB of cache has weight 1. The HRW score is divided by
/// `-ln(x)`, so a peer with 100× more capacity drains roughly 100×
/// more keys per ADR-0002.
pub const DEFAULT_WEIGHT_UNIT_BYTES: u64 = 1024 * 1024 * 1024;

/// Configuration for [`Resolver::spawn`].
#[derive(Debug, Clone)]
pub struct ResolverConfig {
    /// Fully-qualified headless-service hostname, e.g.
    /// `shelf.shelf.svc.cluster.local`.
    pub headless_service: String,
    /// Port the control plane listens on (`/stats`, `/metrics`).
    pub stats_port: u16,
    /// Port the data plane listens on (the value baked into
    /// `Member::endpoint`).
    pub data_port: u16,
    /// Cadence at which DNS is re-resolved and `/stats` re-probed.
    pub dns_refresh: Duration,
    /// Hard wall-clock deadline for one peer's `/stats` probe.
    pub stats_timeout: Duration,
    /// Stable identity of *this* pod (e.g. `shelf-2`). Used to
    /// label trace spans and to filter our own row out of the
    /// peer set when needed.
    pub self_id: String,
    /// Time to advertise `draining: true` on `/stats` before the
    /// process exits. See module docs.
    pub drain_grace: Duration,
    /// Capacity-bytes per weight unit. See [`weight_for_capacity`].
    pub weight_unit_bytes: u64,
}

impl ResolverConfig {
    /// Sensible defaults for the in-cluster deployment shape.
    pub fn for_self(self_id: impl Into<String>, headless_service: impl Into<String>) -> Self {
        Self {
            headless_service: headless_service.into(),
            stats_port: DEFAULT_STATS_PORT,
            data_port: DEFAULT_DATA_PORT,
            dns_refresh: DEFAULT_DNS_REFRESH,
            stats_timeout: DEFAULT_STATS_TIMEOUT,
            self_id: self_id.into(),
            drain_grace: DEFAULT_DRAIN_GRACE,
            weight_unit_bytes: DEFAULT_WEIGHT_UNIT_BYTES,
        }
    }
}

/// One-bit flag carrying the **local** pod's drain state.
///
/// Cheap-clone (`Arc<AtomicBool>`); hand a clone to whichever
/// component is responsible for serving `/stats` so the wire
/// payload reflects the live state without indirection through
/// the Resolver.
#[derive(Debug, Clone, Default)]
pub struct DrainSignal(Arc<AtomicBool>);

impl DrainSignal {
    /// New healthy signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Flip the bit. Idempotent.
    pub fn begin(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Read the current state. `true` means "this pod is draining".
    pub fn is_active(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Async function that fetches a peer's `/stats` payload. Pulled out
/// to a trait so the resolver loop can be exercised against a mock
/// server in tests without bringing the real reqwest stack into the
/// test process. Implementors must enforce their own timeout — the
/// resolver does **not** wrap calls in `tokio::time::timeout`.
pub trait StatsProbe: Send + Sync + 'static {
    /// Probe `<scheme>://<ip>:<port>/stats`. Returning `Err(_)` simply
    /// drops `ip` from this round's ring; the resolver never logs at
    /// `error` for a single failed probe (it's a normal, expected
    /// transient on a draining pod or a slow startup).
    fn probe<'a>(
        &'a self,
        ip: IpAddr,
        port: u16,
        timeout: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Stats>> + Send + 'a>>;
}

/// Default [`StatsProbe`] backed by `reqwest`.
#[derive(Debug)]
pub struct ReqwestProbe {
    http: reqwest::Client,
}

impl ReqwestProbe {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }
}

impl StatsProbe for ReqwestProbe {
    fn probe<'a>(
        &'a self,
        ip: IpAddr,
        port: u16,
        timeout: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Stats>> + Send + 'a>>
    {
        Box::pin(async move {
            // Use literal-IPv6 bracket form so we don't accidentally
            // generate a malformed URL on dual-stack clusters.
            let host = match ip {
                IpAddr::V4(v4) => format!("{v4}"),
                IpAddr::V6(v6) => format!("[{v6}]"),
            };
            let url = format!("http://{host}:{port}/stats");
            let fut = self.http.get(&url).send();
            let resp = tokio::time::timeout(timeout, fut)
                .await
                .map_err(|_| crate::Error::Membership(format!("stats probe timeout: {url}")))?
                .map_err(|e| crate::Error::Membership(format!("stats probe http: {e}")))?;
            if !resp.status().is_success() {
                return Err(crate::Error::Membership(format!(
                    "stats probe non-2xx: {url} status={}",
                    resp.status()
                )));
            }
            let stats: Stats = tokio::time::timeout(timeout, resp.json::<Stats>())
                .await
                .map_err(|_| crate::Error::Membership(format!("stats body timeout: {url}")))?
                .map_err(|e| crate::Error::Membership(format!("stats body parse: {e}")))?;
            Ok(stats)
        })
    }
}

/// DNS-lookup function. Pulled out for the same reason as
/// [`StatsProbe`]: tokio's resolver hits the real OS, which a unit
/// test cannot redirect without elevated privileges.
pub trait HostResolver: Send + Sync + 'static {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Vec<IpAddr>>> + Send + 'a>>;
}

/// Default [`HostResolver`] backed by `tokio::net::lookup_host`.
#[derive(Debug, Default)]
pub struct TokioResolver;

impl HostResolver for TokioResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Vec<IpAddr>>> + Send + 'a>>
    {
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host, port))
                .await
                .map_err(|e| crate::Error::Membership(format!("dns lookup {host}:{port}: {e}")))?;
            let ips: BTreeSet<IpAddr> = addrs.map(|sa| sa.ip()).collect();
            Ok(ips.into_iter().collect())
        })
    }
}

/// Membership resolver handle. Owns the background `JoinHandle` per
/// `agents/4-shelfd-builder.md` Pass 2 ("No `tokio::spawn` without an
/// owner that tracks its `JoinHandle`").
#[derive(Debug)]
pub struct Resolver {
    inner: Arc<ResolverInner>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct ResolverInner {
    config: ResolverConfig,
    router: Arc<Router>,
    drain: DrainSignal,
}

impl Resolver {
    /// Spawn the membership-resolver task with the default
    /// `reqwest` + `tokio::net` I/O stack.
    pub fn spawn(
        config: ResolverConfig,
        router: Arc<Router>,
        drain: DrainSignal,
        shutdown: CancellationToken,
    ) -> crate::Result<Self> {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(2)
            .timeout(config.stats_timeout)
            .build()
            .map_err(|e| crate::Error::Membership(format!("reqwest build: {e}")))?;
        Self::spawn_with(
            config,
            router,
            drain,
            shutdown,
            Arc::new(TokioResolver),
            Arc::new(ReqwestProbe::new(http)),
        )
    }

    /// Test-friendly constructor: inject a custom DNS resolver and a
    /// custom `/stats` probe so the loop can be exercised without
    /// touching the network.
    pub fn spawn_with(
        config: ResolverConfig,
        router: Arc<Router>,
        drain: DrainSignal,
        shutdown: CancellationToken,
        resolver: Arc<dyn HostResolver>,
        probe: Arc<dyn StatsProbe>,
    ) -> crate::Result<Self> {
        let inner = Arc::new(ResolverInner {
            config,
            router,
            drain,
        });
        let loop_inner = Arc::clone(&inner);
        let handle = tokio::spawn(async move {
            run_loop(loop_inner, shutdown, resolver, probe).await;
        });
        Ok(Self {
            inner,
            handle: Mutex::new(Some(handle)),
        })
    }

    /// Mark the local pod as draining. Cheap; safe to call from any
    /// task. Idempotent.
    pub fn begin_drain(&self) {
        self.inner.drain.begin();
    }

    /// Whether the local pod is currently advertising `draining: true`.
    pub fn is_draining(&self) -> bool {
        self.inner.drain.is_active()
    }

    /// Snapshot of the live `ResolverConfig` (cheap clone).
    pub fn config(&self) -> ResolverConfig {
        self.inner.config.clone()
    }

    /// Sleep for `drain_grace`, or until `shutdown` cancels, whichever
    /// fires first. Caller must have already invoked
    /// [`Self::begin_drain`] before this; otherwise peers will not
    /// have had a reason to reroute.
    ///
    /// The race against `shutdown` is intentional: a hard `kill -9`
    /// path needs to be able to skip the grace window. Any code that
    /// genuinely needs the grace to elapse should not also cancel
    /// `shutdown` until after this returns.
    pub async fn wait_drained(&self, shutdown: &CancellationToken) {
        let grace = self.inner.config.drain_grace;
        tokio::select! {
            _ = tokio::time::sleep(grace) => {}
            _ = shutdown.cancelled() => {}
        }
    }

    /// Await the spawned task. Returns once the resolver loop has
    /// observed `shutdown` and exited cleanly. Panics in the spawned
    /// task surface here as `Err(_)`.
    ///
    /// **One-shot.** The internal `JoinHandle` is consumed on the
    /// first call; calling a second time is almost certainly a bug
    /// (the second caller would silently return `Ok(())` without
    /// actually awaiting anything). Named `join_once` rather than
    /// `join` so the call-site is self-documenting, and a
    /// `debug_assert!` catches accidental double-calls in tests.
    pub async fn join_once(&self) -> crate::Result<()> {
        let handle = self.handle.lock().take();
        debug_assert!(
            handle.is_some(),
            "Resolver::join_once called more than once on the same Resolver",
        );
        if let Some(h) = handle {
            h.await.map_err(|e| {
                if e.is_cancelled() {
                    crate::Error::Membership("resolver task cancelled".to_string())
                } else {
                    crate::Error::Membership(format!("resolver task join: {e}"))
                }
            })?;
        }
        Ok(())
    }
}

async fn run_loop(
    inner: Arc<ResolverInner>,
    shutdown: CancellationToken,
    resolver: Arc<dyn HostResolver>,
    probe: Arc<dyn StatsProbe>,
) {
    let mut tick = tokio::time::interval(inner.config.dns_refresh);
    // Don't fire a burst of refreshes after a long stall — one
    // refresh per cycle is enough.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Eat the immediate first tick that `interval` emits; the call
    // site below explicitly runs one refresh before entering the
    // loop, so the first tick would otherwise produce a duplicate.
    tick.tick().await;

    if let Err(e) = refresh_once(&inner, resolver.as_ref(), probe.as_ref()).await {
        tracing::warn!(
            target: "shelfd::membership",
            self_id = %inner.config.self_id,
            error = %e,
            "initial membership refresh failed; ring left empty until next tick"
        );
    }

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(
                    target: "shelfd::membership",
                    self_id = %inner.config.self_id,
                    "shutdown observed; resolver loop exiting"
                );
                return;
            }
            _ = tick.tick() => {
                if let Err(e) = refresh_once(&inner, resolver.as_ref(), probe.as_ref()).await {
                    tracing::warn!(
                        target: "shelfd::membership",
                        self_id = %inner.config.self_id,
                        error = %e,
                        "membership refresh failed; ring view unchanged"
                    );
                }
            }
        }
    }
}

async fn refresh_once(
    inner: &ResolverInner,
    resolver: &dyn HostResolver,
    probe: &dyn StatsProbe,
) -> crate::Result<()> {
    let cfg = &inner.config;
    let ips = resolver
        .resolve(&cfg.headless_service, cfg.stats_port)
        .await?;
    if ips.is_empty() {
        return Err(crate::Error::Membership(format!(
            "dns returned no addresses for {}:{}",
            cfg.headless_service, cfg.stats_port
        )));
    }

    // Probe every IP concurrently. Each probe is independently
    // bounded by `cfg.stats_timeout`; a slow peer cannot stall the
    // round.
    let probes = ips.iter().map(|ip| async move {
        (
            *ip,
            probe.probe(*ip, cfg.stats_port, cfg.stats_timeout).await,
        )
    });
    let results = futures::future::join_all(probes).await;

    let mut snapshots: Vec<(IpAddr, Stats)> = Vec::with_capacity(results.len());
    for (ip, res) in results {
        match res {
            Ok(stats) => snapshots.push((ip, stats)),
            Err(e) => {
                tracing::debug!(
                    target: "shelfd::membership",
                    ip = %ip,
                    error = %e,
                    "stats probe failed; peer dropped from this round"
                );
            }
        }
    }

    let members = build_members(&snapshots, cfg);
    if members.is_empty() {
        // An empty ring after a successful DNS lookup is a soft
        // failure: every probed peer was draining or unreachable.
        // Leave the previous ring view in place by NOT calling
        // `router.update`, so we don't make a transient blip
        // permanent.
        tracing::warn!(
            target: "shelfd::membership",
            self_id = %cfg.self_id,
            probed = snapshots.len(),
            "all peers draining or unreachable; keeping previous ring view"
        );
        return Ok(());
    }

    inner.router.update(members);
    Ok(())
}

/// Build a `Vec<Member>` from the latest `(ip, stats)` probe
/// results.
///
/// Pure function — driven by the loop, exercised directly by unit
/// tests. Filters out:
/// - peers whose `/stats` reports `draining: true`,
/// - peers whose `pod_id` is empty (defensive against a
///   misconfigured peer),
/// - duplicate `pod_id`s (keep the first; this can happen during a
///   StatefulSet pod rename mid-rollout when DNS still has both
///   the old and new IP).
///
/// On the duplicate path, "first" is whichever IP appeared earlier
/// in `snapshots` — typically lexical-smallest because the resolver
/// collects from a `BTreeSet<IpAddr>`. That's not necessarily the
/// warmer or freshest pod, so we emit a `warn!` whenever the dedup
/// fires; per plan B6 the rate of this branch is the operationally
/// useful signal, and the existing `draining: true` filter above
/// already catches the common rolling-restart case.
///
/// Members are returned in `pod_id` order so `Router::update`
/// sees a deterministic input — handy for snapshot tests and for
/// `/admin/ring` to render reproducibly.
pub fn build_members(snapshots: &[(IpAddr, Stats)], cfg: &ResolverConfig) -> Vec<Member> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut first_ip_for: std::collections::HashMap<String, IpAddr> =
        std::collections::HashMap::new();
    let mut members: Vec<Member> = Vec::with_capacity(snapshots.len());
    for (ip, stats) in snapshots {
        if stats.draining {
            continue;
        }
        if stats.pod_id.is_empty() {
            continue;
        }
        if !seen.insert(stats.pod_id.clone()) {
            let kept = first_ip_for.get(&stats.pod_id).copied();
            tracing::warn!(
                pod_id = %stats.pod_id,
                kept_ip = ?kept,
                dropped_ip = %ip,
                "membership: duplicate pod_id from DNS, keeping first IP — \
                 possible mid-rollout pod rename, monitor if persistent",
            );
            continue;
        }
        first_ip_for.insert(stats.pod_id.clone(), *ip);
        let endpoint = match ip {
            IpAddr::V4(v4) => format!("{v4}:{}", cfg.data_port),
            IpAddr::V6(v6) => format!("[{v6}]:{}", cfg.data_port),
        };
        members.push(Member {
            id: stats.pod_id.clone(),
            endpoint,
            weight: weight_for_capacity(stats.capacity_bytes, cfg.weight_unit_bytes),
        });
    }
    members.sort_by(|a, b| a.id.cmp(&b.id));
    members
}

/// Convert a peer's reported capacity into an HRW weight unit.
///
/// Clamped to `[1, u32::MAX]`:
/// - A peer with `0` reported capacity (e.g. mid-startup before the
///   pools are open) still gets a weight of 1, so it doesn't cause
///   `Router` to panic with an empty ring during a roll.
/// - A peer with absurd capacity (multi-EiB on a misconfig) is
///   clamped to `u32::MAX` so the score arithmetic doesn't over-flow.
///
/// 1 GiB == 1 weight unit by default. The caller can pick any other
/// scale via `unit_bytes`.
pub fn weight_for_capacity(capacity_bytes: u64, unit_bytes: u64) -> u32 {
    if unit_bytes == 0 {
        return 1;
    }
    let raw = capacity_bytes / unit_bytes;
    raw.clamp(1, u32::MAX as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{PoolStats, Stats};
    use std::net::Ipv4Addr;
    use std::sync::Mutex as StdMutex;

    fn cfg() -> ResolverConfig {
        ResolverConfig {
            headless_service: "shelf.shelf.svc.cluster.local".to_string(),
            stats_port: 9090,
            data_port: 9092,
            dns_refresh: Duration::from_millis(20),
            stats_timeout: Duration::from_millis(50),
            self_id: "shelf-1".to_string(),
            drain_grace: Duration::from_millis(40),
            weight_unit_bytes: 1024 * 1024 * 1024,
        }
    }

    fn stats(pod_id: &str, capacity: u64, draining: bool) -> Stats {
        Stats {
            pod_id: pod_id.to_string(),
            capacity_bytes: capacity,
            used_bytes: 0,
            metadata_pool: PoolStats {
                capacity_bytes: 0,
                used_bytes: 0,
                disk_used_bytes: 0,
                disk_capacity_bytes: 0,
            },
            rowgroup_pool: PoolStats {
                capacity_bytes: capacity,
                used_bytes: 0,
                disk_used_bytes: 0,
                disk_capacity_bytes: 0,
            },
            pinned_bytes: 0,
            pinned_count: 0,
            draining,
            rss_bytes: 0,
        }
    }

    #[test]
    fn weight_clamps_zero_to_one() {
        assert_eq!(weight_for_capacity(0, 1 << 30), 1);
    }

    #[test]
    fn weight_zero_unit_falls_back_to_one() {
        assert_eq!(weight_for_capacity(123, 0), 1);
    }

    #[test]
    fn weight_scales_linearly() {
        let unit = 1u64 << 30; // 1 GiB
        assert_eq!(weight_for_capacity(unit, unit), 1);
        assert_eq!(weight_for_capacity(5 * unit, unit), 5);
        assert_eq!(weight_for_capacity(32 * unit, unit), 32);
    }

    #[test]
    fn build_members_is_deterministic_order() {
        let cfg = cfg();
        let snaps = vec![
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)),
                stats("shelf-2", 14 << 30, false),
            ),
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                stats("shelf-0", 14 << 30, false),
            ),
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                stats("shelf-1", 14 << 30, false),
            ),
        ];
        let members = build_members(&snaps, &cfg);
        let ids: Vec<&str> = members.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["shelf-0", "shelf-1", "shelf-2"]);
        assert_eq!(members[0].endpoint, "10.0.0.1:9092");
    }

    #[test]
    fn build_members_filters_draining() {
        let cfg = cfg();
        let snaps = vec![
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                stats("shelf-0", 14 << 30, false),
            ),
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                stats("shelf-1", 14 << 30, true),
            ),
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)),
                stats("shelf-2", 14 << 30, false),
            ),
        ];
        let members = build_members(&snaps, &cfg);
        let ids: Vec<&str> = members.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["shelf-0", "shelf-2"]);
    }

    #[test]
    fn build_members_drops_empty_pod_id_and_duplicates() {
        let cfg = cfg();
        let snaps = vec![
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                stats("", 14 << 30, false),
            ),
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
                stats("shelf-1", 14 << 30, false),
            ),
            (
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)),
                stats("shelf-1", 14 << 30, false),
            ),
        ];
        let members = build_members(&snaps, &cfg);
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id, "shelf-1");
        assert_eq!(members[0].endpoint, "10.0.0.2:9092");
    }

    #[test]
    fn drain_signal_round_trips() {
        let s = DrainSignal::new();
        assert!(!s.is_active());
        s.begin();
        assert!(s.is_active());
        // idempotent
        s.begin();
        assert!(s.is_active());

        let s2 = s.clone();
        assert!(s2.is_active());
    }

    /// In-memory `HostResolver` driven by a closure so tests can
    /// rewire the IP set per refresh.
    struct ScriptedResolver {
        plan: StdMutex<Vec<Vec<IpAddr>>>,
    }

    impl ScriptedResolver {
        fn new(plan: Vec<Vec<IpAddr>>) -> Self {
            Self {
                plan: StdMutex::new(plan),
            }
        }
    }

    impl HostResolver for ScriptedResolver {
        fn resolve<'a>(
            &'a self,
            _host: &'a str,
            _port: u16,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::Result<Vec<IpAddr>>> + Send + 'a>,
        > {
            let mut plan = self.plan.lock().unwrap();
            let next = if plan.is_empty() {
                vec![]
            } else if plan.len() == 1 {
                plan[0].clone()
            } else {
                plan.remove(0)
            };
            Box::pin(async move { Ok(next) })
        }
    }

    /// Canned `StatsProbe` returning a static `Stats` per IP.
    struct StaticProbe {
        responses: std::collections::HashMap<IpAddr, Stats>,
    }

    impl StatsProbe for StaticProbe {
        fn probe<'a>(
            &'a self,
            ip: IpAddr,
            _port: u16,
            _timeout: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Stats>> + Send + 'a>>
        {
            let answer = self.responses.get(&ip).cloned();
            Box::pin(async move {
                answer.ok_or_else(|| crate::Error::Membership(format!("no canned stats for {ip}")))
            })
        }
    }

    #[tokio::test]
    async fn resolver_loop_populates_router_from_scripted_io() {
        let cfg = cfg();
        let router = Arc::new(Router::new());
        let drain = DrainSignal::new();
        let shutdown = CancellationToken::new();

        let ip0 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3));
        let dns = Arc::new(ScriptedResolver::new(vec![vec![ip0, ip1, ip2]]));

        let mut responses = std::collections::HashMap::new();
        responses.insert(ip0, stats("shelf-0", 14 << 30, false));
        responses.insert(ip1, stats("shelf-1", 14 << 30, false));
        responses.insert(ip2, stats("shelf-2", 14 << 30, false));
        let probe = Arc::new(StaticProbe { responses });

        let resolver = Resolver::spawn_with(
            cfg,
            Arc::clone(&router),
            drain,
            shutdown.clone(),
            dns,
            probe,
        )
        .expect("spawn resolver");

        // Wait for the first refresh (initial pre-loop call).
        let started = std::time::Instant::now();
        loop {
            if !router.view().members().is_empty() {
                break;
            }
            if started.elapsed() > Duration::from_secs(2) {
                panic!("resolver never populated the ring");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let view = router.view();
        let ids: Vec<&str> = view.members().iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["shelf-0", "shelf-1", "shelf-2"]);

        shutdown.cancel();
        resolver.join_once().await.expect("clean join");
    }

    #[tokio::test]
    async fn resolver_loop_skips_draining_peers() {
        let cfg = cfg();
        let router = Arc::new(Router::new());
        let drain = DrainSignal::new();
        let shutdown = CancellationToken::new();

        let ip0 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        let dns = Arc::new(ScriptedResolver::new(vec![vec![ip0, ip1]]));

        let mut responses = std::collections::HashMap::new();
        responses.insert(ip0, stats("shelf-0", 14 << 30, false));
        responses.insert(ip1, stats("shelf-1", 14 << 30, true));
        let probe = Arc::new(StaticProbe { responses });

        let resolver = Resolver::spawn_with(
            cfg,
            Arc::clone(&router),
            drain,
            shutdown.clone(),
            dns,
            probe,
        )
        .expect("spawn resolver");

        let started = std::time::Instant::now();
        loop {
            if !router.view().members().is_empty() {
                break;
            }
            if started.elapsed() > Duration::from_secs(2) {
                panic!("resolver never populated the ring");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let view = router.view();
        let ids: Vec<&str> = view.members().iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["shelf-0"]);

        shutdown.cancel();
        resolver.join_once().await.expect("clean join");
    }

    #[tokio::test]
    async fn wait_drained_returns_on_grace_elapsed() {
        let mut cfg = cfg();
        cfg.drain_grace = Duration::from_millis(40);
        let router = Arc::new(Router::new());
        let drain = DrainSignal::new();
        let shutdown = CancellationToken::new();

        let dns = Arc::new(ScriptedResolver::new(vec![vec![]]));
        let probe = Arc::new(StaticProbe {
            responses: std::collections::HashMap::new(),
        });

        let resolver = Resolver::spawn_with(
            cfg.clone(),
            Arc::clone(&router),
            drain,
            shutdown.clone(),
            dns,
            probe,
        )
        .expect("spawn resolver");

        resolver.begin_drain();
        assert!(resolver.is_draining());

        let unrelated_shutdown = CancellationToken::new();
        let started = std::time::Instant::now();
        resolver.wait_drained(&unrelated_shutdown).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(35),
            "wait_drained returned early: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "wait_drained ran too long: {elapsed:?}"
        );

        shutdown.cancel();
        resolver.join_once().await.expect("clean join");
    }

    #[tokio::test]
    async fn wait_drained_returns_immediately_on_shutdown() {
        let mut cfg = cfg();
        cfg.drain_grace = Duration::from_secs(60);
        let router = Arc::new(Router::new());
        let drain = DrainSignal::new();
        let shutdown = CancellationToken::new();

        let dns = Arc::new(ScriptedResolver::new(vec![vec![]]));
        let probe = Arc::new(StaticProbe {
            responses: std::collections::HashMap::new(),
        });

        let resolver = Resolver::spawn_with(
            cfg,
            Arc::clone(&router),
            drain,
            shutdown.clone(),
            dns,
            probe,
        )
        .expect("spawn resolver");

        // Hard kill: cancel the same token wait_drained is racing.
        let started = std::time::Instant::now();
        let hard_kill = CancellationToken::new();
        let hard_kill_clone = hard_kill.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            hard_kill_clone.cancel();
        });
        resolver.wait_drained(&hard_kill).await;
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "wait_drained ignored shutdown"
        );

        shutdown.cancel();
        resolver.join_once().await.expect("clean join");
    }

    #[tokio::test]
    async fn refresh_with_no_dns_results_is_a_soft_failure() {
        // DNS returns zero IPs every cycle; the loop must keep
        // running, the ring stays empty, no panic.
        let cfg = cfg();
        let router = Arc::new(Router::new());
        let drain = DrainSignal::new();
        let shutdown = CancellationToken::new();

        let dns = Arc::new(ScriptedResolver::new(vec![vec![]]));
        let probe = Arc::new(StaticProbe {
            responses: std::collections::HashMap::new(),
        });

        let resolver = Resolver::spawn_with(
            cfg,
            Arc::clone(&router),
            drain,
            shutdown.clone(),
            dns,
            probe,
        )
        .expect("spawn resolver");

        // Give the loop a few ticks. Ring should remain empty.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(router.view().members().is_empty());

        shutdown.cancel();
        resolver.join_once().await.expect("clean join");
    }

    #[tokio::test]
    async fn empty_probe_results_keep_previous_ring_view() {
        // If every probed peer is draining/unreachable in a single
        // refresh, the previous ring view must NOT be wiped — that
        // would amplify a transient blip into a "send everything to
        // S3" outage.
        let cfg = cfg();
        let router = Arc::new(Router::new());
        let drain = DrainSignal::new();
        let shutdown = CancellationToken::new();

        let ip0 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        // Pre-seed the router with a healthy view so we can detect
        // its preservation across an all-draining round.
        router.update(vec![Member {
            id: "shelf-0".to_string(),
            endpoint: "10.0.0.1:9092".to_string(),
            weight: 14,
        }]);

        // First DNS round returns ip0; first probe says draining=true.
        // Second DNS round returns nothing.
        let dns = Arc::new(ScriptedResolver::new(vec![vec![ip0], vec![ip0]]));
        let mut responses = std::collections::HashMap::new();
        responses.insert(ip0, stats("shelf-0", 14 << 30, true));
        let probe = Arc::new(StaticProbe { responses });

        let resolver = Resolver::spawn_with(
            cfg,
            Arc::clone(&router),
            drain,
            shutdown.clone(),
            dns,
            probe,
        )
        .expect("spawn resolver");

        // Let a couple of refresh cycles run.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let view = router.view();
        let ids: Vec<&str> = view.members().iter().map(|m| m.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["shelf-0"],
            "ring view should be preserved when every probed peer is draining"
        );

        shutdown.cancel();
        resolver.join_once().await.expect("clean join");
    }
}
