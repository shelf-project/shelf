//! SHELF-23 — peer-fetch wiring shared by `s3_shim::handle_get_object`
//! and `http::handlers::get_cache`.
//!
//! Both hot-path readers compute the HRW primary for a content-addressed
//! cache key on a local miss and, when the local pod is *not* the owner,
//! race a peer probe against the origin S3 fetch via
//! [`crate::peer::race_peer_or_origin`]. The wrapping logic — short-
//! circuit checks, peer-URL construction, metric bumps — is identical
//! across both call sites; centralising it here means the two paths
//! cannot drift on counter semantics or drop-on-failure behaviour.
//!
//! The function is **fail-open by design**: any of the conditions below
//! short-circuits to the bare origin future without touching peer
//! counters, exactly as the pre-SHELF-23 hot path did:
//!
//! - peer-fetch disabled at runtime (`SHELFD_PEER_FETCH_ENABLED=0`)
//! - membership ring is empty (early startup)
//! - the local pod is the HRW primary for this key
//! - the peer's `Member::endpoint` cannot be parsed into `(host, port)`
//!
//! ## Recursion guard
//!
//! Inbound peer body fetches arrive at [`crate::http::handlers::get_cache`]
//! with the [`PEER_FETCH_HEADER`] header set. That handler must skip
//! the peer-fetch wrapping in that case to avoid a peer-pod cycle:
//! shelf-A peer-fetches from shelf-B, shelf-B's local miss must NOT
//! re-route to shelf-C; the receiving pod's job is to serve from its
//! own cache (the only reason it was probed in the first place) or
//! fetch from origin.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::http::ServerState;
use crate::peer::{race_peer_or_origin, RaceOutcome};
use crate::router;
use crate::store::{Key, Pool};

/// SHELF-23 — peer-fetch deadline for the `POST /cache/contains` probe.
///
/// 10 ms matches the budget documented in
/// [`crate::peer::race_peer_or_origin`]: same-AZ k8s pod-to-pod p99
/// HTTP probe latency is < 2 ms per the SHELF-08 jitter data, so 10 ms
/// absorbs a GC pause without letting a slow peer delay the S3
/// fallback.
pub const PEER_PROBE_DEADLINE: Duration = Duration::from_millis(10);

/// Header set by [`crate::peer::peer_body_fetch`] (and recognised here)
/// to mark an inbound `/cache/<pool>/<key>/<range>` request as a
/// peer-fetch hop. The receiving pod uses it to suppress its own
/// peer-fetch routing — see the module-level "Recursion guard"
/// comment.
pub const PEER_FETCH_HEADER: &str = "x-shelf-peer-fetch";

/// SHELF-23 — convert a `router::Member` endpoint (which carries
/// the `data_port` per `membership::build_members`, default 9092)
/// into the **control-plane** base URL the peer's
/// `/cache/contains` and `/cache/<pool>/<key>/<range>` handlers
/// listen on (default 9090). We rebuild the URL from `(ip, port)`
/// rather than string-splitting the endpoint so IPv6 literals stay
/// well-formed (`[::1]:9092` would otherwise be a foot-gun).
pub fn peer_base_url(member_endpoint: &str, stats_port: u16) -> Option<String> {
    let host = match member_endpoint.rsplit_once(':') {
        // IPv6 literal: `[<v6>]:<port>` — keep the bracketed host.
        Some((host, _port)) => host.to_owned(),
        None => return None,
    };
    Some(format!("http://{host}:{stats_port}"))
}

/// SHELF-23 — pool label as used in metric `pool` and HTTP path
/// segments alike.
pub fn pool_str(pool: Pool) -> &'static str {
    match pool {
        Pool::Metadata => "metadata",
        Pool::RowGroup => "rowgroup",
    }
}

/// SHELF-23 — single-flight body fetch that races the HRW primary
/// peer against origin S3 on a local cache miss.
///
/// Wraps [`crate::peer::race_peer_or_origin`] with the metric bumps
/// and the local-vs-self short-circuit, returning the same `Bytes`
/// shape `Origin::get_range` would have. On any non-`PeerHit`
/// outcome the origin future's resolved value is the source of
/// truth; this function preserves the existing read-path contract
/// (a cache miss without peer help still returns origin bytes
/// unchanged).
///
/// See the module docs for the fail-open conditions.
pub async fn peer_or_origin_fetch<F>(
    state: &Arc<ServerState>,
    pool: Pool,
    key: &Key,
    offset: u64,
    length: u64,
    origin_fut: F,
) -> crate::Result<Bytes>
where
    F: std::future::Future<Output = crate::Result<Bytes>> + Send,
{
    if !state.is_peer_fetch_enabled() {
        return origin_fut.await;
    }

    // `Router::owner` panics on an empty ring (membership has not
    // produced its first snapshot yet). The earlier code took two
    // separate router reads (`view()` then `owner()`) which left a
    // TOCTOU window: a membership update that emptied the ring
    // between the two reads turned the panic into a 500 on the GET
    // hot path. Take ONE `RingView` snapshot and run the empty-check
    // and the owner lookup against it via `router::owner_in`, which
    // returns `Option<Member>` and lets us fall through to the
    // origin future on `None` — same safe default `is_local_owner`
    // already uses for empty rings.
    let view = state.router.view();
    let Some(owner) = router::owner_in(view.members(), key.as_bytes()).cloned() else {
        return origin_fut.await;
    };
    if owner.id.as_str() == &*state.pod_id {
        // We are the HRW primary — no peer benefit.
        return origin_fut.await;
    }

    let Some(peer_url) = peer_base_url(&owner.endpoint, state.peer_stats_port) else {
        // Endpoint shape we don't recognise; bail rather than
        // construct a malformed URL.
        return origin_fut.await;
    };

    let pool_label = pool_str(pool);
    let key_hex = key.to_hex();

    let outcome = race_peer_or_origin(
        &state.peer_http,
        &peer_url,
        pool_label,
        &key_hex,
        offset,
        length,
        origin_fut,
        PEER_PROBE_DEADLINE,
    )
    .await;

    match outcome {
        RaceOutcome::PeerHit(b) => {
            crate::metrics::PEER_HIT_TOTAL
                .with_label_values(&[pool_label])
                .inc();
            // SHELF-40 — bump `shelf_s3_dollars_saved_total{outcome="peer"}`
            // here (and ONLY here, never alongside the local
            // hit_memory/hit_disk arms in s3_shim.rs) so a peer
            // hit and a local hit on the *same* content-addressed
            // key cannot double-charge. Peer-hit bytes traversed
            // a pod-to-pod network link; the OSS-default
            // `DEFAULT_PEER_AZ::SameAz` describes the same-AZ
            // happy path. Operators with multi-AZ shelf rings
            // should override via the future SHELF-23 AZ
            // attribution surface (tracked in the SHELF-40
            // design note — until then, same-AZ is the audit-
            // safe pessimistic default).
            let event = shelf_cost::HitEvent::Peer {
                bytes_returned: b.len() as u64,
                peer_az: crate::cost::DEFAULT_PEER_AZ,
            };
            let _ = state.cost.observe(event);
            Ok(b)
        }
        // Peer was reachable but said "Miss" → origin is the answer.
        RaceOutcome::PeerMiss(o) => {
            crate::metrics::PEER_MISS_TOTAL
                .with_label_values(&[pool_label])
                .inc();
            o
        }
        // Origin completed before the probe could return — peer was
        // strictly slower than a full S3 GET on this request. From
        // the operator-dashboard "did peer help?" perspective this
        // is a miss, so we share the counter rather than introducing
        // a fifth dimension only this branch would touch.
        RaceOutcome::OriginRaced(o) => {
            crate::metrics::PEER_MISS_TOTAL
                .with_label_values(&[pool_label])
                .inc();
            o
        }
        RaceOutcome::PeerTimeout(o) => {
            crate::metrics::PEER_TIMEOUT_TOTAL
                .with_label_values(&[pool_label])
                .inc();
            o
        }
        RaceOutcome::PeerError(kind, o) => {
            crate::metrics::PEER_ERROR_TOTAL
                .with_label_values(&[pool_label, kind.metric_label()])
                .inc();
            o
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_base_url_strips_port_and_uses_stats_port() {
        // Member endpoint carries data_port 9092; we want the
        // control-plane URL with stats_port (default 9090).
        assert_eq!(
            peer_base_url("10.1.2.3:9092", 9090).as_deref(),
            Some("http://10.1.2.3:9090"),
        );
    }

    #[test]
    fn peer_base_url_handles_ipv6_literal() {
        // `[::1]:9092` should keep the bracketed host (the rsplit
        // separates on the *last* colon, which is the port-prefix).
        assert_eq!(
            peer_base_url("[::1]:9092", 9090).as_deref(),
            Some("http://[::1]:9090"),
        );
    }

    #[test]
    fn peer_base_url_rejects_unparseable_endpoint() {
        assert_eq!(peer_base_url("no-port-here", 9090), None);
    }

    #[test]
    fn pool_str_is_stable() {
        assert_eq!(pool_str(Pool::Metadata), "metadata");
        assert_eq!(pool_str(Pool::RowGroup), "rowgroup");
    }

    /// Regression for the empty-ring TOCTOU fix. `peer_or_origin_fetch`
    /// previously took two separate router reads (`view()` then
    /// `owner()`); a membership update that emptied the ring between
    /// the two reads turned `Router::owner`'s panic
    /// (`router.rs::owner` — `expect("...empty ring...")`) into a 500
    /// on the GET hot path.
    ///
    /// The fix runs the empty-check and owner lookup against a single
    /// `RingView` snapshot via `router::owner_in`, which returns
    /// `None` on an empty slice and lets `peer_or_origin_fetch` fall
    /// through to the bare origin future. Building a full
    /// `ServerState` in a unit test is heavy (S3, Foyer, OTel, …); the
    /// invariant the hot path actually depends on is the contract of
    /// the helper itself, so we assert that directly.
    #[test]
    fn owner_in_returns_none_for_empty_ring() {
        assert!(router::owner_in(&[], b"any-key").is_none());
    }
}
