//! SHELF-E6 — peer probe + race-against-origin primitives.
//!
//! On a local miss, `shelfd` can ask one or more *peer* replicas
//! (via the SHELF-D7 `POST /cache/contains` bitmap endpoint) whether
//! they already hold the missing key. If a peer does, it is almost
//! always cheaper to pull a single range from that peer over pod
//! network (O(µs) RTT, same AZ, no S3 egress) than to round-trip
//! S3 again for the same bytes.
//!
//! This module owns the wire primitives and the race logic. It does
//! **not** own membership — that is `router::Router` / `membership::Resolver`
//! territory (SHELF-19/20). Once the resolver lands, the `s3_shim`
//! and `store::get_or_fetch` paths will call `race_peer_or_origin`
//! with `peer_url = Some(<owner pod stats-url>)` wherever the local
//! node is *not* the HRW owner of the key.
//!
//! ## Wire contract
//!
//! The peer probe is a straight `POST /cache/contains` against the
//! target pod's data plane. Response shape is identical to the one
//! documented in [`crate::http::handlers::cache_contains`]:
//!
//! ```json
//! {
//!   "pool": "rowgroup",
//!   "count": N,
//!   "hits": H,
//!   "bitmap_b64": "<base64 bitmap, LSB-first>"
//! }
//! ```
//!
//! ## Race semantics
//!
//! The race is **not** best-of-two. We race the probe (fast, cheap)
//! against the first byte of an S3 fetch. If the probe returns
//! `hit=true` before the origin reader has committed to a socket,
//! we fetch from the peer instead. Otherwise the origin fetch
//! stands and the probe result is discarded. This avoids the
//! classic "tie-storm" where both arms complete and we end up
//! paying for both.
//!
//! ## Budgets
//!
//! - Probe deadline: 10 ms (peer round-trip on a same-AZ k8s cluster
//!   is < 2 ms p99 per the SHELF-08 jitter data; 10 ms absorbs GC
//!   pauses without allowing a slow peer to delay the S3 fallback).
//! - Peer fetch deadline: inherits the outer request deadline; the
//!   caller must not set a deadline shorter than `3 * probe_deadline`
//!   or the peer read will be cancelled mid-stream.

use std::time::Duration;

use crate::http::handlers::ContainsBody;
use base64::Engine as _;

/// Outcome of a single-peer probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// Peer reports it holds all queried keys.
    Hit,
    /// Peer reports it holds none of the queried keys.
    Miss,
    /// Peer reports a partial result (some hit, some miss).
    /// Callers that issued a single-key probe will never see this;
    /// batch callers should inspect the returned bitmap directly via
    /// [`ProbeResult::bitmap`].
    Partial,
    /// Peer was unreachable, timed out, or returned a malformed body.
    Unavailable,
}

/// Full probe result, including the hit-bitmap for batch callers.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub outcome: ProbeOutcome,
    /// Byte-packed hit bitmap (LSB-first). `None` when the peer was
    /// unavailable.
    pub bitmap: Option<Vec<u8>>,
    /// Number of hits reported by the peer. `None` on `Unavailable`.
    pub hits: Option<u64>,
}

impl ProbeResult {
    pub fn unavailable() -> Self {
        Self {
            outcome: ProbeOutcome::Unavailable,
            bitmap: None,
            hits: None,
        }
    }
}

/// Probe a peer's `/cache/contains` endpoint for a batch of keys.
///
/// `peer_base_url` is the peer's HTTP base (e.g.
/// `http://shelf-3.shelf-headless.shelf.svc.cluster.local:9090`);
/// the function appends `/cache/contains` itself so the caller
/// cannot accidentally hit a stale route.
///
/// The probe uses `timeout` as a hard wall-clock deadline — any slow
/// peer is mapped to [`ProbeOutcome::Unavailable`] so the caller's
/// race logic always makes forward progress.
pub async fn probe_peer_contains(
    http: &reqwest::Client,
    peer_base_url: &str,
    pool: &str,
    keys: &[String],
    timeout: Duration,
) -> ProbeResult {
    let url = format!("{}/cache/contains", peer_base_url.trim_end_matches('/'));
    let body = ContainsBody {
        pool: pool.to_owned(),
        keys: keys.to_owned(),
    };
    let fut = http.post(&url).json(&body).send();
    let resp = match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(r)) => r,
        _ => return ProbeResult::unavailable(),
    };
    if !resp.status().is_success() {
        return ProbeResult::unavailable();
    }
    let parsed = match resp.json::<ContainsResponse>().await {
        Ok(p) => p,
        Err(_) => return ProbeResult::unavailable(),
    };
    let bitmap = match base64::engine::general_purpose::STANDARD.decode(&parsed.bitmap_b64) {
        Ok(b) => b,
        Err(_) => return ProbeResult::unavailable(),
    };
    let outcome = match (parsed.hits, parsed.count) {
        (0, _) => ProbeOutcome::Miss,
        (h, c) if h == c as u64 => ProbeOutcome::Hit,
        _ => ProbeOutcome::Partial,
    };
    ProbeResult {
        outcome,
        bitmap: Some(bitmap),
        hits: Some(parsed.hits),
    }
}

/// Wire shape matching what [`crate::http::handlers::cache_contains`]
/// emits. Deserialized separately (not using the handler's internal
/// `serde_json::json!` payload) so future wire changes are caught
/// by compile-time tests here, not by runtime 500s against a peer.
#[derive(Debug, serde::Deserialize)]
struct ContainsResponse {
    #[allow(dead_code)]
    pool: String,
    count: usize,
    hits: u64,
    bitmap_b64: String,
}

/// Decide between "pull from peer" and "pull from origin".
///
/// Returns `true` when the caller should fetch from the peer. Returns
/// `false` for every other outcome (miss, partial, unavailable,
/// timeout) — the caller then falls through to its normal S3 path.
///
/// Kept as a free function so higher layers can drive it with any
/// probe future (unit tests substitute a canned `ProbeResult`).
pub fn peer_is_better(probe: &ProbeResult, single_key: bool) -> bool {
    match probe.outcome {
        ProbeOutcome::Hit => true,
        // A single-key probe never legitimately yields `Partial` —
        // the response is one bit, so it is either `Hit` or `Miss`.
        // Treat an unexpected `Partial` as a miss to stay on the safe
        // side (S3 is always correct, even if slower).
        ProbeOutcome::Partial if !single_key => probe.hits.is_some_and(|h| h > 0),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_is_better_picks_peer_on_hit() {
        let r = ProbeResult {
            outcome: ProbeOutcome::Hit,
            bitmap: Some(vec![0x01]),
            hits: Some(1),
        };
        assert!(peer_is_better(&r, true));
    }

    #[test]
    fn peer_is_better_stays_on_origin_on_miss() {
        let r = ProbeResult {
            outcome: ProbeOutcome::Miss,
            bitmap: Some(vec![0x00]),
            hits: Some(0),
        };
        assert!(!peer_is_better(&r, true));
    }

    #[test]
    fn peer_is_better_stays_on_origin_when_unavailable() {
        assert!(!peer_is_better(&ProbeResult::unavailable(), true));
    }

    #[test]
    fn single_key_partial_is_defensive_miss() {
        // Wire-invalid but make sure the race logic degrades
        // gracefully rather than racing against an empty-peer.
        let r = ProbeResult {
            outcome: ProbeOutcome::Partial,
            bitmap: Some(vec![0x00]),
            hits: Some(0),
        };
        assert!(!peer_is_better(&r, true));
    }

    #[test]
    fn batch_partial_picks_peer_when_any_hit() {
        let r = ProbeResult {
            outcome: ProbeOutcome::Partial,
            bitmap: Some(vec![0b0000_0011]),
            hits: Some(2),
        };
        assert!(peer_is_better(&r, false));
    }
}
