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

use bytes::Bytes;

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

/// SHELF-23 — classification of peer-side failures for the
/// `shelf_peer_error_total{kind}` Prometheus dimension. The variants
/// are intentionally coarse: we want to distinguish "the network
/// dropped" from "the peer answered but I couldn't parse it" from
/// "the peer is unhealthy" without exploding cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerErrorKind {
    /// Transport-layer failure: DNS, refused connection, mid-stream
    /// reset, or `reqwest::Error` while reading the body.
    Network,
    /// Peer returned a non-2xx (4xx or 5xx). The probe / body fetch
    /// itself completed at the HTTP layer.
    Status,
    /// Peer returned 2xx but the body did not parse (base64-decode,
    /// `serde_json::Error`, etc.).
    Decode,
}

impl PeerErrorKind {
    /// Stable label used for the `kind` dimension on
    /// `shelf_peer_error_total`. Match the metric's documented domain:
    /// `network` / `status_5xx` / `decode`.
    pub fn metric_label(self) -> &'static str {
        match self {
            PeerErrorKind::Network => "network",
            PeerErrorKind::Status => "status_5xx",
            PeerErrorKind::Decode => "decode",
        }
    }
}

/// SHELF-23 — outcome of the peer-vs-origin race in
/// [`race_peer_or_origin`]. Each variant maps to exactly one of the
/// `shelf_peer_*_total` counters at the call site so the operator can
/// reconstruct "what happened" from /metrics alone:
///
/// | Variant       | Counter                                       |
/// |---------------|-----------------------------------------------|
/// | `PeerHit`     | `shelf_peer_hit_total{pool}`                  |
/// | `PeerMiss`    | `shelf_peer_miss_total{pool}`                 |
/// | `PeerTimeout` | `shelf_peer_timeout_total{pool}`              |
/// | `PeerError`   | `shelf_peer_error_total{pool, kind}`          |
/// | `OriginRaced` | `shelf_peer_miss_total{pool}` (peer too slow) |
///
/// The non-`PeerHit` variants carry the resolved value of the origin
/// future so the caller can complete its read without a second fetch.
#[derive(Debug)]
pub enum RaceOutcome<O> {
    /// Peer probe succeeded and we read the body from the peer's data
    /// plane before the origin fetch completed.
    PeerHit(Bytes),
    /// Peer probe returned `Miss`; the origin future was awaited.
    PeerMiss(O),
    /// Peer probe deadline elapsed; the origin future was awaited.
    PeerTimeout(O),
    /// Peer probe / body fetch failed before the deadline; the origin
    /// future was awaited.
    PeerError(PeerErrorKind, O),
    /// Origin completed before the peer probe could even return a
    /// verdict. Peer was no help on this request; counted as a miss.
    OriginRaced(O),
}

/// SHELF-23 — race a peer-fetch against an in-flight origin fetch.
///
/// This is the primitive `s3_shim::handle_get_object` and
/// `store::get_or_fetch` call on a local cache miss when the HRW
/// primary for the key is some other pod. The returned [`RaceOutcome`]
/// is exhaustive — there is no path that drops both arms unfinished.
///
/// ### Wire shape
///
/// - Peer probe: `POST /cache/contains` (SHELF-D7) with a single-key
///   body. Same handler that powers [`probe_peer_contains`].
/// - Peer body fetch: `GET /cache/<pool>/<key_hex>/<offset>-<end>`
///   (SHELF-06), where `end = offset + length - 1` (inclusive).
///
/// ### Race semantics
///
/// `tokio::select!` with `biased` ordering: origin runs concurrently
/// with the probe so its TCP/TLS handshake is in flight while we
/// probe. If origin finishes before the probe, peer was strictly
/// slower than the entire S3 GET — return [`RaceOutcome::OriginRaced`]
/// without bothering the peer further. If the probe wins and reports
/// `Hit`, race the peer body fetch against origin the same way. The
/// origin future is **never silently dropped**: every non-`PeerHit`
/// arm awaits it to its terminal value before returning.
///
/// ### Deadlines
///
/// `probe_deadline` (caller-specified, typical 10 ms) gates the peer
/// probe alone. The peer body fetch inherits the outer request
/// deadline via the origin race — if origin completes first, the peer
/// body is dropped with a TCP RST. There is no separate body deadline.
///
/// ### Caller contract
///
/// - `length > 0` (the s3_shim hot path short-circuits zero-length
///   reads before ever calling this function).
/// - `key_hex` is the lowercase 64-char hex of the SHELF-04
///   content-addressed cache key, i.e. the path segment a peer
///   would route through `store::Key::from_hex`.
/// - `peer_base_url` is the peer's data-plane base, e.g.
///   `http://shelf-1.shelf-headless.alluxio.svc.cluster.local:9090`.
///   The function appends `/cache/contains` and `/cache/<pool>/...`
///   so the caller cannot accidentally hit a stale route.
// `too_many_arguments`: every argument is a distinct hot-path value
// (peer URL, pool, key, offset+length, the origin future, the
// deadline). Bundling them into a `RaceArgs` struct adds ceremony
// without removing the underlying coupling — they all flow into the
// same select! body. Caller-side wiring is one line either way.
#[allow(clippy::too_many_arguments)]
pub async fn race_peer_or_origin<F, O>(
    http: &reqwest::Client,
    peer_base_url: &str,
    pool: &str,
    key_hex: &str,
    offset: u64,
    length: u64,
    origin_fut: F,
    probe_deadline: Duration,
) -> RaceOutcome<O>
where
    F: std::future::Future<Output = O> + Send,
    O: Send,
{
    use std::pin::pin;

    let mut origin_fut = pin!(origin_fut);

    // Step 1: probe-with-timeout, racing concurrent origin progress.
    // `biased` makes `select!` poll origin first; if origin completes
    // before the probe even returns, peer cannot win, so short-circuit
    // to OriginRaced and skip the body fetch entirely.
    let probe = single_key_probe(http, peer_base_url, pool, key_hex);
    let probe = tokio::time::timeout(probe_deadline, probe);

    let probe_result = tokio::select! {
        biased;
        o = &mut origin_fut => return RaceOutcome::OriginRaced(o),
        r = probe => r,
    };

    let hit = match probe_result {
        Ok(Ok(h)) => h,
        Ok(Err(kind)) => return RaceOutcome::PeerError(kind, origin_fut.await),
        Err(_) => return RaceOutcome::PeerTimeout(origin_fut.await),
    };

    if !hit {
        return RaceOutcome::PeerMiss(origin_fut.await);
    }

    // Step 2: peer body fetch, racing origin one more time. Origin
    // already has had `probe_deadline` of head start; the peer fetch
    // is on a same-AZ pod-to-pod link and typically finishes in
    // single-digit ms, so it usually wins — but if origin is hot
    // (TCP keep-alive, S3 cache hit), origin can still beat the peer.
    let body = peer_body_fetch(http, peer_base_url, pool, key_hex, offset, length);
    let mut body = pin!(body);

    tokio::select! {
        biased;
        o = &mut origin_fut => RaceOutcome::OriginRaced(o),
        b = &mut body => match b {
            Ok(bytes) => RaceOutcome::PeerHit(bytes),
            Err(kind) => RaceOutcome::PeerError(kind, origin_fut.await),
        },
    }
}

/// Single-key wrapper around `POST /cache/contains` that distinguishes
/// the three error classes [`race_peer_or_origin`] needs to count
/// separately on the dashboard. Kept private — the public
/// [`probe_peer_contains`] continues to collapse all errors to
/// `Unavailable` for backward-compat with prior batch callers.
async fn single_key_probe(
    http: &reqwest::Client,
    peer_base_url: &str,
    pool: &str,
    key_hex: &str,
) -> Result<bool, PeerErrorKind> {
    let url = format!("{}/cache/contains", peer_base_url.trim_end_matches('/'));
    let body = ContainsBody {
        pool: pool.to_owned(),
        keys: vec![key_hex.to_owned()],
    };
    let resp = http
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|_| PeerErrorKind::Network)?;
    if !resp.status().is_success() {
        return Err(PeerErrorKind::Status);
    }
    let parsed: ContainsResponse = resp.json().await.map_err(|_| PeerErrorKind::Decode)?;
    // Single-key probe: hits is either 0 or 1. Treat any unexpected
    // shape (>1 hit on a 1-key body) as the safe answer "miss" so a
    // bug at the peer cannot silently push us onto a peer that does
    // not actually have the key.
    Ok(parsed.hits == 1 && parsed.count == 1)
}

/// Pull a byte range from a peer's `/cache/<pool>/<key>/<offset>-<end>`
/// data-plane endpoint. Mirrors the `range_str` shape parsed by
/// [`crate::http::handlers::get_cache`] (inclusive `end`).
async fn peer_body_fetch(
    http: &reqwest::Client,
    peer_base_url: &str,
    pool: &str,
    key_hex: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, PeerErrorKind> {
    // length is contractually > 0; saturate defensively against a
    // future caller bug rather than panicking on `0u64 - 1`.
    let end = offset.saturating_add(length).saturating_sub(1).max(offset);
    let url = format!(
        "{}/cache/{}/{}/{}-{}",
        peer_base_url.trim_end_matches('/'),
        pool,
        key_hex,
        offset,
        end,
    );
    // SHELF-23 — recursion guard. The receiving pod inspects this
    // header on its `/cache/<pool>/<key>/<range>` handler and skips
    // its own peer-fetch wrapping when set, so a peer hop never
    // bounces off a third pod. See `peer_fetch::PEER_FETCH_HEADER`.
    let resp = http
        .get(&url)
        .header(crate::peer_fetch::PEER_FETCH_HEADER, "1")
        .send()
        .await
        .map_err(|_| PeerErrorKind::Network)?;
    if !resp.status().is_success() {
        return Err(PeerErrorKind::Status);
    }
    resp.bytes().await.map_err(|_| PeerErrorKind::Network)
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

    /// SHELF-23 — race tests. Spin a tiny axum mock peer per test on
    /// `127.0.0.1:0` (kernel-allocated port) and exercise each branch
    /// of `race_peer_or_origin`. The mock implements just enough of
    /// the SHELF-D7 + SHELF-06 wire to drive the racer; the real
    /// `http::handlers` module owns the production semantics.
    mod race {
        use super::super::*;
        use axum::body::Bytes as AxumBytes;
        use axum::extract::{Path as AxumPath, Query, State};
        use axum::response::IntoResponse;
        use axum::routing::{get, post};
        use axum::{Json, Router};
        use base64::Engine as Base64Engine;
        use serde::Deserialize;
        use std::net::SocketAddr;
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::net::TcpListener;

        /// Per-test mock-peer behaviour.
        #[derive(Default, Clone)]
        struct PeerBehaviour {
            /// Result of `POST /cache/contains` for a single key.
            ///
            /// - `Some(true)`  → respond hits=1 (single-key Hit)
            /// - `Some(false)` → respond hits=0 (single-key Miss)
            /// - `None`        → return 500 (status-error path)
            probe: Option<bool>,
            /// If set, the probe handler sleeps this long before
            /// answering. Drives the timeout test.
            probe_delay: Option<Duration>,
            /// Body served on `GET /cache/<pool>/<key>/<range>` when
            /// `probe = Some(true)`. `None` makes the body endpoint
            /// 500 (peer-error-on-body path).
            body: Option<Vec<u8>>,
            /// If set, body handler sleeps this long before answering.
            body_delay: Option<Duration>,
        }

        #[derive(Deserialize)]
        struct ContainsReq {
            pool: String,
            keys: Vec<String>,
        }

        async fn handle_contains(
            State(beh): State<Arc<PeerBehaviour>>,
            Json(req): Json<ContainsReq>,
        ) -> axum::response::Response {
            assert_eq!(req.keys.len(), 1, "race tests use single-key probes");
            if let Some(d) = beh.probe_delay {
                tokio::time::sleep(d).await;
            }
            match beh.probe {
                None => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "broken").into_response(),
                Some(hit) => {
                    let hits: u64 = if hit { 1 } else { 0 };
                    let bitmap = if hit { vec![0x01u8] } else { vec![0x00u8] };
                    let body = serde_json::json!({
                        "pool": req.pool,
                        "count": 1,
                        "hits": hits,
                        "bitmap_b64": Base64Engine::encode(&base64::engine::general_purpose::STANDARD, &bitmap),
                    });
                    (axum::http::StatusCode::OK, Json(body)).into_response()
                }
            }
        }

        async fn handle_get_cache(
            State(beh): State<Arc<PeerBehaviour>>,
            AxumPath((_pool, _key, _range)): AxumPath<(String, String, String)>,
            _q: Query<std::collections::HashMap<String, String>>,
        ) -> axum::response::Response {
            if let Some(d) = beh.body_delay {
                tokio::time::sleep(d).await;
            }
            match beh.body.as_ref() {
                Some(b) => {
                    (axum::http::StatusCode::OK, AxumBytes::copy_from_slice(b)).into_response()
                }
                None => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no body").into_response(),
            }
        }

        async fn spawn_mock(beh: PeerBehaviour) -> (String, tokio::task::JoinHandle<()>) {
            let app = Router::new()
                .route("/cache/contains", post(handle_contains))
                .route("/cache/{pool}/{key}/{range}", get(handle_get_cache))
                .with_state(Arc::new(beh));
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr: SocketAddr = listener.local_addr().expect("addr");
            let handle = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            (format!("http://{addr}"), handle)
        }

        fn http_client() -> reqwest::Client {
            reqwest::Client::builder()
                .pool_max_idle_per_host(2)
                .timeout(Duration::from_secs(2))
                .build()
                .expect("reqwest")
        }

        /// Origin future that returns a sentinel `Vec<u8>` after the
        /// given delay. Used to drive both "origin slower than peer"
        /// (long delay) and "origin races peer" (zero delay) paths.
        async fn origin(delay: Duration, payload: Vec<u8>) -> Vec<u8> {
            tokio::time::sleep(delay).await;
            payload
        }

        const KEY_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000001";

        #[tokio::test]
        async fn peer_hit_returns_peer_bytes() {
            let beh = PeerBehaviour {
                probe: Some(true),
                body: Some(b"peer-bytes".to_vec()),
                ..PeerBehaviour::default()
            };
            let (base, _h) = spawn_mock(beh).await;
            let http = http_client();
            // Origin is slow so peer wins the body race.
            let outcome = race_peer_or_origin(
                &http,
                &base,
                "rowgroup",
                KEY_HEX,
                0,
                10,
                origin(Duration::from_secs(2), b"origin-bytes".to_vec()),
                Duration::from_millis(50),
            )
            .await;
            match outcome {
                RaceOutcome::PeerHit(b) => assert_eq!(b.as_ref(), b"peer-bytes"),
                other => panic!("expected PeerHit, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn peer_miss_falls_through_to_origin() {
            let beh = PeerBehaviour {
                probe: Some(false),
                ..PeerBehaviour::default()
            };
            let (base, _h) = spawn_mock(beh).await;
            let http = http_client();
            // Origin delay is generous so axum first-request setup
            // cost (~30-50 ms cold) cannot accidentally race ahead of
            // the probe under parallel test execution and trip the
            // OriginRaced branch — that's tested separately in
            // `fast_origin_wins_race`.
            let outcome = race_peer_or_origin(
                &http,
                &base,
                "rowgroup",
                KEY_HEX,
                0,
                10,
                origin(Duration::from_millis(500), b"origin-bytes".to_vec()),
                Duration::from_millis(200),
            )
            .await;
            match outcome {
                RaceOutcome::PeerMiss(o) => assert_eq!(o, b"origin-bytes"),
                other => panic!("expected PeerMiss, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn peer_probe_timeout_falls_through_to_origin() {
            let beh = PeerBehaviour {
                probe: Some(true),
                // Force the probe past `probe_deadline`.
                probe_delay: Some(Duration::from_millis(120)),
                body: Some(b"unused".to_vec()),
                ..PeerBehaviour::default()
            };
            let (base, _h) = spawn_mock(beh).await;
            let http = http_client();
            let outcome = race_peer_or_origin(
                &http,
                &base,
                "rowgroup",
                KEY_HEX,
                0,
                10,
                origin(Duration::from_millis(300), b"origin-bytes".to_vec()),
                Duration::from_millis(20), // < probe_delay
            )
            .await;
            match outcome {
                RaceOutcome::PeerTimeout(o) => assert_eq!(o, b"origin-bytes"),
                other => panic!("expected PeerTimeout, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn peer_probe_500_is_status_error() {
            let beh = PeerBehaviour {
                probe: None, // → 500 on probe
                ..PeerBehaviour::default()
            };
            let (base, _h) = spawn_mock(beh).await;
            let http = http_client();
            let outcome = race_peer_or_origin(
                &http,
                &base,
                "rowgroup",
                KEY_HEX,
                0,
                10,
                origin(Duration::from_millis(500), b"origin-bytes".to_vec()),
                Duration::from_millis(200),
            )
            .await;
            match outcome {
                RaceOutcome::PeerError(PeerErrorKind::Status, o) => {
                    assert_eq!(o, b"origin-bytes")
                }
                other => panic!("expected PeerError(Status), got {other:?}"),
            }
        }

        #[tokio::test]
        async fn peer_body_500_is_status_error() {
            let beh = PeerBehaviour {
                probe: Some(true),
                body: None, // → 500 on body fetch
                ..PeerBehaviour::default()
            };
            let (base, _h) = spawn_mock(beh).await;
            let http = http_client();
            let outcome = race_peer_or_origin(
                &http,
                &base,
                "rowgroup",
                KEY_HEX,
                0,
                10,
                origin(Duration::from_millis(500), b"origin-bytes".to_vec()),
                Duration::from_millis(200),
            )
            .await;
            match outcome {
                RaceOutcome::PeerError(PeerErrorKind::Status, o) => {
                    assert_eq!(o, b"origin-bytes")
                }
                other => panic!("expected PeerError(Status), got {other:?}"),
            }
        }

        #[tokio::test]
        async fn unreachable_peer_is_network_error() {
            // Bind a TcpListener and drop it so the OS releases the
            // port; the URL we point at will refuse connections. This
            // exercises the network-error branch deterministically.
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr: SocketAddr = listener.local_addr().expect("addr");
            drop(listener);
            let base = format!("http://{addr}");
            let http = http_client();
            let outcome = race_peer_or_origin(
                &http,
                &base,
                "rowgroup",
                KEY_HEX,
                0,
                10,
                origin(Duration::from_millis(400), b"origin-bytes".to_vec()),
                Duration::from_millis(50),
            )
            .await;
            // Either Network (refused) or Timeout (probe deadline) is
            // legitimate here; both fall back to origin. The point is
            // the call does not hang and returns origin bytes.
            match outcome {
                RaceOutcome::PeerError(PeerErrorKind::Network, o) | RaceOutcome::PeerTimeout(o) => {
                    assert_eq!(o, b"origin-bytes")
                }
                other => {
                    panic!("expected PeerError(Network) or PeerTimeout, got {other:?}")
                }
            }
        }

        #[tokio::test]
        async fn fast_origin_wins_race() {
            // Peer is healthy and would have hit, but origin is so
            // fast it short-circuits before the probe responds.
            let beh = PeerBehaviour {
                probe: Some(true),
                probe_delay: Some(Duration::from_millis(40)),
                body: Some(b"peer-bytes".to_vec()),
                ..PeerBehaviour::default()
            };
            let (base, _h) = spawn_mock(beh).await;
            let http = http_client();
            let outcome = race_peer_or_origin(
                &http,
                &base,
                "rowgroup",
                KEY_HEX,
                0,
                10,
                // Origin returns immediately (already-warm S3 ~ TCP keep-alive).
                origin(Duration::from_millis(0), b"origin-bytes".to_vec()),
                Duration::from_millis(100),
            )
            .await;
            match outcome {
                RaceOutcome::OriginRaced(o) => assert_eq!(o, b"origin-bytes"),
                other => panic!("expected OriginRaced, got {other:?}"),
            }
        }
    }
}
