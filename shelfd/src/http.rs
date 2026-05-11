//! HTTP data-plane server for `shelfd`.
//!
//! Ticket ownership:
//! - SHELF-02 — Axum router, `/healthz`, `/readyz`, `/metrics`,
//!   structured `tracing` logging, graceful shutdown.
//! - SHELF-06 — `GET /cache/<pool>/<key>/<offset>-<end>` with Foyer
//!   read-through, single-flight coalescing, `Content-Range` header.
//! - SHELF-07 — `HEAD /cache/<pool>/<key>` for plugin pre-flight
//!   (scaffolded but body deferred).
//! - SHELF-08 — Prometheus `/metrics` + OTel trace spans.
//! - ADR-0004 — Data plane is HTTP/1.1+HTTP/2 via `hyper-util`'s auto
//!   builder. TLS + strict h2-only come with SHELF-28.
//!
//! Route shape (see [`build_router`]): the `:pool` path segment selects
//! between the two Foyer pools instead of a custom header, so every
//! request is self-contained and greppable in access logs.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, head, post};
use axum::Router;
use bytes::Bytes;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{field, Instrument};

use crate::control::{PoolStats, Stats};
use crate::head_lru::{HeadLru, HeadMeta};
use crate::membership::DrainSignal;
use crate::store::{Key, Pool, ReadOutcome, Store};

/// Shared state the router hands to every handler.
#[derive(Debug)]
pub struct ServerState {
    pub store: Arc<crate::store::FoyerStore>,
    pub origin: Arc<crate::origin::S3Origin>,
    pub router: Arc<crate::router::Router>,
    pub admission: Arc<dyn crate::admission::AdmissionPolicy>,
    pub metrics: Arc<crate::metrics::Registry>,
    /// LRU of `HeadObject` responses keyed on `(bucket, s3_key)`
    /// (SHELF-07). Wired in via [`ServerState::with_head_lru_and_pod_id`];
    /// [`ServerState::new`] builds a 10 000-entry default.
    pub head_lru: Arc<HeadLru>,
    /// Pod identity surfaced on `GET /stats` so the plugin can weight
    /// the HRW ring (SHELF-20). Defaults to `SHELFD_POD_ID` →
    /// `HOSTNAME` → `"shelfd-unknown"`.
    pub pod_id: Arc<str>,
    /// SHELF-24: handle to the background pin-list loader. `None`
    /// when the loader is not configured (dev / unit tests) — the
    /// `POST /admin/reload` handler then returns a `503`-equivalent
    /// message without pretending it did a reload.
    pub reload_handle: Option<crate::pinlist::ReloadHandle>,
    /// Set to `true` by `main` after startup probes finish. A future
    /// membership loop may flip it back to false on degradation.
    pub ready: AtomicBool,
    /// SHELF-20: lameduck drain bit. Cloned from `main`, which flips
    /// it on `SIGTERM` before the data plane shuts down. Surfaced on
    /// `GET /stats` so peers' resolvers drop us from their HRW rings
    /// during the grace window.
    pub drain_signal: DrainSignal,
    /// SHELF-22 cap on unbounded `GetObject` — any `GET /:bucket/*key`
    /// without a `Range:` header whose object size exceeds this value
    /// responds `501 NotImplemented` with an S3 XML envelope. Kept as
    /// `AtomicU64` so integration tests can dial it down without
    /// rebuilding `ServerState`. Defaults to 256 MiB; `main` seeds it
    /// from `config.s3_shim.max_full_object_bytes` at startup.
    pub s3_shim_max_full_object_bytes: AtomicU64,
    /// SHELF-G4 — optional `ShelfFilterService`. `None` means
    /// `POST /filter/probe` responds with `fail_open: true` for
    /// every request (the safe default when no signal providers
    /// have been wired in). `main` installs one once D3 and G2
    /// have landed their providers.
    pub filter_service: Option<Arc<crate::filter_service::ShelfFilterService>>,
    /// SHELF-G6 — optional text-index lookup. Keyed by
    /// `(table_fqn, column)`; `None` anywhere along the chain
    /// makes `POST /textindex/probe` fail open. Guarded by a
    /// `RwLock` so operators can hot-swap indexes via admin RPCs
    /// without a restart.
    pub text_index: std::sync::RwLock<
        std::collections::HashMap<(String, String), crate::text_index::KeywordIndex>,
    >,
    /// Track H5 — registry of content-addressed keys that belong to
    /// a pinned Iceberg materialized view. Written by the H3
    /// mv-pin-watcher via `POST /admin/pin` (when the body carries
    /// an `mv_name` field) and read on every served
    /// `GET /cache/:pool/:key` response so the shim can bump the
    /// `shelf_mv_hits_total{mv_name}` + `shelf_mv_bytes_served_total`
    /// counters without adding a dimension to the existing
    /// `shelf_hits_total` / `shelf_s3_shim_response_bytes_total`
    /// series (which would blow cardinality).
    pub mv_registry: Arc<crate::mv_registry::MvRegistry>,
    /// SHELF-23 — shared `reqwest::Client` used for peer-fetch (the
    /// `POST /cache/contains` probe and the `GET /cache/<pool>/<key>/<range>`
    /// body fetch on the HRW primary peer). Kept on `ServerState` so
    /// connections to peer pods stay pooled across requests; the
    /// alternative (per-call `Client::new()`) would burn a TLS / TCP
    /// handshake on every cross-pod cache miss. Off-cluster tests
    /// can substitute a `Client::new()` since this client only has
    /// to speak plain HTTP/1.1 to in-cluster pods.
    pub peer_http: reqwest::Client,
    /// SHELF-23 — port the peer's data plane listens on for
    /// `/cache/contains` and `/cache/<pool>/<key>/<range>`. The
    /// `Member::endpoint` carried by `Router` uses
    /// `ResolverConfig::data_port` (the s3-shim, default 9092), but
    /// peer-fetch needs the **control-plane** port (default 9090).
    /// We store it here so the s3_shim hot path can rewrite the
    /// endpoint port without re-plumbing the membership resolver.
    pub peer_stats_port: u16,
    /// SHELF-23 — runtime kill-switch for peer-fetch. Defaults to
    /// `true`; operator can flip it via env var or admin API
    /// without rebuilding the binary if the racer has to be cut
    /// out of the data plane in an incident. The s3_shim hot path
    /// reads this via `Ordering::Relaxed` once per request.
    pub peer_fetch_enabled: AtomicBool,
    /// SHELF-23 — per-(bucket, key) freshness tracker for the
    /// ETag-conditional GET path. Counts consecutive 304s and gates
    /// whether the s3_shim hot path can skip the conditional round-
    /// trip on a local cache hit. Sized at `2 × head_lru.capacity()`
    /// in `with_head_lru_and_pod_id` because the cardinality model
    /// is the same as the HEAD-LRU.
    pub freshness: Arc<crate::freshness::FreshnessTracker>,
    /// SHELF-23 — runtime kill-switch for ETag-conditional GET on
    /// local cache hits. Defaults to `true`. When false, every read
    /// behaves exactly as the pre-SHELF-23 path: cache hit returns
    /// stored bytes without re-validating origin. Useful as a
    /// fast-revert lever if conditional GETs reveal a hot-bug.
    pub conditional_get_enabled: AtomicBool,
}

impl ServerState {
    /// Construct state with default HEAD-LRU (10 000 entries) and a
    /// `pod_id` derived from env. Suitable for integration tests and
    /// backwards-compatible callers; `main` prefers
    /// [`ServerState::with_head_lru_and_pod_id`] to thread the
    /// operator-supplied values through.
    pub fn new(
        store: Arc<crate::store::FoyerStore>,
        origin: Arc<crate::origin::S3Origin>,
        router: Arc<crate::router::Router>,
        admission: Arc<dyn crate::admission::AdmissionPolicy>,
        metrics: Arc<crate::metrics::Registry>,
    ) -> Self {
        Self::with_head_lru_and_pod_id(
            store,
            origin,
            router,
            admission,
            metrics,
            Arc::new(HeadLru::new(10_000)),
            default_pod_id(),
        )
    }

    /// Explicit-argument constructor used by `main` once config +
    /// env have been parsed.
    pub fn with_head_lru_and_pod_id(
        store: Arc<crate::store::FoyerStore>,
        origin: Arc<crate::origin::S3Origin>,
        router: Arc<crate::router::Router>,
        admission: Arc<dyn crate::admission::AdmissionPolicy>,
        metrics: Arc<crate::metrics::Registry>,
        head_lru: Arc<HeadLru>,
        pod_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            store,
            origin,
            router,
            admission,
            metrics,
            head_lru,
            pod_id: pod_id.into(),
            reload_handle: None,
            ready: AtomicBool::new(false),
            drain_signal: DrainSignal::default(),
            // SHELF-22 default cap: 256 MiB. `main` overwrites
            // this from config before any traffic arrives.
            s3_shim_max_full_object_bytes: AtomicU64::new(256 * 1024 * 1024),
            filter_service: None,
            text_index: std::sync::RwLock::new(std::collections::HashMap::new()),
            mv_registry: Arc::new(crate::mv_registry::MvRegistry::new()),
            peer_http: default_peer_http(),
            // SHELF-23 default — matches `membership::DEFAULT_STATS_PORT`.
            // `main` overrides this with the operator-supplied
            // `config.membership.stats_port` before traffic arrives.
            peer_stats_port: crate::membership::DEFAULT_STATS_PORT,
            peer_fetch_enabled: AtomicBool::new(true),
            // Match the head_lru capacity (passed in above) so the
            // freshness tracker has the same population window. We
            // can't read `head_lru.capacity()` here without losing
            // ownership semantics, so default to 2 × the SHELF-07
            // default. `main` does not override this — it's a small
            // foyer cache and self-evicts.
            freshness: Arc::new(crate::freshness::FreshnessTracker::new(20_000)),
            conditional_get_enabled: AtomicBool::new(true),
        }
    }

    /// SHELF-23 builder: install the operator-supplied peer-fetch
    /// HTTP client and stats port. `main` calls this from the
    /// post-config hookup path; callers that don't need the peer
    /// race (most unit tests) inherit the [`default_peer_http`]
    /// client and the [`crate::membership::DEFAULT_STATS_PORT`]
    /// fallback set by [`Self::with_head_lru_and_pod_id`].
    pub fn with_peer_fetch(mut self, http: reqwest::Client, stats_port: u16) -> Self {
        self.peer_http = http;
        self.peer_stats_port = stats_port;
        self
    }

    /// SHELF-23 — toggle peer-fetch at runtime. Returns the previous
    /// value. Used by `main` (env var `SHELFD_PEER_FETCH_ENABLED`) and
    /// by integration tests that need to assert the off-path still
    /// short-circuits to origin.
    pub fn set_peer_fetch_enabled(&self, enabled: bool) -> bool {
        self.peer_fetch_enabled.swap(enabled, Ordering::Release)
    }

    /// SHELF-23 — read the peer-fetch toggle on the hot path.
    pub fn is_peer_fetch_enabled(&self) -> bool {
        self.peer_fetch_enabled.load(Ordering::Relaxed)
    }

    /// SHELF-23 — toggle ETag-conditional GET on local cache hits at
    /// runtime. Returns the previous value. Mirrors
    /// [`Self::set_peer_fetch_enabled`]; off means the s3_shim hot
    /// path returns cached bytes without re-validating origin (the
    /// pre-SHELF-23 behaviour).
    pub fn set_conditional_get_enabled(&self, enabled: bool) -> bool {
        self.conditional_get_enabled
            .swap(enabled, Ordering::Release)
    }

    /// SHELF-23 — read the conditional-GET toggle on the hot path.
    pub fn is_conditional_get_enabled(&self) -> bool {
        self.conditional_get_enabled.load(Ordering::Relaxed)
    }

    /// SHELF-G4 builder hook: install a `ShelfFilterService`. Kept
    /// separate so callers that don't need predicate pushdown
    /// (most unit tests, the E6 peer-failover path) pay no cost.
    pub fn with_filter_service(
        mut self,
        svc: Arc<crate::filter_service::ShelfFilterService>,
    ) -> Self {
        self.filter_service = Some(svc);
        self
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// SHELF-24 builder: bolt a running pin-list loader onto the
    /// server state so `POST /admin/reload` can trigger it. Kept as a
    /// separate method (rather than another positional arg on the
    /// existing constructor) so the unit-test harness can build a
    /// `ServerState` without an S3 client in scope.
    pub fn with_reload_handle(mut self, handle: crate::pinlist::ReloadHandle) -> Self {
        self.reload_handle = Some(handle);
        self
    }

    /// SHELF-20 builder: install the lameduck `DrainSignal` shared with
    /// `main`. When omitted, the default `DrainSignal` is permanently
    /// inactive — fine for tests but production should always wire the
    /// real one so `/stats` advertises drain on `SIGTERM`.
    pub fn with_drain_signal(mut self, signal: DrainSignal) -> Self {
        self.drain_signal = signal;
        self
    }
}

/// SHELF-23 — fallback peer-fetch HTTP client. The values mirror the
/// production-side defaults configured in `main`: a small idle pool
/// per host (peers are stable; we don't need a large cache), and a
/// short request timeout so a stuck peer never extends the outer
/// request deadline. `main` overrides this via
/// [`ServerState::with_peer_fetch`] with a more thoroughly-tuned
/// client; this helper exists so unit/integration tests get a
/// sensible default without needing to construct one themselves.
fn default_peer_http() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(2)
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .expect("default peer-fetch reqwest::Client")
}

/// Resolve the effective `pod_id` when none was supplied explicitly:
/// `SHELFD_POD_ID` > `HOSTNAME` > `"shelfd-unknown"`.
fn default_pod_id() -> Arc<str> {
    if let Ok(v) = std::env::var("SHELFD_POD_ID") {
        if !v.is_empty() {
            return Arc::from(v);
        }
    }
    if let Ok(v) = std::env::var("HOSTNAME") {
        if !v.is_empty() {
            return Arc::from(v);
        }
    }
    Arc::from("shelfd-unknown")
}

/// Build the Axum router. Pure function — no side effects, no I/O.
///
/// ### Route shapes (SHELF-02, SHELF-06, SHELF-07, SHELF-20)
///
/// - `GET  /healthz` — liveness
/// - `GET  /readyz` — readiness
/// - `GET  /metrics` — Prometheus scrape
/// - `GET  /stats` — JSON for Agent 5's HRW weighting (SHELF-20)
/// - `GET  /cache/:pool/:key/:range` — content-addressed read-through
/// - `HEAD /cache/:pool/origin/:bucket/*s3_key` — HEAD-LRU pre-flight
///   (SHELF-07). The `origin/` separator distinguishes this from a
///   future `HEAD /cache/:pool/:key` by content-addressed hash, which
///   the plugin cannot issue until it has already learned the size.
pub fn build_router(state: Arc<ServerState>) -> Router {
    let router = Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/metrics", get(handlers::metrics))
        .route("/stats", get(handlers::stats))
        .route("/cache/:pool/:key/:range", get(handlers::get_cache))
        .route("/cache/contains", post(handlers::cache_contains))
        // SHELF-G4 — predicate → maybe_match row groups. Returns
        // `fail_open: true` when shelf has no signal for the
        // (file, column) and the engine must scan everything.
        .route("/filter/probe", post(handlers::filter_probe))
        // SHELF-G6 — Lucene-style text-index probe. Fails open
        // until an index is wired via `with_text_index`.
        .route("/textindex/probe", post(handlers::textindex_probe))
        .route(
            "/cache/:pool/origin/:bucket/*s3_key",
            head(handlers::head_cache),
        )
        // SHELF-23 admin surface. We deliberately nest under
        // `/admin/*` so a future reverse-proxy rule can block the
        // whole prefix on the public ingress without enumerating
        // individual routes.
        .route("/admin/ring", get(handlers::admin_ring))
        .route("/admin/pin", post(handlers::admin_pin))
        .route("/admin/unpin", post(handlers::admin_unpin))
        .route("/admin/evict", post(handlers::admin_evict))
        .route("/admin/reload", post(handlers::admin_reload));

    // Embedded admin UI. Same-origin SPA at `/ui` consuming the same
    // JSON contract `shelfctl` uses. Behind a non-default feature so
    // the stock binary stays unchanged.
    #[cfg(feature = "ui")]
    let router = router
        .route("/ui", get(crate::ui::index))
        .route("/ui/", get(crate::ui::index))
        .route("/ui/*path", get(crate::ui::asset));

    router.with_state(state)
}

/// Bind a TCP listener and serve the data plane until `shutdown`
/// cancels.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<ServerState>,
    request_timeout: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) -> crate::Result<()> {
    let app = build_router(state).layer(TraceLayer::new_for_http()).layer(
        TimeoutLayer::with_status_code(StatusCode::GATEWAY_TIMEOUT, request_timeout),
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| crate::Error::Config(format!("http: bind {addr}: {e}")))?;
    tracing::info!(%addr, "shelfd http listener bound");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .map_err(|e| crate::Error::Io(std::io::Error::other(format!("http serve: {e}"))))
}

/// Build the S3-compat shim router (SHELF-22). Intentionally NOT
/// merged into [`build_router`] — the shim listens on a dedicated
/// port (`:9092` by default) so operators can firewall it
/// independently of the native `/cache/...` data plane, and so a
/// misbehaving S3 client cannot starve plugin reads.
pub fn build_s3_shim_router(state: Arc<ServerState>) -> axum::Router {
    crate::s3_shim::router(state)
}

/// Serve the S3-compat shim until `shutdown` fires. Mirrors
/// [`serve`] one-to-one so both listeners share graceful-shutdown
/// semantics and per-request timeout enforcement.
pub async fn serve_s3_shim(
    addr: SocketAddr,
    state: Arc<ServerState>,
    request_timeout: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) -> crate::Result<()> {
    let app = build_s3_shim_router(state)
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::GATEWAY_TIMEOUT,
            request_timeout,
        ));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| crate::Error::Config(format!("s3_shim: bind {addr}: {e}")))?;
    tracing::info!(%addr, "shelfd s3-shim listener bound");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .map_err(|e| crate::Error::Io(std::io::Error::other(format!("s3_shim serve: {e}"))))
}

/// HTTP handlers. Public because the integration tests reach into
/// them directly.
pub mod handlers {
    use super::*;

    /// Liveness: cheap, never reaches into the store.
    pub async fn healthz() -> StatusCode {
        StatusCode::OK
    }

    /// Readiness: gated on [`ServerState::mark_ready`].
    pub async fn readyz(State(state): State<Arc<ServerState>>) -> StatusCode {
        if state.is_ready() {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }

    /// Prometheus scrape endpoint.
    pub async fn metrics() -> Response {
        let families = crate::metrics::REGISTRY.gather();
        let encoder = prometheus::TextEncoder::new();
        match encoder.encode_to_string(&families) {
            Ok(body) => (
                StatusCode::OK,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; version=0.0.4"),
                )],
                body,
            )
                .into_response(),
            Err(e) => {
                tracing::error!(error = %e, "prometheus encode failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }

    /// `GET /cache/:pool/:key/:range` — read-through entry point.
    pub async fn get_cache(
        State(state): State<Arc<ServerState>>,
        Path((pool_str, key_hex, range_str)): Path<(String, String, String)>,
        headers: axum::http::HeaderMap,
    ) -> Response {
        // SHELF-08: wrap the whole handler in a named span so a
        // Tempo trace resolves `http.get_cache → s3.get_object`.
        // `pool` / `status` / `outcome` are recorded as the handler
        // resolves them.
        //
        // SHELF-23: `is_peer_hop` short-circuits the peer-fetch wrapping
        // below when the request itself is an inbound peer-fetch, so we
        // don't bounce off a third pod. See `peer_fetch::PEER_FETCH_HEADER`.
        let is_peer_hop = headers.get(crate::peer_fetch::PEER_FETCH_HEADER).is_some();
        let span = tracing::info_span!(
            "http.get_cache",
            otel.kind = "server",
            route = "/cache/:pool/:key/:range",
            pool = %pool_str,
            peer_hop = is_peer_hop,
            status = field::Empty,
            outcome = field::Empty,
        );
        async move { get_cache_inner(state, pool_str, key_hex, range_str, is_peer_hop).await }
            .instrument(span)
            .await
    }

    async fn get_cache_inner(
        state: Arc<ServerState>,
        pool_str: String,
        key_hex: String,
        range_str: String,
        is_peer_hop: bool,
    ) -> Response {
        let start = std::time::Instant::now();
        let pool = match parse_pool(&pool_str) {
            Ok(p) => p,
            Err((status, detail)) => {
                return record_cache_outcome(
                    &state,
                    start,
                    "bad_request",
                    status,
                    client_error(status, "invalid pool", &detail),
                );
            }
        };
        let key = match Key::from_hex(&key_hex) {
            Ok(k) => k,
            Err(e) => {
                return record_cache_outcome(
                    &state,
                    start,
                    "bad_request",
                    StatusCode::BAD_REQUEST,
                    client_error(StatusCode::BAD_REQUEST, "invalid key", &e.to_string()),
                );
            }
        };
        let (offset, length) = match parse_range(&range_str) {
            Ok(parts) => parts,
            Err((status, detail)) => {
                return record_cache_outcome(
                    &state,
                    start,
                    "bad_request",
                    status,
                    client_error(status, "invalid range", &detail),
                );
            }
        };

        let pool_label = pool_label(pool);

        match state.store.get(pool, &key).await {
            Ok(Some(bytes)) => {
                state
                    .metrics
                    .hits_total
                    .with_label_values(&[pool_label])
                    .inc();
                // Track H5 — if this key was pinned on behalf of an
                // MV, bump the per-MV counters. `record_hit` is
                // O(1) and a no-op for non-MV keys, so it stays on
                // the hot path.
                let served_bytes = slice_len(&bytes, offset, length);
                state.mv_registry.record_hit(&key_hex, served_bytes);
                return record_cache_outcome(
                    &state,
                    start,
                    "hit",
                    StatusCode::OK,
                    ok_range(bytes, offset, length),
                );
            }
            Ok(None) => {}
            Err(e) => {
                return record_cache_outcome(
                    &state,
                    start,
                    "error",
                    StatusCode::BAD_GATEWAY,
                    upstream_error("store.get", &e.to_string()),
                );
            }
        }

        // Miss: delegate to `FoyerStore::get_or_fetch`, which handles
        // single-flight dedup, admission check, and insertion.
        //
        // Phase-0: the plugin passes the real `(bucket, object_key,
        // offset, length)` tuple in-URL. Until SHELF-12, we encode the
        // object key as `<key_hex>` and let the integration test seed
        // MinIO with that exact name. See tests/it_read_path.rs.
        let origin = state.origin.clone();
        let admission = state.admission.clone();
        let bucket = origin.bucket().to_owned();
        let object_key = key_hex.clone();
        let origin_fut = async move {
            use crate::origin::Origin;
            origin
                .as_ref()
                .get_range(&bucket, &object_key, offset, length)
                .await
        };

        // SHELF-23 — race the HRW primary peer against origin on a
        // local cache miss, *unless* this request is itself a peer
        // hop (in which case we are the peer being probed and must
        // not recurse).
        let state_for_peer = state.clone();
        let key_for_peer = key.clone();
        let fetcher = async move {
            if is_peer_hop {
                origin_fut.await
            } else {
                crate::peer_fetch::peer_or_origin_fetch(
                    &state_for_peer,
                    pool,
                    &key_for_peer,
                    offset,
                    length,
                    origin_fut,
                )
                .await
            }
        };

        let outcome = state
            .store
            .get_or_fetch(pool, key, admission.as_ref(), fetcher)
            .await;

        match outcome {
            Ok(ReadOutcome::Hit(bytes, tier)) => {
                // Rare: raced the fastpath `get` above. Still a hit.
                state
                    .metrics
                    .hits_total
                    .with_label_values(&[pool_label])
                    .inc();
                let served_bytes = slice_len(&bytes, offset, length);
                state.mv_registry.record_hit(&key_hex, served_bytes);
                record_cache_outcome(
                    &state,
                    start,
                    tier.outcome_label(),
                    StatusCode::OK,
                    ok_range(bytes, offset, length),
                )
            }
            Ok(ReadOutcome::Miss(bytes)) => {
                state
                    .metrics
                    .misses_total
                    .with_label_values(&[pool_label])
                    .inc();
                record_cache_outcome(
                    &state,
                    start,
                    "miss",
                    StatusCode::OK,
                    ok_range(bytes, offset, length),
                )
            }
            Err(e) => record_cache_outcome(
                &state,
                start,
                "error",
                StatusCode::BAD_GATEWAY,
                upstream_error("origin/store", &e.to_string()),
            ),
        }
    }

    /// Observe `shelf_request_seconds{path="/cache",outcome}` and
    /// record `status` + `outcome` on the current span before the
    /// response is returned.
    fn record_cache_outcome(
        state: &Arc<ServerState>,
        start: std::time::Instant,
        outcome: &'static str,
        status: StatusCode,
        resp: Response,
    ) -> Response {
        let elapsed = start.elapsed().as_secs_f64();
        state
            .metrics
            .request_seconds
            .with_label_values(&["/cache", outcome])
            .observe(elapsed);
        let span = tracing::Span::current();
        span.record("status", status.as_u16());
        span.record("outcome", outcome);
        resp
    }

    /// `HEAD /cache/:pool/origin/:bucket/*s3_key` — plugin pre-flight
    /// (SHELF-07).
    ///
    /// Contract:
    /// - 200 with `Content-Length` (+ optional `X-Shelf-ETag`,
    ///   `X-Shelf-LastModified`) on HEAD-LRU hit **or** successful
    ///   `HeadObject`.
    /// - 404 when S3 returns `NoSuchKey` (mapped via
    ///   [`crate::origin::S3Origin::head`] returning `Ok(None)`).
    /// - 502 for any other origin failure.
    ///
    /// The handler **never** issues a `GetObject`: the worst case is
    /// a single `HeadObject` + LRU populate.
    pub async fn head_cache(
        State(state): State<Arc<ServerState>>,
        Path((pool_str, bucket, s3_key)): Path<(String, String, String)>,
    ) -> Response {
        let span = tracing::info_span!(
            "http.head_cache",
            otel.kind = "server",
            route = "/cache/:pool/origin/:bucket/*s3_key",
            pool = %pool_str,
            status = field::Empty,
        );
        async move { head_cache_inner(state, pool_str, bucket, s3_key).await }
            .instrument(span)
            .await
    }

    async fn head_cache_inner(
        state: Arc<ServerState>,
        pool_str: String,
        bucket: String,
        s3_key: String,
    ) -> Response {
        let start = std::time::Instant::now();
        let pool = match parse_pool(&pool_str) {
            Ok(p) => p,
            Err((status, detail)) => {
                return record_head_outcome(
                    &state,
                    start,
                    "bad_request",
                    status,
                    client_error(status, "invalid pool", &detail),
                );
            }
        };
        let pool_label = pool_label(pool);

        if bucket.is_empty() {
            return record_head_outcome(
                &state,
                start,
                "bad_request",
                StatusCode::BAD_REQUEST,
                client_error(
                    StatusCode::BAD_REQUEST,
                    "invalid bucket",
                    "bucket segment must be non-empty",
                ),
            );
        }
        if s3_key.is_empty() {
            return record_head_outcome(
                &state,
                start,
                "bad_request",
                StatusCode::BAD_REQUEST,
                client_error(
                    StatusCode::BAD_REQUEST,
                    "invalid key",
                    "s3_key segment must be non-empty",
                ),
            );
        }

        // Fast path: HEAD-LRU.
        if let Some(meta) = state.head_lru.get(&bucket, &s3_key) {
            state
                .metrics
                .head_hits_total
                .with_label_values(&[pool_label])
                .inc();
            return record_head_outcome(
                &state,
                start,
                "hit",
                StatusCode::OK,
                ok_head(meta.as_ref()),
            );
        }

        state
            .metrics
            .head_misses_total
            .with_label_values(&[pool_label])
            .inc();

        use crate::origin::Origin;
        let origin = state.origin.clone();
        match origin.as_ref().head(&bucket, &s3_key).await {
            Ok(Some(head)) => {
                let meta = HeadMeta::from(head);
                state.head_lru.insert(bucket, s3_key, meta.clone());
                record_head_outcome(&state, start, "miss", StatusCode::OK, ok_head(&meta))
            }
            Ok(None) => {
                let body =
                    serde_json::json!({"error": "not_found", "detail": "origin object absent"});
                record_head_outcome(
                    &state,
                    start,
                    "not_found",
                    StatusCode::NOT_FOUND,
                    (StatusCode::NOT_FOUND, axum::Json(body)).into_response(),
                )
            }
            Err(e) => record_head_outcome(
                &state,
                start,
                "error",
                StatusCode::BAD_GATEWAY,
                upstream_error("origin.head", &e.to_string()),
            ),
        }
    }

    /// Observe `shelf_request_seconds{path="/cache/head",outcome}` and
    /// stamp `status` onto the current span.
    fn record_head_outcome(
        state: &Arc<ServerState>,
        start: std::time::Instant,
        outcome: &'static str,
        status: StatusCode,
        resp: Response,
    ) -> Response {
        let elapsed = start.elapsed().as_secs_f64();
        state
            .metrics
            .request_seconds
            .with_label_values(&["/cache/head", outcome])
            .observe(elapsed);
        tracing::Span::current().record("status", status.as_u16());
        resp
    }

    /// `GET /stats` — JSON capacity + usage surface (SHELF-20).
    ///
    /// Consumed by the plugin's HRW weighting loop. The key set here
    /// is the contract Agent 5 depends on:
    ///
    /// ```json
    /// {
    ///   "pod_id": "shelf-2",
    ///   "capacity_bytes": 12884901888,
    ///   "used_bytes":      3221225472,
    ///   "metadata_pool": {"capacity_bytes": ..., "used_bytes": ...},
    ///   "rowgroup_pool": {"capacity_bytes": ..., "used_bytes": ...}
    /// }
    /// ```
    pub async fn stats(State(state): State<Arc<ServerState>>) -> Response {
        let span = tracing::info_span!(
            "http.stats",
            otel.kind = "server",
            route = "/stats",
            status = 200,
        );
        let _e = span.enter();
        let start = std::time::Instant::now();
        let resp = stats_inner(&state).await;
        state
            .metrics
            .request_seconds
            .with_label_values(&["/stats", "ok"])
            .observe(start.elapsed().as_secs_f64());
        resp
    }

    async fn stats_inner(state: &Arc<ServerState>) -> Response {
        let metadata = PoolStats {
            capacity_bytes: state.store.capacity_bytes(Pool::Metadata),
            used_bytes: state.store.used_bytes(Pool::Metadata),
            disk_used_bytes: state.store.disk_bytes_used(Pool::Metadata),
            disk_capacity_bytes: state.store.disk_bytes_capacity(Pool::Metadata),
        };
        let rowgroup = PoolStats {
            capacity_bytes: state.store.capacity_bytes(Pool::RowGroup),
            used_bytes: state.store.used_bytes(Pool::RowGroup),
            disk_used_bytes: state.store.disk_bytes_used(Pool::RowGroup),
            disk_capacity_bytes: state.store.disk_bytes_capacity(Pool::RowGroup),
        };
        // Keep the dashboard-bound Prometheus gauges aligned with
        // the `/stats` payload so scrapes arriving between the two
        // observe a consistent snapshot.
        state
            .metrics
            .disk_bytes_used
            .with_label_values(&["rowgroup"])
            .set(rowgroup.disk_used_bytes as i64);
        state
            .metrics
            .disk_bytes_capacity
            .with_label_values(&["rowgroup"])
            .set(rowgroup.disk_capacity_bytes as i64);
        let stats = Stats {
            pod_id: state.pod_id.as_ref().to_owned(),
            capacity_bytes: metadata
                .capacity_bytes
                .saturating_add(rowgroup.capacity_bytes),
            used_bytes: metadata.used_bytes.saturating_add(rowgroup.used_bytes),
            metadata_pool: metadata,
            rowgroup_pool: rowgroup,
            // SHELF-24 pin-set accounting.
            pinned_bytes: state.store.pinned_bytes(),
            pinned_count: state.store.pinned_count(),
            // SHELF-20 — flipped by `main` on SIGTERM via the shared
            // `DrainSignal`. Peers' resolvers drop us from their HRW
            // rings within `dns_refresh` of seeing this transition.
            draining: state.drain_signal.is_active(),
        };
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            )],
            axum::Json(stats),
        )
            .into_response()
    }

    // ---- helpers ----

    pub(crate) fn parse_pool(s: &str) -> Result<Pool, (StatusCode, String)> {
        match s {
            "metadata" => Ok(Pool::Metadata),
            "rowgroup" => Ok(Pool::RowGroup),
            other => Err((
                StatusCode::BAD_REQUEST,
                format!("unknown pool '{other}'; expected 'metadata' or 'rowgroup'"),
            )),
        }
    }

    /// Parse `<offset>-<end>` where `end` is the INCLUSIVE last byte,
    /// matching the HTTP `Range: bytes=` convention. Returns
    /// `(offset, length)`.
    pub(crate) fn parse_range(s: &str) -> Result<(u64, u64), (StatusCode, String)> {
        let (start, end) = s.split_once('-').ok_or((
            StatusCode::BAD_REQUEST,
            "range must be '<offset>-<end>'".to_owned(),
        ))?;
        let offset: u64 = start.parse().map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "offset must be an unsigned integer".to_owned(),
            )
        })?;
        let end: u64 = end.parse().map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "end must be an unsigned integer".to_owned(),
            )
        })?;
        if end < offset {
            return Err((StatusCode::BAD_REQUEST, "end must be >= offset".to_owned()));
        }
        // `end - offset + 1` can overflow `u64` at the edges (e.g.
        // `offset = 0, end = u64::MAX`). Route that to a clean 400
        // instead of panicking in debug / wrapping to 0 in release.
        let length = match end.checked_sub(offset).and_then(|d| d.checked_add(1)) {
            Some(n) => n,
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "range length overflows u64".to_owned(),
                ))
            }
        };
        Ok((offset, length))
    }

    fn pool_label(p: Pool) -> &'static str {
        match p {
            Pool::Metadata => "metadata",
            Pool::RowGroup => "rowgroup",
        }
    }

    /// Build a 200 HEAD response from `meta`, decorating headers with
    /// `Content-Length`, `X-Shelf-ETag`, and `X-Shelf-LastModified`
    /// when present.
    fn ok_head(meta: &HeadMeta) -> Response {
        let mut headers = HeaderMap::new();
        if let Ok(v) = HeaderValue::from_str(&meta.content_length.to_string()) {
            headers.insert(header::CONTENT_LENGTH, v);
        }
        if let Some(etag) = meta.etag.as_deref() {
            if let Ok(v) = HeaderValue::from_str(etag) {
                headers.insert(HeaderName::from_static("x-shelf-etag"), v);
            }
        }
        if let Some(lm) = meta.last_modified.as_deref() {
            if let Ok(v) = HeaderValue::from_str(lm) {
                headers.insert(HeaderName::from_static("x-shelf-lastmodified"), v);
            }
        }
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        (StatusCode::OK, headers).into_response()
    }

    /// Number of bytes `ok_range` will send in its response body for
    /// `(offset, length)` against `bytes`. Pulled out so the H5
    /// MV-hit accounting can charge the per-MV byte counter with
    /// the exact count the client will see.
    fn slice_len(bytes: &Bytes, _offset: u64, length: u64) -> u64 {
        (length as usize).min(bytes.len()) as u64
    }

    fn ok_range(bytes: Bytes, offset: u64, length: u64) -> Response {
        // The body must match the `Content-Range` we advertise —
        // otherwise `Content-Length` (implicit from the body) will
        // disagree with `Content-Range` and the response is invalid
        // HTTP. Previously `ok_range` returned the full cached
        // `bytes` regardless of `length`, so a client requesting the
        // same cache key with a different `<offset>-<end>` on the
        // URL could receive more bytes than the range declared.
        //
        // `length` is in bytes and capped at `bytes.len()` so we
        // never slice past the end on a short cache entry.
        let take = (length as usize).min(bytes.len());
        let sliced = bytes.slice(0..take);
        let effective_length = sliced.len() as u64;

        let mut headers = HeaderMap::new();
        let last_byte = offset.saturating_add(effective_length).saturating_sub(1);
        let content_range = format!("bytes {}-{}/*", offset, last_byte);
        if let Ok(v) = HeaderValue::from_str(&content_range) {
            headers.insert(HeaderName::from_static("content-range"), v);
        }
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        (StatusCode::OK, headers, sliced).into_response()
    }

    fn client_error(status: StatusCode, kind: &str, detail: &str) -> Response {
        let body = serde_json::json!({"error": kind, "detail": detail});
        (status, axum::Json(body)).into_response()
    }

    fn upstream_error(kind: &str, detail: &str) -> Response {
        tracing::warn!(kind, detail, "upstream error -> 502");
        let body = serde_json::json!({"error": kind, "detail": detail});
        (StatusCode::BAD_GATEWAY, axum::Json(body)).into_response()
    }

    // -----------------------------------------------------------------
    // SHELF-23 admin surface — `/admin/*`.
    //
    // These handlers never read or write cache bytes; they are the
    // operator control plane. JSON-only so `shelfctl` renders them
    // uniformly.
    // -----------------------------------------------------------------

    /// Request body for `/admin/pin` and `/admin/evict`.
    ///
    /// `key_hex` is a 64-char lower-case hex rendering of the SHELF-04
    /// content-addressed key; `pool` selects which Foyer pool the
    /// action targets. Both fields are required — a client that knows
    /// enough to pin a key knows which pool it was admitted into.
    #[derive(Debug, serde::Deserialize)]
    pub struct PinEvictBody {
        pub key_hex: String,
        pub pool: String,
        /// Track H5 — optional fully-qualified materialized view
        /// name the key belongs to (`schema.table`). When present,
        /// `/admin/pin` writes to the [`crate::mv_registry`] so the
        /// read path can bump per-MV counters on each hit.
        /// Unused by `/admin/evict`; extra fields are ignored by
        /// design because serde's default skips unknown fields.
        #[serde(default)]
        pub mv_name: Option<String>,
    }

    /// Request body for `/admin/unpin`. No `pool` field: unpin is
    /// pool-agnostic because a SHELF-04 key is unique across both
    /// pools by construction (sha-256 over `etag || offset || length
    /// || rg_ordinal`).
    #[derive(Debug, serde::Deserialize)]
    pub struct UnpinBody {
        pub key_hex: String,
    }

    #[allow(clippy::result_large_err)]
    fn parse_hex_key(hex: &str) -> Result<Key, Response> {
        Key::from_hex(hex)
            .map_err(|e| client_error(StatusCode::BAD_REQUEST, "invalid_key", &e.to_string()))
    }

    #[allow(clippy::result_large_err)]
    fn parse_pool_field(s: &str) -> Result<Pool, Response> {
        parse_pool(s).map_err(|(status, detail)| client_error(status, "invalid_pool", &detail))
    }

    /// `GET /admin/ring` — dump the HRW ring view.
    ///
    /// Shape (stable contract — `shelfctl ring` and Track G dashboards
    /// depend on it):
    ///
    /// ```json
    /// {
    ///   "self_id": "shelf-2",
    ///   "draining": false,
    ///   "ring_size": 3,
    ///   "members": [
    ///     {"pod_id": "shelf-0", "endpoint": "10.0.1.4:9092",
    ///      "weight": 14, "is_self": false},
    ///     {"pod_id": "shelf-1", "endpoint": "10.0.1.7:9092",
    ///      "weight": 14, "is_self": false},
    ///     {"pod_id": "shelf-2", "endpoint": "10.0.1.9:9092",
    ///      "weight": 14, "is_self": true}
    ///   ]
    /// }
    /// ```
    ///
    /// `members` is the *post-filter* set: peers advertising
    /// `draining: true` have already been dropped by the local
    /// `Resolver`, so an operator looking at this endpoint sees what
    /// HRW will actually consider on the next route call. An empty
    /// `members` array is a real signal that DNS or `/stats` probes
    /// are failing — it is **not** the boot-time placeholder.
    pub async fn admin_ring(State(state): State<Arc<ServerState>>) -> Response {
        #[derive(serde::Serialize)]
        struct Row<'a> {
            pod_id: &'a str,
            endpoint: &'a str,
            weight: u32,
            is_self: bool,
        }
        #[derive(serde::Serialize)]
        struct Body<'a> {
            self_id: &'a str,
            draining: bool,
            ring_size: usize,
            members: Vec<Row<'a>>,
        }
        let view = state.router.view();
        let members_slice = view.members();
        let self_id = state.pod_id.as_ref();
        let rows: Vec<Row<'_>> = members_slice
            .iter()
            .map(|m| Row {
                pod_id: m.id.as_str(),
                endpoint: m.endpoint.as_str(),
                weight: m.weight,
                is_self: m.id == self_id,
            })
            .collect();
        let body = Body {
            self_id,
            draining: state.drain_signal.is_active(),
            ring_size: rows.len(),
            members: rows,
        };
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            )],
            axum::Json(body),
        )
            .into_response()
    }

    /// `POST /admin/pin {"key_hex":"<hex>", "pool":"metadata"|"rowgroup"}`.
    pub async fn admin_pin(
        State(state): State<Arc<ServerState>>,
        axum::Json(body): axum::Json<PinEvictBody>,
    ) -> Response {
        let key = match parse_hex_key(&body.key_hex) {
            Ok(k) => k,
            Err(r) => return r,
        };
        let pool = match parse_pool_field(&body.pool) {
            Ok(p) => p,
            Err(r) => return r,
        };
        if !state.store.pin(pool, &key) {
            return client_error(
                StatusCode::NOT_FOUND,
                "not_resident",
                "key is not resident in the requested pool — pin refused",
            );
        }
        // Track H5 — the H3 mv-pin-watcher passes `mv_name` on every
        // pin it performs. Registering here keeps the "which MV does
        // this key belong to" lookup table in the same shelfd process
        // that serves the key, which is what the hit-accounting path
        // needs to bump per-MV counters without a second hop.
        if let Some(mv_name) = body.mv_name.as_deref() {
            if !mv_name.is_empty() {
                state.mv_registry.pin(&body.key_hex, mv_name);
            }
        }
        (
            StatusCode::OK,
            axum::Json(serde_json::json!({
                "pinned": body.key_hex,
                "pool": body.pool,
                "pinned_bytes": state.store.pinned_bytes(),
                "pinned_count": state.store.pinned_count(),
                "mv_name": body.mv_name,
            })),
        )
            .into_response()
    }

    /// `POST /admin/unpin {"key_hex":"<hex>"}`.
    pub async fn admin_unpin(
        State(state): State<Arc<ServerState>>,
        axum::Json(body): axum::Json<UnpinBody>,
    ) -> Response {
        let key = match parse_hex_key(&body.key_hex) {
            Ok(k) => k,
            Err(r) => return r,
        };
        if !state.store.unpin(&key) {
            return client_error(StatusCode::NOT_FOUND, "not_pinned", "key was not pinned");
        }
        // Track H5 — keep the MV registry in sync with the store so
        // an evicted MV file doesn't continue to bump per-MV counters
        // after a later, unrelated admission reuses its content hash
        // (extremely unlikely, but SHA-256 collisions *must* fail
        // safe).
        state.mv_registry.unpin(&body.key_hex);
        (
            StatusCode::OK,
            axum::Json(serde_json::json!({
                "unpinned": body.key_hex,
                "pinned_bytes": state.store.pinned_bytes(),
                "pinned_count": state.store.pinned_count(),
            })),
        )
            .into_response()
    }

    /// `POST /admin/evict {"key_hex":"<hex>", "pool":"metadata"|"rowgroup"}`.
    pub async fn admin_evict(
        State(state): State<Arc<ServerState>>,
        axum::Json(body): axum::Json<PinEvictBody>,
    ) -> Response {
        let key = match parse_hex_key(&body.key_hex) {
            Ok(k) => k,
            Err(r) => return r,
        };
        let pool = match parse_pool_field(&body.pool) {
            Ok(p) => p,
            Err(r) => return r,
        };
        if !state.store.evict(pool, &key).await {
            return client_error(
                StatusCode::NOT_FOUND,
                "not_resident",
                "key was not resident in the requested pool",
            );
        }
        (
            StatusCode::OK,
            axum::Json(serde_json::json!({
                "evicted": body.key_hex,
                "pool": body.pool,
            })),
        )
            .into_response()
    }

    /// `POST /admin/reload` — trigger an out-of-band pin-list reload.
    ///
    /// Returns `{pinned_bytes, pinned_count, reload_ok}` on success
    /// so the operator does not need a second `/stats` call. When
    /// no loader is configured (dev cluster, unit-test harness) we
    /// still return `200` with `reload_ok: true` and zeros — the
    /// daemon has nothing to reload, which is a success state, not
    /// an error. Only a loader that was configured *and* failed
    /// returns a non-2xx.
    pub async fn admin_reload(State(state): State<Arc<ServerState>>) -> Response {
        let Some(handle) = state.reload_handle.as_ref() else {
            return (
                StatusCode::OK,
                axum::Json(serde_json::json!({
                    "pinned_bytes": state.store.pinned_bytes(),
                    "pinned_count": state.store.pinned_count(),
                    "reload_ok": true,
                    "note": "no pin_list configured; nothing to reload",
                })),
            )
                .into_response();
        };
        match handle.reload_now().await {
            Ok(stats) => (
                StatusCode::OK,
                axum::Json(serde_json::json!({
                    "pinned_bytes": stats.pinned_bytes,
                    "pinned_count": stats.pinned_count,
                    "reload_ok": true,
                })),
            )
                .into_response(),
            Err(e) => upstream_error("reload_failed", &e.to_string()),
        }
    }

    // -----------------------------------------------------------------
    // SHELF-D7 — batch residency probe.
    //
    // `POST /cache/contains` is the wire primitive that lets a peer
    // replica (SHELF-E6) or the Trino event-listener plugin
    // (SHELF-G5) ask "of these N keys, which are you already holding?"
    // in a single round-trip. The response is a dense bitmap — one
    // bit per input key, LSB-first, packed into little-endian bytes —
    // so the payload is O(N/8) rather than O(N) JSON booleans.
    //
    // The endpoint is deliberately `POST` (not `GET`) because the
    // key list can run into the thousands for whole-query planning
    // and comfortably overflows URL-length budgets on common load
    // balancers (8 KiB on ALB, 4 KiB on some NGINX defaults).
    // -----------------------------------------------------------------

    /// Request body for `POST /cache/contains`.
    ///
    /// `pool` is applied uniformly to every entry in `keys` — the
    /// caller almost always batches by pool because rowgroup and
    /// metadata probes are issued in different phases of the Trino
    /// planner. Mixing would force the callee to parse a pool per
    /// entry, which adds more CPU than it saves bytes.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct ContainsBody {
        pub pool: String,
        pub keys: Vec<String>,
    }

    /// `POST /cache/contains {"pool":"rowgroup","keys":["<hex>", ...]}`.
    ///
    /// Response shape:
    ///
    /// ```json
    /// {
    ///   "pool": "rowgroup",
    ///   "count": 1024,
    ///   "hits": 873,
    ///   "bitmap_b64": "<base64 of a count-bit bitmap, LSB-first>"
    /// }
    /// ```
    ///
    /// A key that fails hex-parse is counted as a miss (bit 0) rather
    /// than aborting the whole batch — the Trino planner would
    /// otherwise have to re-issue the probe for N-1 keys just because
    /// one split carried a corrupt cache-key annotation. The handler
    /// caps the batch at 65_536 keys (≈8 KiB bitmap) to bound the
    /// amount of blocking work a single request can schedule.
    pub async fn cache_contains(
        State(state): State<Arc<ServerState>>,
        axum::Json(body): axum::Json<ContainsBody>,
    ) -> Response {
        const MAX_BATCH: usize = 65_536;

        let pool = match parse_pool_field(&body.pool) {
            Ok(p) => p,
            Err(r) => return r,
        };
        if body.keys.len() > MAX_BATCH {
            return client_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "batch_too_large",
                &format!("requested {} keys; max is {MAX_BATCH}", body.keys.len()),
            );
        }

        let count = body.keys.len();
        let mut bitmap = vec![0u8; (count + 7) / 8];
        let mut hits: u64 = 0;
        for (i, hex) in body.keys.iter().enumerate() {
            let Ok(key) = Key::from_hex(hex) else {
                continue;
            };
            if state.store.contains(pool, &key).await {
                bitmap[i / 8] |= 1u8 << (i % 8);
                hits += 1;
            }
        }

        use base64::Engine as _;
        let bitmap_b64 = base64::engine::general_purpose::STANDARD.encode(&bitmap);
        (
            StatusCode::OK,
            axum::Json(serde_json::json!({
                "pool": body.pool,
                "count": count,
                "hits": hits,
                "bitmap_b64": bitmap_b64,
            })),
        )
            .into_response()
    }

    /// SHELF-G4 — `POST /filter/probe`. Body is
    /// [`crate::filter_service::ProbeRequest`] JSON. Returns a
    /// [`crate::filter_service::ProbeResponse`]. When no filter
    /// service is wired we reply with `fail_open: true` and an
    /// empty `maybe_match`, matching the service's own failure
    /// semantics so callers don't need to special-case the
    /// "service unwired" path.
    pub async fn filter_probe(
        State(state): State<Arc<ServerState>>,
        axum::Json(req): axum::Json<crate::filter_service::ProbeRequest>,
    ) -> Response {
        let resp = match state.filter_service.as_ref() {
            Some(svc) => svc.probe(&req),
            None => crate::filter_service::ProbeResponse {
                maybe_match: Vec::new(),
                fail_open: true,
            },
        };
        (StatusCode::OK, axum::Json(resp)).into_response()
    }

    /// SHELF-G6 — `POST /textindex/probe`. Looks up an in-memory
    /// `KeywordIndex` keyed by `(table_fqn, column)` and returns
    /// the matching row-group ordinals. A missing index is
    /// *not* an error: the response sets `fail_open: true` and
    /// the caller re-issues the full scan.
    pub async fn textindex_probe(
        State(state): State<Arc<ServerState>>,
        axum::Json(req): axum::Json<crate::text_index::TextProbeRequest>,
    ) -> Response {
        let guard = match state.text_index.read() {
            Ok(g) => g,
            Err(_) => {
                return (
                    StatusCode::OK,
                    axum::Json(crate::text_index::TextProbeResponse {
                        row_group_ordinals: Vec::new(),
                        fail_open: true,
                    }),
                )
                    .into_response();
            }
        };
        let key = (req.table_fqn.clone(), req.column.clone());
        let resp = match guard.get(&key) {
            Some(idx) => crate::text_index::TextProbeResponse {
                row_group_ordinals: idx.maybe_match(&req.pattern).into_iter().collect(),
                fail_open: false,
            },
            None => crate::text_index::TextProbeResponse {
                row_group_ordinals: Vec::new(),
                fail_open: true,
            },
        };
        (StatusCode::OK, axum::Json(resp)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_parses_round_trip() {
        let (off, len) = handlers::parse_range("0-65535").expect("ok");
        assert_eq!(off, 0);
        assert_eq!(len, 65_536);
    }

    #[test]
    fn range_rejects_reversed() {
        assert!(handlers::parse_range("10-5").is_err());
    }

    #[test]
    fn range_rejects_missing_dash() {
        assert!(handlers::parse_range("nope").is_err());
    }

    #[test]
    fn pool_parses_metadata_and_rowgroup() {
        assert_eq!(handlers::parse_pool("metadata").unwrap(), Pool::Metadata);
        assert_eq!(handlers::parse_pool("rowgroup").unwrap(), Pool::RowGroup);
        assert!(handlers::parse_pool("bogus").is_err());
    }

    /// The `/stats` JSON shape is the wire contract Agent 5 (SHELF-20)
    /// consumes to weight the HRW ring. Changing any top-level key
    /// here breaks the plugin — the golden-keys assertion is the
    /// load-bearing part of this test.
    #[test]
    fn stats_payload_has_contract_keys() {
        let stats = Stats {
            pod_id: "shelf-7".into(),
            capacity_bytes: 2048,
            used_bytes: 512,
            metadata_pool: PoolStats {
                capacity_bytes: 1024,
                used_bytes: 128,
                disk_used_bytes: 0,
                disk_capacity_bytes: 0,
            },
            rowgroup_pool: PoolStats {
                capacity_bytes: 1024,
                used_bytes: 384,
                disk_used_bytes: 8,
                disk_capacity_bytes: 2048,
            },
            pinned_bytes: 0,
            pinned_count: 0,
            draining: false,
        };
        let v = serde_json::to_value(&stats).expect("serialize");
        let obj = v.as_object().expect("object");
        for key in [
            "pod_id",
            "capacity_bytes",
            "used_bytes",
            "metadata_pool",
            "rowgroup_pool",
            "pinned_bytes",
            "pinned_count",
            // SHELF-20: peer drain advertisement.
            "draining",
        ] {
            assert!(
                obj.contains_key(key),
                "/stats must carry `{key}` — Agent 5 contract"
            );
        }
        assert_eq!(obj["pod_id"], serde_json::json!("shelf-7"));
        let md = obj["metadata_pool"].as_object().expect("metadata_pool");
        for key in ["capacity_bytes", "used_bytes"] {
            assert!(md.contains_key(key), "metadata_pool.{key} missing");
        }
        let rg = obj["rowgroup_pool"].as_object().expect("rowgroup_pool");
        for key in [
            "capacity_bytes",
            "used_bytes",
            // SHELF-18: additive disk-tier exposition.
            "disk_used_bytes",
            "disk_capacity_bytes",
        ] {
            assert!(rg.contains_key(key), "rowgroup_pool.{key} missing");
        }
        assert_eq!(rg["disk_used_bytes"].as_u64(), Some(8));
        assert_eq!(rg["disk_capacity_bytes"].as_u64(), Some(2048));
    }
}
