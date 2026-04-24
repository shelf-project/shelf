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
use crate::store::{Key, Pool, ReadOutcome, Store};

/// Shared state the router hands to every handler.
#[derive(Debug)]
pub struct ServerState {
    pub store: Arc<crate::store::FoyerStore>,
    pub origin: Arc<crate::origin::S3Origin>,
    pub router: Arc<crate::router::Router>,
    pub admission: Arc<crate::admission::SizeThresholdPolicy>,
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
    /// SHELF-22 cap on unbounded `GetObject` — any `GET /:bucket/*key`
    /// without a `Range:` header whose object size exceeds this value
    /// responds `501 NotImplemented` with an S3 XML envelope. Kept as
    /// `AtomicU64` so integration tests can dial it down without
    /// rebuilding `ServerState`. Defaults to 256 MiB; `main` seeds it
    /// from `config.s3_shim.max_full_object_bytes` at startup.
    pub s3_shim_max_full_object_bytes: AtomicU64,
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
        admission: Arc<crate::admission::SizeThresholdPolicy>,
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
        admission: Arc<crate::admission::SizeThresholdPolicy>,
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
            // SHELF-22 default cap: 256 MiB. `main` overwrites
            // this from config before any traffic arrives.
            s3_shim_max_full_object_bytes: AtomicU64::new(256 * 1024 * 1024),
        }
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
    Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/metrics", get(handlers::metrics))
        .route("/stats", get(handlers::stats))
        .route("/cache/:pool/:key/:range", get(handlers::get_cache))
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
        .route("/admin/reload", post(handlers::admin_reload))
        .with_state(state)
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
    ) -> Response {
        // SHELF-08: wrap the whole handler in a named span so a
        // Tempo trace resolves `http.get_cache → s3.get_object`.
        // `pool` / `status` / `outcome` are recorded as the handler
        // resolves them.
        let span = tracing::info_span!(
            "http.get_cache",
            otel.kind = "server",
            route = "/cache/:pool/:key/:range",
            pool = %pool_str,
            status = field::Empty,
            outcome = field::Empty,
        );
        async move { get_cache_inner(state, pool_str, key_hex, range_str).await }
            .instrument(span)
            .await
    }

    async fn get_cache_inner(
        state: Arc<ServerState>,
        pool_str: String,
        key_hex: String,
        range_str: String,
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
        let fetcher = async move {
            use crate::origin::Origin;
            origin
                .as_ref()
                .get_range(&bucket, &object_key, offset, length)
                .await
        };

        let outcome = state
            .store
            .get_or_fetch(pool, key, admission.as_ref(), fetcher)
            .await;

        match outcome {
            Ok(ReadOutcome::Hit(bytes)) => {
                // Rare: raced the fastpath `get` above. Still a hit.
                state
                    .metrics
                    .hits_total
                    .with_label_values(&[pool_label])
                    .inc();
                record_cache_outcome(
                    &state,
                    start,
                    "hit",
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
        let length = end - offset + 1;
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

    fn ok_range(bytes: Bytes, offset: u64, length: u64) -> Response {
        let mut headers = HeaderMap::new();
        let content_range = format!(
            "bytes {}-{}/*",
            offset,
            offset.saturating_add(length).saturating_sub(1)
        );
        if let Ok(v) = HeaderValue::from_str(&content_range) {
            headers.insert(HeaderName::from_static("content-range"), v);
        }
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        (StatusCode::OK, headers, bytes).into_response()
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
    pub async fn admin_ring(State(state): State<Arc<ServerState>>) -> Response {
        #[derive(serde::Serialize)]
        struct Row<'a> {
            pod_id: &'a str,
            weight: f64,
            healthy: bool,
        }
        // TODO(SHELF-20): populate from `crate::membership::Ring`.
        let rows = [Row {
            pod_id: state.pod_id.as_ref(),
            weight: 1.0,
            healthy: state.is_ready(),
        }];
        (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            )],
            axum::Json(rows),
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
        (
            StatusCode::OK,
            axum::Json(serde_json::json!({
                "pinned": body.key_hex,
                "pool": body.pool,
                "pinned_bytes": state.store.pinned_bytes(),
                "pinned_count": state.store.pinned_count(),
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
        if !state.store.evict(pool, &key) {
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
