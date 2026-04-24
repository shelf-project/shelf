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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, head};
use axum::Router;
use bytes::Bytes;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

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
    /// Set to `true` by `main` after startup probes finish. A future
    /// membership loop may flip it back to false on degradation.
    pub ready: AtomicBool,
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
            ready: AtomicBool::new(false),
        }
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
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
        let pool = match parse_pool(&pool_str) {
            Ok(p) => p,
            Err((status, detail)) => return client_error(status, "invalid pool", &detail),
        };
        let key = match Key::from_hex(&key_hex) {
            Ok(k) => k,
            Err(e) => return client_error(StatusCode::BAD_REQUEST, "invalid key", &e.to_string()),
        };
        let (offset, length) = match parse_range(&range_str) {
            Ok(parts) => parts,
            Err((status, detail)) => return client_error(status, "invalid range", &detail),
        };

        let pool_label = pool_label(pool);

        match state.store.get(pool, &key).await {
            Ok(Some(bytes)) => {
                state
                    .metrics
                    .hits_total
                    .with_label_values(&[pool_label])
                    .inc();
                return ok_range(bytes, offset, length);
            }
            Ok(None) => {}
            Err(e) => return upstream_error("store.get", &e.to_string()),
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
                ok_range(bytes, offset, length)
            }
            Ok(ReadOutcome::Miss(bytes)) => {
                state
                    .metrics
                    .misses_total
                    .with_label_values(&[pool_label])
                    .inc();
                ok_range(bytes, offset, length)
            }
            Err(e) => upstream_error("origin/store", &e.to_string()),
        }
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
        let pool = match parse_pool(&pool_str) {
            Ok(p) => p,
            Err((status, detail)) => return client_error(status, "invalid pool", &detail),
        };
        let pool_label = pool_label(pool);

        if bucket.is_empty() {
            return client_error(
                StatusCode::BAD_REQUEST,
                "invalid bucket",
                "bucket segment must be non-empty",
            );
        }
        if s3_key.is_empty() {
            return client_error(
                StatusCode::BAD_REQUEST,
                "invalid key",
                "s3_key segment must be non-empty",
            );
        }

        // Fast path: HEAD-LRU.
        if let Some(meta) = state.head_lru.get(&bucket, &s3_key) {
            state
                .metrics
                .head_hits_total
                .with_label_values(&[pool_label])
                .inc();
            return ok_head(meta.as_ref());
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
                ok_head(&meta)
            }
            Ok(None) => {
                // NoSuchKey — the plugin expects a clean 404 so it
                // can fall through to S3 without retrying.
                let body =
                    serde_json::json!({"error": "not_found", "detail": "origin object absent"});
                (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
            }
            Err(e) => upstream_error("origin.head", &e.to_string()),
        }
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
        let metadata = PoolStats {
            capacity_bytes: state.store.capacity_bytes(Pool::Metadata),
            used_bytes: state.store.used_bytes(Pool::Metadata),
        };
        let rowgroup = PoolStats {
            capacity_bytes: state.store.capacity_bytes(Pool::RowGroup),
            used_bytes: state.store.used_bytes(Pool::RowGroup),
        };
        let stats = Stats {
            pod_id: state.pod_id.as_ref().to_owned(),
            capacity_bytes: metadata
                .capacity_bytes
                .saturating_add(rowgroup.capacity_bytes),
            used_bytes: metadata.used_bytes.saturating_add(rowgroup.used_bytes),
            metadata_pool: metadata,
            rowgroup_pool: rowgroup,
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
            },
            rowgroup_pool: PoolStats {
                capacity_bytes: 1024,
                used_bytes: 384,
            },
        };
        let v = serde_json::to_value(&stats).expect("serialize");
        let obj = v.as_object().expect("object");
        for key in [
            "pod_id",
            "capacity_bytes",
            "used_bytes",
            "metadata_pool",
            "rowgroup_pool",
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
        for key in ["capacity_bytes", "used_bytes"] {
            assert!(rg.contains_key(key), "rowgroup_pool.{key} missing");
        }
    }
}
