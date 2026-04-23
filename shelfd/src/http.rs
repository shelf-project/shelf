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

use crate::store::{Key, Pool, ReadOutcome, Store};

/// Shared state the router hands to every handler.
#[derive(Debug)]
pub struct ServerState {
    pub store: Arc<crate::store::FoyerStore>,
    pub origin: Arc<crate::origin::S3Origin>,
    pub router: Arc<crate::router::Router>,
    pub admission: Arc<crate::admission::SizeThresholdPolicy>,
    pub metrics: Arc<crate::metrics::Registry>,
    /// Set to `true` by `main` after startup probes finish. A future
    /// membership loop may flip it back to false on degradation.
    pub ready: AtomicBool,
}

impl ServerState {
    pub fn new(
        store: Arc<crate::store::FoyerStore>,
        origin: Arc<crate::origin::S3Origin>,
        router: Arc<crate::router::Router>,
        admission: Arc<crate::admission::SizeThresholdPolicy>,
        metrics: Arc<crate::metrics::Registry>,
    ) -> Self {
        Self {
            store,
            origin,
            router,
            admission,
            metrics,
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

/// Build the Axum router. Pure function — no side effects, no I/O.
pub fn build_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/metrics", get(handlers::metrics))
        .route("/cache/:pool/:key/:range", get(handlers::get_cache))
        .route("/cache/:pool/:key", head(handlers::head_cache))
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

    /// `HEAD /cache/:pool/:key` — pre-flight. SHELF-07 landing ticket.
    pub async fn head_cache(
        State(_state): State<Arc<ServerState>>,
        Path((_pool, _key_hex)): Path<(String, String)>,
    ) -> Response {
        (StatusCode::NOT_IMPLEMENTED, "SHELF-07 pending").into_response()
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
}
