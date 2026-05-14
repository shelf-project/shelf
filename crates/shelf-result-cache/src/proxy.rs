//! HTTP proxy that intercepts Trino queries and serves cached results.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::cache::{CachedResult, ResultCache};
use crate::canonicalizer::PlanCanonicalizer;
use crate::config::Config;
use crate::snapshot::SnapshotResolver;

/// Shared state for the result cache proxy.
pub struct ProxyState {
    /// Configuration.
    pub config: Config,
    /// Result cache.
    pub cache: ResultCache,
    /// Plan canonicalizer.
    pub canonicalizer: PlanCanonicalizer,
    /// Snapshot resolver.
    pub snapshot_resolver: SnapshotResolver,
    /// HTTP client for forwarding to Trino.
    pub http_client: reqwest::Client,
}

impl std::fmt::Debug for ProxyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyState")
            .field("config", &self.config)
            .field("cache", &self.cache)
            .finish()
    }
}

/// The result cache proxy server.
#[derive(Debug)]
pub struct ResultCacheProxy {
    state: Arc<ProxyState>,
}

impl ResultCacheProxy {
    /// Create a new result cache proxy.
    pub fn new(config: Config) -> Self {
        let cache = ResultCache::new(config.clone());
        let canonicalizer = PlanCanonicalizer::new();
        let snapshot_resolver = SnapshotResolver::new(
            config.shelfd_url.clone(),
            std::time::Duration::from_secs(60),
        );
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .expect("Failed to create HTTP client");

        let state = Arc::new(ProxyState {
            config,
            cache,
            canonicalizer,
            snapshot_resolver,
            http_client,
        });

        Self { state }
    }

    /// Build the Axum router for the proxy.
    pub fn router(&self) -> Router {
        let state = Arc::clone(&self.state);

        Router::new()
            // Health check
            .route("/healthz", get(health_check))
            // Metrics
            .route("/metrics", get(metrics_handler))
            // Cache stats
            .route("/cache/stats", get(cache_stats))
            // Cache invalidation
            .route("/cache/invalidate/:snapshot_id", any(invalidate_snapshot))
            // Cache clear
            .route("/cache/clear", any(clear_cache))
            // Proxy all other requests to Trino
            .fallback(proxy_handler)
            .with_state(state)
    }

    /// Get a reference to the proxy state.
    pub fn state(&self) -> &Arc<ProxyState> {
        &self.state
    }
}

/// Health check endpoint.
async fn health_check() -> &'static str {
    "ok"
}

/// Prometheus metrics endpoint.
async fn metrics_handler() -> impl IntoResponse {
    use prometheus::Encoder;

    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();

    encoder
        .encode(&metric_families, &mut buffer)
        .expect("Failed to encode metrics");

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        buffer,
    )
}

/// Cache statistics endpoint.
async fn cache_stats(State(state): State<Arc<ProxyState>>) -> impl IntoResponse {
    let stats = state.cache.stats();
    axum::Json(serde_json::json!({
        "entries": stats.entries,
        "total_bytes": stats.total_bytes,
        "max_bytes": stats.max_bytes,
        "max_entries": stats.max_entries,
        "utilization_pct": (stats.total_bytes as f64 / stats.max_bytes as f64) * 100.0,
    }))
}

/// Invalidate cache entries for a snapshot.
async fn invalidate_snapshot(
    State(state): State<Arc<ProxyState>>,
    axum::extract::Path(snapshot_id): axum::extract::Path<i64>,
) -> impl IntoResponse {
    state.cache.invalidate_snapshot(snapshot_id);
    format!("Invalidated cache entries for snapshot {}", snapshot_id)
}

/// Clear all cache entries.
async fn clear_cache(State(state): State<Arc<ProxyState>>) -> impl IntoResponse {
    state.cache.clear();
    "Cache cleared"
}

/// Proxy handler for Trino requests.
async fn proxy_handler(
    State(state): State<Arc<ProxyState>>,
    request: Request<Body>,
) -> impl IntoResponse {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path();

    debug!(method = %method, path = %path, "Received request");

    // Only try to cache SELECT queries via the /v1/statement endpoint
    if !state.config.enabled || method != http::Method::POST || !path.starts_with("/v1/statement") {
        return forward_to_trino(&state, request).await;
    }

    // Extract query from request body
    // TODO: Parse the request body to extract the SQL query
    // For now, forward all requests to Trino

    forward_to_trino(&state, request).await
}

/// Forward a request to the upstream Trino coordinator.
async fn forward_to_trino(
    state: &ProxyState,
    request: Request<Body>,
) -> Response<Body> {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();

    // Build the upstream URL
    let upstream_url = format!("{}{}", state.config.trino_url, uri.path_and_query().map(|p| p.as_str()).unwrap_or("/"));

    debug!(upstream_url = %upstream_url, "Forwarding to Trino");

    // Read the request body
    let body_bytes = match axum::body::to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(error = %e, "Failed to read request body");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("Failed to read request body: {}", e)))
                .unwrap();
        }
    };

    // Build the upstream request
    let mut upstream_request = state.http_client.request(method, &upstream_url);

    // Copy headers
    for (name, value) in headers.iter() {
        if name != http::header::HOST {
            upstream_request = upstream_request.header(name.as_str(), value.to_str().unwrap_or(""));
        }
    }

    // Send the request
    let start = Instant::now();
    let response = match upstream_request.body(body_bytes.to_vec()).send().await {
        Ok(resp) => resp,
        Err(e) => {
            warn!(error = %e, "Failed to forward request to Trino");
            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Body::from(format!("Failed to forward request: {}", e)))
                .unwrap();
        }
    };

    let latency_ms = start.elapsed().as_millis() as u64;
    let status = response.status();

    debug!(
        status = %status,
        latency_ms = latency_ms,
        "Received response from Trino"
    );

    crate::metrics::REQUESTS_FORWARDED_TOTAL.inc();

    // Build the response
    let mut builder = Response::builder().status(status);

    for (name, value) in response.headers().iter() {
        builder = builder.header(name.as_str(), value.to_str().unwrap_or(""));
    }

    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(error = %e, "Failed to read response body");
            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Body::from(format!("Failed to read response: {}", e)))
                .unwrap();
        }
    };

    builder
        .body(Body::from(body_bytes.to_vec()))
        .unwrap_or_else(|e| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Failed to build response: {}", e)))
                .unwrap()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_creation() {
        let config = Config::default();
        let proxy = ResultCacheProxy::new(config);

        let stats = proxy.state().cache.stats();
        assert_eq!(stats.entries, 0);
    }
}
