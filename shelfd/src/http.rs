//! HTTP/2 data-plane server for `shelfd`.
//!
//! Ticket ownership:
//! - SHELF-02 — Axum router skeleton with `/healthz`, `/readyz`,
//!   `/metrics`, structured `tracing` logging, graceful shutdown.
//! - SHELF-06 — `GET /cache/<key>/<offset>-<len>` with Foyer
//!   read-through, single-flight coalescing, `Content-Range` header.
//! - SHELF-07 — `HEAD /cache/<key>` for plugin pre-flight.
//! - SHELF-08 — Prometheus `/metrics` + OTel trace spans on every
//!   request (server + S3 span).
//! - ADR-0004 — **HTTP/2 only in v1**. No Arrow Flight. No HTTP/1.1
//!   accepted on the data port (control port may speak HTTP/1.1 for
//!   health probes).
//!
//! The router returned by `build_router()` is intentionally a thin
//! shape so tests can hit it without a network listener.

use axum::routing::{get, head};
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;

/// Dependencies the data-plane router needs at request time.
///
/// The concrete `FoyerStore` / `S3Origin` types are held directly
/// because both traits use `impl Future` (RPITIT) and are not yet
/// dyn-compatible. If a `dyn Store` shape becomes necessary (e.g. for
/// in-memory test doubles) SHELF-NN will either box the futures or
/// introduce a separate object-safe trait.
#[derive(Debug)]
pub struct ServerState {
    pub store: Arc<crate::store::FoyerStore>,
    pub origin: Arc<crate::origin::S3Origin>,
    pub router: Arc<crate::router::Router>,
    pub admission: Arc<crate::admission::SizeThresholdPolicy>,
    pub metrics: Arc<crate::metrics::Registry>,
}

/// Build the Axum router. Pure function — no side effects, no IO.
/// Tests call this directly.
pub fn build_router(_state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/healthz", get(handlers::healthz))
        .route("/readyz", get(handlers::readyz))
        .route("/cache/:key/:range", get(handlers::get_cache))
        .route("/cache/:key", head(handlers::head_cache))
}

/// Bind and serve the data plane. HTTP/2 only per ADR-0004.
pub async fn serve(
    _addr: SocketAddr,
    _state: Arc<ServerState>,
    _shutdown: tokio_util::sync::CancellationToken,
) -> crate::Result<()> {
    todo!(
        "SHELF-02 + SHELF-06: http: bind HTTP/2-only listener on addr, \
         mount build_router(state), drive graceful shutdown via the \
         CancellationToken; see 03-plan.md §4 SHELF-02/SHELF-06 and \
         agents/out/adr/0004-http2-only-in-v1.md"
    )
}

/// HTTP handlers. Kept in a submodule so tests can target them
/// directly.
pub mod handlers {
    use axum::http::StatusCode;

    // Return concrete `StatusCode` (rather than `impl IntoResponse`)
    // until each ticket replaces the body with a richer response.
    // This avoids never-type-fallback warnings on `todo!()`.

    pub async fn healthz() -> StatusCode {
        // Liveness is intentionally cheap: if the tokio runtime is
        // responsive, we are alive. Do NOT reach into the store here
        // — that is the readyz path. See SHELF-02 acceptance.
        StatusCode::OK
    }

    pub async fn readyz() -> StatusCode {
        todo!(
            "SHELF-02: http: readyz must return 200 iff the Foyer pools \
             opened, origin client is reachable, and membership has a \
             non-empty view; 503 otherwise; see 03-plan.md §4 SHELF-02"
        )
    }

    pub async fn get_cache() -> StatusCode {
        todo!(
            "SHELF-06: http: parse path params, derive key, call \
             store::get; on miss call origin::get_range, admission::decide, \
             store::insert, stream to client with Content-Range; see \
             03-plan.md §4 SHELF-06"
        )
    }

    pub async fn head_cache() -> StatusCode {
        todo!(
            "SHELF-07: http: HEAD returns S3 Content-Length via origin::head \
             + small DRAM LRU; see 03-plan.md §4 SHELF-07"
        )
    }
}
