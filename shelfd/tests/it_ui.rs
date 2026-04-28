//! Integration tests for the embedded admin UI (feature `ui`).
//!
//! Mirrors the spawn-a-server harness from `it_admin.rs` but asserts
//! on the `/ui` routes added by [`shelfd::ui`]. Compiled only when
//! the `ui` feature is active — when the feature is off, the crate
//! has no `ui` module, the routes don't exist, and this file is
//! invisible to the test runner.
//!
//! Run with: `cargo test -p shelfd --features ui --test it_ui`.

#![cfg(feature = "ui")]

use std::net::SocketAddr;
use std::sync::Arc;

use shelfd::{
    admission::SizeThresholdPolicy,
    config::{AdmissionConfig, MetadataPoolConfig, PoolsConfig, RowGroupPoolConfig},
    http::{self, ServerState},
    metrics,
    router::Router as ShelfRouter,
    store::FoyerStore,
};
use tokio_util::sync::CancellationToken;

fn test_pools() -> PoolsConfig {
    PoolsConfig {
        metadata: MetadataPoolConfig {
            dram_bytes: 1 << 20,
        },
        rowgroup: RowGroupPoolConfig {
            dram_bytes: 1 << 20,
            nvme_dir: std::path::PathBuf::from("/tmp/shelf_ui_unused"),
            nvme_bytes: 0,
            eviction_policy: shelfd::config::EvictionPolicy::default(),
            disk_cache: shelfd::config::RowGroupDiskCacheConfig::default(),
        },
    }
}

async fn spawn_ui_server() -> (SocketAddr, CancellationToken) {
    // Prometheus registry is process-global; share it across tests
    // in the same binary via OnceCell just like `it_admin.rs`.
    use tokio::sync::OnceCell;
    static METRICS: OnceCell<Arc<metrics::Registry>> = OnceCell::const_new();
    let metrics = METRICS
        .get_or_init(|| async { Arc::new(metrics::Registry::init().expect("metrics")) })
        .await
        .clone();

    let store = Arc::new(FoyerStore::open(&test_pools()).await.expect("store"));
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
    }
    let origin = Arc::new(
        shelfd::origin::S3Origin::new(&shelfd::config::OriginConfig {
            bucket: "unused".to_owned(),
            endpoint_url: Some("http://127.0.0.1:1".to_owned()),
            region: Some("us-east-1".to_owned()),
            max_inflight: 1,
        })
        .await
        .expect("origin"),
    );
    let router = Arc::new(ShelfRouter::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&AdmissionConfig {
        size_threshold_bytes: 1 << 30,
        pinned_bypass: true,
    }));
    let state = Arc::new(ServerState::new(store, origin, router, admission, metrics));
    state.mark_ready();

    let cancel = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().unwrap();
    let app = http::build_router(state);
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel_clone.cancelled().await })
            .await
            .expect("serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, cancel)
}

fn url(addr: &SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

#[tokio::test]
async fn ui_index_is_served_with_html_content_type() {
    let (addr, cancel) = spawn_ui_server().await;
    let resp = reqwest::get(url(&addr, "/ui")).await.expect("GET /ui");
    assert!(resp.status().is_success(), "/ui should return 200");
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("text/html"),
        "/ui must return text/html, got {ct:?}",
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<html") || body.contains("<!DOCTYPE"),
        "/ui must return an HTML document, got body prefix: {}",
        body.chars().take(80).collect::<String>(),
    );
    cancel.cancel();
}

#[tokio::test]
async fn ui_unknown_asset_is_404() {
    let (addr, cancel) = spawn_ui_server().await;
    let resp = reqwest::get(url(&addr, "/ui/does-not-exist.js"))
        .await
        .expect("GET");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    cancel.cancel();
}

#[tokio::test]
async fn ui_does_not_shadow_existing_routes() {
    let (addr, cancel) = spawn_ui_server().await;
    // Sanity — mounting the UI must not break the JSON surface the
    // SPA itself consumes.
    for path in ["/stats", "/admin/ring", "/healthz"] {
        let r = reqwest::get(url(&addr, path))
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        assert!(
            r.status().is_success(),
            "{path} must still succeed with ui feature on, got {}",
            r.status(),
        );
    }
    cancel.cancel();
}
