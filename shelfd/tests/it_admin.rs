//! SHELF-23 + SHELF-24 integration tests for the `/admin/*` HTTP
//! surface.
//!
//! Unlike the other `it_*.rs` suites, this one does NOT need MinIO —
//! we seed the cache directly via [`FoyerStore::insert`] and only
//! exercise the `/admin/*` routes. That keeps the test runnable on
//! plain `cargo test -p shelfd --test it_admin` without the
//! docker-compose harness.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use shelfd::{
    admission::SizeThresholdPolicy,
    config::{AdmissionConfig, MetadataPoolConfig, PoolsConfig, RowGroupPoolConfig},
    http::{self, ServerState},
    metrics,
    router::Router as ShelfRouter,
    store::{key_from_tuple, FoyerStore, Key, Pool, Store},
};
use tokio_util::sync::CancellationToken;

fn test_pools() -> PoolsConfig {
    PoolsConfig {
        metadata: MetadataPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
        },
        rowgroup: RowGroupPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
            nvme_dir: std::path::PathBuf::from("/tmp/shelf_admin_unused"),
            nvme_bytes: 0,
        },
    }
}

/// Build a minimal `ServerState`, spawn the HTTP server on an
/// ephemeral port, return `(addr, store, cancel)`. The `store` Arc
/// lets tests seed / assert on the cache without a `/stats` round
/// trip.
async fn spawn_admin_server() -> (SocketAddr, Arc<FoyerStore>, CancellationToken) {
    // Per-test registries would collide on the process-global
    // Prometheus registry if we rebuilt. Use a OnceCell.
    use tokio::sync::OnceCell;
    static METRICS: OnceCell<Arc<metrics::Registry>> = OnceCell::const_new();
    let metrics = METRICS
        .get_or_init(|| async { Arc::new(metrics::Registry::init().expect("metrics")) })
        .await
        .clone();

    let store = Arc::new(FoyerStore::open(&test_pools()).await.expect("store"));
    // `S3Origin` needs AWS credentials — tests that don't exercise
    // the origin can leave it unset, but `ServerState::new` demands
    // one. We build a throwaway S3 client pointed at localhost.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
    }
    let origin_cfg = shelfd::config::OriginConfig {
        bucket: "unused".to_owned(),
        endpoint_url: Some("http://127.0.0.1:1".to_owned()),
        region: Some("us-east-1".to_owned()),
        max_inflight: 1,
    };
    let origin = Arc::new(
        shelfd::origin::S3Origin::new(&origin_cfg)
            .await
            .expect("origin"),
    );

    let router = Arc::new(ShelfRouter::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&AdmissionConfig {
        size_threshold_bytes: 1 << 30,
        pinned_bypass: true,
    }));
    let state = Arc::new(ServerState::new(
        store.clone(),
        origin,
        router,
        admission,
        metrics,
    ));
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
            .expect("axum serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, store, cancel)
}

fn url(addr: &SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

fn seed_key(seed: u8) -> Key {
    key_from_tuple(&[seed; 8], 0, 1, 0).expect("key")
}

#[tokio::test]
async fn admin_ring_returns_json_array() {
    let (addr, _store, cancel) = spawn_admin_server().await;
    let resp = reqwest::get(url(&addr, "/admin/ring"))
        .await
        .expect("GET /admin/ring");
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.expect("json");
    let arr = body.as_array().expect("array");
    assert!(!arr.is_empty(), "/admin/ring must list at least self");
    let row = &arr[0];
    for key in ["pod_id", "weight", "healthy"] {
        assert!(row.get(key).is_some(), "/admin/ring row must carry `{key}`");
    }
    cancel.cancel();
}

#[tokio::test]
async fn admin_pin_rejects_unknown_key() {
    let (addr, _store, cancel) = spawn_admin_server().await;
    let client = reqwest::Client::new();
    let body = serde_json::json!({"key_hex": seed_key(1).to_hex(), "pool": "rowgroup"});
    let resp = client
        .post(url(&addr, "/admin/pin"))
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    cancel.cancel();
}

#[tokio::test]
async fn admin_pin_raises_pinned_bytes_on_stats() {
    let (addr, store, cancel) = spawn_admin_server().await;
    // Seed the key so `/admin/pin` accepts it.
    let key = seed_key(2);
    store
        .insert(Pool::RowGroup, key.clone(), Bytes::from_static(&[0u8; 128]))
        .await
        .expect("seed");

    let client = reqwest::Client::new();
    // Baseline
    let stats0: serde_json::Value = client
        .get(url(&addr, "/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats0["pinned_bytes"].as_u64(), Some(0));
    assert_eq!(stats0["pinned_count"].as_u64(), Some(0));

    let body = serde_json::json!({"key_hex": key.to_hex(), "pool": "rowgroup"});
    let resp = client
        .post(url(&addr, "/admin/pin"))
        .json(&body)
        .send()
        .await
        .expect("post");
    assert!(resp.status().is_success(), "{:?}", resp.status());

    let stats1: serde_json::Value = client
        .get(url(&addr, "/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats1["pinned_bytes"].as_u64(), Some(128));
    assert_eq!(stats1["pinned_count"].as_u64(), Some(1));
    cancel.cancel();
}

#[tokio::test]
async fn admin_evict_drops_entry() {
    let (addr, store, cancel) = spawn_admin_server().await;
    let key = seed_key(3);
    store
        .insert(Pool::Metadata, key.clone(), Bytes::from_static(b"x"))
        .await
        .expect("seed");
    assert!(store.get(Pool::Metadata, &key).await.unwrap().is_some());

    let client = reqwest::Client::new();
    let body = serde_json::json!({"key_hex": key.to_hex(), "pool": "metadata"});
    let resp = client
        .post(url(&addr, "/admin/evict"))
        .json(&body)
        .send()
        .await
        .expect("post");
    assert!(resp.status().is_success());
    assert!(store.get(Pool::Metadata, &key).await.unwrap().is_none());
    cancel.cancel();
}

#[tokio::test]
async fn admin_reload_returns_200_when_loader_disabled() {
    // Ticket §4 SHELF-23: POST /admin/reload with `pin_list = None`
    // returns 200 with `{pinned_bytes: 0, pinned_count: 0,
    // reload_ok: true}` — the daemon has nothing to reload, which
    // is a success state, not an error. The test harness never
    // configures a loader, so this path is the one we cover here.
    let (addr, _store, cancel) = spawn_admin_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(url(&addr, "/admin/reload"))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["pinned_bytes"].as_u64(), Some(0));
    assert_eq!(body["pinned_count"].as_u64(), Some(0));
    assert_eq!(body["reload_ok"].as_bool(), Some(true));
    cancel.cancel();
}

#[tokio::test]
async fn admin_unpin_then_pin_round_trip() {
    let (addr, store, cancel) = spawn_admin_server().await;
    let key = seed_key(4);
    store
        .insert(Pool::RowGroup, key.clone(), Bytes::from_static(&[0u8; 16]))
        .await
        .expect("seed");
    let client = reqwest::Client::new();
    let pin_body = serde_json::json!({"key_hex": key.to_hex(), "pool": "rowgroup"});
    let unpin_body = serde_json::json!({"key_hex": key.to_hex()});
    // Pin first.
    let resp = client
        .post(url(&addr, "/admin/pin"))
        .json(&pin_body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    // Then unpin (no pool needed — unpin is pool-agnostic).
    let resp = client
        .post(url(&addr, "/admin/unpin"))
        .json(&unpin_body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    // Unpinning again: 404 (was not pinned).
    let resp = client
        .post(url(&addr, "/admin/unpin"))
        .json(&unpin_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    cancel.cancel();
}
