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
    membership::DrainSignal,
    metrics,
    router::{Member, Router as ShelfRouter},
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
            eviction_policy: shelfd::config::EvictionPolicy::default(),
            disk_cache: shelfd::config::RowGroupDiskCacheConfig::default(),
        },
    }
}

/// Test harness handle. Wraps the data needed to drive the
/// `/admin/*` and `/stats` surfaces from a unit test:
///
/// - `addr`     — bind address of the spawned axum server.
/// - `store`    — the live `FoyerStore` so tests can seed / assert.
/// - `router`   — shared `Arc<Router>` so tests can publish ring
///                snapshots and observe them via `/admin/ring`.
/// - `drain`    — shared `DrainSignal` so tests can flip the
///                lameduck bit and observe `/stats.draining`.
/// - `cancel`   — graceful shutdown trigger.
struct AdminHarness {
    addr: SocketAddr,
    store: Arc<FoyerStore>,
    router: Arc<ShelfRouter>,
    drain: DrainSignal,
    cancel: CancellationToken,
}

/// Build a minimal `ServerState`, spawn the HTTP server on an
/// ephemeral port. Returns an `AdminHarness` exposing every knob
/// tests need.
async fn spawn_admin_server() -> AdminHarness {
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
    let drain = DrainSignal::new();
    let state = Arc::new(
        ServerState::new(store.clone(), origin, router.clone(), admission, metrics)
            .with_drain_signal(drain.clone()),
    );
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
    AdminHarness {
        addr,
        store,
        router,
        drain,
        cancel,
    }
}

fn url(addr: &SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

fn seed_key(seed: u8) -> Key {
    key_from_tuple(&[seed; 8], 0, 1, 0).expect("key")
}

/// Empty-ring response shape is part of the contract: `members: []`,
/// `ring_size: 0`, `draining: false`. An empty ring is a real ops
/// signal (DNS or `/stats` probes are failing) — it must NOT be
/// confused with the boot-time placeholder that the old admin_ring
/// implementation returned.
#[tokio::test]
async fn admin_ring_empty_shape() {
    let h = spawn_admin_server().await;
    let resp = reqwest::get(url(&h.addr, "/admin/ring"))
        .await
        .expect("GET /admin/ring");
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.expect("json");
    let obj = body.as_object().expect("object");
    for key in ["self_id", "draining", "ring_size", "members"] {
        assert!(obj.contains_key(key), "/admin/ring must carry `{key}`");
    }
    assert_eq!(obj["draining"].as_bool(), Some(false));
    assert_eq!(obj["ring_size"].as_u64(), Some(0));
    let arr = obj["members"].as_array().expect("members array");
    assert!(arr.is_empty(), "freshly-booted ring is empty");
    h.cancel.cancel();
}

/// `/admin/ring` reflects the live `Router` view. Publish a 3-node
/// ring and assert each member round-trips with `pod_id`, `endpoint`,
/// `weight`, and `is_self`.
#[tokio::test]
async fn admin_ring_reflects_router_view() {
    let h = spawn_admin_server().await;
    h.router.update(vec![
        Member {
            id: "shelf-0".to_owned(),
            endpoint: "10.0.1.4:9092".to_owned(),
            weight: 14,
        },
        Member {
            id: "shelf-1".to_owned(),
            endpoint: "10.0.1.7:9092".to_owned(),
            weight: 14,
        },
        Member {
            id: "shelf-2".to_owned(),
            endpoint: "10.0.1.9:9092".to_owned(),
            weight: 28,
        },
    ]);
    let resp: serde_json::Value = reqwest::get(url(&h.addr, "/admin/ring"))
        .await
        .expect("GET")
        .json()
        .await
        .expect("json");
    assert_eq!(resp["ring_size"].as_u64(), Some(3));
    let members = resp["members"].as_array().expect("members");
    assert_eq!(members.len(), 3);
    for (i, expected_id) in ["shelf-0", "shelf-1", "shelf-2"].iter().enumerate() {
        let row = &members[i];
        assert_eq!(row["pod_id"].as_str(), Some(*expected_id));
        for key in ["pod_id", "endpoint", "weight", "is_self"] {
            assert!(row.get(key).is_some(), "row must carry `{key}`");
        }
    }
    assert_eq!(members[2]["weight"].as_u64(), Some(28));
    // self_id matches the default test pod id; none of the seeded
    // pods share that id so no row is_self=true. The bit just has
    // to round-trip — concrete identity is exercised in the unit
    // tests in `http::tests`.
    for m in members {
        assert_eq!(m["is_self"].as_bool(), Some(false));
    }
    h.cancel.cancel();
}

/// SHELF-20 contract: flipping the local `DrainSignal` makes
/// `/stats.draining` return `true` on the very next probe. The Java
/// plugin and peer resolvers depend on this transition being
/// instantaneous (no buffering, no cache).
#[tokio::test]
async fn stats_reflects_drain_signal() {
    let h = spawn_admin_server().await;
    let stats0: serde_json::Value = reqwest::get(url(&h.addr, "/stats"))
        .await
        .expect("GET /stats")
        .json()
        .await
        .expect("json");
    assert_eq!(stats0["draining"].as_bool(), Some(false));

    h.drain.begin();

    let stats1: serde_json::Value = reqwest::get(url(&h.addr, "/stats"))
        .await
        .expect("GET /stats")
        .json()
        .await
        .expect("json");
    assert_eq!(
        stats1["draining"].as_bool(),
        Some(true),
        "draining bit must flip the next round trip"
    );
    h.cancel.cancel();
}

#[tokio::test]
async fn admin_pin_rejects_unknown_key() {
    let h = spawn_admin_server().await;
    let addr = h.addr;
    let cancel = h.cancel;
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
    let h = spawn_admin_server().await;
    let addr = h.addr;
    let store = h.store;
    let cancel = h.cancel;
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
    let h = spawn_admin_server().await;
    let addr = h.addr;
    let store = h.store;
    let cancel = h.cancel;
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
    let h = spawn_admin_server().await;
    let addr = h.addr;
    let cancel = h.cancel;
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
    let h = spawn_admin_server().await;
    let addr = h.addr;
    let store = h.store;
    let cancel = h.cancel;
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
