//! RC6 P1.2 — integration test for `GET /admin/cap-ready`.
//!
//! Spawns one shelfd server on an ephemeral port, exercises the
//! end-to-end gate behaviour, asserts the JSON contract:
//!
//!   - `200` body carries `ready: true`, `max_rss_gib`, `peers_probed`,
//!     `threshold_bytes`.
//!   - 503 path is exercised in the unit tests in
//!     `src/capacity_check.rs` because forcing a peer over the
//!     22 GiB threshold requires either a synthetic environment
//!     variable hook (which we do not ship) or a bespoke probe stub
//!     — the unit tests already cover that branch with mocked stats.
//!
//! Like `it_admin.rs` this suite does not need MinIO; the cap-ready
//! gate never reaches into the cache.

use std::net::SocketAddr;
use std::sync::Arc;

use shelfd::{
    admission::SizeThresholdPolicy,
    config::{AdmissionConfig, MetadataPoolConfig, PoolsConfig, RowGroupPoolConfig},
    http::{self, ServerState},
    membership::DrainSignal,
    metrics,
    router::Router as ShelfRouter,
    store::FoyerStore,
};
use tokio_util::sync::CancellationToken;

fn test_pools() -> PoolsConfig {
    PoolsConfig {
        metadata: MetadataPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
        },
        rowgroup: RowGroupPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
            nvme_dir: std::path::PathBuf::from("/tmp/shelf_cap_ready_unused"),
            nvme_bytes: 0,
            eviction_policy: shelfd::config::EvictionPolicy::default(),
            disk_cache: shelfd::config::RowGroupDiskCacheConfig::default(),
            compression: shelfd::config::CompressionConfig::default(),
        },
    }
}

struct Harness {
    addr: SocketAddr,
    cancel: CancellationToken,
}

async fn spawn_server() -> Harness {
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
        ServerState::new(store, origin, router, admission, metrics).with_drain_signal(drain),
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
    Harness { addr, cancel }
}

fn url(addr: &SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

/// Empty router view + dev-host self-RSS=0 ⇒ the gate replies 200
/// with `ready: true`. This is the production happy path on a
/// freshly-booted cluster (no peers yet, self under threshold).
#[tokio::test]
async fn cap_ready_empty_ring_reports_200_ready() {
    let h = spawn_server().await;
    let resp = reqwest::get(url(&h.addr, "/admin/cap-ready"))
        .await
        .expect("GET /admin/cap-ready");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "empty-ring + no peer under threshold must be 200"
    );
    let body: serde_json::Value = resp.json().await.expect("json");
    let obj = body.as_object().expect("object");
    for key in [
        "ready",
        "max_rss_gib",
        "max_rss_bytes",
        "peers_probed",
        "threshold_bytes",
    ] {
        assert!(
            obj.contains_key(key),
            "/admin/cap-ready response must carry `{key}`"
        );
    }
    assert_eq!(obj["ready"].as_bool(), Some(true));
    assert_eq!(obj["peers_probed"].as_u64(), Some(1), "self only");
    assert_eq!(
        obj["threshold_bytes"].as_u64(),
        Some(22 * 1024 * 1024 * 1024),
        "default threshold is 22 GiB"
    );
    h.cancel.cancel();
}

/// `?caller=<replica>` is opaque audit metadata — the response
/// shape must be identical regardless. Verified separately because
/// the only way to catch a typo in the query-deserialize path
/// (e.g. accidentally requiring `caller`) is to hit the route with
/// the parameter present.
#[tokio::test]
async fn cap_ready_accepts_caller_param() {
    let h = spawn_server().await;
    let resp = reqwest::get(url(&h.addr, "/admin/cap-ready?caller=rep-0"))
        .await
        .expect("GET");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["ready"].as_bool(), Some(true));
    h.cancel.cancel();
}

/// `/stats` must now carry the new `rss_bytes` field for the
/// cap-ready gate to consume across peers. Verifies the additive
/// wire change shipped in the same PR.
#[tokio::test]
async fn stats_now_carries_rss_bytes() {
    let h = spawn_server().await;
    let resp = reqwest::get(url(&h.addr, "/stats"))
        .await
        .expect("GET /stats");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert!(
        body.get("rss_bytes").is_some(),
        "/stats must carry rss_bytes for cap-ready gate"
    );
    // We can't assert a non-zero value because the test host is
    // typically macOS in dev — but it must be a u64-shaped number.
    assert!(body["rss_bytes"].is_u64());
    h.cancel.cancel();
}
