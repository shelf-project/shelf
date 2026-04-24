//! SHELF-18 — integration tests for the hybrid NVMe `rowgroup` pool.
//!
//! Unlike `it_read_path.rs` these tests do NOT need MinIO: the hybrid
//! tier is exercised by seeding the store directly and asserting on
//! the exposed HTTP / metric surface. That keeps the tests runnable
//! with a plain `cargo test -p shelfd --test it_hybrid_pool`.
//!
//! Test contract:
//! - `hybrid_pool_uses_tempdir_under_nvme_bytes` — boot a store with
//!   an NVMe-enabled `rowgroup` and assert the store reports DRAM
//!   capacity on `/stats` plus the NVMe capacity on `disk_capacity_bytes`.
//! - `hybrid_pool_survives_store_recreation` — proxy for the PVC-backed
//!   "survives pod restart" AC: insert, drop, reopen against the same
//!   dir, and verify the reopen does not crash.
//! - `disk_metrics_are_registered` — every SHELF-18 series appears on
//!   the `/metrics` scrape once the hybrid pool has served a miss.
//! - `zero_nvme_bytes_stays_dram_only` — regression guard that the
//!   DRAM-only path does not touch `nvme_dir`.

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

fn hybrid_pools(nvme_dir: std::path::PathBuf, nvme_bytes: u64) -> PoolsConfig {
    PoolsConfig {
        metadata: MetadataPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
        },
        rowgroup: RowGroupPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
            nvme_dir,
            nvme_bytes,
        },
    }
}

fn seed_key(seed: u8) -> Key {
    key_from_tuple(&[seed; 8], 0, 1, 0).expect("key")
}

async fn spawn_server(store: Arc<FoyerStore>) -> (SocketAddr, CancellationToken) {
    use tokio::sync::OnceCell;
    static METRICS: OnceCell<Arc<metrics::Registry>> = OnceCell::const_new();
    let metrics = METRICS
        .get_or_init(|| async { Arc::new(metrics::Registry::init().expect("metrics")) })
        .await
        .clone();

    // Origin client pointed at a black-hole URL — these tests never
    // issue a read-through miss that would require S3.
    // SAFETY: writing env vars is `unsafe` in the 2024 edition but
    // this is the only place in the test that touches them and we
    // keep the names local to the AWS SDK.
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
            .expect("axum serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, cancel)
}

fn url(addr: &SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

#[tokio::test]
async fn hybrid_pool_uses_tempdir_under_nvme_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pools = hybrid_pools(dir.path().to_path_buf(), 64 * 1024 * 1024);
    let store = Arc::new(FoyerStore::open(&pools).await.expect("open hybrid"));
    let (addr, cancel) = spawn_server(store.clone()).await;

    let body: serde_json::Value = reqwest::get(url(&addr, "/stats"))
        .await
        .expect("GET /stats")
        .json()
        .await
        .expect("json");
    let rg = body["rowgroup_pool"]
        .as_object()
        .expect("rowgroup_pool object");
    // SHELF-18 contract: DRAM capacity on `capacity_bytes`, NVMe
    // capacity on the new `disk_capacity_bytes` field.
    assert_eq!(rg["capacity_bytes"].as_u64(), Some(4 * 1024 * 1024));
    assert_eq!(
        rg["disk_capacity_bytes"].as_u64(),
        Some(64 * 1024 * 1024),
        "disk_capacity_bytes must reflect pools.rowgroup.nvme_bytes"
    );
    // The metadata pool stays DRAM-only per ADR-0008.
    let md = body["metadata_pool"]
        .as_object()
        .expect("metadata_pool object");
    assert_eq!(md["disk_capacity_bytes"].as_u64(), Some(0));

    cancel.cancel();
}

#[tokio::test]
async fn zero_nvme_bytes_stays_dram_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Point `nvme_dir` at an existing temp dir but keep
    // `nvme_bytes = 0`. The store must not touch the directory.
    let before: Vec<_> = std::fs::read_dir(dir.path()).expect("readdir").collect();
    assert!(before.is_empty());
    let pools = hybrid_pools(dir.path().to_path_buf(), 0);
    let store = FoyerStore::open(&pools).await.expect("open dram");
    assert_eq!(store.disk_bytes_capacity(Pool::RowGroup), 0);
    assert_eq!(store.disk_bytes_used(Pool::RowGroup), 0);
    let after: Vec<_> = std::fs::read_dir(dir.path())
        .expect("readdir")
        .filter_map(Result::ok)
        .collect();
    assert!(
        after.is_empty(),
        "DRAM-only rowgroup must not write to nvme_dir; found {after:?}"
    );
}

/// Proxy for the cluster-gated "pod restart" AC.
///
/// The PVC-backed data-integrity-across-restart test lives in
/// `tests/integration/SHELF-18-cluster.md`. In-process, we insert
/// a value, drop the store, and reopen it against the same dir —
/// the reopen must not crash.
///
/// Foyer 0.12's `HybridCache` opens with `RecoverMode::Quiet` by
/// default, which attempts to recover state from on-disk regions.
/// Whether the specific key returns byte-identical depends on
/// whether the storage enqueue had time to flush before the first
/// open was dropped; since SHELF-18's in-process scope cannot
/// guarantee a clean shutdown we assert only on "reopen does not
/// crash and the byte we wrote is not corrupt (either absent or
/// byte-identical)". The stronger per-key durability check runs
/// in the PVC-backed chaos suite.
///
// TODO(SHELF-18-ops): fold in the PVC-backed assertion once
// `charts/shelf/tests/pvc-restart.sh` exists — the in-process
// test below cannot reach cluster-gated durability.
#[tokio::test]
async fn hybrid_pool_survives_store_recreation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pools = hybrid_pools(dir.path().to_path_buf(), 32 * 1024 * 1024);

    let payload = Bytes::from(vec![0xABu8; 16 * 1024 * 1024]);
    let key = seed_key(200);
    {
        let store = FoyerStore::open(&pools).await.expect("open-1");
        store
            .insert(Pool::RowGroup, key.clone(), payload.clone())
            .await
            .expect("insert");
        // Memory-tier hit — proves the first open works end-to-end.
        let got = store.get(Pool::RowGroup, &key).await.unwrap();
        assert_eq!(got.as_deref(), Some(payload.as_ref()));
        // Explicit drop so Foyer's background flusher stops cleanly.
        drop(store);
        // Give the async flusher a moment to settle.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // Reopen against the same dir. Must succeed; the key may or
    // may not be recovered depending on flush timing.
    let reopened = FoyerStore::open(&pools).await.expect("open-2");
    let got = reopened.get(Pool::RowGroup, &key).await.expect("get");
    if let Some(bytes) = got {
        assert_eq!(
            bytes.as_ref(),
            payload.as_ref(),
            "on-disk recovery must be byte-identical when it happens"
        );
    }
}

#[tokio::test]
async fn disk_metrics_are_registered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pools = hybrid_pools(dir.path().to_path_buf(), 32 * 1024 * 1024);
    let store = Arc::new(FoyerStore::open(&pools).await.expect("open"));
    // Touch each series so the Prometheus registry gather() call
    // below has at least one observed child per counter/gauge.
    // The `/stats` handler populates disk_bytes_used and
    // disk_bytes_capacity; a single miss populates the disk miss
    // counter.
    let _ = store
        .get(Pool::RowGroup, &seed_key(99))
        .await
        .expect("get miss");
    let (addr, cancel) = spawn_server(store).await;

    // `/stats` triggers disk_bytes_used / disk_bytes_capacity sets.
    let _ = reqwest::get(url(&addr, "/stats")).await.expect("stats");

    let body = reqwest::get(url(&addr, "/metrics"))
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("body");
    for series in [
        "shelf_disk_hits_total",
        "shelf_disk_misses_total",
        "shelf_disk_bytes_used",
        "shelf_disk_bytes_capacity",
    ] {
        assert!(
            body.contains(series),
            "/metrics missing `{series}`:\n{body}"
        );
    }
    // Bonus: the miss we just issued should show up with a
    // non-zero value in the counter family.
    assert!(
        body.lines()
            .filter(|l| l.starts_with("shelf_disk_misses_total"))
            .any(|l| l.contains("rowgroup")),
        "expected a shelf_disk_misses_total{{pool=\"rowgroup\"}} line in /metrics:\n{body}"
    );
    cancel.cancel();
}
