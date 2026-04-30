//! RC6 P1.3 integration tests for the widened `/admin/pin` schema.
//!
//! Mirrors the existing `it_admin.rs` harness shape — minimal
//! `ServerState`, axum on an ephemeral port, no MinIO. The new
//! coverage exercises the replay-list shapes (single + batch) and
//! asserts the per-entry response contract that pre-warm tooling
//! depends on.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use shelfd::{
    admission::SizeThresholdPolicy,
    config::{AdmissionConfig, MetadataPoolConfig, PoolsConfig, RowGroupPoolConfig},
    http::{self, ServerState},
    membership::DrainSignal,
    metrics,
    router::Router as ShelfRouter,
    store::{key_from_tuple, FoyerStore, Pool, Store},
};
use tokio_util::sync::CancellationToken;

fn test_pools() -> PoolsConfig {
    PoolsConfig {
        metadata: MetadataPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
        },
        rowgroup: RowGroupPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
            nvme_dir: std::path::PathBuf::from("/tmp/shelf_admin_pin_flex_unused"),
            nvme_bytes: 0,
            eviction_policy: shelfd::config::EvictionPolicy::default(),
            disk_cache: shelfd::config::RowGroupDiskCacheConfig::default(),
            compression: shelfd::config::CompressionConfig::default(),
        },
    }
}

struct Harness {
    addr: SocketAddr,
    store: Arc<FoyerStore>,
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
        ServerState::new(store.clone(), origin, router, admission, metrics)
            .with_drain_signal(drain),
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
    Harness {
        addr,
        store,
        cancel,
    }
}

fn url(addr: &SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

/// Pre-RC6 strict-shape callers (`shelfctl pin`, the H3
/// mv-pin-watcher) must continue to work bit-for-bit. The
/// response shape MUST still carry the legacy `pinned`/`pool`/
/// `pinned_bytes`/`pinned_count`/`mv_name` keys; introducing the
/// new optional `audit` key is fine because clients should
/// ignore unknown JSON fields.
#[tokio::test]
async fn strict_shape_round_trips_unchanged() {
    let h = spawn_server().await;
    // Seed a key the strict pin can latch onto.
    let etag = b"e_strict";
    let key = key_from_tuple(etag, 0, 64, 0).expect("key");
    h.store
        .insert(Pool::Metadata, key.clone(), Bytes::from_static(&[0u8; 64]))
        .await
        .expect("seed");

    let body = serde_json::json!({
        "key_hex": key.to_hex(),
        "pool": "metadata",
    });
    let resp: serde_json::Value = reqwest::Client::new()
        .post(url(&h.addr, "/admin/pin"))
        .json(&body)
        .send()
        .await
        .expect("post")
        .json()
        .await
        .expect("json");
    for k in ["pinned", "pool", "pinned_bytes", "pinned_count", "mv_name"] {
        assert!(
            resp.get(k).is_some(),
            "strict response must keep legacy key `{k}`"
        );
    }
    assert_eq!(resp["pool"].as_str(), Some("metadata"));
    assert_eq!(resp["pinned"].as_str(), Some(key.to_hex().as_str()));
    h.cancel.cancel();
}

/// RC6 P1.3 — single replay-list entry resolves to the same
/// content-addressed key the Python tool would compute, which lets
/// the strict-pin code path latch on.
#[tokio::test]
async fn replay_single_object_pins_resident_key() {
    let h = spawn_server().await;
    let etag = b"e_replay_single";
    let size = 128u64;
    let key = key_from_tuple(etag, 0, size, 0).expect("key");
    h.store
        .insert(Pool::RowGroup, key.clone(), Bytes::from_static(&[0u8; 128]))
        .await
        .expect("seed");

    let body = serde_json::json!({
        "bucket": "shelf-test",
        "key":    "table/manifest-001.avro",
        "etag":   "\"e_replay_single\"",
        "size_bytes": size,
        "pool":   "rowgroup",
    });
    let client = reqwest::Client::new();
    let resp = client
        .post(url(&h.addr, "/admin/pin"))
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "single replay pin");
    let v: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(v["pinned"].as_str(), Some(key.to_hex().as_str()));
    assert_eq!(v["pool"].as_str(), Some("rowgroup"));
    assert!(
        v["audit"]
            .as_str()
            .unwrap_or("")
            .contains("s3://shelf-test/table/manifest-001.avro"),
        "single-entry response must surface audit metadata"
    );

    // /stats should reflect the pin.
    let stats: serde_json::Value = client
        .get(url(&h.addr, "/stats"))
        .send()
        .await
        .expect("stats")
        .json()
        .await
        .expect("json");
    assert!(stats["pinned_count"].as_u64().unwrap_or(0) >= 1);
    h.cancel.cancel();
}

/// RC6 P1.3 — replay-list batch (top-level array) returns the
/// per-entry results envelope and the `pinned_count` reflects
/// every successful pin.
#[tokio::test]
async fn replay_batch_array_returns_per_entry_results() {
    let h = spawn_server().await;
    // Seed two keys so two entries land cleanly; leave the third
    // unseeded so we can assert the `not_resident` per-entry status.
    let entries: Vec<(Vec<u8>, u64, &str)> = vec![
        (b"e_batch_1".to_vec(), 64, "metadata"),
        (b"e_batch_2".to_vec(), 96, "rowgroup"),
    ];
    let mut expected_keys: Vec<String> = Vec::new();
    for (etag, size, pool_str) in &entries {
        let key = key_from_tuple(etag, 0, *size, 0).expect("key");
        let pool = if *pool_str == "metadata" {
            Pool::Metadata
        } else {
            Pool::RowGroup
        };
        h.store
            .insert(
                pool,
                key.clone(),
                Bytes::from_iter(vec![0u8; *size as usize]),
            )
            .await
            .expect("seed");
        expected_keys.push(key.to_hex());
    }
    let unseeded_etag = b"e_batch_unseeded";
    let unseeded_size = 32u64;
    let unseeded_key = key_from_tuple(unseeded_etag, 0, unseeded_size, 0).expect("k");

    let body = serde_json::json!([
        {
            "bucket": "b", "key": "k1", "etag": "e_batch_1",
            "size_bytes": 64, "pool": "metadata"
        },
        {
            "bucket": "b", "key": "k2", "etag": "e_batch_2",
            "size_bytes": 96, "pool": "rowgroup"
        },
        {
            "bucket": "b", "key": "k3", "etag": "e_batch_unseeded",
            "size_bytes": 32, "pool": "metadata"
        }
    ]);
    let resp: serde_json::Value = reqwest::Client::new()
        .post(url(&h.addr, "/admin/pin"))
        .json(&body)
        .send()
        .await
        .expect("post")
        .json()
        .await
        .expect("json");

    assert_eq!(resp["accepted"].as_u64(), Some(2), "two seeded keys pin OK");
    assert_eq!(
        resp["rejected"].as_u64(),
        Some(1),
        "the unseeded key falls through as not_resident"
    );
    let results = resp["results"].as_array().expect("results array");
    assert_eq!(results.len(), 3, "one row per input entry");

    // Every result row carries the contract keys.
    for row in results {
        for k in ["key_hex", "pool", "status", "audit"] {
            assert!(row.get(k).is_some(), "row must carry `{k}`: {row}");
        }
    }
    assert_eq!(results[0]["status"].as_str(), Some("pinned"));
    assert_eq!(results[1]["status"].as_str(), Some("pinned"));
    assert_eq!(results[2]["status"].as_str(), Some("not_resident"));
    assert_eq!(
        results[0]["key_hex"].as_str(),
        Some(expected_keys[0].as_str())
    );
    assert_eq!(
        results[2]["key_hex"].as_str(),
        Some(unseeded_key.to_hex().as_str())
    );

    h.cancel.cancel();
}

/// Defensive: malformed `pool` field on a replay entry returns 400
/// `invalid_pool`, not 500. The replay-list shape is operator-supplied
/// so a typo in the pool column must not crash the daemon.
#[tokio::test]
async fn replay_unknown_pool_returns_400() {
    let h = spawn_server().await;
    let body = serde_json::json!({
        "bucket": "b", "key": "k", "etag": "e",
        "size_bytes": 1, "pool": "wrong"
    });
    let resp = reqwest::Client::new()
        .post(url(&h.addr, "/admin/pin"))
        .json(&body)
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let v: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(v["error"].as_str(), Some("invalid_pool"));
    h.cancel.cancel();
}
