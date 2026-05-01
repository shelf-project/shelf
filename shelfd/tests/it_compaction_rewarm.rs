//! SHELF-45 — end-to-end re-warm reactor test against MinIO.
//!
//! Gate: `SHELF_INTEGRATION=1` plus a running MinIO at
//! `127.0.0.1:9000` (see `shelfd/tests/docker-compose.yml`). The
//! test boots a real `FoyerStore`, seeds a synthetic compaction
//! event whose `added_files` reference live MinIO objects, drives
//! the [`CompactionReactor`] through one tick, and asserts the new
//! file's content-addressed key is resident in the rowgroup pool
//! afterwards. No Iceberg metadata is touched — that arrives via
//! the SHELF-37 producer in a follow-up PR.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::future::BoxFuture;
use shelfd::admission::SizeThresholdPolicy;
use shelfd::compaction_rewarm::{CompactionReactor, FileSpec, IcebergSnapshotEvent, RewarmFetcher};
use shelfd::config::{
    AdmissionConfig, MetadataPoolConfig, PoolsConfig, RewarmConfig, RowGroupPoolConfig,
};
use shelfd::store::{key_from_tuple, FoyerStore, Pool};
use tokio_util::sync::CancellationToken;

/// MinIO-backed fetcher: pulls an exact byte range via the SDK and
/// returns the body. Mirrors the production wiring without dragging
/// the full `S3Origin` trait in (which uses RPITIT and thus is not
/// dyn-safe — see `compaction_rewarm.rs` module doc).
#[derive(Debug)]
struct S3Fetcher {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl RewarmFetcher for S3Fetcher {
    fn fetch_file(&self, file: &FileSpec) -> BoxFuture<'static, shelfd::Result<Bytes>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = file.path.clone();
        Box::pin(async move {
            let resp = client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| shelfd::Error::Origin(format!("get_object: {e}")))?;
            let body = resp
                .body
                .collect()
                .await
                .map_err(|e| shelfd::Error::Origin(format!("collect: {e}")))?
                .into_bytes();
            Ok(body)
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn rewarm_reactor_warms_minio_object_on_synthetic_compaction() {
    if common::skip_if_offline() {
        return;
    }

    let s3 = common::s3_client().await;
    common::ensure_bucket(&s3).await;

    // Seed two "old" files + one "merged" replacement. The reactor
    // needs the merged file's ETag, so we fetch it via head_object
    // after the put.
    let merged_key = "rewarm/it/merged.parquet";
    let merged_body = Bytes::from(vec![0xABu8; 8 * 1024]);
    common::put_object(&s3, merged_key, merged_body.clone()).await;
    let head = s3
        .head_object()
        .bucket(common::TEST_BUCKET)
        .key(merged_key)
        .send()
        .await
        .expect("head merged");
    let merged_etag = head
        .e_tag()
        .expect("etag must come back from MinIO")
        .as_bytes()
        .to_vec();
    let merged_size = merged_body.len() as u64;

    // Build a temporary FoyerStore (DRAM-only is enough — the
    // reactor's content-addressed key is the same shape on hybrid
    // pools, and the integration test only validates the wiring).
    let pools = PoolsConfig {
        metadata: MetadataPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
        },
        rowgroup: RowGroupPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
            nvme_dir: PathBuf::from("/tmp/shelf45-it-unused"),
            nvme_bytes: 0,
            eviction_policy: shelfd::config::EvictionPolicy::default(),
            disk_cache: shelfd::config::RowGroupDiskCacheConfig::default(),
            compression: Default::default(),
        },
    };
    let store = Arc::new(FoyerStore::open(&pools).await.expect("open store"));
    let admission = Arc::new(SizeThresholdPolicy::from_config(&AdmissionConfig {
        size_threshold_bytes: 64 * 1024 * 1024,
        pinned_bypass: true,
    }));
    let fetcher: Arc<dyn RewarmFetcher> = Arc::new(S3Fetcher {
        client: s3.clone(),
        bucket: common::TEST_BUCKET.to_owned(),
    });

    let cfg = RewarmConfig {
        enabled: true,
        max_bytes_per_sec: 32 * 1024 * 1024,
        max_concurrent_files: 4,
        queue_capacity: 8,
        snapshot_lag_tolerance: Duration::from_secs(60),
        byte_equality_tolerance_bps: 500,
        // A3 (rc.7) — fields shared with the metadata-json
        // poller; left at their library defaults for the
        // reactor-only integration test.
        poll_interval: Duration::from_secs(30),
        tables: Vec::new(),
        max_bytes_per_snapshot: 5 * 1024 * 1024 * 1024,
    };
    let reactor = CompactionReactor::new(cfg, store.clone(), fetcher, admission);
    let cancel = CancellationToken::new();
    let (tx, handle) = reactor.spawn(cancel.clone());

    // Compaction-class diff: 4 → 1 with byte equality (fake old
    // ETags so the predicate's structural check passes).
    let removed: Vec<FileSpec> = (0..4u8)
        .map(|i| FileSpec {
            path: format!("rewarm/it/old-{i}.parquet"),
            etag: vec![i, b'a', b'b', b'c'],
            size_bytes: merged_size / 4,
        })
        .collect();
    let added = vec![FileSpec {
        path: merged_key.to_owned(),
        etag: merged_etag.clone(),
        size_bytes: merged_size,
    }];

    tx.send(IcebergSnapshotEvent {
        table_id: "shelf.it.compaction".into(),
        old_snapshot_id: 1,
        new_snapshot_id: 2,
        added_files: added,
        removed_files: removed,
        committed_at: SystemTime::now(),
    })
    .await
    .expect("publish event");
    drop(tx);

    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    // Assert the merged file's content-addressed key is now
    // resident in the rowgroup pool.
    let key = key_from_tuple(&merged_etag, 0, merged_size, 0).expect("key derive");
    let resident = store.contains(Pool::RowGroup, &key).await;
    assert!(
        resident,
        "merged file's key must be resident after compaction re-warm"
    );
}
