//! Shared helpers for `shelfd` integration tests.
//!
//! Every consumer of this module must be gated on `SHELF_INTEGRATION=1`
//! because the helpers expect a running MinIO at `127.0.0.1:9000`. See
//! `shelfd/tests/docker-compose.yml` for the spin-up command.
//!
//! Kept deliberately small: anything more specialised belongs in the
//! test file that needs it.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use shelfd::{
    admission::SizeThresholdPolicy,
    config::{AdmissionConfig, MetadataPoolConfig, OriginConfig, PoolsConfig, RowGroupPoolConfig},
    head_lru::HeadLru,
    http::{self, ServerState},
    metrics,
    origin::S3Origin,
    router::Router,
    store::FoyerStore,
};
use tokio_util::sync::CancellationToken;

pub const MINIO_ENDPOINT: &str = "http://127.0.0.1:9000";
pub const MINIO_ACCESS_KEY: &str = "minioadmin";
pub const MINIO_SECRET_KEY: &str = "minioadmin";
pub const TEST_BUCKET: &str = "shelf-it";

pub fn skip_if_offline() -> bool {
    if std::env::var("SHELF_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("SKIP: set SHELF_INTEGRATION=1 + run docker-compose to enable");
        return true;
    }
    false
}

pub async fn s3_client() -> S3Client {
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .load()
        .await;
    let conf = S3ConfigBuilder::from(&shared)
        .endpoint_url(MINIO_ENDPOINT)
        .force_path_style(true)
        .credentials_provider(Credentials::new(
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
            None,
            None,
            "it-static",
        ))
        .build();
    S3Client::from_conf(conf)
}

pub async fn ensure_bucket(client: &S3Client) {
    let _ = client.create_bucket().bucket(TEST_BUCKET).send().await;
}

pub async fn put_object(client: &S3Client, key: &str, body: Bytes) {
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await
        .expect("put_object");
}

pub async fn delete_object(client: &S3Client, key: &str) {
    client
        .delete_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .send()
        .await
        .expect("delete");
}

pub fn test_config() -> (OriginConfig, PoolsConfig, AdmissionConfig) {
    let origin = OriginConfig {
        bucket: TEST_BUCKET.to_owned(),
        endpoint_url: Some(MINIO_ENDPOINT.to_owned()),
        region: Some("us-east-1".to_owned()),
        max_inflight: 256,
    };
    let pools = PoolsConfig {
        metadata: MetadataPoolConfig {
            dram_bytes: 16 * 1024 * 1024,
        },
        rowgroup: RowGroupPoolConfig {
            dram_bytes: 16 * 1024 * 1024,
            nvme_dir: PathBuf::from("/tmp/it_unused"),
            nvme_bytes: 0,
            eviction_policy: shelfd::config::EvictionPolicy::default(),
            disk_cache: Default::default(),
        },
    };
    let admission = AdmissionConfig {
        size_threshold_bytes: 8 * 1024 * 1024,
        pinned_bypass: true,
    };
    (origin, pools, admission)
}

/// Process-global metrics registry shared across every test in a
/// single test binary. `prometheus::Registry::register_*` rejects
/// duplicates, so building it twice in one process panics.
static METRICS: tokio::sync::OnceCell<Arc<metrics::Registry>> = tokio::sync::OnceCell::const_new();

pub async fn build_state_with_pod_id(pod_id: &str) -> Arc<ServerState> {
    // SAFETY: tests share process-global env; we write the same
    // MinIO credentials on every call, so the writes are idempotent
    // under `--test-threads=N`.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY);
        std::env::set_var("AWS_REGION", "us-east-1");
    }
    let (origin_cfg, pools_cfg, admission_cfg) = test_config();
    let origin = Arc::new(S3Origin::new(&origin_cfg).await.expect("origin"));
    let store = Arc::new(FoyerStore::open(&pools_cfg).await.expect("store"));
    let router = Arc::new(Router::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&admission_cfg));
    let head_lru = Arc::new(HeadLru::new(10_000));
    let metrics_reg = METRICS
        .get_or_init(|| async { Arc::new(metrics::Registry::init().expect("metrics init")) })
        .await
        .clone();
    let state = Arc::new(ServerState::with_head_lru_and_pod_id(
        store,
        origin,
        router,
        admission,
        metrics_reg,
        head_lru,
        pod_id.to_owned(),
    ));
    state.mark_ready();
    state
}

pub async fn spawn_server(state: Arc<ServerState>) -> (SocketAddr, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let app = http::build_router(state);
    let cancel_for_serve = cancel.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel_for_serve.cancelled().await })
            .await
            .expect("axum serve");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, cancel)
}

/// Spawn the native router **and** the SHELF-22 S3-compat shim on
/// two independent ephemeral ports. Returns `(native_addr, shim_addr,
/// cancel)`; cancel drops both listeners.
pub async fn spawn_server_with_shim(
    state: std::sync::Arc<ServerState>,
) -> (SocketAddr, SocketAddr, CancellationToken) {
    let native_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind native");
    let shim_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind shim");
    let native_addr = native_listener.local_addr().unwrap();
    let shim_addr = shim_listener.local_addr().unwrap();

    let cancel = CancellationToken::new();

    let native_app = http::build_router(state.clone());
    let shim_app = http::build_s3_shim_router(state.clone());

    let cancel_native = cancel.clone();
    tokio::spawn(async move {
        axum::serve(native_listener, native_app)
            .with_graceful_shutdown(async move { cancel_native.cancelled().await })
            .await
            .expect("axum native");
    });
    let cancel_shim = cancel.clone();
    tokio::spawn(async move {
        axum::serve(shim_listener, shim_app)
            .with_graceful_shutdown(async move { cancel_shim.cancelled().await })
            .await
            .expect("axum shim");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (native_addr, shim_addr, cancel)
}

/// Build a full `ServerState` and override the SHELF-22 unbounded-GET
/// cap in one call. Tests use this to force the 501 path without
/// allocating a GiB-scale fixture.
pub async fn build_state_with_shim_cap(pod_id: &str, cap: u64) -> std::sync::Arc<ServerState> {
    let state = build_state_with_pod_id(pod_id).await;
    state
        .s3_shim_max_full_object_bytes
        .store(cap, std::sync::atomic::Ordering::Relaxed);
    state
}
