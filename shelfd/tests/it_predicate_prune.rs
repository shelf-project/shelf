// Licensed under the Apache License, Version 2.0.
// See <http://www.apache.org/licenses/LICENSE-2.0>.

//! SHELF-34 — integration tests for the `/predicate-prune` page-index
//! sidecar.
//!
//! Two test classes:
//!
//! 1. **No-MinIO tests** (always run): exercise the validator + 4xx
//!    error paths that short-circuit before any origin traffic.
//!    These guard the security envelope (allowlist, predicate
//!    shape) and verify the metric labels register correctly.
//!
//! 2. **MinIO-gated tests** (`SHELF_INTEGRATION=1`): seed a real
//!    Parquet file into MinIO, hit `/predicate-prune` against it,
//!    and assert the response is structurally well-formed
//!    (`pages` array of `[offset, length]` pairs, page-level
//!    min/max NEVER exposed). Pre-flight:
//!
//!    ```
//!    cd shelfd/tests && docker compose up -d minio
//!    SHELF_INTEGRATION=1 cargo test -p shelfd --test it_predicate_prune
//!    ```
//!
//! Without `SHELF_INTEGRATION=1` the MinIO-gated tests print a
//! one-line skip notice and exit cleanly. **Never** silently pass
//! them — the test prompt warns that absent the env var, the suite
//! exits in 0.00 s pretending to pass; we explicitly mark them as
//! skipped.

#![cfg(test)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use shelfd::{
    admission::SizeThresholdPolicy,
    config::{AdmissionConfig, MetadataPoolConfig, OriginConfig, PoolsConfig, RowGroupPoolConfig},
    http::{self, ServerState},
    metrics,
    origin::S3Origin,
    router::Router,
    store::FoyerStore,
};
use tokio_util::sync::CancellationToken;

// --- MinIO-fixture constants (mirror it_read_path.rs verbatim).

const MINIO_ENDPOINT: &str = "http://127.0.0.1:9000";
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const TEST_BUCKET: &str = "shelf-it-predicate-prune";

fn integration_enabled() -> bool {
    std::env::var("SHELF_INTEGRATION").as_deref() == Ok("1")
}

fn skip_if_offline() -> bool {
    if !integration_enabled() {
        eprintln!(
            "SKIP: set SHELF_INTEGRATION=1 + run docker-compose up -d minio to enable this test"
        );
        return true;
    }
    false
}

fn test_pools_minimal() -> PoolsConfig {
    PoolsConfig {
        metadata: MetadataPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
        },
        rowgroup: RowGroupPoolConfig {
            dram_bytes: 4 * 1024 * 1024,
            nvme_dir: PathBuf::from("/tmp/it_predicate_prune_unused"),
            nvme_bytes: 0,
            eviction_policy: shelfd::config::EvictionPolicy::default(),
            disk_cache: Default::default(),
            compression: Default::default(),
        },
    }
}

/// Process-global metrics registry shared across every test in this
/// binary. `prometheus::Registry::register_*` rejects duplicates.
static METRICS: tokio::sync::OnceCell<Arc<metrics::Registry>> = tokio::sync::OnceCell::const_new();

async fn metrics_registry() -> Arc<metrics::Registry> {
    METRICS
        .get_or_init(|| async { Arc::new(metrics::Registry::init().expect("metrics init")) })
        .await
        .clone()
}

/// Build a test `ServerState` with a stub-ish `S3Origin` pointed at
/// a localhost-deadletter address. Tests that don't need to talk to
/// the origin (the validator-rejection cases) bind this state and
/// trust their request to short-circuit before any S3 traffic.
async fn build_offline_state(allowlist: Vec<String>) -> Arc<ServerState> {
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
    }
    let origin_cfg = OriginConfig {
        bucket: "unused".to_owned(),
        endpoint_url: Some("http://127.0.0.1:1".to_owned()),
        region: Some("us-east-1".to_owned()),
        max_inflight: 1,
    };
    let origin = Arc::new(S3Origin::new(&origin_cfg).await.expect("origin"));
    let store = Arc::new(
        FoyerStore::open(&test_pools_minimal())
            .await
            .expect("store"),
    );
    let router = Arc::new(Router::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&AdmissionConfig {
        size_threshold_bytes: 1 << 30,
        pinned_bypass: true,
    }));
    let metrics_reg = metrics_registry().await;
    let state = Arc::new(
        ServerState::new(store, origin, router, admission, metrics_reg)
            .with_predicate_allowlist(allowlist),
    );
    state.mark_ready();
    state
}

/// Build a `ServerState` whose origin reaches the local MinIO. Used
/// by the `SHELF_INTEGRATION=1`-gated tests.
async fn build_minio_state(allowlist: Vec<String>) -> Arc<ServerState> {
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY);
        std::env::set_var("AWS_REGION", "us-east-1");
    }
    let origin_cfg = OriginConfig {
        bucket: TEST_BUCKET.to_owned(),
        endpoint_url: Some(MINIO_ENDPOINT.to_owned()),
        region: Some("us-east-1".to_owned()),
        max_inflight: 32,
    };
    let origin = Arc::new(S3Origin::new(&origin_cfg).await.expect("origin"));
    let store = Arc::new(
        FoyerStore::open(&test_pools_minimal())
            .await
            .expect("store"),
    );
    let router = Arc::new(Router::new());
    let admission = Arc::new(SizeThresholdPolicy::from_config(&AdmissionConfig {
        size_threshold_bytes: 1 << 30,
        pinned_bypass: true,
    }));
    let metrics_reg = metrics_registry().await;
    let state = Arc::new(
        ServerState::new(store, origin, router, admission, metrics_reg)
            .with_predicate_allowlist(allowlist),
    );
    state.mark_ready();
    state
}

async fn spawn_server(state: Arc<ServerState>) -> (SocketAddr, CancellationToken) {
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

fn url(addr: &SocketAddr, path: &str) -> String {
    format!("http://{addr}{path}")
}

// ---- (1) No-MinIO tests — security envelope coverage. -------------

/// The OSS default is an empty allowlist. Every request to
/// `/predicate-prune` must therefore 400 with `invalid_path` —
/// regardless of the path supplied. This is the SHELF-34 security
/// review item §1 ("Path-traversal containment") in action.
#[tokio::test]
async fn empty_allowlist_rejects_every_path() {
    let state = build_offline_state(Vec::new()).await;
    let (addr, cancel) = spawn_server(state).await;

    let resp = reqwest::get(url(
        &addr,
        "/predicate-prune?path=s3a://anywhere/foo.parquet&col=id&min=0&max=100",
    ))
    .await
    .expect("GET");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "invalid_path");

    cancel.cancel();
}

/// A path that does not match any allowlisted bucket is rejected
/// at the validator. Verifies that the allowlist is enforced
/// even when populated.
#[tokio::test]
async fn unallowed_bucket_rejected() {
    let state = build_offline_state(vec!["allowed-bucket".to_owned()]).await;
    let (addr, cancel) = spawn_server(state).await;

    let resp = reqwest::get(url(
        &addr,
        "/predicate-prune?path=s3a://other-bucket/file.parquet&col=id&min=0&max=10",
    ))
    .await
    .expect("GET");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "invalid_path");

    cancel.cancel();
}

/// A path with `..` traversal is rejected even if the bucket is
/// allowlisted. Security review §1.
#[tokio::test]
async fn path_traversal_rejected() {
    let state = build_offline_state(vec!["allowed-bucket".to_owned()]).await;
    let (addr, cancel) = spawn_server(state).await;

    let resp = reqwest::get(url(
        &addr,
        "/predicate-prune?path=s3a://allowed-bucket/foo/../etc/passwd&col=id&min=0&max=10",
    ))
    .await
    .expect("GET");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "invalid_path");

    cancel.cancel();
}

/// Supplying both `min` AND `eq`, or only one of (min, max) without
/// the other, must 400 — the wire shape is strict v1 to keep the
/// scan-everything degenerate case from leaking through.
#[tokio::test]
async fn ambiguous_predicate_shape_rejected() {
    let state = build_offline_state(vec!["allowed-bucket".to_owned()]).await;
    let (addr, cancel) = spawn_server(state).await;

    // Only min, no max — invalid.
    let resp = reqwest::get(url(
        &addr,
        "/predicate-prune?path=s3a://allowed-bucket/x.parquet&col=id&min=0",
    ))
    .await
    .expect("GET");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "invalid_predicate");

    // Both min/max AND eq — invalid.
    let resp = reqwest::get(url(
        &addr,
        "/predicate-prune?path=s3a://allowed-bucket/x.parquet&col=id&min=0&max=10&eq=5",
    ))
    .await
    .expect("GET");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["error"], "invalid_predicate");

    cancel.cancel();
}

// ---- (2) MinIO-gated end-to-end test ------------------------------

/// Build a tiny INT64 Parquet file in memory using the parquet
/// crate's writer. Mirrors the unit-test fixture so this binary
/// pulls only well-known, vetted Parquet output through the
/// network path.
fn build_test_parquet_bytes() -> Bytes {
    use parquet::basic::Repetition;
    use parquet::basic::Type as PhysicalType;
    use parquet::data_type::Int64Type;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;

    let id_field = Type::primitive_type_builder("id", PhysicalType::INT64)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .expect("id field");
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![Arc::new(id_field)])
            .build()
            .expect("schema"),
    );
    let props = Arc::new(
        WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(2)
            .set_write_batch_size(2)
            .build(),
    );

    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer = SerializedFileWriter::new(&mut buffer, schema, props).expect("writer");
        for rg_offset in [0i64, 100i64] {
            let mut rg_writer = writer.next_row_group().expect("rg writer");
            let mut col_writer = rg_writer
                .next_column()
                .expect("col writer")
                .expect("non-empty");
            let values: Vec<i64> = (0..4).map(|i| rg_offset + i).collect();
            col_writer
                .typed::<Int64Type>()
                .write_batch(&values, None, None)
                .expect("write_batch");
            col_writer.close().expect("col close");
            rg_writer.close().expect("rg close");
        }
        writer.close().expect("file close");
    }
    Bytes::from(buffer)
}

/// SHELF-34 end-to-end: seed MinIO with a real Parquet file, hit
/// `/predicate-prune`, and verify (a) HTTP 200, (b) `pages` is a
/// JSON array of `[offset, length]` pairs, (c) the response carries
/// only structural mappings — no `min` / `max` fields surface
/// page-level byte values (PII containment, security review §4).
#[tokio::test]
async fn end_to_end_predicate_prune_against_minio() {
    if skip_if_offline() {
        return;
    }
    use aws_sdk_s3::{
        config::{Builder as S3ConfigBuilder, Credentials, Region},
        primitives::ByteStream,
        Client as S3Client,
    };

    // Seed the bucket + the Parquet object.
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .load()
        .await;
    let s3_conf = S3ConfigBuilder::from(&shared)
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
    let client = S3Client::from_conf(s3_conf);
    let _ = client.create_bucket().bucket(TEST_BUCKET).send().await;
    let key = "shelf34/round-trip.parquet";
    let body = build_test_parquet_bytes();
    client
        .put_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .expect("put_object");

    // Spin up shelfd with the test bucket on the allowlist.
    let state = build_minio_state(vec![TEST_BUCKET.to_owned()]).await;
    let (addr, cancel) = spawn_server(state).await;

    let request_url = format!(
        "http://{addr}/predicate-prune?path=s3a://{TEST_BUCKET}/{key}&col=id&min=50&max=200"
    );
    let resp = reqwest::get(&request_url).await.expect("GET");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "expected 200 on a real parquet, got {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    let body: serde_json::Value = resp.json().await.expect("json");
    // Wire contract assertions.
    assert_eq!(body["column"], "id");
    let pages = body["pages"].as_array().expect("pages array");
    // Predicate `[50, 200]` keeps at least one page from the second
    // row group.
    assert!(!pages.is_empty(), "predicate must keep ≥1 page");
    for entry in pages {
        let pair = entry.as_array().expect("entry is [offset, length]");
        assert_eq!(pair.len(), 2);
        assert!(pair[0].as_u64().is_some(), "offset must be u64");
        assert!(pair[1].as_u64().unwrap_or_default() > 0, "length > 0");
    }
    // PII containment — security review §4. The handler must NEVER
    // include the page-level `min` / `max` byte values in the
    // response, only structural offsets/lengths. We assert by JSON
    // key on top-level + each entry shape.
    let obj = body.as_object().expect("body is an object");
    for forbidden in ["min", "max", "readable_metrics", "stats"] {
        assert!(
            !obj.contains_key(forbidden),
            "response must NOT carry `{forbidden}` (PII containment)"
        );
    }

    // Second hit on the same object should be a cache `hit`. The
    // metric counter is process-global; we cannot assert exact
    // counts from a parallel test binary, but we can assert the
    // second response is also 200 and structurally identical.
    let resp2 = reqwest::get(&request_url).await.expect("GET 2");
    assert_eq!(resp2.status(), reqwest::StatusCode::OK);
    let body2: serde_json::Value = resp2.json().await.expect("json 2");
    assert_eq!(body2["pages"], body["pages"]);

    cancel.cancel();
}
