//! Integration tests for the SHELF-22 S3-compat read shim.
//!
//! Gated on `SHELF_INTEGRATION=1`; see `tests/docker-compose.yml` for
//! the MinIO spin-up. The tests use the **same** helpers as
//! `it_head_stats.rs` / `it_read_path.rs` so the MinIO seed path is
//! shared.
//!
//! Scope (SHELF-22 acceptance):
//! - HEAD parity headers (`Content-Length`, `ETag`, `Last-Modified`,
//!   `Accept-Ranges`, `x-amz-request-id`).
//! - Ranged GET → 206 + `Content-Range: bytes X-Y/Z`.
//! - Unranged GET on a small object → 200 full body.
//! - Unranged GET on a "huge" object (cap forced tiny) → 501.
//! - Missing key → 404 with `<Code>NoSuchKey</Code>`.

#![cfg(test)]

mod common;

use bytes::Bytes;
use common::{
    build_state_with_pod_id, build_state_with_shim_cap, ensure_bucket, put_object, s3_client,
    skip_if_offline, spawn_server_with_shim, TEST_BUCKET,
};

#[tokio::test]
async fn head_object_returns_s3_parity_headers() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "shim-head-parity";
    let payload = Bytes::from(vec![0xAB; 2048]);
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_pod_id("shelf-shim-head").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let http = reqwest::Client::new();
    let resp = http.head(&url).send().await.expect("head");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let cl = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .expect("Content-Length")
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert_eq!(cl, payload.len());

    // ETag may be quoted; we only assert presence + non-empty.
    let etag = resp
        .headers()
        .get(reqwest::header::ETAG)
        .expect("ETag")
        .to_str()
        .unwrap();
    assert!(!etag.is_empty(), "ETag must not be empty: {etag:?}");

    assert!(
        resp.headers().get(reqwest::header::LAST_MODIFIED).is_some(),
        "Last-Modified must be present"
    );
    assert_eq!(
        resp.headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .unwrap()
            .to_str()
            .unwrap(),
        "bytes",
    );
    let rid = resp
        .headers()
        .get("x-amz-request-id")
        .expect("x-amz-request-id")
        .to_str()
        .unwrap();
    assert_eq!(
        rid.len(),
        16,
        "request-id must be 16 hex chars, got {rid:?}"
    );

    cancel.cancel();
}

#[tokio::test]
async fn get_object_with_range_serves_bytes() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "shim-range-get";
    // Distinct per-byte pattern so we can verify the slice exactly.
    let payload: Bytes = (0u8..=255).cycle().take(1024).collect::<Vec<u8>>().into();
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_pod_id("shelf-shim-range").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let http = reqwest::Client::new();
    let resp = http
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=16-31")
        .send()
        .await
        .expect("ranged get");
    assert_eq!(resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let cr = resp
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .expect("Content-Range")
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(cr, format!("bytes 16-31/{}", payload.len()));
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 16);
    assert_eq!(&body[..], &payload[16..32]);

    cancel.cancel();
}

#[tokio::test]
async fn get_object_without_range_returns_full_object() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "shim-full-get";
    let payload = Bytes::from(vec![0x5A; 4 * 1024]);
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_pod_id("shelf-shim-full").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let http = reqwest::Client::new();
    let resp = http.get(&url).send().await.expect("full get");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body, payload);

    cancel.cancel();
}

#[tokio::test]
async fn get_object_rejects_huge_unbounded_with_501() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "shim-oversized";
    // Object is 8 KiB but the shim cap is forced to 1 KiB so we
    // exercise the 501 path without a GiB-scale fixture.
    let payload = Bytes::from(vec![0x77; 8 * 1024]);
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_shim_cap("shelf-shim-oversized", 1024).await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let http = reqwest::Client::new();
    let resp = http.get(&url).send().await.expect("oversized get");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<Code>NotImplemented</Code>"),
        "body must carry the S3 NotImplemented code: {body}"
    );

    cancel.cancel();
}

#[tokio::test]
async fn get_object_suffix_range_serves_tail_bytes() {
    // The shim must honour `bytes=-N` (RFC 9110 §14.1.2), because
    // Trino's `io.trino.filesystem.s3.S3Input.readTail(n)` — the
    // reader that loads Parquet/Avro footers — issues exactly that
    // shape. The original parser treated suffix ranges as malformed
    // and returned 416, breaking every Iceberg query that hit the
    // shim. See SHELF-22 SPI wiring.
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "shim-suffix-range";
    let payload: Bytes = (0u8..=255).cycle().take(1024).collect::<Vec<u8>>().into();
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_pod_id("shelf-shim-suffix").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let http = reqwest::Client::new();
    let resp = http
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=-8")
        .send()
        .await
        .expect("suffix get");
    assert_eq!(resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let cr = resp
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .expect("Content-Range")
        .to_str()
        .unwrap()
        .to_owned();
    // Last 8 bytes of a 1024-byte object: offset 1016..=1023.
    assert_eq!(cr, format!("bytes 1016-1023/{}", payload.len()));
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.len(), 8);
    assert_eq!(&body[..], &payload[1016..1024]);

    cancel.cancel();
}

#[tokio::test]
async fn shim_read_bumps_hits_and_misses_counters() {
    // Parity with the native `/cache` data plane: warm reads through
    // the shim must increment `shelf_hits_total{pool=...}` so the
    // smoke harness and Grafana dashboards actually see traffic once
    // a catalog's `s3.endpoint` is swapped to shelfd. Without this,
    // the `run-smoke.sh` hit-ratio gate stays pinned at 0 — which is
    // exactly the regression this test is here to prevent.
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "shim-hit-miss-accounting.parquet"; // routes to rowgroup pool
    let payload = Bytes::from(vec![0x42; 4 * 1024]);
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_pod_id("shelf-shim-hitmiss").await;
    let metrics = state.metrics.clone();
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let http = reqwest::Client::new();

    // Prometheus counters live on a process-wide registry, so absolute
    // values leak across tests in a single `cargo test` run. Pin
    // baselines here and assert on deltas instead.
    let hits_base = metrics.hits_total.with_label_values(&["rowgroup"]).get();
    let misses_base = metrics.misses_total.with_label_values(&["rowgroup"]).get();

    // Cold read → miss.
    let r = http
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-4095")
        .send()
        .await
        .expect("cold");
    assert_eq!(r.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(r.bytes().await.unwrap().len(), 4096);
    let misses_cold = metrics.misses_total.with_label_values(&["rowgroup"]).get();
    let hits_cold = metrics.hits_total.with_label_values(&["rowgroup"]).get();
    assert_eq!(
        misses_cold - misses_base,
        1,
        "cold read must be counted as a miss"
    );
    assert_eq!(hits_cold - hits_base, 0, "cold read must not bump hits");

    // Warm read (same content-addressed key) → hit.
    let r = http
        .get(&url)
        .header(reqwest::header::RANGE, "bytes=0-4095")
        .send()
        .await
        .expect("warm");
    assert_eq!(r.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let hits_warm = metrics.hits_total.with_label_values(&["rowgroup"]).get();
    let misses_warm = metrics.misses_total.with_label_values(&["rowgroup"]).get();
    assert_eq!(hits_warm - hits_base, 1, "warm read must bump hits");
    assert_eq!(
        misses_warm, misses_cold,
        "warm read must not bump misses again"
    );

    cancel.cancel();
}

#[tokio::test]
async fn get_object_on_missing_key_returns_404_xml() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;

    let state = build_state_with_pod_id("shelf-shim-404").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/this-key-does-not-exist");
    let http = reqwest::Client::new();
    let resp = http.get(&url).send().await.expect("missing get");
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("<Code>NoSuchKey</Code>"),
        "body must carry the S3 NoSuchKey code: {body}"
    );

    cancel.cancel();
}
