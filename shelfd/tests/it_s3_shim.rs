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
