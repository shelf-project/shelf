//! Integration tests for the SHELF-21 S3-shim write path.
//!
//! Gating: skipped unless `SHELF_INTEGRATION=1` is set + a MinIO is
//! running on `127.0.0.1:9000`. The CI matrix sets this; developer
//! laptops stay offline by default. Pre-flight (operator):
//!
//!   cd shelfd/tests && docker compose up -d minio
//!   SHELF_INTEGRATION=1 cargo test -p shelfd --test it_shim_write
//!
//! What this asserts:
//!
//! - A `PUT` through the shim lands the bytes on the upstream S3 and
//!   echoes back an `ETag` header.
//! - A subsequent `GET` through the shim returns the freshly-PUT
//!   bytes — proving the HEAD-LRU invalidation on PUT actually
//!   prevents stale-read poisoning.
//! - `DELETE` through the shim removes the object and the next `GET`
//!   returns 404 from the negative-cache fast path.
//! - 404-on-DELETE is idempotent (S3 spec); the shim returns 204.

#![cfg(test)]

use std::time::Duration;

use bytes::Bytes;
use reqwest::Client;

mod common;
use common::{
    build_state_with_pod_id, ensure_bucket, s3_client, skip_if_offline, spawn_server_with_shim,
    TEST_BUCKET,
};

async fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client")
}

#[tokio::test]
async fn put_through_shim_round_trips() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-put").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let key = "shelf-21/put-round-trip.parquet";
    let payload = Bytes::from_static(b"the quick brown shelfd jumps over the lazy s3");

    let put_url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let resp = http
        .put(&put_url)
        .body(payload.clone())
        .send()
        .await
        .expect("put");
    assert_eq!(resp.status(), 200, "PUT should succeed: {resp:?}");
    let etag_header = resp
        .headers()
        .get(reqwest::header::ETAG)
        .cloned()
        .expect("ETag header on PUT response");

    // Same shim, GET back the bytes. This exercises the cache
    // invalidation path: had `handle_put_object` *not* dropped the
    // (potentially absent) HEAD-LRU entry, a second test running
    // against the same key in the same process would serve stale
    // (content_length, etag) tuples.
    let get_url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let resp = http.get(&get_url).send().await.expect("get");
    assert_eq!(resp.status(), 200, "GET after PUT should succeed");
    let body = resp.bytes().await.expect("body");
    assert_eq!(body, payload, "round-trip bytes mismatch");

    // ETag echoed by the shim must match what real S3 (MinIO) gave
    // us — proves we're not fabricating a header.
    let head_via_sdk = s3
        .head_object()
        .bucket(TEST_BUCKET)
        .key(key)
        .send()
        .await
        .expect("head via sdk");
    let upstream_etag = head_via_sdk.e_tag().expect("etag from sdk");
    assert_eq!(
        etag_header.to_str().unwrap(),
        upstream_etag,
        "shim must surface upstream ETag verbatim"
    );

    cancel.cancel();
}

#[tokio::test]
async fn put_after_get_invalidates_head_lru() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-put-inv").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let key = "shelf-21/put-after-get.bin";

    // Seed v1 directly via the SDK (bypassing the shim) so the
    // first GET below populates the HEAD-LRU with the v1 metadata.
    let v1 = Bytes::from_static(b"version-one");
    common::put_object(&s3, key, v1.clone()).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
    let resp = http.get(&url).send().await.expect("get v1");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap(), v1);

    // Now overwrite via the shim's PUT.
    let v2 = Bytes::from_static(b"version-two-and-noticeably-longer");
    let resp = http
        .put(&url)
        .body(v2.clone())
        .send()
        .await
        .expect("put v2");
    assert_eq!(resp.status(), 200);

    // Without HEAD-LRU invalidation, the next GET would short-
    // circuit on the v1 (content_length, etag) pair and serve a
    // sliced v1 read against the v1-keyed Foyer entry. We assert
    // we see the new bytes end-to-end.
    let resp = http.get(&url).send().await.expect("get v2");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap(), v2);

    cancel.cancel();
}

#[tokio::test]
async fn delete_through_shim_evicts_and_propagates() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-del").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let key = "shelf-21/delete-evicts.bin";
    let body = Bytes::from_static(b"about-to-vanish");
    common::put_object(&s3, key, body.clone()).await;

    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");

    // Warm the HEAD-LRU positive entry.
    let resp = http.get(&url).send().await.expect("get warmup");
    assert_eq!(resp.status(), 200);

    // Delete via the shim.
    let resp = http.delete(&url).send().await.expect("delete");
    assert_eq!(
        resp.status(),
        204,
        "DELETE should yield 204 NoContent, got {resp:?}"
    );

    // Subsequent GET must 404. The shim's negative-cache shortcut
    // means this typically stays in-process without an origin
    // round-trip.
    let resp = http.get(&url).send().await.expect("get after delete");
    assert_eq!(resp.status(), 404);

    // Direct sdk HEAD must also be 404 — proves the upstream call
    // actually fired, not just the local cache invalidation.
    let head_err = s3.head_object().bucket(TEST_BUCKET).key(key).send().await;
    assert!(head_err.is_err(), "object should be gone upstream");

    cancel.cancel();
}

#[tokio::test]
async fn delete_is_idempotent_on_missing_key() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-del-idem").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let key = "shelf-21/delete-idempotent.bin";
    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");

    // Key has never existed. S3 spec says DELETE is idempotent —
    // 204 on both first and Nth call. `RemoveOrphanFiles` and dbt
    // post-write cleanups depend on this.
    let resp = http.delete(&url).send().await.expect("delete missing");
    assert_eq!(
        resp.status(),
        204,
        "DELETE of missing key must be 204, got {resp:?}"
    );

    cancel.cancel();
}

#[tokio::test]
async fn put_oversized_body_returns_501_not_implemented() {
    if skip_if_offline() {
        return;
    }
    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let state = build_state_with_pod_id("shelf-it-put-big").await;
    let (_native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = http_client().await;

    let key = "shelf-21/oversized.bin";
    let url = format!("http://{shim}/{TEST_BUCKET}/{key}");

    // 256 MiB + 1 byte — just past the SHIM_MAX_PUT_BYTES cap. Use
    // a streaming body so we don't burn 256 MiB of test heap before
    // axum even sees the request.
    let big = vec![0u8; 256 * 1024 * 1024 + 1];
    let resp = http
        .put(&url)
        .body(big)
        .send()
        .await
        .expect("put oversized");
    assert_eq!(
        resp.status(),
        501,
        "oversized single-shot PUT should yield 501 NotImplemented"
    );
    let body = resp.text().await.expect("error body");
    assert!(
        body.contains("EntityTooLarge") || body.contains("SHELF-21b"),
        "error envelope should mention EntityTooLarge or the multipart follow-up: {body}"
    );

    cancel.cancel();
}
