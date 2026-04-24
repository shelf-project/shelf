//! End-to-end integration tests for HEAD + `/stats`.
//!
//! Ticket ownership:
//! - SHELF-07 — `HEAD /cache/:pool/origin/:bucket/*s3_key` with the
//!   10 000-entry HEAD-LRU.
//! - SHELF-20 (shelfd half) — the `/stats` JSON contract Agent 5's
//!   plugin membership loader consumes.
//!
//! Gated on `SHELF_INTEGRATION=1`; see `tests/docker-compose.yml`.

#![cfg(test)]

mod common;

use bytes::Bytes;
use common::{
    build_state_with_pod_id, delete_object, ensure_bucket, put_object, s3_client, skip_if_offline,
    spawn_server, TEST_BUCKET,
};

#[tokio::test]
async fn head_returns_content_length_matching_object_size() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "head-size";
    let payload = Bytes::from(vec![0xCD; 4096]);
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_pod_id("shelf-head-1").await;
    let (addr, cancel) = spawn_server(state).await;

    let url = format!(
        "http://{addr}/cache/metadata/origin/{bucket}/{key}",
        bucket = TEST_BUCKET
    );
    let http = reqwest::Client::new();
    let resp = http.head(&url).send().await.expect("head");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cl = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .expect("Content-Length present")
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert_eq!(cl, payload.len(), "Content-Length must match the object");

    cancel.cancel();
}

/// The core SHELF-07 acceptance: a second HEAD after the origin
/// object has been deleted must still return 200 from the LRU. If
/// the LRU were absent, the second HEAD would 404.
#[tokio::test]
async fn second_head_hits_lru_after_origin_delete() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "head-lru-survives-delete";
    let payload = Bytes::from(vec![0x13; 1_234]);
    put_object(&client, key, payload.clone()).await;

    let state = build_state_with_pod_id("shelf-head-2").await;
    let (addr, cancel) = spawn_server(state).await;
    let url = format!(
        "http://{addr}/cache/metadata/origin/{bucket}/{key}",
        bucket = TEST_BUCKET
    );
    let http = reqwest::Client::new();

    // Cold HEAD populates the LRU.
    let resp = http.head(&url).send().await.expect("cold head");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let cl_cold: usize = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(cl_cold, payload.len());

    // Delete on the origin; without the LRU, the next HEAD would
    // hit S3 and 404.
    delete_object(&client, key).await;

    let resp = http.head(&url).send().await.expect("warm head");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "warm HEAD must be served from the LRU even after origin delete"
    );
    let cl_warm: usize = resp
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(cl_warm, payload.len());

    cancel.cancel();
}

#[tokio::test]
async fn head_on_missing_object_returns_404() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;

    let state = build_state_with_pod_id("shelf-head-3").await;
    let (addr, cancel) = spawn_server(state).await;
    let url = format!(
        "http://{addr}/cache/metadata/origin/{bucket}/does-not-exist",
        bucket = TEST_BUCKET
    );
    let http = reqwest::Client::new();
    let resp = http.head(&url).send().await.expect("head");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "S3 NoSuchKey must surface as 404, not 502"
    );
    cancel.cancel();
}

/// SHELF-20 contract: `/stats` must expose a `pod_id` matching what
/// the operator configured and must reflect actual pool usage once
/// the cache is populated.
#[tokio::test]
async fn stats_reflects_pool_usage_after_cache_populate() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let pod_id = "shelf-stats-demo";
    let state = build_state_with_pod_id(pod_id).await;
    let (addr, cancel) = spawn_server(state).await;
    let http = reqwest::Client::new();

    // Baseline stats before any GET.
    let resp = http
        .get(format!("http://{addr}/stats"))
        .send()
        .await
        .expect("stats");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        "application/json; charset=utf-8",
    );
    let before: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(before["pod_id"], serde_json::json!(pod_id));
    for key in [
        "capacity_bytes",
        "used_bytes",
        "metadata_pool",
        "rowgroup_pool",
    ] {
        assert!(before.get(key).is_some(), "/stats missing {key}");
    }
    let used_before = before["used_bytes"].as_u64().unwrap();

    // Populate the rowgroup pool via GET. key_hex == s3_key per the
    // phase-0 shortcut used by it_read_path.
    let key_hex: String = "a".repeat(64);
    let payload = Bytes::from(vec![0x77; 32 * 1024]);
    put_object(&client, &key_hex, payload.clone()).await;
    let url = format!(
        "http://{addr}/cache/rowgroup/{key_hex}/0-{}",
        payload.len() - 1
    );
    let resp = http.get(&url).send().await.expect("get");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.bytes().await.unwrap();
    assert_eq!(body, payload);

    // Stats after cache populate.
    let resp = http
        .get(format!("http://{addr}/stats"))
        .send()
        .await
        .expect("stats2");
    let after: serde_json::Value = resp.json().await.expect("json");
    let used_after = after["used_bytes"].as_u64().unwrap();
    let rg_used_after = after["rowgroup_pool"]["used_bytes"].as_u64().unwrap();
    assert!(
        used_after > used_before,
        "top-level used_bytes must grow after a cache populate: {used_before} -> {used_after}"
    );
    assert!(
        rg_used_after >= payload.len() as u64,
        "rowgroup pool used_bytes must reflect at least the inserted payload: \
         {rg_used_after} < {}",
        payload.len(),
    );

    cancel.cancel();
}
