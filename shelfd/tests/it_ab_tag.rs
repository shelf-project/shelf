//! SHELF-42 — integration tests for the A/B query-tagging receive
//! path on the S3 shim.
//!
//! Gated on `--features integration`; reuses the same MinIO fixture as
//! `it_s3_shim.rs`. The contract under test:
//!
//! - shelfd parses `X-Shelf-Tag` from incoming GETs;
//! - per-tag counters (`shelf_hits_by_tag_total`,
//!   `shelf_misses_by_tag_total`,
//!   `shelf_s3_shim_response_bytes_by_tag_total`) split observations
//!   across distinct tag wire-forms;
//! - requests with no header land on the `tag="none"` series;
//! - cap-violations bump `shelf_ab_tag_cap_violations_total` exactly
//!   once per offending tag per scrape window.
//!
//! The metric assertions here are coarse — we count *deltas* against a
//! pre-test snapshot, because `/metrics` is process-global and other
//! integration tests may have warmed counters first.

#![cfg(test)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{
    build_state_with_pod_id, ensure_bucket, put_object, s3_client, skip_if_offline,
    spawn_server_with_shim, TEST_BUCKET,
};
use shelfd::ab_tag::AbTagState;

const TAG_A: &str = "%7B%22experiment%22%3A%22b1_on%22%7D";
const TAG_B: &str = "%7B%22experiment%22%3A%22b1_off%22%7D";
const TAG_C: &str = "%7B%22experiment%22%3A%22b1_baseline%22%7D";

/// Read the `/metrics` Prometheus scrape and pull out the counter for a
/// specific labelled series. Returns `0` when the series has not been
/// touched (Prometheus prunes empty `*Vec` children).
async fn counter_value(http: &reqwest::Client, base: &str, want: &str) -> u64 {
    let body = http
        .get(format!("http://{base}/metrics"))
        .send()
        .await
        .expect("scrape /metrics")
        .text()
        .await
        .expect("body");
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if !line.starts_with(want) {
            continue;
        }
        // Format: `name{labels} value`
        if let Some((_, rhs)) = line.rsplit_once(' ') {
            return rhs.parse().unwrap_or(0);
        }
    }
    0
}

async fn make_state_with_ab_tag_enabled(
    pod_id: &str,
    max_distinct: usize,
) -> Arc<shelfd::http::ServerState> {
    let mut state = build_state_with_pod_id(pod_id).await;
    let ab = AbTagState::new(true, max_distinct, Duration::from_secs(60));
    {
        let s = Arc::get_mut(&mut state).expect("test owns the only Arc here");
        s.ab_tag = ab;
    }
    state
}

#[tokio::test]
async fn x_shelf_tag_splits_per_tag_hit_counters() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key_a = "ab-tag-it/a";
    let key_b = "ab-tag-it/b";
    put_object(&client, key_a, Bytes::from(vec![0xAA; 1024])).await;
    put_object(&client, key_b, Bytes::from(vec![0xBB; 1024])).await;

    let state = make_state_with_ab_tag_enabled("shelf-42-split", 16).await;
    let (native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = reqwest::Client::new();

    // Snapshot the per-tag counters before the test so we can compute
    // deltas; the global registry may already carry observations from
    // other parallel tests.
    let want_a = format!(
        "shelf_misses_by_tag_total{{pool=\"rowgroup\",tag=\"{}\"}}",
        TAG_A
    );
    let want_b = format!(
        "shelf_misses_by_tag_total{{pool=\"rowgroup\",tag=\"{}\"}}",
        TAG_B
    );
    let want_none = "shelf_misses_by_tag_total{pool=\"rowgroup\",tag=\"none\"}".to_owned();
    let before_a = counter_value(&http, &native.to_string(), &want_a).await;
    let before_b = counter_value(&http, &native.to_string(), &want_b).await;
    let before_none = counter_value(&http, &native.to_string(), &want_none).await;

    // Three distinct tag values; each request is a cold miss the first
    // time and at least bumps `*_by_tag_total{... outcome="miss"}` for
    // its labelled tag.
    for (key, tag) in [(key_a, Some(TAG_A)), (key_b, Some(TAG_B)), (key_a, None)] {
        let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
        let mut req = http.get(&url);
        if let Some(t) = tag {
            req = req.header("X-Shelf-Tag", t);
        }
        let resp = req.send().await.expect("get");
        assert!(
            resp.status().is_success() || resp.status() == reqwest::StatusCode::PARTIAL_CONTENT
        );
        // Drain the body so the response future completes and the
        // shim's metric bumps are visible.
        let _ = resp.bytes().await.expect("body");
    }

    let after_a = counter_value(&http, &native.to_string(), &want_a).await;
    let after_b = counter_value(&http, &native.to_string(), &want_b).await;
    let after_none = counter_value(&http, &native.to_string(), &want_none).await;

    assert!(
        after_a > before_a,
        "tag={TAG_A:?} miss counter should have advanced; before={before_a}, after={after_a}"
    );
    assert!(
        after_b > before_b,
        "tag={TAG_B:?} miss counter should have advanced; before={before_b}, after={after_b}"
    );
    assert!(
        after_none > before_none,
        "tag=none miss counter should have advanced; before={before_none}, after={after_none}"
    );
    // The `none` series must not be the only thing that moves.
    assert_ne!(after_a, after_b, "per-tag series must split, not collapse");

    cancel.cancel();
}

#[tokio::test]
async fn cardinality_cap_folds_into_other_sentinel() {
    if skip_if_offline() {
        return;
    }
    let client = s3_client().await;
    ensure_bucket(&client).await;
    let key = "ab-tag-it/cardinality";
    put_object(&client, key, Bytes::from(vec![0xCC; 256])).await;

    // Force the cap to 2 so we can blow past it cheaply (3 distinct tags
    // → the third lands on `tag="other"`).
    let state = make_state_with_ab_tag_enabled("shelf-42-cap", 2).await;
    let (native, shim, cancel) = spawn_server_with_shim(state).await;
    let http = reqwest::Client::new();

    for tag in [TAG_A, TAG_B, TAG_C] {
        let url = format!("http://{shim}/{TEST_BUCKET}/{key}");
        let resp = http
            .get(&url)
            .header("X-Shelf-Tag", tag)
            .send()
            .await
            .expect("get");
        assert!(
            resp.status().is_success() || resp.status() == reqwest::StatusCode::PARTIAL_CONTENT
        );
        let _ = resp.bytes().await.expect("body");
    }

    let want_other = "shelf_misses_by_tag_total{pool=\"rowgroup\",tag=\"other\"}".to_owned();
    let after_other = counter_value(&http, &native.to_string(), &want_other).await;
    assert!(
        after_other > 0,
        "third distinct tag must fold into 'other' sentinel"
    );

    let want_violations = "shelf_ab_tag_cap_violations_total{reason=\"cardinality\"}".to_owned();
    let after_violations = counter_value(&http, &native.to_string(), &want_violations).await;
    assert!(
        after_violations >= 1,
        "cap-violation counter must register the offending tag"
    );

    cancel.cancel();
}
